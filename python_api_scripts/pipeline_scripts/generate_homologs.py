#!/usr/bin/env python3
"""
Generate the per-AGI paralog and ortholog files that the TAIR locus page reads
through phylogenes-api (/api/panther/paralogs/<AGI>, /api/panther/orthologs/<AGI>).

TAIR3-1098. Replaces the Java PantherETLPipeline.saveParalogS3_tairids() /
saveOrthologS3_tairids() path, which left ~2,100 PhyloGenes AGIs without files:

  * Arabidopsis leaves whose PANTHER gene id is Gene_OrderedLocusName:At5g15110
    were keyed by the mixed-case name, so they never matched an AGI.
  * Per-AGI failures were swallowed with a bare "Not saved" line.
  * A single paralog (PANTHER returns an object, not a list) was dropped.

Three steps, run in order:

  mapping    Walk the PhyloGenes trees and write AGI -> UniProt for every
             Arabidopsis leaf (resources/tairid2uniprots.csv by default).
  paralogs   For each AGI, call PANTHER homologOther and write <AGI>.json and
             <AGI>_paralog.txt.
  orthologs  For each AGI, call PANTHER matchortho and write <AGI>.json and
             <AGI>_ortholog.txt.

Nothing is written to S3 unless --upload is given. Without it, files go to
--out-dir so a run can be inspected first.

Usage:
    python generate_homologs.py mapping [--family-ids ids.txt]
    python generate_homologs.py paralogs  --out-dir /tmp/homologs [--agis agis.txt]
    python generate_homologs.py orthologs --upload --skip-existing

Environment (.env at the repo root, same as the other pipeline scripts):
    SOLR_HOST, PANTHER_COLLECTION         family ids when --family-ids is not given
    PG_TREE_BUCKET                        default phg-panther-data-19
    PG_PARALOG_BUCKET, PG_PARALOG_DL_BUCKET   default phg-paralogs-19, phg-paralogs-download-19
    PG_ORTHO_BUCKET, PG_ORTHO_DL_BUCKET       default phg-orthologs-19, phg-orthologs-download-19
    AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION   only for --upload / --skip-existing with --upload
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv

current_dir = os.path.dirname(os.path.abspath(__file__))
api_scripts_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(api_scripts_dir)
resources_dir = os.path.join(api_scripts_dir, 'resources')
logs_dir = os.path.join(api_scripts_dir, 'logs')

load_dotenv(os.path.join(project_root, '.env'), override=True)

PANTHER_ROOT = os.getenv('PANTHER_SERVER_URL', 'https://pantherdb.org/services/oai/pantherdb').rstrip('/')
ARATH_TAXON = 3702

TREE_BUCKET = os.getenv('PG_TREE_BUCKET', 'phg-panther-data-19')
BUCKETS = {
    'paralogs': (os.getenv('PG_PARALOG_BUCKET', 'phg-paralogs-19'),
                 os.getenv('PG_PARALOG_DL_BUCKET', 'phg-paralogs-download-19')),
    'orthologs': (os.getenv('PG_ORTHO_BUCKET', 'phg-orthologs-19'),
                  os.getenv('PG_ORTHO_DL_BUCKET', 'phg-orthologs-download-19')),
}
TXT_SUFFIX = {'paralogs': '_paralog.txt', 'orthologs': '_ortholog.txt'}

DEFAULT_MAPPING = os.path.join(resources_dir, 'tairid2uniprots.csv')
LOCUS_MAPPING = os.path.join(resources_dir, 'AGI_locusId_mapping_20200410.csv')
ORGANISM_DISPLAY = os.path.join(resources_dir, 'organism_to_display.csv')
ORGANISM_GROUPS = os.path.join(resources_dir, 'organisms_for_homologs.csv')

AGI_RE = re.compile(r'^AT[1-5CM]G\d{5}$')

# Field order of the Java Mapped / OrthoMapped beans, so the files keep the shape
# the locus page already reads.
PARALOG_FIELDS = ['target_gene_symbol', 'species_after_duplication', 'persistent_id',
                  'target_persistent_id', 'ortholog', 'gene', 'taxon_preceding_duplication',
                  'target_gene', 'target_uniprot', 'target_agi', 'id',
                  'species_preceding_duplication', 'taxon_after_duplication', 'primary_symbol']
ORTHOLOG_FIELDS = ['target_gene_symbol', 'persistent_id', 'target_persistent_id', 'ortholog',
                   'gene', 'target_gene', 'uniprot_id', 'organism', 'target_gene_id', 'id',
                   'full_name', 'common_name', 'group_name']

os.makedirs(logs_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(logs_dir, 'generate_homologs.log'), encoding='utf-8'),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------- id helpers

def load_locus_mapping(path: str = LOCUS_MAPPING) -> Dict[str, str]:
    """TAIR locus id (e.g. 2200935) -> AGI."""
    mapping = {}
    with open(path, newline='', encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            mapping[row['locus_id'].strip()] = row['AGI_id'].strip().upper()
    return mapping


def agi_from_gene_ref(ref: str, locus_mapping: Dict[str, str]) -> Optional[str]:
    """
    Resolve an Arabidopsis gene reference from PANTHER to an upper-case AGI.

    Accepts the value after the namespace in either a tree gene_id
    ("TAIR:AT1G01010", "Gene_OrderedLocusName:At5g15110") or a homolog
    target_gene ("TAIR=locus=2200935", "Gene_OrderedLocusName=At5g15110").
    """
    if not ref:
        return None
    value = re.split(r'[:=]', ref, maxsplit=1)[-1]
    if value.startswith('locus='):
        return locus_mapping.get(value[len('locus='):])
    value = value.upper()
    return value if AGI_RE.match(value) else None


def uniprot_from(name: str) -> Optional[str]:
    if name and 'UniProtKB=' in name:
        return name.split('UniProtKB=', 1)[1]
    return None


def read_id_file(path: str) -> List[str]:
    with open(path, encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]


# ---------------------------------------------------------------- step 1: mapping

def get_family_ids(family_ids_file: Optional[str]) -> List[str]:
    if family_ids_file:
        return read_id_file(family_ids_file)
    import pysolr
    solr_url = f"{os.getenv('SOLR_HOST', 'http://localhost:8983')}/solr/{os.getenv('PANTHER_COLLECTION', 'panther')}"
    logger.info(f"Listing families from Solr {solr_url}")
    results = pysolr.Solr(solr_url, timeout=60).search('*:*', fl='id', sort='id asc', rows=100000)
    return [doc['id'] for doc in results if doc.get('id')]


def fetch_tree(session: requests.Session, family_id: str, cache_dir: Optional[str]) -> Optional[dict]:
    """Tree JSON as served from the PhyloGenes tree bucket (objects are public-read)."""
    cache_path = os.path.join(cache_dir, f'{family_id}.json') if cache_dir else None
    if cache_path and os.path.exists(cache_path):
        with open(cache_path, encoding='utf-8') as f:
            return json.load(f)
    url = f'https://{TREE_BUCKET}.s3-us-west-2.amazonaws.com/{family_id}.json'
    resp = session.get(url, timeout=120)
    if resp.status_code != 200:
        logger.warning(f'{family_id}: tree fetch returned {resp.status_code}')
        return None
    data = resp.json()
    if cache_path:
        with open(cache_path, 'w', encoding='utf-8') as f:
            f.write(resp.text)
    return data


def tree_root(data: dict) -> Optional[dict]:
    if 'jsonString' in data:
        data = json.loads(data['jsonString'])
    search = data.get('search') or {}
    return search.get('annotation_node') or (search.get('tree_topology') or {}).get('annotation_node')


def arabidopsis_leaves(node) -> Iterable[dict]:
    stack = [node]
    while stack:
        n = stack.pop()
        if isinstance(n, list):
            stack.extend(n)
        elif isinstance(n, dict):
            if n.get('tree_node_type') == 'LEAF' and n.get('organism') == 'Arabidopsis thaliana':
                yield n
            children = n.get('children')
            if children:
                stack.append(children.get('annotation_node') if isinstance(children, dict) else children)


def build_mapping(args) -> None:
    locus_mapping = load_locus_mapping()
    family_ids = get_family_ids(args.family_ids)
    logger.info(f'{len(family_ids)} families')
    if args.tree_cache:
        os.makedirs(args.tree_cache, exist_ok=True)

    agi2uniprot: Dict[str, str] = {}
    agi2family: Dict[str, str] = {}
    problems: List[Tuple[str, str, str, str]] = []
    session = requests.Session()
    for i, fid in enumerate(family_ids, 1):
        try:
            data = fetch_tree(session, fid, args.tree_cache)
        except Exception as e:
            problems.append((fid, '', '', f'tree fetch failed: {e}'))
            continue
        root = tree_root(data) if data else None
        if root is None:
            problems.append((fid, '', '', 'tree missing or unreadable'))
            continue
        for leaf in arabidopsis_leaves(root):
            gene_id, uniprot = leaf.get('gene_id'), uniprot_from(leaf.get('node_name'))
            agi = agi_from_gene_ref(gene_id, locus_mapping)
            if not agi or not uniprot:
                problems.append((fid, gene_id or '', leaf.get('node_name') or '', 'no AGI or UniProt'))
                continue
            if agi in agi2uniprot and agi2uniprot[agi] != uniprot:
                problems.append((fid, gene_id, uniprot,
                                 f'AGI already mapped to {agi2uniprot[agi]} in {agi2family[agi]}; kept first'))
                continue
            agi2uniprot.setdefault(agi, uniprot)
            agi2family.setdefault(agi, fid)
        if i % 500 == 0:
            logger.info(f'{i}/{len(family_ids)} families, {len(agi2uniprot)} AGIs')

    with open(args.out, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['tair_agi', 'uniprot', 'family_id'])
        for agi in sorted(agi2uniprot):
            w.writerow([agi, agi2uniprot[agi], agi2family[agi]])
    problems_path = os.path.join(logs_dir, 'homolog_mapping_problems.csv')
    with open(problems_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['family_id', 'gene_id', 'node_name_or_uniprot', 'problem'])
        w.writerows(problems)
    logger.info(f'Wrote {len(agi2uniprot)} AGIs to {args.out}; {len(problems)} problems in {problems_path}')


def load_mapping(path: str) -> Dict[str, str]:
    with open(path, newline='', encoding='utf-8') as f:
        return {row['tair_agi'].strip().upper(): row['uniprot'].strip() for row in csv.DictReader(f)}


# ---------------------------------------------------------------- PANTHER calls

class PantherError(Exception):
    pass


def panther_get(session: requests.Session, path: str, params: dict, retries: int = 4) -> dict:
    url = f'{PANTHER_ROOT}/{path}'
    delay = 5
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, params=params, timeout=120)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code not in (429, 500, 502, 503, 504):
                raise PantherError(f'HTTP {resp.status_code}')
            err = f'HTTP {resp.status_code}'
        except (requests.ConnectionError, requests.Timeout, ValueError) as e:
            err = str(e)
        if attempt < retries:
            time.sleep(delay)
            delay *= 2
    raise PantherError(f'gave up after {retries} attempts: {err}')


def mapped_list(payload: dict) -> List[dict]:
    """
    search.mapping.mapped is a list, or a single object when there is one homolog.
    With no homologs PANTHER answers with a bare {"id": <query>} placeholder.
    """
    mapped = ((payload.get('search') or {}).get('mapping') or {}).get('mapped')
    if mapped is None:
        return []
    mapped = mapped if isinstance(mapped, list) else [mapped]
    return [m for m in mapped if isinstance(m, dict) and m.get('target_gene')]


def as_str(value):
    return None if value is None else str(value)


# ---------------------------------------------------------------- step 2: paralogs

def build_paralogs(payload: dict, ctx: dict) -> Tuple[list, str, int]:
    rows, dropped = [], 0
    for m in mapped_list(payload):
        target_gene = m.get('target_gene') or ''
        parts = target_gene.split('|')
        agi = agi_from_gene_ref(parts[1], ctx['locus']) if len(parts) > 1 else None
        target_uniprot = uniprot_from(target_gene)
        if not agi and target_uniprot:
            agi = ctx['uniprot2agi'].get(target_uniprot)
        if not agi:
            dropped += 1
            continue
        row = {k: as_str(m.get(k)) for k in PARALOG_FIELDS}
        row.update(target_uniprot=target_uniprot, target_agi=agi, primary_symbol=ctx['symbols'].get(agi))
        rows.append(row)
    rows.sort(key=lambda r: r['target_agi'])
    txt = 'AGI ID\tLocus primary symbol\n' + ''.join(
        f"{r['target_agi']}\t{r['primary_symbol'] or ''}\n" for r in rows)
    return rows, txt, dropped


# ---------------------------------------------------------------- step 3: orthologs

def load_organisms() -> Tuple[List[int], Dict[str, Tuple[str, str, str]]]:
    taxa = []
    with open(ORGANISM_DISPLAY, newline='', encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            tid = (row.get('taxonID') or '').strip()
            if tid.isdigit() and int(tid) != ARATH_TAXON:
                taxa.append(int(tid))
    groups = {}
    with open(ORGANISM_GROUPS, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            groups[row['organism_code']] = (row['full_name'], row['common_name'], row['group_name'])
    return taxa, groups


def build_orthologs(payload: dict, ctx: dict) -> Tuple[list, str, int]:
    rows, dropped = [], 0
    for m in mapped_list(payload):
        target_gene = m.get('target_gene') or ''
        parts = target_gene.split('|')
        if len(parts) < 2 or '=' not in parts[1]:
            dropped += 1
            continue
        organism = parts[0]
        full_name, common_name, group_name = ctx['groups'].get(organism, ('Unknown', '', 'Unknown'))
        row = {k: as_str(m.get(k)) for k in ORTHOLOG_FIELDS}
        row.update(uniprot_id=uniprot_from(target_gene) or 'error', organism=organism,
                   target_gene_id=parts[1].split('=')[-1],
                   full_name=full_name, common_name=common_name, group_name=group_name)
        rows.append(row)
    rows.sort(key=lambda r: (r['full_name'].lower(), r['target_gene_id'].lower()))
    lines = ['Organism\tUniProt ID\tGene ID\tOrtholog Type\n']
    for r in rows:
        organism = r['full_name'] + (f" ({r['common_name']})" if r['common_name'] else '')
        lines.append(f"{organism}\t{r['uniprot_id']}\t{r['target_gene_id']}\t{r['ortholog']}\n")
    return rows, ''.join(lines), dropped


# ---------------------------------------------------------------- output

class Writer:
    def __init__(self, kind: str, out_dir: Optional[str], upload: bool):
        self.kind = kind
        self.json_bucket, self.txt_bucket = BUCKETS[kind]
        self.out_dir = out_dir
        self.s3 = None
        if upload:
            import boto3
            self.s3 = boto3.client(
                's3',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
                aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
                region_name=os.getenv('AWS_REGION', 'us-west-2'),
            )
            for bucket in (self.json_bucket, self.txt_bucket):
                self.s3.head_bucket(Bucket=bucket)  # fail fast on a wrong bucket or missing access
            logger.info(f'Uploading to s3://{self.json_bucket} and s3://{self.txt_bucket}')
        else:
            for sub in (self.json_bucket, self.txt_bucket):
                os.makedirs(os.path.join(out_dir, sub), exist_ok=True)
            logger.info(f'Dry run: writing under {out_dir}')

    def exists(self, agi: str) -> bool:
        keys = ((self.json_bucket, f'{agi}.json'), (self.txt_bucket, f'{agi}{TXT_SUFFIX[self.kind]}'))
        if self.s3 is None:
            return all(os.path.exists(os.path.join(self.out_dir, b, k)) for b, k in keys)
        from botocore.exceptions import ClientError
        try:
            for bucket, key in keys:
                self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False

    def write(self, agi: str, rows: list, txt: str) -> None:
        body = json.dumps(rows, separators=(',', ':'))
        txt_key = f'{agi}{TXT_SUFFIX[self.kind]}'
        if self.s3 is None:
            with open(os.path.join(self.out_dir, self.json_bucket, f'{agi}.json'), 'w', encoding='utf-8') as f:
                f.write(body)
            with open(os.path.join(self.out_dir, self.txt_bucket, txt_key), 'w', encoding='utf-8') as f:
                f.write(txt)
            return
        self.s3.put_object(Bucket=self.json_bucket, Key=f'{agi}.json', Body=body.encode('utf-8'),
                           ContentType='application/json')
        self.s3.put_object(Bucket=self.txt_bucket, Key=txt_key, Body=txt.encode('utf-8'),
                           ContentType='text/plain; charset=utf-8')


def load_symbols(path: Optional[str]) -> Dict[str, str]:
    """AGI -> primary symbol. Accepts the Java symbols.json ([{locusName, symbolName}]) or a 2-column CSV."""
    if not path:
        logger.warning('No --symbols file: primary_symbol will be null in paralog files')
        return {}
    if path.endswith('.json'):
        with open(path, encoding='utf-8') as f:
            return {str(o['locusName']).upper(): str(o['symbolName']) for o in json.load(f)}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)
        return {r[0].strip().upper(): r[1].strip() for r in reader if len(r) >= 2 and r[1].strip()}


def run_homologs(kind: str, args) -> None:
    if not args.upload and not args.out_dir:
        sys.exit('Give --out-dir for a dry run, or --upload to write to S3.')
    agi2uniprot = load_mapping(args.mapping)
    ctx = {'locus': load_locus_mapping(), 'uniprot2agi': {}, 'symbols': {}, 'groups': {}}
    for agi, uniprot in agi2uniprot.items():
        ctx['uniprot2agi'].setdefault(uniprot, agi)

    if kind == 'paralogs':
        ctx['symbols'] = load_symbols(args.symbols)
        path, params, build = 'ortholog/homologOther', {'organism': ARATH_TAXON, 'homologType': 'P'}, build_paralogs
    else:
        taxa, ctx['groups'] = load_organisms()
        params = {'organism': ARATH_TAXON, 'targetOrganism': ','.join(map(str, taxa)), 'orthologType': 'all'}
        path, build = 'ortholog/matchortho', build_orthologs

    agis = sorted(agi2uniprot)
    not_in_panther = []
    if args.agis:
        wanted = [a.upper() for a in read_id_file(args.agis)]
        not_in_panther = [a for a in wanted if a not in agi2uniprot]
        agis = [a for a in wanted if a in agi2uniprot]
    if args.limit:
        agis = agis[:args.limit]

    writer = Writer(kind, args.out_dir, args.upload)
    session = requests.Session()
    report_path = os.path.join(logs_dir, f'{kind}_report_{time.strftime("%Y%m%d_%H%M%S")}.csv')
    counts = {'written': 0, 'empty': 0, 'skipped_existing': 0, 'failed': 0, 'not_in_panther': len(not_in_panther)}
    with open(report_path, 'w', newline='', encoding='utf-8') as rf:
        report = csv.writer(rf)
        report.writerow(['agi', 'uniprot', 'status', 'homologs', 'dropped_targets', 'detail'])
        for agi in not_in_panther:
            report.writerow([agi, '', 'not_in_panther', '', '', 'no Arabidopsis leaf in the PhyloGenes trees'])
        for i, agi in enumerate(agis, 1):
            uniprot = agi2uniprot[agi]
            try:
                if args.skip_existing and writer.exists(agi):
                    counts['skipped_existing'] += 1
                    report.writerow([agi, uniprot, 'skipped_existing', '', '', ''])
                    continue
                payload = panther_get(session, path, dict(params, geneInputList=uniprot))
                rows, txt, dropped = build(payload, ctx)
                writer.write(agi, rows, txt)
                status = 'written' if rows else 'empty'
                counts[status] += 1
                report.writerow([agi, uniprot, status, len(rows), dropped, ''])
            except Exception as e:
                counts['failed'] += 1
                report.writerow([agi, uniprot, 'failed', '', '', str(e)])
                logger.error(f'{agi} ({uniprot}): {e}')
            rf.flush()
            if i % 200 == 0:
                logger.info(f'{kind}: {i}/{len(agis)} {counts}')
            if args.sleep:
                time.sleep(args.sleep)
    logger.info(f'{kind} done: {counts}. Per-AGI report: {report_path}')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='step', required=True)

    p = sub.add_parser('mapping', help='build AGI -> UniProt from the PhyloGenes trees')
    p.add_argument('--family-ids', help='file of PANTHER family ids, one per line (default: all ids in Solr)')
    p.add_argument('--tree-cache', help='directory to cache downloaded tree JSON')
    p.add_argument('--out', default=DEFAULT_MAPPING)

    for kind in ('paralogs', 'orthologs'):
        p = sub.add_parser(kind, help=f'generate {kind} files')
        p.add_argument('--mapping', default=DEFAULT_MAPPING, help='CSV from the mapping step')
        p.add_argument('--agis', help='only these AGIs, one per line (e.g. the ones missing today)')
        p.add_argument('--limit', type=int, help='stop after this many AGIs')
        p.add_argument('--out-dir', help='dry run: write files here instead of S3')
        p.add_argument('--upload', action='store_true', help='write to the S3 buckets')
        p.add_argument('--skip-existing', action='store_true', help='skip AGIs that already have both files')
        p.add_argument('--sleep', type=float, default=0.2, help='seconds between PANTHER calls')
        if kind == 'paralogs':
            p.add_argument('--symbols', help='AGI -> primary symbol: symbols.json or a CSV (agi,symbol)')

    args = parser.parse_args()
    if args.step == 'mapping':
        build_mapping(args)
    else:
        run_homologs(args.step, args)


if __name__ == '__main__':
    main()
