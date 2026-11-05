# TAIR3-1098 - Regenerate PANTHER paralog/ortholog files

## Overview

The TAIR locus page shows "PANTHER Plant Homologs" from per-AGI files served by
phylogenes-api (`/api/panther/paralogs/<AGI>`, `/api/panther/orthologs/<AGI>`).
`python_api_scripts/pipeline_scripts/generate_homologs.py` builds those files from
the PANTHER 19 trees and the PANTHER ortholog API.

It replaces `PantherETLPipeline.saveParalogS3_tairids()` / `saveOrthologS3_tairids()`,
whose PANTHER 17 run left about 2,100 PhyloGenes AGIs without files:

- Arabidopsis leaves with a `Gene_OrderedLocusName:At5g15110` id were keyed by the
  mixed-case name and never matched an AGI (all 1,039 such genes were missing).
- Per-AGI failures were dropped with a bare "Not saved" line.
- A gene with exactly one paralog got an empty file (PANTHER returns an object, not a list).

## Buckets

| Data | Bucket (env var, default) | Key |
| --- | --- | --- |
| Paralog JSON | `PG_PARALOG_BUCKET`, `phg-paralogs-19` | `<AGI>.json` |
| Paralog download | `PG_PARALOG_DL_BUCKET`, `phg-paralogs-download-19` | `<AGI>_paralog.txt` |
| Ortholog JSON | `PG_ORTHO_BUCKET`, `phg-orthologs-19` | `<AGI>.json` |
| Ortholog download | `PG_ORTHO_DL_BUCKET`, `phg-orthologs-download-19` | `<AGI>_ortholog.txt` |

The buckets must exist and allow public `GetObject`, like the `-17` ones, before uploading.
Then point phylogenes-api `config/default.json` at them.

## Inputs

- `resources/AGI_locusId_mapping_20200410.csv`: TAIR locus id to AGI (existing).
- `resources/organism_to_display.csv`: ortholog target taxa (existing).
- `resources/organisms_for_homologs.csv`: organism code to full name, common name and the
  group (`eudicotyledons`, `monocotyledons`, `other plants`) the locus page groups by.
  Rebuilt from the PANTHER 17 ortholog files, plus BRACM, which is new in PANTHER 19.
- AGI to primary symbol (`--symbols`): the Java `symbols.json` (`[{locusName, symbolName}]`)
  or a CSV of `agi,symbol` exported from TAIR. Without it `primary_symbol` is null and the
  download file's symbol column is blank.

## Steps

Run from `python_api_scripts`, with the same `.env` as the other pipeline scripts.

1. Build the AGI to UniProt mapping from every tree listed in Solr:

   ```bash
   python pipeline_scripts/generate_homologs.py mapping --tree-cache /data/phg-trees-19
   ```

   Writes `resources/tairid2uniprots.csv` (`tair_agi,uniprot,family_id`).
   Leaves that can't be mapped go to `logs/homolog_mapping_problems.csv`.
   `--family-ids FILE` limits the run to some families.

2. Dry run on a sample and look at the files:

   ```bash
   python pipeline_scripts/generate_homologs.py paralogs  --limit 50 --out-dir /tmp/homologs --symbols symbols.json
   python pipeline_scripts/generate_homologs.py orthologs --limit 50 --out-dir /tmp/homologs
   ```

3. Upload. `--skip-existing` makes a rerun resume where it stopped:

   ```bash
   python pipeline_scripts/generate_homologs.py paralogs  --upload --skip-existing --symbols symbols.json
   python pipeline_scripts/generate_homologs.py orthologs --upload --skip-existing
   ```

   `--agis FILE` limits a run to a list of AGIs, for example the ones still missing.

Each run writes `logs/<paralogs|orthologs>_report_<timestamp>.csv` with one row per AGI:

| Status | Meaning |
| --- | --- |
| `written` | File written with at least one homolog. |
| `empty` | PANTHER has no homologs for the gene; an empty list `[]` is written. |
| `failed` | PANTHER call failed after retries; nothing is written. Rerun with `--skip-existing`. |
| `not_in_panther` | An `--agis` entry with no Arabidopsis leaf in the trees. |
| `skipped_existing` | Both files already exist. |

Calls are spaced by `--sleep` (0.2 s by default), so a full run of about 27,000 AGIs
takes several hours for each step.
