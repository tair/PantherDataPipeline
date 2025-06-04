import json
from typing import List, Optional
from pysolr import Solr
import csv
import argparse
from tqdm import tqdm
import requests
import pysolr
import os
import glob
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env.sandbox')

EVIDENCE_CODES = ["EXP", "IDA", "IEP", "IGI", "IMP", "IPI"]

class PaintAnno:
	def __init__(self, gene_product_id: str, go_id: str, go_name: str, 
				 go_aspect: str, evidence_code: str, reference: str):
		self.geneProductId = gene_product_id
		self.goId = go_id
		self.goName = go_name
		self.goAspect = go_aspect
		self.evidenceCode = evidence_code
		self.reference = reference

def preprocess_go_terms(go_basic: dict) -> dict:
	"""Preprocess GO terms into a dictionary with GO IDs as keys."""
	go_terms = {}
	nodes = go_basic.get("graphs", [{}])[0].get("nodes", [])
	
	for node in nodes:
		go_id = node.get("id", "").split("/")[-1]  # Get GO_XXXXXX from the full URL
		lbl = node.get("lbl", "")
		
		# Get the aspect from basicPropertyValues
		aspect = ""
		for prop in node.get("meta", {}).get("basicPropertyValues", []):
			if prop.get("pred") == "http://www.geneontology.org/formats/oboInOwl#hasOBONamespace":
				aspect = prop.get("val", "")
				break
		
		go_terms[go_id] = {
			"lbl": lbl,
			"aspect": aspect
		}
	
	print(f"Preprocessed {len(go_terms)} GO terms")
	return go_terms

def process_paint_anno(cols: List[str], go_terms: dict) -> Optional[PaintAnno]:
	try:
		# Fields from CSV
		gene_product_id = cols[0].split("UniProtKB=")[1]  # First column
		go_id = cols[1]  # Second column
		evidence_code = cols[2]  # Third column
		reference = cols[4]  # Fifth column

		# Get GO term info from preprocessed dictionary
		go_term_id = f"GO_{go_id.split(':')[1]}"  # Convert GO:XXXXXX to GO_XXXXXX
		go_term = go_terms.get(go_term_id, {})
		
		go_name = go_term.get("lbl", "")
		go_aspect = go_term.get("aspect", "")

		return PaintAnno(
			gene_product_id=gene_product_id,
			go_id=go_id,
			go_name=go_name,
			go_aspect=go_aspect,
			evidence_code=evidence_code,
			reference=reference
		)
	except Exception as e:
		print(f"Error processing annotation: {e}")
		return None

def save_go_exp_annotations_to_solr(csv_path: str, go_basic_path: str, solr_client: Solr, batch_size: int = 10000, clear_solr: bool = False) -> None:
	if clear_solr:
		print("Clearing SOLR collection")
		solr_client.delete(q='*:*')
		solr_client.commit()
	# Load GO basic data
	with open(go_basic_path, 'r') as f:
		go_basic = json.load(f)
	go_terms = preprocess_go_terms(go_basic)

	batch = []
	count = 0
	try:
		# Count total lines for progress bar
		with open(csv_path, 'r') as file:
			total_lines = sum(1 for _ in file)
		with open(csv_path, 'r') as file:
			pbar = tqdm(total=total_lines, desc="Processing lines", unit="lines")
			for line in file:
				cols = line.strip().split()
				anno = process_paint_anno(cols, go_terms)
				if anno:
					doc = {
						"uniprot_id": anno.geneProductId,
						"go_annotations": json.dumps({
							"geneProductId": anno.geneProductId,
							"goId": anno.goId,
							"goName": anno.goName,
							"goAspect": anno.goAspect,
							"evidenceCode": anno.evidenceCode,
							"reference": anno.reference
						})
					}
					batch.append(doc)
					count += 1

				if len(batch) >= batch_size:
					solr_client.add(batch)
					solr_client.commit()
					print(f"Committed batch of {len(batch)} docs, total processed: {count}")
					batch = []
				pbar.update(1)

			# Commit any remaining docs
			if batch:
				solr_client.add(batch)
				solr_client.commit()
				print(f"Committed final batch of {len(batch)} docs, total processed: {count}")
			pbar.close()

	except Exception as e:
		print(f"Error processing file: {e}")
		raise

def get_go_annotations_for_tree(uniprot_ids, paint_solr, uniprot_solr, uniprot_rows):
	"""
	For a list of uniprot_ids, fetch all go_annotations from paint_db and uniprot_db.
	Returns a list of JSON strings, each representing a GOAnnotationData object.
	"""
	# Initialize aggregated counts
	go_annotation_data_list = []
	total_paint = 0
	total_uniprot = 0
	for uniprot_id in uniprot_ids:
		# Collect raw annotation dicts
		raw_annotations: List[dict] = []
		# Query paint_db (uppercase)
		results1 = paint_solr.search(f'uniprot_id:{uniprot_id.upper()}', rows=uniprot_rows, fl='go_annotations')
		for r in results1:
			if 'go_annotations' in r:
				# filter the annotations by evidence code
				annos = json.loads(r['go_annotations'])
				# if uniprot_id == "Q9LSF8":
				# 	print(annos)
				if annos['evidenceCode'] in EVIDENCE_CODES:
					raw_annotations.append(annos)
					total_paint += 1
					
		# Query uniprot_db (lowercase)
		results2 = uniprot_solr.search(f'uniprot_id:{uniprot_id.lower()}', rows=uniprot_rows, fl='go_annotations')
		for r in results2:
			if 'go_annotations' in r:
				annos = json.loads(r['go_annotations'])
				annos['evidenceCode'] = "GO_REF," + annos['evidenceCode'] + ",phylogeny"
				if uniprot_id == "k4dgw8":
					print("IN UNIPROT")
					print(annos)
				raw_annotations.append(annos)
				total_uniprot += 1
						
		if raw_annotations:
			go_annotation_data = {
				"uniprot_id": uniprot_id.lower(),
				"go_annotations": json.dumps(raw_annotations)
			}
			go_annotation_data_list.append(json.dumps(go_annotation_data))
	# Log aggregated totals from both collections
	print(f"get_go_annotations_for_tree totals: exp={total_paint}, iba={total_uniprot}, combined={total_paint+total_uniprot}")
	return go_annotation_data_list

def update_go_annotations():
	#configuration
	SOLR_HOST = os.getenv('SOLR_HOST', 'http://localhost:8983')
	PAINT_DB_COLLECTION = os.getenv('PAINT_DB_COLLECTION', 'paint_db')
	UNIPROT_DB_COLLECTION = os.getenv('UNIPROT_DB_COLLECTION', 'uniprot_db')
	PANTHER_COLLECTION = os.getenv('PANTHER_COLLECTION', 'panther')
	
	GO_EXP_SOLR_URL = f"{SOLR_HOST}/solr/{PAINT_DB_COLLECTION}"
	GO_IBA_SOLR_URL = f"{SOLR_HOST}/solr/{UNIPROT_DB_COLLECTION}"
	PANTHER_SOLR_URL = f"{SOLR_HOST}/solr/{PANTHER_COLLECTION}"
	
	panther_solr = pysolr.Solr(PANTHER_SOLR_URL, timeout=60)
	paint_solr = pysolr.Solr(GO_EXP_SOLR_URL, timeout=60)
	uniprot_solr = pysolr.Solr(GO_IBA_SOLR_URL, timeout=60)

	# 1. Get all panther docs (example: id:PTHR47947, or all: '*:*')
	query = '*:*'
	# query = 'id:PTHR47947'
	print("Querying Panther collection")
	results = panther_solr.search(query, fl='id,uniprot_ids,go_annotations', sort='id asc', rows=1000000)
	print(f"Total Panther docs: {len(results)}")

	# 2. Get the max number of uniprot_id results in uniprot_db (facet)
	facet_params = {
		'facet': 'on',
		'facet.field': 'uniprot_id',
		'facet.limit': -1,
		'facet.sort': 'count',
		'rows': 0
	}
	facet_response = paint_solr.search('*:*', **facet_params)
	uniprot_rows = 1000  # Default fallback
	if 'facet_counts' in facet_response.raw_response:
		facets = facet_response.raw_response['facet_counts']['facet_fields']['uniprot_id']
		if facets and len(facets) > 1:
			uniprot_rows = facets[1]  # The count of the most common uniprot_id
	print("Uniprot DB result rows set to:", uniprot_rows)

	# 3. Update each panther doc with a progress bar
	with tqdm(total=len(results), desc="Processing Panther docs", unit="doc") as pbar:
		for i, doc in enumerate(results):
			panther_id = doc.get('id')
			pbar.set_description(f"Panther ID: {panther_id}")
			uniprot_ids = doc.get('uniprot_ids', [])
			if isinstance(uniprot_ids, str):
				uniprot_ids = [uniprot_ids]
			go_annotation_data_list = get_go_annotations_for_tree(uniprot_ids, paint_solr, uniprot_solr, uniprot_rows)
			update_doc = {
				"id": panther_id,
				"go_annotations": {"set": go_annotation_data_list}
			}
			panther_solr.add([update_doc])
			panther_solr.commit()
			pbar.update(1)
			# Optionally, print after each commit
			# print("committed:", panther_id)

def save_go_iba_annotations_to_solr(solr_client, clear_solr: bool = False, batch_size: int = 10000) -> None:
	#configuration
	GOIDNAME_PATH = r"C:\Users\swapp\Documents\MyProjects\Work\panther_storage\resources\iba\goidname.properties"
	IBA_GAF_DIR = r"C:\Users\swapp\Documents\MyProjects\Work\panther_storage\resources\iba"
	"""Read GAF files and load GO-IBA annotations into Solr in batches."""
	#clear the collection
	if clear_solr:
		solr_client.delete(q='*:*')
		solr_client.commit()

	# Load GO names from properties file
	props = {}
	with open(GOIDNAME_PATH, 'r') as pf:
		for line in pf:
			line = line.strip()
			if not line or line.startswith('#'):
				continue
			k, v = line.split('=', 1)
			props[k] = v
	print("Loaded GO names from properties file ", len(props))

	
	batch = []
	count = 0
	for filepath in glob.glob(os.path.join(IBA_GAF_DIR, '*.gaf')):
		print(f"Processing GAF file: {filepath}")
		# Count total lines for progress bar
		with open(filepath, 'r') as br:
			total_lines = sum(1 for _ in br)
		# Process with progress bar
		with open(filepath, 'r') as br:
			pbar = tqdm(br, total=total_lines, desc=f"Processing {os.path.basename(filepath)}", unit="lines")
			for line in pbar:
				if not line.strip() or line.startswith('!'):
					continue
				cols = line.strip().split('\t')
				if len(cols) < 11:
					continue
				go_aspect = cols[8]
				gene_product_id = cols[10][10:].split('|')[0].lower()
				go_id = cols[4]
				key = go_id.split(':')[1]
				go_name = props.get(key, '')
				reference = cols[5]
				evidence_code = cols[6]
				doc = {
					'uniprot_id': gene_product_id,
					'go_annotations': json.dumps({
						'geneProductId': gene_product_id,
						'goId': go_id,
						'goName': go_name,
						'goAspect': go_aspect,
						'evidenceCode': evidence_code,
						'reference': reference
					})
				}
				batch.append(doc)
				count += 1
				if len(batch) >= batch_size:
					solr_client.add(batch)
					solr_client.commit()
					batch = []
			if batch:
				solr_client.add(batch)
				solr_client.commit()
			pbar.close()

def main():
	BASE_DIR = os.getenv('BASE_DIR', r"C:\Users\Documents\panther_storage\resources")
	CSV_PATH = os.path.join(BASE_DIR, "paint", "Pthr_GO_19.0.tsv")
	GO_BASIC_PATH = os.path.join(BASE_DIR, "paint", "go-basic.json")
	SOLR_HOST = os.getenv('SOLR_HOST', 'http://localhost:8983')
	PAINT_DB_COLLECTION = os.getenv('PAINT_DB_COLLECTION', 'paint_db')
	UNIPROT_DB_COLLECTION = os.getenv('UNIPROT_DB_COLLECTION', 'uniprot_db')
	GO_EXP_SOLR_URL = f"{SOLR_HOST}/solr/{PAINT_DB_COLLECTION}"
	GO_IBA_SOLR_URL = f"{SOLR_HOST}/solr/{UNIPROT_DB_COLLECTION}"
	
	## Save the go_annotations to the SOLR collection
	# solr_client = Solr(GO_EXP_SOLR_URL, timeout=60)
	# save_go_exp_annotations_to_solr(
	# 	csv_path=CSV_PATH,
	# 	go_basic_path=GO_BASIC_PATH,
	# 	solr_client=solr_client,
	# 	clear_solr=False
	# )

	# solr_client = Solr(GO_IBA_SOLR_URL, timeout=60)
	# save_go_iba_annotations_to_solr(solr_client, clear_solr=True)

	# Update the panther collection in Solr with new go_annotations
	update_go_annotations()


if __name__ == "__main__":
	main()
