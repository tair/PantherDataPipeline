import json
from typing import List, Optional, Dict, Tuple
from pysolr import Solr
import csv
import argparse
from tqdm import tqdm
import requests
import pysolr
import os
import glob
from dotenv import load_dotenv

# Get project root directory (panther-pipeline/)
current_dir = os.path.dirname(os.path.abspath(__file__))  # pipeline_scripts/
api_scripts_dir = os.path.dirname(current_dir)  # python_api_scripts/
project_root = os.path.dirname(api_scripts_dir)  # panther-pipeline/

# Load environment variables from project root
load_dotenv(os.path.join(project_root, '.env.sandbox'))

# Constants
EVIDENCE_CODES = ["EXP", "IDA", "IEP", "IGI", "IMP", "IPI"]

class PaintAnno:
	"""Data class for PAINT annotations."""
	def __init__(self, gene_product_id: str, go_id: str, go_name: str, 
				 go_aspect: str, evidence_code: str, reference: str):
		self.geneProductId = gene_product_id
		self.goId = go_id
		self.goName = go_name
		self.goAspect = go_aspect
		self.evidenceCode = evidence_code
		self.reference = reference

class SolrConfig:
	"""Configuration class for Solr connections."""
	def __init__(self):
		self.SOLR_HOST = os.getenv('SOLR_HOST', 'http://localhost:8983')
		self.PAINT_DB_COLLECTION = os.getenv('PAINT_DB_COLLECTION', 'paint_db')
		self.UNIPROT_DB_COLLECTION = os.getenv('UNIPROT_DB_COLLECTION', 'uniprot_db')
		self.PANTHER_COLLECTION = os.getenv('PANTHER_COLLECTION', 'panther')
		
		self.GO_EXP_SOLR_URL = f"{self.SOLR_HOST}/solr/{self.PAINT_DB_COLLECTION}"
		self.GO_IBA_SOLR_URL = f"{self.SOLR_HOST}/solr/{self.UNIPROT_DB_COLLECTION}"
		self.PANTHER_SOLR_URL = f"{self.SOLR_HOST}/solr/{self.PANTHER_COLLECTION}"

class SolrManager:
	"""Manager class for Solr operations."""
	def __init__(self, config: SolrConfig):
		self.config = config
		self.panther_solr = pysolr.Solr(config.PANTHER_SOLR_URL, timeout=60)
		self.paint_solr = pysolr.Solr(config.GO_EXP_SOLR_URL, timeout=60)
		self.uniprot_solr = pysolr.Solr(config.GO_IBA_SOLR_URL, timeout=60)
		self._uniprot_rows = None

	def get_optimal_uniprot_rows(self) -> int:
		"""Get optimal number of rows for UniProt queries using faceting."""
		if self._uniprot_rows is not None:
			return self._uniprot_rows
			
		facet_params = {
			'facet': 'on',
			'facet.field': 'uniprot_id',
			'facet.limit': -1,
			'facet.sort': 'count',
			'rows': 0
		}
		facet_response = self.paint_solr.search('*:*', **facet_params)
		self._uniprot_rows = 1000  # Default fallback
		
		if 'facet_counts' in facet_response.raw_response:
			facets = facet_response.raw_response['facet_counts']['facet_fields']['uniprot_id']
			if facets and len(facets) > 1:
				self._uniprot_rows = facets[1]  # The count of the most common uniprot_id
		
		print(f"Optimal UniProt DB result rows set to: {self._uniprot_rows}")
		return self._uniprot_rows

	def get_panther_document(self, panther_id: str) -> Optional[Dict]:
		"""Retrieve a single Panther document by ID."""
		query = f'id:{panther_id}'
		results = self.panther_solr.search(query, fl='id,uniprot_ids,go_annotations', rows=1)
		return results.docs[0] if results.docs else None

	def get_all_panther_documents(self, query: str = '*:*') -> List[Dict]:
		"""Retrieve all Panther documents matching the query."""
		print("Querying Panther collection...")
		results = self.panther_solr.search(query, fl='id,uniprot_ids,go_annotations', sort='id asc', rows=1000000)
		print(f"Found {len(results)} Panther documents")
		return results.docs

	def update_panther_document_annotations(self, panther_id: str, go_annotation_data_list: List[str]) -> bool:
		"""Update GO annotations for a single Panther document."""
		try:
			update_doc = {
				"id": panther_id,
				"go_annotations": {"set": go_annotation_data_list}
			}
			self.panther_solr.add([update_doc])
			self.panther_solr.commit()
			return True
		except Exception as e:
			print(f"Error updating document {panther_id}: {e}")
			return False

# ============================================================================
# GO TERM PROCESSING
# ============================================================================

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

def load_go_properties(properties_path: str) -> Dict[str, str]:
	"""Load GO ID to name mapping from properties file."""
	props = {}
	try:
		with open(properties_path, 'r') as pf:
			for line in pf:
				line = line.strip()
				if not line or line.startswith('#'):
					continue
				k, v = line.split('=', 1)
				props[k] = v
		print(f"Loaded {len(props)} GO names from properties file")
	except Exception as e:
		print(f"Warning: Could not load GO names properties file: {e}")
	return props

# ============================================================================
# ANNOTATION PROCESSING
# ============================================================================

def process_paint_annotation(cols: List[str], go_terms: dict) -> Optional[PaintAnno]:
	"""Process a single PAINT annotation from CSV columns."""
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
		print(f"Error processing PAINT annotation: {e}")
		return None

def get_go_annotations_for_uniprot_ids(uniprot_ids: List[str], solr_manager: SolrManager) -> List[str]:
	"""
	Fetch GO annotations (both EXP and IBA) for a list of UniProt IDs.
	Returns a list of JSON strings, each representing a GOAnnotationData object.
	"""
	go_annotation_data_list = []
	total_exp = 0
	total_iba = 0
	uniprot_rows = solr_manager.get_optimal_uniprot_rows()
	
	for uniprot_id in uniprot_ids:
		raw_annotations: List[dict] = []
		
		# Query paint_db for EXP annotations (uppercase)
		exp_results = solr_manager.paint_solr.search(
			f'uniprot_id:{uniprot_id.upper()}', 
			rows=uniprot_rows, 
			fl='go_annotations'
		)
		for result in exp_results:
			if 'go_annotations' in result:
				annotation = json.loads(result['go_annotations'])
				if annotation['evidenceCode'] in EVIDENCE_CODES:
					raw_annotations.append(annotation)
					total_exp += 1
		
		# Query uniprot_db for IBA annotations (lowercase)
		iba_results = solr_manager.uniprot_solr.search(
			f'uniprot_id:{uniprot_id.lower()}', 
			rows=uniprot_rows, 
			fl='go_annotations'
		)
		for result in iba_results:
			if 'go_annotations' in result:
				annotation = json.loads(result['go_annotations'])
				annotation['evidenceCode'] = "GO_REF," + annotation['evidenceCode'] + ",phylogeny"
				raw_annotations.append(annotation)
				total_iba += 1
		
		# Create combined annotation data if any annotations found
		if raw_annotations:
			go_annotation_data = {
				"uniprot_id": uniprot_id.lower(),
				"go_annotations": json.dumps(raw_annotations)
			}
			go_annotation_data_list.append(json.dumps(go_annotation_data))
	
	print(f"Retrieved annotations - EXP: {total_exp}, IBA: {total_iba}, Combined: {total_exp + total_iba}")
	return go_annotation_data_list

# ============================================================================
# SOLR INDEXING FUNCTIONS
# ============================================================================

def index_exp_annotations_to_solr(csv_path: str, go_basic_path: str, solr_client: Solr, 
								  batch_size: int = 10000, clear_solr: bool = False) -> None:
	"""Index experimental GO annotations from PAINT CSV to Solr."""
	print("Indexing EXP annotations to Solr...")
	
	if clear_solr:
		print("Clearing Solr collection...")
		solr_client.delete(q='*:*')
		solr_client.commit()
	
	# Load and preprocess GO terms
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
			pbar = tqdm(total=total_lines, desc="Processing EXP annotations", unit="lines")
			for line in file:
				cols = line.strip().split()
				annotation = process_paint_annotation(cols, go_terms)
				
				if annotation:
					doc = {
						"uniprot_id": annotation.geneProductId,
						"go_annotations": json.dumps({
							"geneProductId": annotation.geneProductId,
							"goId": annotation.goId,
							"goName": annotation.goName,
							"goAspect": annotation.goAspect,
							"evidenceCode": annotation.evidenceCode,
							"reference": annotation.reference
						})
					}
					batch.append(doc)
					count += 1

				if len(batch) >= batch_size:
					solr_client.add(batch)
					print(f"Committed batch of {len(batch)} docs, total processed: {count}")
					batch = []
				
				pbar.update(1)

			# Commit any remaining docs
			if batch:
				solr_client.add(batch)
				print(f"Committed final batch of {len(batch)} docs, total processed: {count}")
			
			pbar.close()
			print(f"Successfully indexed {count} EXP annotations")

	except Exception as e:
		print(f"Error processing EXP annotations: {e}")
		raise

def index_iba_annotations_to_solr(solr_client: Solr, clear_solr: bool = False, batch_size: int = 10000) -> None:
	"""Index IBA GO annotations from GAF files to Solr."""
	print("Indexing IBA annotations to Solr...")
	
	# Configuration paths
	GOIDNAME_PATH = r"C:\Users\swapp\Documents\MyProjects\Work\panther_storage\resources\iba\goidname.properties"
	IBA_GAF_DIR = r"C:\Users\swapp\Documents\MyProjects\Work\panther_storage\resources\iba"
	
	if clear_solr:
		print("Clearing Solr collection...")
		solr_client.delete(q='*:*')
		solr_client.commit()

	# Load GO names from properties file
	go_properties = load_go_properties(GOIDNAME_PATH)
	
	batch = []
	count = 0
	gaf_files = glob.glob(os.path.join(IBA_GAF_DIR, '*.gaf'))
	
	if not gaf_files:
		print(f"No GAF files found in {IBA_GAF_DIR}")
		return
	
	for filepath in gaf_files:
		print(f"Processing GAF file: {os.path.basename(filepath)}")
		
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
				
				# Extract annotation data from GAF format
				go_aspect = cols[8]
				gene_product_id = cols[10][10:].split('|')[0].lower()
				go_id = cols[4]
				key = go_id.split(':')[1]
				go_name = go_properties.get(key, '')
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
	
	print(f"Successfully indexed {count} IBA annotations")

# ============================================================================
# PANTHER COLLECTION UPDATE FUNCTIONS
# ============================================================================

def update_single_panther_go_annotations(panther_id: str) -> bool:
	"""Update GO annotations for a single Panther document."""
	print(f"Updating GO annotations for Panther ID: {panther_id}")
	
	config = SolrConfig()
	solr_manager = SolrManager(config)
	
	# Get the Panther document
	doc = solr_manager.get_panther_document(panther_id)
	if not doc:
		print(f"No document found with ID: {panther_id}")
		return False
	
	print(f"Found Panther document: {panther_id}")
	
	# Get UniProt IDs for this document
	uniprot_ids = doc.get('uniprot_ids', [])
	if isinstance(uniprot_ids, str):
		uniprot_ids = [uniprot_ids]
	
	if not uniprot_ids:
		print(f"No UniProt IDs found for {panther_id}")
		return False
	
	print(f"Processing {len(uniprot_ids)} UniProt IDs for {panther_id}")
	
	# Get GO annotations for all UniProt IDs
	go_annotation_data_list = get_go_annotations_for_uniprot_ids(uniprot_ids, solr_manager)
	
	# Update the document
	success = solr_manager.update_panther_document_annotations(panther_id, go_annotation_data_list)
	
	if success:
		print(f"Successfully updated GO annotations for {panther_id}")
		print(f"Added {len(go_annotation_data_list)} GO annotation entries")
	
	return success

def update_all_panther_go_annotations(query: str = '*:*') -> None:
	"""Update GO annotations for all Panther documents matching the query."""
	print("Updating GO annotations for all Panther documents...")
	
	config = SolrConfig()
	solr_manager = SolrManager(config)
	
	# Get all Panther documents
	documents = solr_manager.get_all_panther_documents(query)
	
	# Process each document with progress bar
	with tqdm(total=len(documents), desc="Processing Panther documents", unit="doc") as pbar:
		for doc in documents:
			panther_id = doc.get('id')
			pbar.set_description(f"Processing: {panther_id}")
			
			uniprot_ids = doc.get('uniprot_ids', [])
			if isinstance(uniprot_ids, str):
				uniprot_ids = [uniprot_ids]
			
			if uniprot_ids:
				go_annotation_data_list = get_go_annotations_for_uniprot_ids(uniprot_ids, solr_manager)
				solr_manager.update_panther_document_annotations(panther_id, go_annotation_data_list)
			
			pbar.update(1)
	
	print("Completed updating all Panther documents")

def update_panther_go_annotations_from_csv(csv_file_path: str = "solr_indexed_documents.csv") -> bool:
	"""Update GO annotations for Panther documents listed in a CSV file."""
	print(f"Updating GO annotations from CSV file: {csv_file_path}")
	
	# Check if CSV file exists
	if not os.path.exists(csv_file_path):
		print(f"CSV file not found: {csv_file_path}")
		return False

	# Read document IDs from CSV file
	document_ids = []
	try:
		with open(csv_file_path, 'r', newline='') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				document_id = row.get('document_id', '').strip()
				if document_id:
					document_ids.append(document_id)
		
		print(f"Found {len(document_ids)} document IDs in CSV file")
		if not document_ids:
			print("No document IDs found in CSV file")
			return False
			
	except Exception as e:
		print(f"Error reading CSV file: {e}")
		return False

	config = SolrConfig()
	solr_manager = SolrManager(config)
	
	# Process each document ID with progress bar
	successful_updates = 0
	failed_updates = 0
	
	with tqdm(total=len(document_ids), desc="Processing document IDs", unit="doc") as pbar:
		for panther_id in document_ids:
			pbar.set_description(f"Processing: {panther_id}")
			
			try:
				doc = solr_manager.get_panther_document(panther_id)
				if not doc:
					print(f"No document found with ID: {panther_id}")
					failed_updates += 1
					pbar.update(1)
					continue
				
				# Get uniprot_ids for this document
				uniprot_ids = doc.get('uniprot_ids', [])
				if isinstance(uniprot_ids, str):
					uniprot_ids = [uniprot_ids]
				
				if not uniprot_ids:
					print(f"No UniProt IDs found for {panther_id}")
					failed_updates += 1
					pbar.update(1)
					continue
				
				# Get GO annotations and update document
				go_annotation_data_list = get_go_annotations_for_uniprot_ids(uniprot_ids, solr_manager)
				
				if solr_manager.update_panther_document_annotations(panther_id, go_annotation_data_list):
					successful_updates += 1
				else:
					failed_updates += 1
				
			except Exception as e:
				print(f"Error processing {panther_id}: {e}")
				failed_updates += 1
			
			pbar.update(1)

	# Summary
	print(f"\nProcessing complete!")
	print(f"Successfully updated: {successful_updates} documents")
	print(f"Failed updates: {failed_updates} documents")
	print(f"Total processed: {len(document_ids)} documents")
	
	return successful_updates > 0

# ============================================================================
# ANALYSIS FUNCTIONS
# ============================================================================

def analyze_paint_csv_structure(csv_path: str, num_lines: int = 10):
	"""Analyze the structure of the PAINT CSV file."""
	print(f"\nAnalyzing first {num_lines} lines of PAINT CSV file:")
	print("-" * 80)
	
	with open(csv_path, 'r') as file:
		for i, line in enumerate(file):
			if i >= num_lines:
				break
				
			print(f"\nLine {i+1}:")
			print("-" * 40)
			
			# Split the line and show each field
			cols = line.strip().split()
			print(f"Number of columns: {len(cols)}")
			
			# Show each column with its index
			for idx, col in enumerate(cols):
				print(f"Column {idx}: {col}")
			
			# Try to process the line as a PAINT annotation
			try:
				annotation = process_paint_annotation(cols, {})
				if annotation:
					print("\nSuccessfully parsed as PAINT annotation:")
					print(f"Gene Product ID: {annotation.geneProductId}")
					print(f"GO ID: {annotation.goId}")
					print(f"Evidence Code: {annotation.evidenceCode}")
					print(f"Reference: {annotation.reference}")
			except Exception as e:
				print(f"\nError processing line: {str(e)}")
			
			print("-" * 40)
	
	print("\nAnalysis complete!")

def analyze_gene_product_annotations(csv_path: str, gene_product_id: str):
    """Analyze and export annotations for a specific gene product ID from both EXP and IBA sources."""
    print(f"\nAnalyzing annotations for gene product ID: {gene_product_id}")
    print("-" * 80)
    
    # Configuration for IBA GAF files
    GOIDNAME_PATH = r"C:\Users\swapp\Documents\MyProjects\Work\panther_storage\resources\iba\goidname.properties"
    IBA_GAF_DIR = r"C:\Users\swapp\Documents\MyProjects\Work\panther_storage\resources\iba"
    
    # Load GO names from properties file for IBA processing
    go_properties = load_go_properties(GOIDNAME_PATH)
    
    # Lists to store found entries
    exp_entries = []
    iba_entries = []
    
    # 1. Scan PAINT CSV file for EXP annotations
    print(f"\n1. Scanning PAINT CSV file for EXP annotations...")
    print("-" * 50)
    
    with open(csv_path, 'r') as file:
        total_lines = sum(1 for _ in file)
    
    with open(csv_path, 'r') as file:
        pbar = tqdm(total=total_lines, desc="Searching PAINT CSV", unit="lines")
        for line in file:
            cols = line.strip().split()
            if not cols:
                pbar.update(1)
                continue
                
            # Check if this line contains our gene product ID
            if gene_product_id in cols[0]:
                entry_data = ','.join(cols)
                exp_entries.append(entry_data)
                print(f"\nFound EXP entry: {entry_data}")
                
                # Try to parse as PAINT annotation
                try:
                    annotation = process_paint_annotation(cols, {})
                    if annotation:
                        print(f"Parsed as: GO:{annotation.goId} ({annotation.evidenceCode}) - Ref: {annotation.reference}")
                except Exception as e:
                    print(f"Error parsing line: {str(e)}")
                print("-" * 40)
            
            pbar.update(1)
        pbar.close()
    
    # 2. Scan GAF files for IBA annotations
    print(f"\n2. Scanning GAF files for IBA annotations...")
    print("-" * 50)
    
    gaf_files = glob.glob(os.path.join(IBA_GAF_DIR, '*.gaf'))
    if not gaf_files:
        print(f"No GAF files found in {IBA_GAF_DIR}")
    else:
        print(f"Found {len(gaf_files)} GAF files")
        
        for filepath in gaf_files:
            print(f"Processing GAF file: {os.path.basename(filepath)}")
            
            with open(filepath, 'r') as br:
                total_lines = sum(1 for _ in br)
            
            with open(filepath, 'r') as br:
                pbar = tqdm(br, total=total_lines, desc=f"Searching {os.path.basename(filepath)}", unit="lines")
                for line in pbar:
                    if not line.strip() or line.startswith('!'):
                        continue
                    cols = line.strip().split('\t')
                    if len(cols) < 11:
                        continue
                    
                    # Extract gene product ID from GAF format
                    gaf_gene_product_id = cols[10][10:].split('|')[0].lower()
                    
                    # Check if this matches our target gene product ID
                    if gene_product_id.lower() == gaf_gene_product_id:
                        # Extract relevant information
                        go_aspect = cols[8]
                        go_id = cols[4]
                        key = go_id.split(':')[1]
                        go_name = go_properties.get(key, '')
                        reference = cols[5]
                        evidence_code = cols[6]
                        
                        entry_data = f"{gaf_gene_product_id},{go_id},{go_name},{go_aspect},{evidence_code},{reference}"
                        iba_entries.append(entry_data)
                        print(f"\nFound IBA entry: {entry_data}")
                        print("-" * 40)
                pbar.close()
    
    # 3. Create CSV files for the results
    print(f"\n3. Creating CSV files...")
    print("-" * 50)
    
    # Create EXP CSV file
    exp_csv_filename = f"exp_annotations_{gene_product_id}.csv"
    with open(exp_csv_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['gene_product_id', 'go_id', 'evidence_code', 'col3', 'reference', 'additional_cols'])
        for entry in exp_entries:
            writer.writerow(entry.split(','))
    
    # Create IBA CSV file
    iba_csv_filename = f"iba_annotations_{gene_product_id}.csv"
    with open(iba_csv_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['gene_product_id', 'go_id', 'go_name', 'go_aspect', 'evidence_code', 'reference'])
        for entry in iba_entries:
            writer.writerow(entry.split(','))
    
    # 4. Summary
    print(f"\nSummary for gene product ID: {gene_product_id}")
    print("-" * 80)
    print(f"EXP annotations found: {len(exp_entries)}")
    print(f"IBA annotations found: {len(iba_entries)}")
    print(f"Total annotations found: {len(exp_entries) + len(iba_entries)}")
    
    if exp_entries:
        print(f"EXP annotations saved to: {exp_csv_filename}")
    if iba_entries:
        print(f"IBA annotations saved to: {iba_csv_filename}")
    
    if not exp_entries and not iba_entries:
        print(f"No annotations found for gene product ID: {gene_product_id}")
    else:
        print("Analysis complete!")

# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
	"""Main function to run various annotation processing tasks."""
	# Configuration
	BASE_DIR = os.getenv('BASE_DIR', r"C:\Users\Documents\panther_storage\resources")
	CSV_PATH = os.path.join(BASE_DIR, "paint", "Pthr_GO_19.0.tsv")
	GO_BASIC_PATH = os.path.join(BASE_DIR, "paint", "go-basic.json")
	
	config = SolrConfig()
	
	# Initialize Solr clients
	exp_solr_client = Solr(config.GO_EXP_SOLR_URL, timeout=60)
	iba_solr_client = Solr(config.GO_IBA_SOLR_URL, timeout=60)
	
	# ========================================================================
	# ANALYSIS TASKS (Uncomment as needed)
	# ========================================================================
	
	# Analyze PAINT CSV structure
	# analyze_paint_csv_structure(CSV_PATH)
	
	# Analyze specific gene product
	# analyze_gene_product_annotations(CSV_PATH, "P60981")
	
	# ========================================================================
	# INDEXING TASKS (Uncomment as needed)
	# ========================================================================
	
	# Index EXP annotations to Solr
	# index_exp_annotations_to_solr(
	# 	csv_path=CSV_PATH,
	# 	go_basic_path=GO_BASIC_PATH,
	# 	solr_client=exp_solr_client,
	# 	clear_solr=False
	# )

	# Index IBA annotations to Solr
	# index_iba_annotations_to_solr(iba_solr_client, clear_solr=True)

	# ========================================================================
	# PANTHER COLLECTION UPDATE TASKS (Uncomment as needed)
	# ========================================================================
	
	# Update all Panther documents with GO annotations
	# update_all_panther_go_annotations()

	# Update single Panther document
	# update_single_panther_go_annotations("PTHR48493")

	# Update from CSV file
	update_panther_go_annotations_from_csv()

if __name__ == "__main__":
	main() 