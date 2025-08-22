import json
import os
import csv
import requests
import pysolr
from dotenv import load_dotenv
from tqdm import tqdm
import boto3
import sys

# Get project root directory (panther-pipeline/)
current_dir = os.path.dirname(os.path.abspath(__file__))  # pipeline_scripts/
api_scripts_dir = os.path.dirname(current_dir)  # python_api_scripts/
project_root = os.path.dirname(api_scripts_dir)  # panther-pipeline/

# Add parent directory to path for utils import
sys.path.insert(0, api_scripts_dir)
from utils.taxon_utils import get_agi_locus_mapping

# Load environment variables from project root
load_dotenv(os.path.join(project_root, '.env.sandbox'), override=True)

# Ensure AWS credentials are loaded from .env if present
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_DEFAULT_REGION')

BASE_DIR = os.getenv('BASE_DIR', r"C:\Users\Documents\panther_storage\resources")
# Removed: MAPPING_CSV_PATH - now using centralized utility
SOLR_HOST = os.getenv('SOLR_HOST', 'http://localhost:8983')
PANTHER_COLLECTION = os.getenv('PANTHER_COLLECTION', 'panther')
PANTHER_SOLR_URL = f"{SOLR_HOST}/solr/{PANTHER_COLLECTION}"
OUTPUT_DIR = os.getenv('CSV_OUTPUT_DIR', os.path.join(BASE_DIR, 'panther_csv'))

def get_boto3_client(service, region_name=None):
	"""Return a boto3 client using credentials from env if available."""
	if aws_access_key and aws_secret_key:
		return boto3.client(
			service,
			aws_access_key_id=aws_access_key,
			aws_secret_access_key=aws_secret_key,
			region_name=region_name or aws_region
		)
	else:
		return boto3.client(service, region_name=region_name or aws_region)

def check_s3_access():
	"""Check if AWS credentials are valid and have permission to list the S3 bucket."""
	S3_BUCKET = os.getenv('S3_BUCKET', 'phg-panther-data-19')
	s3 = get_boto3_client('s3')
	try:
		response = s3.list_objects_v2(Bucket=S3_BUCKET, MaxKeys=1)
		print(f"[SUCCESS] Able to list objects in bucket '{S3_BUCKET}'. Your credentials are valid.")
		return True
	except Exception as e:
		print(f"[ERROR] S3 access check failed: {e}")
		return False

# Removed: load_agi_locus_mapping - now using centralized utility

def get_s3_tree_data(panther_id):
	"""Fetch tree JSON data from S3."""
	S3_BASE_URL = os.getenv('S3_BASE_URL', 'https://phg-panther-data-19.s3.us-west-2.amazonaws.com')
	url = f"{S3_BASE_URL}/{panther_id}.json"
	
	try:
		response = requests.get(url, timeout=30)
		if response.status_code != 200:
			print(f"Failed to fetch S3 file for {panther_id}: {response.status_code}")
			return None
		return response.json()
	except Exception as e:
		print(f"Error fetching S3 data for {panther_id}: {e}")
		return None

def extract_leaf_nodes(tree_data):
	"""Extract all leaf nodes (genes) from the tree structure."""
	leaf_nodes = []
	
	def traverse_tree(node):
		if isinstance(node, dict):
			# Check if this is a leaf node (has gene information)
			if node.get('tree_node_type') == 'LEAF':
				# print(node)
				leaf_nodes.append(node)
			else:
				# Traverse children
				children = node.get('children', {})
				if isinstance(children, dict):
					annotation_nodes = children.get('annotation_node', [])
					if isinstance(annotation_nodes, list):
						for child in annotation_nodes:
							traverse_tree(child)
					elif isinstance(annotation_nodes, dict):
						traverse_tree(annotation_nodes)
	
	# Start traversal from the tree topology
	tree_topology = tree_data.get('search', {}).get('tree_topology', {}).get('annotation_node', {})

	# Log the first level of the tree
	# if isinstance(tree_topology, dict):
	# 	print("First level of tree_topology:")
	# 	for k, v in tree_topology.items():
	# 		print(f"  Key: {k}, Type: {type(v)}")
	# else:
	# 	print(f"tree_topology is not a dict, type: {type(tree_topology)}")
	
	traverse_tree(tree_topology)
	
	return leaf_nodes

def get_go_annotations_from_solr(panther_id, solr_client):
	"""Get GO annotations for a specific tree from Solr."""
	try:
		results = solr_client.search(f'id:{panther_id}', fl='go_annotations', rows=1)
		if results and len(results.docs) > 0:
			go_annotations = results.docs[0].get('go_annotations', [])
			print(f"Found {len(go_annotations)} annotation entries for {panther_id}")
			return go_annotations
	except Exception as e:
		print(f"Error getting GO annotations for {panther_id}: {e}")
	return []

def parse_go_annotations(go_annotations_list):
	"""Parse GO annotations into a structured format."""
	annotations_by_uniprot = {}
	
	for annotation_json in go_annotations_list:
		try:
			# Parse the outer JSON string
			if isinstance(annotation_json, str):
				annotation_data = json.loads(annotation_json)
			else:
				annotation_data = annotation_json
			
			uniprot_id = annotation_data.get('uniprot_id', '').lower()
			go_annos_str = annotation_data.get('go_annotations', [])
			
			# Parse the inner JSON string (go_annotations is a JSON string)
			if isinstance(go_annos_str, str):
				go_annos = json.loads(go_annos_str)
			else:
				go_annos = go_annos_str
			
			# Ensure go_annos is a list
			if not isinstance(go_annos, list):
				go_annos = [go_annos]
			
			# Process each annotation
			for anno in go_annos:
				if isinstance(anno, dict):
					go_name = anno.get('goName', '').strip()
					go_id = anno.get('goId', '').strip()
					go_aspect = anno.get('goAspect', '').strip()
					evidence_code = anno.get('evidenceCode', '')
					
					# Filter out cellular_component annotations
					if go_aspect in ['cellular_component', 'C']:
						# print(f"Skipping cellular_component annotation: {go_name} ({go_id})")
						continue
					
					if go_name and go_id:  # Only add if both go_name and go_id are present
						if uniprot_id not in annotations_by_uniprot:
							annotations_by_uniprot[uniprot_id] = []
						
						annotations_by_uniprot[uniprot_id].append({
							'go_name': go_name,
							'go_id': go_id,
							'evidence_code': evidence_code
						})
		except Exception as e:
			print(f"Error parsing GO annotation: {e}")
			print(f"Problematic annotation: {annotation_json}")
			continue
	
	print(f"Parsed annotations for {len(annotations_by_uniprot)} UniProt IDs")
	return annotations_by_uniprot

def get_unique_go_terms(annotations_by_uniprot):
	"""Get unique GO terms across all annotations, formatted as 'go_name (go_id)'."""
	unique_terms = set()
	for uniprot_id, annotations in annotations_by_uniprot.items():
		for anno in annotations:
			go_name = anno.get('go_name', '').strip()
			go_id = anno.get('go_id', '').strip()
			if go_name and go_id:
				col_name = f"{go_name} ({go_id})"
				unique_terms.add(col_name)

	return sorted(list(unique_terms), key=str.lower)

def generate_csv_for_tree(panther_id, agi_locus_mapping, solr_client, output_dir):
	"""Generate CSV file for a single Panther tree."""
	try:
		# Get tree data from S3
		tree_data = get_s3_tree_data(panther_id)
		if not tree_data:
			return False
		
		# Extract leaf nodes
		leaf_nodes = extract_leaf_nodes(tree_data)
		if not leaf_nodes:
			print(f"No leaf nodes found for {panther_id}")
			return False
		
		# Get GO annotations from Solr
		go_annotations_list = get_go_annotations_from_solr(panther_id, solr_client)
		annotations_by_uniprot = parse_go_annotations(go_annotations_list)
		# print(annotations_by_uniprot)
		unique_go_terms = get_unique_go_terms(annotations_by_uniprot)
		
		# Create CSV file
		csv_filename = os.path.join(output_dir, f"{panther_id}.csv")
		os.makedirs(output_dir, exist_ok=True)
		
		with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
			# Define columns
			base_columns = ['Uniprot ID', 'Gene', 'Gene ID', 'Gene name', 'Organism', 'Subfamily name']
			all_columns = base_columns + unique_go_terms
			
			writer = csv.writer(csvfile)
			
			# Write intro text
			intro_text = ("Columns after 'Subfamily name', if any, are GO annotations. "
						 "Each column is a GO molecular function or biological process term "
						 "annotated to at least one member of the gene family. "
						 "A '0' indicates absence of annotations.")
			writer.writerow([intro_text])
			
			# Write headers
			writer.writerow(all_columns)
			
			# Write data for each leaf node
			for node in leaf_nodes:
				row = []
				
				# Extract basic information
				uniprot_id = node.get('node_name', '').split('UniProtKB=')[-1] if 'UniProtKB=' in node.get('node_name', '') else ''
				gene_symbol = node.get('gene_symbol', '')
				gene_id = node.get('gene_id', '')
				organism = node.get('organism', '')
				sf_name = node.get('sf_name', '')
				
				# Process gene_id for TAIR entries
				processed_gene_id = gene_id
				if gene_id.startswith('TAIR:locus='):
					locus_id = gene_id.split('TAIR:locus=')[1]
					agi_id = agi_locus_mapping.get(locus_id)
					if agi_id:
						processed_gene_id = f'TAIR:{agi_id}'
				
				# Build row
				row.extend([
					uniprot_id,
					gene_symbol or processed_gene_id,
					processed_gene_id,
					gene_symbol,
					organism,
					sf_name
				])
				
				# Add GO annotation columns
				user_annotations = annotations_by_uniprot.get(uniprot_id.lower(), [])
				for go_term in unique_go_terms:
					annotation_present = '0'  # Default
					# Extract GO name from format "go_name (go_id)"
					go_name_to_match = go_term.split('(')[0].strip()
					
					for anno in user_annotations:
						if anno.get('go_name', '').strip() == go_name_to_match:
							evidence_code = anno.get('evidence_code', '')
							if 'IBA' in evidence_code:
								annotation_present = 'IBA'
							elif any(exp_code in evidence_code for exp_code in ['EXP', 'IDA', 'IEP', 'IGI', 'IMP', 'IPI']):
								annotation_present = 'EXP'
							break
					row.append(annotation_present)
				
				writer.writerow(row)
		
		print(f"Generated CSV for {panther_id} with {len(leaf_nodes)} genes")
		return True
		
	except Exception as e:
		print(f"Error generating CSV for {panther_id}: {e}")
		return False

def generate_all_panther_csvs():
	"""Generate CSV files for all Panther trees."""
	# Initialize Solr client
	solr_client = pysolr.Solr(PANTHER_SOLR_URL, timeout=60)
	
	# Load AGI locus mapping using centralized utility
	agi_locus_mapping = get_agi_locus_mapping()
	
	# Query all Panther tree IDs
	query = '*:*'
	# query = 'id:PTHR11913'
	print("Querying Panther collection for all tree IDs...")
	try:
		results = solr_client.search(query, fl='id', sort='id asc', rows=100000)
		total_trees = len(results)
		print(f"Found {total_trees} Panther trees")
	except Exception as e:
		print(f"Error querying Solr: {e}")
		return
	
	# Create output directory
	os.makedirs(OUTPUT_DIR, exist_ok=True)
	print(f"Output directory: {OUTPUT_DIR}")
	
	# Process each tree
	successful = 0
	failed = 0
	
	with tqdm(total=total_trees, desc="Generating CSVs", unit="tree") as pbar:
		for doc in results:
			panther_id = doc.get('id')
			pbar.set_description(f"Processing {panther_id}")

			if panther_id == 'PTHR11530':
				if generate_csv_for_tree(panther_id, agi_locus_mapping, solr_client, OUTPUT_DIR):
					successful += 1
				else:
					failed += 1
				sys.exit()
			pbar.update(1)
	
	print(f"\nCompleted: {successful} successful, {failed} failed")

def main():
	if not check_s3_access():
		print("S3 access check failed. Please check your credentials.")
		sys.exit(1)
	
	print("Starting Panther CSV generation...")
	generate_all_panther_csvs()
	print("CSV generation completed!")

if __name__ == "__main__":
	main() 