from dotenv import load_dotenv
import sys
import os
from boto3 import client as get_boto3_client
from pysolr import Solr
import glob
from tqdm import tqdm
import json
import csv
from datetime import datetime

# Get project root directory (panther-pipeline/)
current_dir = os.path.dirname(os.path.abspath(__file__))  # pipeline_scripts/
api_scripts_dir = os.path.dirname(current_dir)  # python_api_scripts/
project_root = os.path.dirname(api_scripts_dir)  # panther-pipeline/

# Load environment variables from project root
load_dotenv(os.path.join(project_root, '.env.sandbox'), override=True)

def flatten_tree(children, annotations):
	"""Recursively flatten the tree structure to extract all annotations."""
	if not children or not children.get('annotation_node'):
		return
	
	annotation_nodes = children['annotation_node']
	if not isinstance(annotation_nodes, list):
		annotation_nodes = [annotation_nodes]
	
	for annotation in annotation_nodes:
		annotations.append(annotation)
		if annotation.get('children'):
			flatten_tree(annotation['children'], annotations)

def add_to_list_from_annotation(annotation, panther_data):
	"""Extract fields from annotation and add to panther_data lists."""
	# sf_id
	if annotation.get('sf_id') and annotation['sf_id'] not in panther_data.get('sf_ids', []):
		panther_data.setdefault('sf_ids', []).append(annotation['sf_id'])
	
	# sf_name
	if annotation.get('sf_name') and annotation['sf_name'] not in panther_data.get('sf_names', []):
		panther_data.setdefault('sf_names', []).append(annotation['sf_name'])
	
	# species - only add the first one
	if annotation.get('species') and len(panther_data.get('species_list', [])) == 0:
		panther_data.setdefault('species_list', []).append(annotation['species'])
	
	# taxonomic_range_root - only add the first one
	if annotation.get('taxonomic_range') and len(panther_data.get('taxonomic_ranges_root', [])) == 0:
		panther_data.setdefault('taxonomic_ranges_root', []).append(annotation['taxonomic_range'])
	
	# taxonomic_range
	if annotation.get('taxonomic_range') and annotation['taxonomic_range'] not in panther_data.get('taxonomic_ranges', []):
		panther_data.setdefault('taxonomic_ranges', []).append(annotation['taxonomic_range'])
	
	# branch_length
	if annotation.get('branch_length') and annotation['branch_length'] not in panther_data.get('branch_lengths', []):
		panther_data.setdefault('branch_lengths', []).append(annotation['branch_length'])
	
	# gene_id
	if annotation.get('gene_id') and annotation['gene_id'] not in panther_data.get('gene_ids', []):
		panther_data.setdefault('gene_ids', []).append(annotation['gene_id'])
	
	# gene_symbol
	if annotation.get('gene_symbol') and annotation['gene_symbol'] not in panther_data.get('gene_symbols', []):
		panther_data.setdefault('gene_symbols', []).append(annotation['gene_symbol'])
	
	# definition
	if annotation.get('definition') and annotation['definition'] not in panther_data.get('definitions', []):
		panther_data.setdefault('definitions', []).append(annotation['definition'])
	
	# node_name and extract uniprot_id
	if annotation.get('node_name') and annotation['node_name'] not in panther_data.get('node_names', []):
		panther_data.setdefault('node_names', []).append(annotation['node_name'])
		# Extract UniProtKB ID if present
		if 'UniProtKB=' in annotation['node_name']:
			uniprot_id = annotation['node_name'].split('UniProtKB=')[1]
			if uniprot_id and uniprot_id not in panther_data.get('uniprot_ids', []):
				panther_data.setdefault('uniprot_ids', []).append(uniprot_id)
	
	# persistent_id
	if annotation.get('persistent_id') and annotation['persistent_id'] not in panther_data.get('persistent_ids', []):
		panther_data.setdefault('persistent_ids', []).append(annotation['persistent_id'])
	
	# organism
	if annotation.get('organism') and annotation['organism'] not in panther_data.get('organisms', []):
		panther_data.setdefault('organisms', []).append(annotation['organism'])

def build_list_items(annotations, panther_data):
	"""Process all annotations and build the searchable lists."""
	for annotation in annotations:
		add_to_list_from_annotation(annotation, panther_data)

def get_json_data_from_local(file_id):
	"""Read JSON data from local folder using the file ID."""
	try:
		folder_path = os.getenv('PRUNED_TREES_PATH', 'panther_storage\\pruned_panther_files')
		file_path = os.path.join(folder_path, f"{file_id}.json")
		
		if not os.path.exists(file_path):
			print(f"File not found: {file_path}")
			return None
		
		with open(file_path, 'r', encoding='utf-8') as file:
			json_data = json.load(file)
		
		return json_data
		
	except Exception as e:
		print(f"Error reading JSON file {file_id}: {str(e)}")
		return None

def convert_json_to_solr_document(json_data, file_id):
	"""Convert JSON data to Solr document format similar to Java convertJsonToSolrDocument."""
	try:
		# Initialize the result document
		panther_data = {
			'id': file_id,
			'family_name': json_data.get('family_name', 'Unknown')
		}
		
		# Parse JSON if it's a string
		if isinstance(json_data, str):
			json_data = json.loads(json_data)
		
		# Get the jsonString field which contains the actual tree data
		json_string = json_data.get('jsonString')
		if not json_string:
			print(f"Error in: {file_id} - No jsonString field found")
			return None
		
		# Parse the jsonString to get the tree data
		tree_data = json.loads(json_string)
		
		# Check if search data exists in the tree data
		if not tree_data.get('search'):
			print(f"Error in: {file_id} - No search data found in jsonString")
			return None
		
		# Get root annotation node from tree_topology
		tree_topology = tree_data['search'].get('tree_topology')
		if not tree_topology:
			print(f"Error in: {file_id} - No tree_topology found")
			return None
			
		root_annotation = tree_topology.get('annotation_node')
		if not root_annotation:
			print(f"Error in: {file_id} - No root annotation node found")
			return None
		
		# Initialize annotations list
		annotations = []
		
		# Add root annotation to the list
		add_to_list_from_annotation(root_annotation, panther_data)
		
		# Flatten the tree structure
		if root_annotation.get('children'):
			flatten_tree(root_annotation['children'], annotations)
		
		# Build searchable lists from all annotations
		build_list_items(annotations, panther_data)
		
		return panther_data
		
	except Exception as e:
		print(f"Error in building list for {file_id}: {str(e)}")
		return None

def add_document_to_solr(solr_document):
	"""Add a single document to the Solr collection and log it to CSV."""
	try:
		solr_host = os.getenv('SOLR_HOST', 'http://localhost:8983')
		solr_collection = os.getenv('SOLR_COLLECTION', 'panther')
		solr_url = f"{solr_host}/solr/{solr_collection}"
		solr = Solr(solr_url)
		
		if not solr_document:
			print("Error: No document to add")
			return False
		
		# Add the document to Solr
		solr.add([solr_document])
		
		# Commit the changes to make them visible
		solr.commit()
		
		# Log to CSV file
		csv_filename = "solr_indexed_documents.csv"
		file_exists = os.path.exists(csv_filename)
		
		with open(csv_filename, 'a', newline='') as csvfile:
			writer = csv.writer(csvfile)
			if not file_exists:
				writer.writerow(['document_id', 'family_name'])
			writer.writerow([
				solr_document.get('id', 'Unknown'),
				solr_document.get('family_name', 'Unknown'),
			])
		
		# print(f"Added: {solr_document.get('id', 'Unknown')}")
		return True
		
	except Exception as e:
		print(f"Error adding document to Solr: {str(e)}")
		return False

def list_json_files():
	"""List all JSON files in the specified folder."""
	folder_path = os.getenv('PRUNED_TREES_PATH', 'panther_storage\\pruned_panther_files')
	json_pattern = os.path.join(folder_path, '*.json')
	json_files = glob.glob(json_pattern)
	print(f"Found {len(json_files)} JSON files in {folder_path}")
	for file in json_files:
		# print(f"- {os.path.basename(file)}")
		pass
	return json_files

def index_panther_solr():
	print("Indexing panther Solr collection")
	solr_host = os.getenv('SOLR_HOST', 'http://localhost:8983')
	solr_collection = os.getenv('SOLR_COLLECTION', 'panther')
	solr_url = f"{solr_host}/solr/{solr_collection}"
	solr = Solr(solr_url)
	json_files = list_json_files()
	not_found_count = 0
	processed_count = 0
	
	for file in tqdm(json_files, desc="Processing files"):
		file_id = os.path.splitext(os.path.basename(file))[0]
		# Check if document exists in Solr
		results = solr.search(f'id:{file_id}', rows=1)
		if results.hits == 0:
			not_found_count += 1
			# Process the file
			json_data = get_json_data_from_local(file_id)
			if json_data:
				solr_document = convert_json_to_solr_document(json_data, file_id)
				if solr_document:
					if add_document_to_solr(solr_document):
						processed_count += 1
	print(f"Not found count: {not_found_count}")
	print(f"Successfully processed and indexed: {processed_count} files")

def check_s3_access():
	"""Check if AWS credentials are valid and have permission to list the S3 bucket. Returns True if access is OK, False otherwise."""
	S3_BUCKET = os.getenv('S3_BUCKET', 'phg-panther-data-19')
	s3 = get_boto3_client('s3')
	try:
		response = s3.list_objects_v2(Bucket=S3_BUCKET, MaxKeys=1)
		print(f"[SUCCESS] Able to list objects in bucket '{S3_BUCKET}'. Your credentials are valid and have list permission.")
		if 'Contents' in response:
			# print("Sample object(s):", [obj['Key'] for obj in response['Contents']])
			pass
		else:
			print("Bucket is empty or you do not have permission to list contents.")
		return True
	except Exception as e:
		print(f"[ERROR] S3 access check failed: {e}")
		return False

def main():
	if not check_s3_access():
		sys.exit(1)
	# list_json_files()

	# Goes through all the files in the local folder and adds them to Solr if they are not already in Solr
	index_panther_solr()

	#Test one file
	# file_id = "PTHR48493"
	# json_data = get_json_data_from_local(file_id)
	# # print(json_data)
	# solr_document = convert_json_to_solr_document(json_data, file_id)
	# # print(solr_document)
	# add_document_to_solr(solr_document)

if __name__ == "__main__":
	main()