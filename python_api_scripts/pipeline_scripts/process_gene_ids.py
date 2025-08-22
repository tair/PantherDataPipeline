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
import boto3
import io
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
aws_region = os.getenv('AWS_REGION')

# Removed: MAPPING_CSV_PATH - now using centralized utility
SOLR_HOST = os.getenv('SOLR_HOST', 'http://localhost:8983')
PANTHER_DB_COLLECTION = os.getenv('PANTHER_COLLECTION', 'panther')
PANTHER_SOLR_URL = f"{SOLR_HOST}/solr/{PANTHER_DB_COLLECTION}"

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
	
# Removed: load_agi_locus_mapping - now using centralized utility

def update_gene_ids_in_list(gene_ids, agi_locus_mapping):
	updated_gene_ids = []
	changed = False
	for gene_id in gene_ids:
		if isinstance(gene_id, str) and gene_id.startswith('TAIR:locus='):
			locus_id = gene_id.split('TAIR:locus=')[1]
			agi_id = agi_locus_mapping.get(locus_id)
			if agi_id:
				updated_gene_ids.append(f'TAIR:{agi_id}')
				changed = True
			else:
				updated_gene_ids.append(gene_id)
		else:
			updated_gene_ids.append(gene_id)
	return updated_gene_ids, changed


def update_gene_ids_in_json(obj, agi_locus_mapping):
	changed = False
	if isinstance(obj, dict):
		for k, v in obj.items():
			if k == 'gene_id' and isinstance(v, str) and v.startswith('TAIR:locus='):
				locus_id = v.split('TAIR:locus=')[1]
				agi_id = agi_locus_mapping.get(locus_id)
				if agi_id:
					obj[k] = f'TAIR:{agi_id}'
					changed = True
			elif k == 'gene_id' and isinstance(v, str) and v.startswith('Gene_ORFName:'):
				# Optionally handle other gene_id formats here
				pass
			else:
				if update_gene_ids_in_json(v, agi_locus_mapping):
					changed = True
	elif isinstance(obj, list):
		for item in obj:
			if update_gene_ids_in_json(item, agi_locus_mapping):
				changed = True
	return changed


def update_s3_json_gene_ids(panther_id, agi_locus_mapping):
	S3_BUCKET = os.getenv('S3_BUCKET', 'phg-panther-data-19')
	S3_REGION = os.getenv('S3_REGION', 'us-west-2')
	S3_BASE_URL = os.getenv('S3_BASE_URL', f'https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com')
	key = f"{panther_id}.json"
	url = f"{S3_BASE_URL}/{key}"

	response = requests.get(url)
	if response.status_code != 200:
		print(f"Failed to fetch S3 file for {panther_id}: {response.status_code}")
		return False
	try:
		data = response.json()
	except Exception as e:
		print(f"Error parsing JSON for {panther_id}: {e}")
		return False

	changed = update_gene_ids_in_json(data, agi_locus_mapping)
	if changed:
		s3 = get_boto3_client('s3', region_name=S3_REGION)
		json_bytes = json.dumps(data, ensure_ascii=False).encode('utf-8')
		s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json_bytes, ContentType='application/json')
		print(f"Updated gene_id fields in S3 for {panther_id}")
	return changed


def update_panther_gene_ids(agi_locus_mapping):
	panther_solr = pysolr.Solr(PANTHER_SOLR_URL, timeout=60)

	# query = 'id:PTHR11913'
	query = '*:*'
	print("Querying Panther collection")
	results = panther_solr.search(query, fl='id,gene_ids', sort='id asc', rows=1000000)
	print(f"Total Panther docs: {len(results)}")

	with tqdm(total=len(results), desc="Processing Panther docs", unit="doc") as pbar:
		for i, doc in enumerate(results):
			panther_id = doc.get('id')
			pbar.set_description(f"Panther ID: {panther_id}")
			gene_ids = doc.get('gene_ids', [])
			updated_gene_ids, solr_changed = update_gene_ids_in_list(gene_ids, agi_locus_mapping)
			s3_changed = update_s3_json_gene_ids(panther_id, agi_locus_mapping)
			if solr_changed or s3_changed:
				if solr_changed:
					# Debug: Check for problematic gene_ids
					for gene_id in updated_gene_ids:
						if isinstance(gene_id, str) and ('"' in gene_id or '\\' in gene_id):
							print(f"WARNING: Potentially problematic gene_id in {panther_id}: {gene_id}")
					
					update_doc = {
						"id": panther_id,
						"gene_ids": {"set": updated_gene_ids}
					}
					try:
						panther_solr.add([update_doc])
						panther_solr.commit()
						# print(f"Updated gene_ids in Solr for {panther_id}")
					except Exception as e:
						print(f"[ERROR] Failed to update Solr for {panther_id}: {e}")
						print(f"Gene IDs causing issue: {updated_gene_ids}")
						# Continue processing other documents
						continue
				# if s3_changed:
				# 	# print(f"Updated gene_ids in S3 for {panther_id}")
			pbar.update(1)

def check_s3_access():
	"""Check if AWS credentials are valid and have permission to list the S3 bucket. Returns True if access is OK, False otherwise."""
	S3_BUCKET = os.getenv('S3_BUCKET', 'phg-panther-data-19')
	s3 = get_boto3_client('s3')
	try:
		response = s3.list_objects_v2(Bucket=S3_BUCKET, MaxKeys=1)
		print(f"[SUCCESS] Able to list objects in bucket '{S3_BUCKET}'. Your credentials are valid and have list permission.")
		if 'Contents' in response:
			print("Sample object(s):", [obj['Key'] for obj in response['Contents']])
		else:
			print("Bucket is empty or you do not have permission to list contents.")
		return True
	except Exception as e:
		print(f"[ERROR] S3 access check failed: {e}")
		return False

def main():
	if not check_s3_access():
		sys.exit(1)
	agi_locus_mapping = get_agi_locus_mapping()
	update_panther_gene_ids(agi_locus_mapping)


if __name__ == "__main__":
	main()
