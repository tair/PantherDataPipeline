#!/usr/bin/env python3
"""
Process All MSA Script
Generates MSA files for all Panther tree IDs found in Solr and uploads them to S3.
Also provides validation functionality to compare gene_id counts with MSA sequence counts.

This script:
1. Queries Solr to get all Panther tree IDs
2. Uses PantherMSAGenerator to process each tree ID
3. Shows progress with a progress bar
4. Uploads results to S3 if enabled
5. Validates MSA sequence counts against Solr gene_id counts

Usage:
    python process_all_msa.py
    python process_all_msa.py --upload-s3
    python process_all_msa.py --local-path /custom/path
    python process_all_msa.py --validate-counts
"""

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import pysolr
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# Get project root directory (panther-pipeline/)
current_dir = os.path.dirname(os.path.abspath(__file__))  # pipeline_scripts/
api_scripts_dir = os.path.dirname(current_dir)  # python_api_scripts/
project_root = os.path.dirname(api_scripts_dir)  # panther-pipeline/

# Add parent directory to path for imports
sys.path.insert(0, api_scripts_dir)

# Import the MSA generator from the existing script
from generate_msa_file import PantherMSAGenerator, MSA_LOCAL_PATH
from services.s3_service import S3Service

# Load environment variables from project root
load_dotenv(os.path.join(project_root, '.env'), override=True)

# Solr Configuration
SOLR_HOST = os.getenv('SOLR_HOST', 'http://localhost:8983')
PANTHER_COLLECTION = os.getenv('PANTHER_COLLECTION', 'panther')
PANTHER_SOLR_URL = f"{SOLR_HOST}/solr/{PANTHER_COLLECTION}"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(api_scripts_dir, 'logs', 'process_all_msa.log'), encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class PantherMSABatchProcessor:
    """
    Batch processor for generating MSA files for all Panther trees in Solr
    """
    
    def __init__(self, local_path: str = MSA_LOCAL_PATH, upload_s3: bool = False):
        self.local_path = local_path
        self.upload_s3 = upload_s3
        self.solr_client = None
        self.msa_generator = None
        
        # Initialize Solr client
        self._initialize_solr_client()
        
        # Initialize MSA generator
        self._initialize_msa_generator()
    
    def _initialize_solr_client(self):
        """Initialize Solr client"""
        try:
            self.solr_client = pysolr.Solr(PANTHER_SOLR_URL, timeout=60)
            logger.info(f"Solr client initialized: {PANTHER_SOLR_URL}")
        except Exception as e:
            logger.error(f"Failed to initialize Solr client: {str(e)}")
            raise
    
    def _initialize_msa_generator(self):
        """Initialize MSA generator"""
        try:
            self.msa_generator = PantherMSAGenerator(
                local_path=self.local_path,
                upload_s3=self.upload_s3
            )
            logger.info("MSA generator initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize MSA generator: {str(e)}")
            raise
    
    def get_all_panther_tree_ids(self) -> list:
        """
        Query Solr to get all Panther tree IDs
        Based on generate_all_panther_csvs() in generate_panther_csvs.py
        
        Returns:
            List of Panther tree IDs
        """
        try:
            logger.info("Querying Panther collection for all tree IDs...")
            
            # Query all documents, get only the ID field
            query = '*:*'
            results = self.solr_client.search(query, fl='id', sort='id asc', rows=100000)
            
            # Extract tree IDs
            tree_ids = [doc.get('id') for doc in results if doc.get('id')]
            
            logger.info(f"Found {len(tree_ids)} Panther trees in Solr")
            return tree_ids
            
        except Exception as e:
            logger.error(f"Error querying Solr for tree IDs: {str(e)}")
            return []
    
    def process_all_msa_files(self) -> Dict[str, Any]:
        """
        Process MSA files for all Panther trees found in Solr
        
        Returns:
            Dictionary with processing statistics
        """
        try:
            # Get all tree IDs from Solr
            tree_ids = self.get_all_panther_tree_ids()
            
            if not tree_ids:
                logger.error("No tree IDs found in Solr")
                return {'total': 0, 'successful': 0, 'failed': 0, 'failed_ids': []}
            
            logger.info(f"Starting MSA processing for {len(tree_ids)} trees")
            
            # Process statistics
            results = {
                'total': len(tree_ids),
                'successful': 0,
                'failed': 0,
                'failed_ids': []
            }
            
            # Process each tree with progress bar
            with tqdm(total=len(tree_ids), desc="Processing MSA files", unit="tree") as pbar:
                for tree_id in tree_ids:
                    pbar.set_description(f"Processing {tree_id}")
                    
                    # Process single family using the MSA generator
                    success = self.msa_generator.process_single_family(tree_id)
                    
                    if success:
                        results['successful'] += 1
                        logger.debug(f"Successfully processed MSA for {tree_id}")
                    else:
                        results['failed'] += 1
                        results['failed_ids'].append(tree_id)
                        logger.warning(f"Failed to process MSA for {tree_id}")
                    
                    pbar.update(1)
            
            # Log final results
            logger.info(f"MSA processing complete: {results['successful']}/{results['total']} successful")
            
            if results['failed_ids']:
                logger.warning(f"Failed tree IDs: {', '.join(results['failed_ids'][:10])}")
                if len(results['failed_ids']) > 10:
                    logger.warning(f"... and {len(results['failed_ids']) - 10} more")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in batch MSA processing: {str(e)}")
            return {'total': 0, 'successful': 0, 'failed': 0, 'failed_ids': []}


class MSAValidator:
    """Validates MSA sequence counts against Solr gene_id counts"""
    
    def __init__(self, msa_dir: str = MSA_LOCAL_PATH, logs_dir: str = None):
        self.msa_dir = Path(msa_dir)
        self.logs_dir = Path(logs_dir) if logs_dir else Path(api_scripts_dir) / "logs"
        self.solr_client = pysolr.Solr(PANTHER_SOLR_URL, timeout=60)
        
        # Ensure directories exist
        self.logs_dir.mkdir(exist_ok=True)
        
        logger.info(f"MSA directory: {self.msa_dir}")
        logger.info(f"Logs directory: {self.logs_dir}")
        logger.info(f"Solr URL: {PANTHER_SOLR_URL}")
    
    def get_solr_persistent_ids(self) -> Dict[str, List[str]]:
        """
        Query Solr to get persistent_ids for all panther trees
        
        Returns:
            Dictionary mapping panther_id -> list of persistent_ids
        """
        try:
            logger.info("Querying Solr for persistent_ids...")
            
            # Query all documents to get their IDs and persistent_ids
            query = '*:*'
            results = self.solr_client.search(query, fl='id,persistent_ids', sort='id asc', rows=1000000)
            
            persistent_ids_data = {}
            logger.info(f"Processing {len(results)} Solr documents...")
            
            with tqdm(total=len(results), desc="Processing Solr docs", unit="doc") as pbar:
                for doc in results:
                    panther_id = doc.get('id')
                    persistent_ids = doc.get('persistent_ids', [])
                    
                    if panther_id:
                        # Ensure persistent_ids is a list
                        if isinstance(persistent_ids, list):
                            persistent_ids_list = persistent_ids
                        elif isinstance(persistent_ids, str):
                            # Single persistent_id as string
                            persistent_ids_list = [persistent_ids]
                        else:
                            persistent_ids_list = []
                        
                        persistent_ids_data[panther_id] = persistent_ids_list
                    
                    pbar.update(1)
            
            logger.info(f"Retrieved persistent_ids for {len(persistent_ids_data)} panther trees")
            return persistent_ids_data
            
        except Exception as e:
            logger.error(f"Error querying Solr: {str(e)}")
            return {}
    
    def get_msa_persistent_ids(self) -> Dict[str, List[str]]:
        """
        Extract persistent_ids from local MSA JSON files
        
        Returns:
            Dictionary mapping panther_id -> list of persistent_ids from sequences
        """
        try:
            logger.info(f"Scanning MSA directory: {self.msa_dir}")
            
            # Find all JSON files in MSA directory
            json_files = list(self.msa_dir.glob("PTHR*.json"))
            logger.info(f"Found {len(json_files)} MSA JSON files")
            
            msa_persistent_ids = {}
            
            with tqdm(total=len(json_files), desc="Processing MSA files", unit="file") as pbar:
                for json_file in json_files:
                    # Extract panther_id from filename (e.g., PTHR10000.json -> PTHR10000)
                    panther_id = json_file.stem
                    
                    try:
                        # Read and parse JSON file
                        with open(json_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        # Extract persistent_ids from MSA data
                        persistent_ids = self._extract_persistent_ids_from_msa_data(data)
                        msa_persistent_ids[panther_id] = persistent_ids
                        
                    except Exception as e:
                        logger.warning(f"Error processing {json_file}: {str(e)}")
                        msa_persistent_ids[panther_id] = []
                    
                    pbar.update(1)
            
            logger.info(f"Retrieved persistent_ids for {len(msa_persistent_ids)} MSA files")
            return msa_persistent_ids
            
        except Exception as e:
            logger.error(f"Error scanning MSA directory: {str(e)}")
            return {}
    
    def _extract_persistent_ids_from_msa_data(self, msa_data: dict) -> List[str]:
        """
        Extract persistent_ids from MSA JSON data structure
        
        Args:
            msa_data: Parsed MSA JSON data
            
        Returns:
            List of persistent_ids found in sequences
        """
        try:
            # Navigate through the JSON structure: familyNames[0].msa_data
            family_names = msa_data.get('familyNames', [])
            if not family_names:
                return []
            
            first_family = family_names[0]
            msa_data_str = first_family.get('msa_data', '')
            
            if not msa_data_str:
                return []
            
            # Parse the msa_data string as JSON
            try:
                msa_json = json.loads(msa_data_str)
            except json.JSONDecodeError:
                logger.warning("Could not parse msa_data as JSON")
                return []
            
            # Navigate to sequence_info: search.MSA_list.sequence_info
            search_data = msa_json.get('search', {})
            msa_list = search_data.get('MSA_list', {})
            sequence_info = msa_list.get('sequence_info', [])
            
            # Extract persistent_ids from each sequence
            persistent_ids = []
            for seq in sequence_info:
                persistent_id = seq.get('persistent_id')
                if persistent_id:
                    persistent_ids.append(persistent_id)
            
            return persistent_ids
            
        except Exception as e:
            logger.warning(f"Error extracting persistent_ids from MSA data: {str(e)}")
            return []
    
    def validate_counts(self, output_csv: Optional[str] = None) -> Tuple[int, int, List[Dict]]:
        """
        Compare Solr persistent_ids with MSA sequence persistent_ids
        
        Args:
            output_csv: Optional path to save validation errors as CSV
            
        Returns:
            Tuple of (total_compared, mismatches_found, mismatch_details)
        """
        logger.info("Starting MSA persistent_id validation...")
        
        # Get persistent_ids from both sources
        solr_persistent_ids = self.get_solr_persistent_ids()
        msa_persistent_ids = self.get_msa_persistent_ids()
        
        if not solr_persistent_ids:
            logger.error("No Solr data retrieved. Cannot proceed with validation.")
            return 0, 0, []
        
        if not msa_persistent_ids:
            logger.error("No MSA data retrieved. Cannot proceed with validation.")
            return 0, 0, []
        
        # Find common panther IDs
        common_ids = set(solr_persistent_ids.keys()) & set(msa_persistent_ids.keys())
        logger.info(f"Comparing persistent_ids for {len(common_ids)} common panther IDs")
        
        # Compare persistent_ids and collect mismatches
        mismatches = []
        total_compared = 0
        
        with tqdm(total=len(common_ids), desc="Validating persistent_ids", unit="tree") as pbar:
            for panther_id in sorted(common_ids):
                total_compared += 1
                
                solr_ids = set(solr_persistent_ids[panther_id])
                msa_ids = set(msa_persistent_ids[panther_id])
                
                solr_count = len(solr_ids)
                msa_count = len(msa_ids)
                
                # Check if Solr has fewer persistent_ids than MSA sequences
                count_mismatch = solr_count < msa_count
                
                # Find missing persistent_ids (in Solr but not in MSA)
                missing_in_msa = solr_ids - msa_ids
                
                # Find extra persistent_ids (in MSA but not in Solr)
                extra_in_msa = msa_ids - solr_ids
                
                if count_mismatch or missing_in_msa or extra_in_msa:
                    mismatch = {
                        'panther_id': panther_id,
                        'solr_persistent_id_count': solr_count,
                        'msa_sequence_count': msa_count,
                        'count_mismatch': count_mismatch,
                        'missing_in_msa_count': len(missing_in_msa),
                        'missing_in_msa': list(missing_in_msa),
                        'extra_in_msa_count': len(extra_in_msa),
                        'extra_in_msa': list(extra_in_msa)
                    }
                    mismatches.append(mismatch)
                    
                    # Log detailed mismatch information
                    if count_mismatch:
                        logger.warning(f"Count mismatch for {panther_id}: Solr={solr_count} < MSA={msa_count}")
                    if missing_in_msa:
                        logger.warning(f"Missing in MSA for {panther_id}: {len(missing_in_msa)} persistent_ids not found in sequences")
                    if extra_in_msa:
                        logger.warning(f"Extra in MSA for {panther_id}: {len(extra_in_msa)} persistent_ids not in Solr")
                
                pbar.update(1)
        
        # Log summary
        logger.info(f"Validation complete: {total_compared} trees compared, {len(mismatches)} mismatches found")
        
        # Save mismatches to CSV if requested or if mismatches found
        if output_csv or mismatches:
            csv_path = output_csv or os.path.join(self.logs_dir, 'msa_validation_errors.csv')
            self._save_mismatches_to_csv(mismatches, csv_path)
        
        # Report missing IDs
        solr_only = set(solr_persistent_ids.keys()) - set(msa_persistent_ids.keys())
        msa_only = set(msa_persistent_ids.keys()) - set(solr_persistent_ids.keys())
        
        if solr_only:
            logger.warning(f"{len(solr_only)} panther IDs found in Solr but not in MSA files")
            logger.debug(f"Solr-only IDs: {sorted(list(solr_only))[:10]}...")  # Show first 10
        
        if msa_only:
            logger.warning(f"{len(msa_only)} panther IDs found in MSA files but not in Solr")
            logger.debug(f"MSA-only IDs: {sorted(list(msa_only))[:10]}...")  # Show first 10
        
        return total_compared, len(mismatches), mismatches
    
    def _save_mismatches_to_csv(self, mismatches: List[Dict], csv_path: str):
        """
        Save validation mismatches to CSV file
        
        Args:
            mismatches: List of mismatch dictionaries
            csv_path: Path to save CSV file
        """
        try:
            logger.info(f"Saving {len(mismatches)} mismatches to {csv_path}")
            
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                if mismatches:
                    fieldnames = [
                        'panther_id', 
                        'solr_persistent_id_count', 
                        'msa_sequence_count',
                        'count_mismatch',
                        'missing_in_msa_count',
                        'missing_in_msa',
                        'extra_in_msa_count', 
                        'extra_in_msa'
                    ]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    writer.writeheader()
                    for mismatch in mismatches:
                        # Convert lists to string representation for CSV
                        row = mismatch.copy()
                        row['missing_in_msa'] = '; '.join(row['missing_in_msa']) if row['missing_in_msa'] else ''
                        row['extra_in_msa'] = '; '.join(row['extra_in_msa']) if row['extra_in_msa'] else ''
                        writer.writerow(row)
                else:
                    # Write header even if no mismatches
                    writer = csv.writer(csvfile)
                    writer.writerow([
                        'panther_id', 
                        'solr_persistent_id_count', 
                        'msa_sequence_count',
                        'count_mismatch',
                        'missing_in_msa_count',
                        'missing_in_msa',
                        'extra_in_msa_count', 
                        'extra_in_msa'
                    ])
                    writer.writerow(['# No mismatches found'])
            
            logger.info(f"Mismatches saved to: {csv_path}")
            
        except Exception as e:
            logger.error(f"Error saving mismatches to CSV: {str(e)}")


class MSABatchUploader:
    """Batch uploader for existing local MSA JSON files to S3"""
    
    def __init__(self, msa_dir: str = MSA_LOCAL_PATH):
        self.msa_dir = Path(msa_dir)
        self.s3_service = S3Service()
        
        logger.info(f"MSA directory: {self.msa_dir}")
        
        # Check if S3 service is available
        if not self.s3_service.is_available():
            raise RuntimeError("S3 service not available. Check AWS credentials and configuration.")
    
    def upload_all_msa_files(self) -> Dict[str, Any]:
        """
        Upload all existing MSA JSON files to S3 bucket
        
        Returns:
            Dictionary with upload statistics
        """
        try:
            logger.info(f"Scanning for MSA files in: {self.msa_dir}")
            
            # Find all JSON files in MSA directory
            json_files = list(self.msa_dir.glob("PTHR*.json"))
            
            if not json_files:
                logger.warning("No PTHR*.json files found in MSA directory")
                return {'total': 0, 'successful': 0, 'failed': 0, 'failed_files': []}
            
            logger.info(f"Found {len(json_files)} MSA JSON files to upload")
            
            # Upload statistics
            results = {
                'total': len(json_files),
                'successful': 0,
                'failed': 0,
                'failed_files': []
            }
            
            # Upload each file with progress bar
            with tqdm(total=len(json_files), desc="Uploading MSA files to S3", unit="file") as pbar:
                for json_file in json_files:
                    # Extract family_id from filename (e.g., PTHR10000.json -> PTHR10000)
                    family_id = json_file.stem
                    pbar.set_description(f"Uploading {family_id}")
                    
                    try:
                        # Read JSON file content
                        with open(json_file, 'r', encoding='utf-8') as f:
                            json_content = f.read()
                        
                        # Upload to S3 (will overwrite existing files)
                        upload_success = self.s3_service.upload_msa_json(family_id, json_content)
                        
                        if upload_success:
                            results['successful'] += 1
                            logger.debug(f"Successfully uploaded {family_id}")
                        else:
                            results['failed'] += 1
                            results['failed_files'].append(family_id)
                            logger.error(f"Failed to upload {family_id}")
                            # Stop on first error as requested
                            raise RuntimeError(f"Upload failed for {family_id}, stopping batch upload")
                        
                    except Exception as e:
                        results['failed'] += 1
                        results['failed_files'].append(family_id)
                        logger.error(f"Error uploading {family_id}: {str(e)}")
                        # Stop on first error as requested
                        raise RuntimeError(f"Upload failed for {family_id}: {str(e)}")
                    
                    pbar.update(1)
            
            logger.info(f"Batch upload complete: {results['successful']}/{results['total']} files uploaded successfully")
            return results
            
        except Exception as e:
            logger.error(f"Error in batch MSA upload: {str(e)}")
            # Return current results even if failed
            return results


class FastaValidator:
    """Validates FASTA API responses against Solr gene ID counts"""
    
    def __init__(self, base_url: str = "https://phylogenes-data-sandbox.arabidopsis.org"):
        self.base_url = base_url
        self.solr_client = pysolr.Solr(PANTHER_SOLR_URL, timeout=60)
        self.logs_dir = Path(api_scripts_dir) / "logs"
        
        # Ensure logs directory exists
        self.logs_dir.mkdir(exist_ok=True)
        
        logger.info(f"FASTA API base URL: {self.base_url}")
        logger.info(f"Solr URL: {PANTHER_SOLR_URL}")
    
    def get_fasta_from_api(self, panther_id: str, taxon_ids_to_show: Optional[List[str]] = None) -> Optional[str]:
        """
        Get FASTA document from the phylogenes API using POST request
        
        Args:
            panther_id: Panther tree ID (e.g., PTHR11913)
            taxon_ids_to_show: Optional list of taxon IDs to filter (defaults to empty list)
            
        Returns:
            FASTA content as string, or None if failed
        """
        try:
            fasta_url = f"{self.base_url}/panther/fastadoc/{panther_id}"
            logger.debug(f"Requesting FASTA from: {fasta_url}")
            
            # Prepare POST payload
            payload = {
                "taxonIdsToShow": taxon_ids_to_show or []
            }
            
            # Set proper headers for JSON content
            headers = {
                "Content-Type": "application/json",
                "Accept": "text/plain"  # Expecting FASTA text response
            }
            
            logger.debug(f"POST payload: {payload}")
            logger.debug(f"Request headers: {headers}")
            
            # Make POST request with JSON payload
            response = requests.post(fasta_url, json=payload, headers=headers, timeout=30)
            
            logger.debug(f"Response status code: {response.status_code}")
            logger.debug(f"Response headers: {dict(response.headers)}")
            
            response.raise_for_status()
            
            fasta_content = response.text
            logger.debug(f"Retrieved FASTA content for {panther_id}: {len(fasta_content)} characters")
            
            return fasta_content
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get FASTA for {panther_id}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting FASTA for {panther_id}: {str(e)}")
            return None
    
    def count_fasta_sequences(self, fasta_content: str) -> int:
        """
        Count the number of sequences in FASTA content
        
        Args:
            fasta_content: FASTA format string
            
        Returns:
            Number of sequences (headers starting with '>')
        """
        try:
            if not fasta_content:
                return 0
            
            # Count lines starting with '>'
            sequence_count = len([line for line in fasta_content.split('\n') if line.strip().startswith('>')])
            logger.debug(f"Counted {sequence_count} sequences in FASTA content")
            
            return sequence_count
            
        except Exception as e:
            logger.error(f"Error counting FASTA sequences: {str(e)}")
            return 0
    
    def get_solr_gene_ids(self, panther_id: str) -> List[str]:
        """
        Get gene IDs from Solr for a specific panther tree
        
        Args:
            panther_id: Panther tree ID
            
        Returns:
            List of gene_ids from Solr
        """
        try:
            logger.debug(f"Querying Solr for gene IDs of {panther_id}")
            
            # Query Solr for this specific panther ID
            query = f'id:"{panther_id}"'
            results = self.solr_client.search(query, fl='gene_ids', rows=1)
            
            if not results:
                logger.warning(f"No Solr document found for {panther_id}")
                return []
            
            doc = list(results)[0]
            gene_ids_field = doc.get('gene_ids', [])
            
            # Ensure gene_ids is a list
            if isinstance(gene_ids_field, list):
                gene_ids = gene_ids_field
            elif isinstance(gene_ids_field, str):
                gene_ids = [gene_ids_field]
            else:
                gene_ids = []
            
            logger.debug(f"Found {len(gene_ids)} gene IDs in Solr for {panther_id}")
            return gene_ids
            
        except Exception as e:
            logger.error(f"Error querying Solr for {panther_id}: {str(e)}")
            return []
    
    def validate_fasta_for_panther_id(self, panther_id: str, taxon_ids_to_show: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Validate FASTA API response against Solr gene ID count for a single panther ID
        
        Args:
            panther_id: Panther tree ID to validate
            taxon_ids_to_show: Optional list of taxon IDs to filter (defaults to empty list)
            
        Returns:
            Dictionary with validation results
        """
        try:
            logger.info(f"Validating FASTA for {panther_id}")
            
            # Get FASTA from API
            fasta_content = self.get_fasta_from_api(panther_id, taxon_ids_to_show)
            if fasta_content is None:
                return {
                    'panther_id': panther_id,
                    'is_valid': False,
                    'error': 'Failed to retrieve FASTA from API',
                    'fasta_sequence_count': 0,
                    'solr_gene_id_count': 0
                }
            
            # Count FASTA sequences
            fasta_count = self.count_fasta_sequences(fasta_content)
            
            # Get Solr gene IDs
            solr_gene_ids = self.get_solr_gene_ids(panther_id)
            solr_count = len(solr_gene_ids)
            
            # Compare counts
            is_valid = fasta_count == solr_count
            
            result = {
                'panther_id': panther_id,
                'is_valid': is_valid,
                'fasta_sequence_count': fasta_count,
                'solr_gene_id_count': solr_count,
                'count_difference': fasta_count - solr_count
            }
            
            if is_valid:
                logger.info(f"✓ {panther_id}: FASTA sequences ({fasta_count}) match Solr gene IDs ({solr_count})")
            else:
                logger.warning(f"✗ {panther_id}: FASTA sequences ({fasta_count}) != Solr gene IDs ({solr_count}), diff: {result['count_difference']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error validating FASTA for {panther_id}: {str(e)}")
            return {
                'panther_id': panther_id,
                'is_valid': False,
                'error': str(e),
                'fasta_sequence_count': 0,
                'solr_gene_id_count': 0
            }
    
    def validate_multiple_panther_ids(self, panther_ids: List[str], output_csv: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate FASTA API responses for multiple panther IDs
        
        Args:
            panther_ids: List of panther tree IDs to validate
            output_csv: Optional path to save validation results as CSV
            
        Returns:
            Dictionary with validation statistics and results
        """
        try:
            logger.info(f"Starting FASTA validation for {len(panther_ids)} panther IDs")
            
            results = []
            valid_count = 0
            invalid_count = 0
            error_count = 0
            
            with tqdm(total=len(panther_ids), desc="Validating FASTA responses", unit="tree") as pbar:
                for panther_id in panther_ids:
                    pbar.set_description(f"Validating {panther_id}")
                    
                    result = self.validate_fasta_for_panther_id(panther_id)
                    results.append(result)
                    
                    if result.get('error'):
                        error_count += 1
                    elif result['is_valid']:
                        valid_count += 1
                    else:
                        invalid_count += 1
                    
                    pbar.update(1)
            
            # Save results to CSV if requested or if there are validation issues
            if output_csv or invalid_count > 0 or error_count > 0:
                csv_path = output_csv or os.path.join(self.logs_dir, 'fasta_validation_results.csv')
                self._save_results_to_csv(results, csv_path)
            
            summary = {
                'total_validated': len(panther_ids),
                'valid_count': valid_count,
                'invalid_count': invalid_count,
                'error_count': error_count,
                'results': results
            }
            
            logger.info(f"FASTA validation complete: {valid_count} valid, {invalid_count} invalid, {error_count} errors")
            return summary
            
        except Exception as e:
            logger.error(f"Error in batch FASTA validation: {str(e)}")
            return {
                'total_validated': 0,
                'valid_count': 0,
                'invalid_count': 0,
                'error_count': 0,
                'results': []
            }
    
    def _save_results_to_csv(self, results: List[Dict], csv_path: str):
        """
        Save validation results to CSV file
        
        Args:
            results: List of validation result dictionaries
            csv_path: Path to save CSV file
        """
        try:
            logger.info(f"Saving {len(results)} validation results to {csv_path}")
            
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                if results:
                    fieldnames = [
                        'panther_id',
                        'is_valid',
                        'fasta_sequence_count',
                        'solr_gene_id_count',
                        'count_difference',
                        'error'
                    ]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    writer.writeheader()
                    for result in results:
                        # Ensure all fields are present
                        row = {field: result.get(field, '') for field in fieldnames}
                        writer.writerow(row)
                else:
                    # Write header even if no results
                    writer = csv.writer(csvfile)
                    writer.writerow([
                        'panther_id',
                        'is_valid',
                        'fasta_sequence_count',
                        'solr_gene_id_count',
                        'count_difference',
                        'error'
                    ])
                    writer.writerow(['# No validation results'])
            
            logger.info(f"Validation results saved to: {csv_path}")
            
        except Exception as e:
            logger.error(f"Error saving validation results to CSV: {str(e)}")


#python process_all_msa.py --validate-counts --output-csv validation_errors.csv
#python process_all_msa.py --upload-existing-s3
#python process_all_msa.py --validate-fasta PTHR11913
#python process_all_msa.py --validate-fasta-batch --output-csv fasta_validation.csv
def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(
        description='Process MSA files for all Panther trees in Solr, validate counts, and upload to S3',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s --upload-s3
  %(prog)s --local-path /custom/path --upload-s3
  %(prog)s --upload-existing-s3
  %(prog)s --validate-counts
  %(prog)s --validate-counts --output-csv validation_errors.csv
  %(prog)s --validate-fasta PTHR11913
  %(prog)s --validate-fasta-batch
  %(prog)s --validate-fasta-batch --output-csv fasta_validation.csv
  %(prog)s --verbose
        """
    )
    
    # Processing options
    parser.add_argument('--upload-s3', action='store_true', 
                       help='Upload MSA files to S3 bucket')
    parser.add_argument('--upload-existing-s3', action='store_true',
                       help='Upload existing local MSA JSON files to S3 bucket')
    parser.add_argument('--local-path', type=str, default=MSA_LOCAL_PATH,
                       help=f'Local path for MSA files (default: {MSA_LOCAL_PATH})')
    
    # Validation options
    parser.add_argument('--validate-counts', action='store_true',
                       help='Validate MSA sequence persistent_ids against Solr persistent_ids')
    parser.add_argument('--validate-fasta', type=str, metavar='PANTHER_ID',
                       help='Validate FASTA API response for a single panther ID (e.g., PTHR11913)')
    parser.add_argument('--validate-fasta-batch', action='store_true',
                       help='Validate FASTA API responses for all panther IDs in Solr')
    parser.add_argument('--output-csv', type=str,
                       help='Path to save validation errors CSV (default: logs/validation_errors.csv)')
    
    # Utility options
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Handle single FASTA validation mode
        if args.validate_fasta:
            logger.info(f"Running FASTA validation for {args.validate_fasta}...")
            fasta_validator = FastaValidator()
            result = fasta_validator.validate_fasta_for_panther_id(args.validate_fasta)
            
            # Print validation result
            logger.info("="*60)
            logger.info("FASTA VALIDATION RESULT")
            logger.info("="*60)
            logger.info(f"Panther ID: {result['panther_id']}")
            
            if result.get('error'):
                logger.error(f"Error: {result['error']}")
                sys.exit(1)
            elif result['is_valid']:
                logger.info(f"✓ FASTA sequences ({result['fasta_sequence_count']}) match Solr gene IDs ({result['solr_gene_id_count']})")
                logger.info("[SUCCESS] FASTA validation passed! ✓")
                sys.exit(0)
            else:
                logger.warning(f"✗ FASTA sequences ({result['fasta_sequence_count']}) != Solr gene IDs ({result['solr_gene_id_count']})")
                logger.warning(f"Count difference: {result['count_difference']}")
                sys.exit(1)
        
        # Handle batch FASTA validation mode
        if args.validate_fasta_batch:
            logger.info("Running batch FASTA validation for all panther IDs...")
            
            # Get all panther IDs from Solr
            batch_processor = PantherMSABatchProcessor()
            panther_ids = batch_processor.get_all_panther_tree_ids()
            
            if not panther_ids:
                logger.error("No panther IDs found in Solr")
                sys.exit(1)
            
            # Run validation
            fasta_validator = FastaValidator()
            results = fasta_validator.validate_multiple_panther_ids(panther_ids, args.output_csv)
            
            # Print validation summary
            logger.info("="*60)
            logger.info("BATCH FASTA VALIDATION SUMMARY")
            logger.info("="*60)
            logger.info(f"Total trees validated: {results['total_validated']}")
            logger.info(f"Valid: {results['valid_count']}")
            logger.info(f"Invalid: {results['invalid_count']}")
            logger.info(f"Errors: {results['error_count']}")
            
            if results['invalid_count'] > 0 or results['error_count'] > 0:
                logger.warning(f"Validation issues found - check CSV output in logs directory")
                
                # Show top 5 invalid results
                invalid_results = [r for r in results['results'] if not r['is_valid'] and not r.get('error')]
                if invalid_results:
                    logger.info(f"Top 5 count mismatches:")
                    for i, result in enumerate(invalid_results[:5]):
                        logger.info(f"  {i+1}. {result['panther_id']}: FASTA={result['fasta_sequence_count']}, Solr={result['solr_gene_id_count']}, diff={result['count_difference']}")
                
                sys.exit(1)
            else:
                logger.info("[SUCCESS] All FASTA validations passed! ✓")
                sys.exit(0)
        
        # Handle MSA count validation mode
        if args.validate_counts:
            logger.info("Running MSA count validation...")
            validator = MSAValidator(msa_dir=args.local_path)
            total_compared, mismatches_found, mismatch_details = validator.validate_counts(args.output_csv)
            
            # Print validation summary
            logger.info("="*60)
            logger.info("MSA VALIDATION SUMMARY")
            logger.info("="*60)
            logger.info(f"Total trees compared: {total_compared}")
            logger.info(f"Mismatches found: {mismatches_found}")
            
            if mismatches_found > 0:
                logger.warning(f"Validation errors saved to CSV in logs directory")
                logger.info(f"Top 5 mismatches:")
                for i, mismatch in enumerate(mismatch_details[:5]):
                    panther_id = mismatch['panther_id']
                    solr_count = mismatch['solr_persistent_id_count']
                    msa_count = mismatch['msa_sequence_count']
                    missing_count = mismatch['missing_in_msa_count']
                    extra_count = mismatch['extra_in_msa_count']
                    
                    logger.info(f"  {i+1}. {panther_id}: Solr={solr_count}, MSA={msa_count}")
                    if missing_count > 0:
                        logger.info(f"      Missing in MSA: {missing_count} persistent_ids")
                    if extra_count > 0:
                        logger.info(f"      Extra in MSA: {extra_count} persistent_ids")
                sys.exit(1)  # Exit with error code if mismatches found
            else:
                logger.info("[SUCCESS] All persistent_ids match! ✓")
                sys.exit(0)
        
        # Handle batch S3 upload mode
        if args.upload_existing_s3:
            logger.info("Running batch S3 upload for existing MSA files...")
            uploader = MSABatchUploader(msa_dir=args.local_path)
            results = uploader.upload_all_msa_files()
            
            # Print upload summary
            logger.info("="*60)
            logger.info("S3 BATCH UPLOAD SUMMARY")
            logger.info("="*60)
            logger.info(f"Total files found: {results['total']}")
            logger.info(f"Successfully uploaded: {results['successful']}")
            logger.info(f"Failed uploads: {results['failed']}")
            
            if results['failed'] > 0:
                logger.error(f"Failed files: {', '.join(results['failed_files'])}")
                sys.exit(1)  # Exit with error code if uploads failed
            else:
                logger.info("[SUCCESS] All files uploaded successfully! ✓")
                sys.exit(0)
        
        # Initialize batch processor for MSA generation
        logger.info("Initializing MSA batch processor...")
        processor = PantherMSABatchProcessor(
            local_path=args.local_path,
            upload_s3=args.upload_s3
        )
        
        # Process all MSA files
        logger.info("Starting batch MSA processing...")
        results = processor.process_all_msa_files()
        
        # Print summary
        logger.info("="*60)
        logger.info("BATCH PROCESSING SUMMARY")
        logger.info("="*60)
        logger.info(f"Total trees processed: {results['total']}")
        logger.info(f"Successful: {results['successful']}")
        logger.info(f"Failed: {results['failed']}")
        
        if results['failed_ids']:
            logger.warning(f"Failed tree IDs ({len(results['failed_ids'])}): {', '.join(results['failed_ids'][:5])}")
            if len(results['failed_ids']) > 5:
                logger.warning(f"... and {len(results['failed_ids']) - 5} more")
        
        # Exit with appropriate code
        if results['failed'] == 0:
            logger.info("[SUCCESS] All MSA files processed successfully")
            sys.exit(0)
        elif results['successful'] > 0:
            logger.warning(f"[PARTIAL SUCCESS] {results['successful']} succeeded, {results['failed']} failed")
            sys.exit(0)  # Still consider partial success as success
        else:
            logger.error("[ERROR] All MSA processing failed")
            sys.exit(1)
    
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
