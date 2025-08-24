#!/usr/bin/env python3
"""
Process All MSA Script
Generates MSA files for all Panther tree IDs found in Solr and uploads them to S3.

This script:
1. Queries Solr to get all Panther tree IDs
2. Uses PantherMSAGenerator to process each tree ID
3. Shows progress with a progress bar
4. Uploads results to S3 if enabled

Usage:
    python process_all_msa.py
    python process_all_msa.py --upload-s3
    python process_all_msa.py --local-path /custom/path
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Any
import pysolr
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


def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(
        description='Process MSA files for all Panther trees in Solr',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s --upload-s3
  %(prog)s --local-path /custom/path --upload-s3
  %(prog)s --verbose
        """
    )
    
    # Processing options
    parser.add_argument('--upload-s3', action='store_true', 
                       help='Upload MSA files to S3 bucket')
    parser.add_argument('--local-path', type=str, default=MSA_LOCAL_PATH,
                       help=f'Local path for MSA files (default: {MSA_LOCAL_PATH})')
    
    # Utility options
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Initialize batch processor
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
