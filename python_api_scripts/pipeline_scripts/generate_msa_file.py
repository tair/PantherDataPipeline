#!/usr/bin/env python3
"""
MSA File Generator Script
Based on PantherETLPipeline.updateOrSaveMSAData() and related Java methods

This script generates MSA (Multiple Sequence Alignment) files for given tree IDs
following the same process as the Java pipeline:
1. Retrieve MSA data from Panther server
2. Save locally in standardized JSON format
3. Optionally upload to S3 bucket

Usage:
    python generate_msa_file.py --tree-id PTHR12345
    python generate_msa_file.py --tree-id PTHR12345 --upload-s3
    python generate_msa_file.py --batch-file tree_ids.txt
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# Get project root directory (panther-pipeline/)
current_dir = os.path.dirname(os.path.abspath(__file__))  # pipeline_scripts/
api_scripts_dir = os.path.dirname(current_dir)  # python_api_scripts/
project_root = os.path.dirname(api_scripts_dir)  # panther-pipeline/

# Add parent directory to path for utils and services import
sys.path.insert(0, api_scripts_dir)
from utils.taxon_utils import get_all_taxon_ids
from services.s3_service import S3Service

# Load environment variables from project root
load_dotenv(os.path.join(project_root, '.env'), override=True)

# Configuration from environment
BASE_DIR = os.getenv('DATA_PATH', r"C:\Users\Documents\panther_storage\resources")
MSA_LOCAL_PATH = os.getenv('MSA_LOCAL_PATH', os.path.join(BASE_DIR, "msa_jsons"))
PANTHER_SERVER_URL = os.getenv('PANTHER_SERVER_URL', 'https://pantherdb.org/services/oai/pantherdb')
MSA_API_URL = f"{PANTHER_SERVER_URL}/familymsa"

# S3 Configuration (handled by S3Service)
# PG_MSA_BUCKET = os.getenv('PG_MSA_BUCKET', 'phg-panther-msa-data-19')

# Taxon filters loaded dynamically from organism_to_display.csv
def get_default_taxon_filters() -> List[int]:
    """Get taxon filters from centralized utility"""
    return get_all_taxon_ids()

# Setup logging with UTF-8 encoding to handle Unicode characters
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(api_scripts_dir, 'logs', 'msa_generator.log'), encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class PantherMSAGenerator:
    """
    MSA Generator class based on Java PantherETLPipeline MSA methods
    """
    
    def __init__(self, local_path: str = MSA_LOCAL_PATH, upload_s3: bool = False):
        self.local_path = Path(local_path)
        self.upload_s3 = upload_s3
        self.s3_service = None
        
        # Create local directory if it doesn't exist
        self.local_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"MSA local path: {self.local_path}")
        
        # Initialize S3 service if upload is enabled
        if self.upload_s3:
            self._initialize_s3_service()
    
    def _initialize_s3_service(self):
        """Initialize S3 service for uploads"""
        try:
            self.s3_service = S3Service()
            
            if not self.s3_service.is_available():
                logger.warning("S3 service not available, S3 upload will be disabled")
                self.upload_s3 = False
                return
            
            # Test S3 connection
            test_result = self.s3_service.test_connection()
            if test_result['status'] == 'success' and test_result.get('msa_bucket_access', False):
                logger.info(f"S3 service initialized successfully. MSA Bucket: {test_result.get('msa_bucket')}")
            else:
                logger.warning("S3 MSA bucket access failed, S3 upload will be disabled")
                self.upload_s3 = False
            
        except Exception as e:
            logger.error(f"Failed to initialize S3 service: {str(e)}")
            self.upload_s3 = False
    
    def read_msa_from_server(self, family_id: str, taxon_filters: Optional[List[int]] = None) -> Optional[str]:
        """
        Retrieve MSA data from Panther server
        Based on PantherServerWrapper.readMsaByIdFromServer()
        
        Args:
            family_id: Panther family ID (e.g., PTHR12345)
            taxon_filters: List of taxon IDs to filter by
            
        Returns:
            Raw MSA data as string, or None if failed
        """
        try:
            if taxon_filters is None:
                taxon_filters = get_default_taxon_filters()
            
            # Convert taxon filters to comma-separated string
            taxon_filter_param = ','.join(map(str, taxon_filters))
            
            # Construct URL (same as Java: BASE_MSA_URL + "?family=" + family_id + "&taxonFltr=" + taxonFiltersParam)
            url = f"{MSA_API_URL}?family={family_id}&taxonFltr={taxon_filter_param}"
            
            logger.debug(f"Fetching MSA data from: {url}")
            
            # Make HTTP request
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            msa_data = response.text
            logger.info(f"Retrieved MSA data for {family_id}: {len(msa_data)} characters")
            
            return msa_data
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch MSA data for {family_id}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching MSA for {family_id}: {str(e)}")
            return None
    
    def save_msa_as_json_file(self, family_id: str, msa_data: str) -> Optional[str]:
        """
        Save MSA data as local JSON file
        Based on PantherLocalWrapper.saveMSADataAsJsonFile()
        
        Args:
            family_id: Panther family ID
            msa_data: Raw MSA data string from Panther server
            
        Returns:
            JSON string that was saved, or None if failed
        """
        try:
            logger.debug(f"Saving MSA data for {family_id}: {len(msa_data)} characters")
            
            # Ensure MSA data is stored as compact JSON (no extra formatting)
            # If msa_data is already JSON, parse and re-serialize it compactly
            try:
                # Try to parse msa_data as JSON to ensure it's valid and compact
                parsed_msa = json.loads(msa_data)
                compact_msa_data = json.dumps(parsed_msa, separators=(',', ':'), ensure_ascii=False)
            except json.JSONDecodeError:
                # If it's not valid JSON, store as-is (might be plain text)
                compact_msa_data = msa_data
            
            # Create JSON structure (same as Java)
            json_structure = {
                "familyNames": [
                    {
                        "id": family_id,
                        "msa_data": compact_msa_data
                    }
                ]
            }
            
            # Convert to JSON string with formatting for outer structure only
            json_string = json.dumps(json_structure, ensure_ascii=False, indent=2)
            
            # Save to file
            file_path = self.local_path / f"{family_id}.json"
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(json_string)
            
            logger.info(f"Saved MSA JSON file: {file_path}")
            return json_string
            
        except Exception as e:
            logger.error(f"Failed to save MSA JSON for {family_id}: {str(e)}")
            return None
    
    def upload_json_to_s3(self, family_id: str, json_content: str) -> bool:
        """
        Upload MSA JSON to S3 bucket using S3Service
        
        Args:
            family_id: Panther family ID
            json_content: JSON content to upload
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.s3_service or not self.s3_service.is_available():
                logger.warning("S3 service not available, skipping upload")
                return False
            
            return self.s3_service.upload_msa_json(family_id, json_content)
            
        except Exception as e:
            logger.error(f"Failed to upload MSA JSON to S3 for {family_id}: {str(e)}")
            return False
    
    def process_single_family(self, family_id: str) -> bool:
        """
        Process MSA data for a single family
        Based on the main loop in PantherETLPipeline.updateOrSaveMSAData()
        
        Args:
            family_id: Panther family ID to process
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Processing MSA for family: {family_id}")
            
            # Step 1: Retrieve MSA data from server
            msa_data = self.read_msa_from_server(family_id)
            
            if not msa_data or len(msa_data) < 3:
                logger.warning(f"MSA data is empty or too short for {family_id}")
                return False
            
            # Step 2: Save as local JSON file
            json_content = self.save_msa_as_json_file(family_id, msa_data)
            
            if not json_content:
                logger.error(f"Failed to save MSA JSON for {family_id}")
                return False
            
            # Step 3: Upload to S3 if enabled
            if self.upload_s3:
                upload_success = self.upload_json_to_s3(family_id, json_content)
                if not upload_success:
                    logger.warning(f"S3 upload failed for {family_id}, but local file saved")
            
            logger.info(f"Successfully processed MSA for {family_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing MSA for {family_id}: {str(e)}")
            return False
    
    def process_batch_from_file(self, batch_file: str) -> Dict[str, Any]:
        """
        Process multiple families from a file
        
        Args:
            batch_file: Path to file containing family IDs (one per line)
            
        Returns:
            Dictionary with processing statistics
        """
        try:
            # Read family IDs from file
            with open(batch_file, 'r') as f:
                family_ids = [line.strip() for line in f if line.strip()]
            
            logger.info(f"Processing {len(family_ids)} families from {batch_file}")
            
            # Process each family
            results = {
                'total': len(family_ids),
                'successful': 0,
                'failed': 0,
                'failed_ids': []
            }
            
            for family_id in tqdm(family_ids, desc="Processing families"):
                success = self.process_single_family(family_id)
                
                if success:
                    results['successful'] += 1
                else:
                    results['failed'] += 1
                    results['failed_ids'].append(family_id)
            
            logger.info(f"Batch processing complete: {results['successful']}/{results['total']} successful")
            return results
            
        except Exception as e:
            logger.error(f"Error processing batch file {batch_file}: {str(e)}")
            return {'total': 0, 'successful': 0, 'failed': 0, 'failed_ids': []}
    
    def get_local_msa_file(self, family_id: str) -> Optional[str]:
        """
        Get MSA JSON content from local file
        Based on PantherLocalWrapper.getMSAJsonFile()
        
        Args:
            family_id: Panther family ID
            
        Returns:
            JSON content as string, or None if not found
        """
        try:
            file_path = self.local_path / f"{family_id}.json"
            
            if not file_path.exists():
                logger.warning(f"Local MSA file not found: {file_path}")
                return None
            
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            logger.debug(f"Loaded local MSA file: {file_path}")
            return content
            
        except Exception as e:
            logger.error(f"Error reading local MSA file for {family_id}: {str(e)}")
            return None
    
    def validate_msa_format(self, family_id: str) -> bool:
        """
        Validate that the saved MSA file has the correct format (compact JSON in msa_data)
        
        Args:
            family_id: Panther family ID to validate
            
        Returns:
            True if format is correct, False otherwise
        """
        try:
            content = self.get_local_msa_file(family_id)
            if not content:
                return False
            
            # Parse the outer JSON structure
            data = json.loads(content)
            
            # Check structure
            if 'familyNames' not in data or not isinstance(data['familyNames'], list):
                logger.error(f"Invalid structure in {family_id}: missing familyNames array")
                return False
            
            if len(data['familyNames']) == 0:
                logger.error(f"Invalid structure in {family_id}: empty familyNames array")
                return False
            
            family_data = data['familyNames'][0]
            if 'msa_data' not in family_data:
                logger.error(f"Invalid structure in {family_id}: missing msa_data field")
                return False
            
            msa_data = family_data['msa_data']
            
            # Check that msa_data is a compact JSON string (no \n characters)
            if '\\n' in msa_data:
                logger.warning(f"MSA data in {family_id} contains escaped newlines (not compact format)")
                return False
            
            # Try to parse msa_data as JSON to ensure it's valid
            try:
                json.loads(msa_data)
                logger.debug(f"MSA format validation passed for {family_id}")
                return True
            except json.JSONDecodeError:
                logger.warning(f"MSA data in {family_id} is not valid JSON")
                return False
                
        except Exception as e:
            logger.error(f"Error validating MSA format for {family_id}: {str(e)}")
            return False

# python generate_msa_file.py --tree-id PTHR11101 --upload-s3
def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(
        description='Generate MSA files for Panther tree IDs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --tree-id PTHR12345
  %(prog)s --tree-id PTHR12345 --upload-s3
  %(prog)s --batch-file tree_ids.txt --upload-s3
  %(prog)s --tree-id PTHR12345 --local-path /custom/path
  %(prog)s --validate PTHR12345
  %(prog)s --test-s3
        """
    )
    
    # Input options
    input_group = parser.add_mutually_exclusive_group(required=False)
    input_group.add_argument('--tree-id', type=str, help='Single tree ID to process')
    input_group.add_argument('--batch-file', type=str, help='File containing tree IDs (one per line)')
    
    # Processing options
    parser.add_argument('--upload-s3', action='store_true', help='Upload results to S3 bucket')
    parser.add_argument('--local-path', type=str, default=MSA_LOCAL_PATH, 
                       help=f'Local path for MSA files (default: {MSA_LOCAL_PATH})')
    parser.add_argument('--taxon-filters', type=str, 
                       help='Comma-separated taxon IDs to filter by (default: use built-in list)')
    
    # Utility options
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--test-s3', action='store_true', help='Test S3 connection and exit')
    parser.add_argument('--validate', type=str, help='Validate format of existing MSA file by family ID')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Parse taxon filters if provided
    taxon_filters = None
    if args.taxon_filters:
        try:
            taxon_filters = [int(x.strip()) for x in args.taxon_filters.split(',')]
            logger.info(f"Using custom taxon filters: {taxon_filters}")
        except ValueError:
            logger.error("Invalid taxon filters format. Use comma-separated integers.")
            sys.exit(1)
    
    # Initialize MSA generator
    generator = PantherMSAGenerator(
        local_path=args.local_path,
        upload_s3=args.upload_s3
    )
    
         # Test S3 connection if requested
    if args.test_s3:
        if generator.s3_service and generator.s3_service.is_available():
            try:
                test_result = generator.s3_service.test_connection()
                if test_result['status'] == 'success':
                    logger.info(f"[SUCCESS] S3 connection successful. MSA Bucket: {test_result.get('msa_bucket')} (Access: {test_result.get('msa_bucket_access')})")
                else:
                    logger.error(f"[ERROR] S3 connection failed: {test_result.get('message')}")
                    sys.exit(1)
            except Exception as e:
                logger.error(f"[ERROR] S3 connection test failed: {str(e)}")
                sys.exit(1)
        else:
            logger.error("[ERROR] S3 service not initialized or not available")
            sys.exit(1)
        return
    
    # Validate existing file if requested
    if args.validate:
        is_valid = generator.validate_msa_format(args.validate)
        if is_valid:
            logger.info(f"[SUCCESS] MSA file format is correct for {args.validate}")
            sys.exit(0)
        else:
            logger.error(f"[ERROR] MSA file format is incorrect for {args.validate}")
            sys.exit(1)
    
    # Check that at least one action is specified
    if not args.tree_id and not args.batch_file:
        logger.error("Error: Must specify either --tree-id or --batch-file")
        parser.print_help()
        sys.exit(1)
    
    # Process input
    try:
        if args.tree_id:
            # Process single tree ID
            success = generator.process_single_family(args.tree_id)
            if success:
                logger.info(f"[SUCCESS] Successfully processed {args.tree_id}")
                sys.exit(0)
            else:
                logger.error(f"[ERROR] Failed to process {args.tree_id}")
                sys.exit(1)
        
        elif args.batch_file:
            # Process batch file
            if not os.path.exists(args.batch_file):
                logger.error(f"Batch file not found: {args.batch_file}")
                sys.exit(1)
            
            results = generator.process_batch_from_file(args.batch_file)
            
            # Print summary
            logger.info(f"Batch processing summary:")
            logger.info(f"  Total families: {results['total']}")
            logger.info(f"  Successful: {results['successful']}")
            logger.info(f"  Failed: {results['failed']}")
            
            if results['failed_ids']:
                logger.warning(f"Failed IDs: {', '.join(results['failed_ids'])}")
            
            # Exit with appropriate code
            if results['failed'] == 0:
                logger.info("[SUCCESS] All families processed successfully")
                sys.exit(0)
            else:
                logger.warning(f"[WARNING] {results['failed']} families failed")
                sys.exit(1)
    
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
