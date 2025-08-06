import json
from typing import List, Optional, Dict, Tuple
from pysolr import Solr
import csv
import argparse
from tqdm import tqdm
import requests
import pysolr
import os
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env.sandbox')

class Uniprot2PubMapping:
    """Data class for UniProt to Publication mapping."""
    def __init__(self, uniprot_id: str, pub_count: int):
        self.uniprot_id = uniprot_id.lower()
        self.pub_count = pub_count
    
    def to_dict(self):
        return {
            "uniprot_id": self.uniprot_id,
            "pub_count": self.pub_count
        }

class SolrConfig:
    """Configuration class for Solr connections."""
    def __init__(self):
        self.SOLR_HOST = os.getenv('SOLR_HOST', 'http://localhost:8983')
        self.PANTHER_COLLECTION = os.getenv('PANTHER_COLLECTION', 'panther')
        self.PANTHER_SOLR_URL = f"{self.SOLR_HOST}/solr/{self.PANTHER_COLLECTION}"

class PublicationsServerWrapper:
    """Wrapper for fetching publications from UniProt API."""
    def __init__(self, base_url: str = "https://rest.uniprot.org/uniprotkb/stream"):
        self.base_url = base_url
        self.session = requests.Session()
        # Set up session with retries and backoff
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def get_publications_by_uniprot_id(self, uniprot_id: str) -> Optional[List[str]]:
        """
        Fetch publications for a given UniProt ID from UniProt REST API.
        Returns list of PubMed IDs or None if error occurs.
        """
        try:
            params = {
                'fields': 'lit_pubmed_id',
                'format': 'tsv',
                'query': f'accession:{uniprot_id}'
            }
            
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            # Parse TSV response
            lines = response.text.strip().split('\n')
            if len(lines) <= 1:  # Only header or empty
                return []
            
            publications = []
            for line in lines[1:]:  # Skip header
                line = line.strip()
                if line and line != '-':  # Skip empty or null values
                    # Split by semicolon as UniProt may return multiple PubMed IDs
                    pubmed_ids = [pid.strip() for pid in line.split(';') if pid.strip()]
                    publications.extend(pubmed_ids)
            
            return publications
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching publications for {uniprot_id}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error for {uniprot_id}: {e}")
            return None

class SolrManager:
    """Manager class for Solr operations."""
    def __init__(self, config: SolrConfig):
        self.config = config
        self.panther_solr = pysolr.Solr(config.PANTHER_SOLR_URL, timeout=60)

    def get_panther_document(self, panther_id: str) -> Optional[Dict]:
        """Retrieve a single Panther document by ID."""
        query = f'id:{panther_id}'
        results = self.panther_solr.search(query, fl='id,uniprot_ids,publications_count', rows=1)
        return results.docs[0] if results.docs else None

    def get_all_panther_documents(self, query: str = '*:*') -> List[Dict]:
        """Retrieve all Panther documents matching the query."""
        print("Querying Panther collection for publication count update...")
        results = self.panther_solr.search(query, fl='id,uniprot_ids', sort='id asc', rows=1000000)
        print(f"Found {len(results)} Panther documents")
        return results.docs

    def update_panther_document_publications(self, panther_id: str, publications_count_list: List[str]) -> bool:
        """Update publications_count for a single Panther document."""
        try:
            update_doc = {
                "id": panther_id,
                "publications_count": {"set": publications_count_list}
            }
            self.panther_solr.add([update_doc])
            self.panther_solr.commit()
            return True
        except Exception as e:
            print(f"Error updating document {panther_id}: {e}")
            return False

# ============================================================================
# PUBLICATION PROCESSING FUNCTIONS
# ============================================================================

def get_publications_count_for_uniprot_ids(uniprot_ids: List[str], 
                                          publications_server: PublicationsServerWrapper) -> List[str]:
    """
    Fetch publication counts for a list of UniProt IDs.
    Returns a list of JSON strings, each representing a Uniprot2PubMapping object.
    """
    publication_count_list = []
    total_publications = 0
    
    # Add progress bar for processing UniProt IDs
    with tqdm(total=len(uniprot_ids), desc="Fetching publications", unit="uniprot") as pbar:
        for i, uniprot_id in enumerate(uniprot_ids):
            # Add small delay to avoid overwhelming the API
            time.sleep(0.005)
            
            pbar.set_description(f"Fetching publications: {uniprot_id}")
            
            publications = publications_server.get_publications_by_uniprot_id(uniprot_id)
            
            if publications is None:
                # Error occurred, skip this uniprot_id
                pbar.update(1)
                continue
            
            pub_count = len(publications)
            total_publications += pub_count
            
            # Create Uniprot2PubMapping object
            uni2pub = Uniprot2PubMapping(uniprot_id, pub_count)
            publication_count_list.append(json.dumps(uni2pub.to_dict()))
            
            # Show progress with current stats
            inner_progress_percentage = (i + 1) * 100 // len(uniprot_ids)
            pbar.set_postfix({
                'processed': f'{len(publication_count_list)}/{len(uniprot_ids)}',
                'total_pubs': total_publications,
                'progress': f'{inner_progress_percentage}%'
            })
            
            pbar.update(1)
    
    print(f"Retrieved publication counts for {len(publication_count_list)} UniProt IDs, "
          f"total publications: {total_publications}")
    return publication_count_list

# ============================================================================
# PANTHER COLLECTION UPDATE FUNCTIONS
# ============================================================================

def update_single_panther_publications_count(panther_id: str) -> bool:
    """Update publication counts for a single Panther document."""
    print(f"Updating publication counts for Panther ID: {panther_id}")
    
    config = SolrConfig()
    solr_manager = SolrManager(config)
    publications_server = PublicationsServerWrapper()
    
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
    
    # Get publication counts for all UniProt IDs
    publications_count_list = get_publications_count_for_uniprot_ids(uniprot_ids, publications_server)
    
    # Update the document
    success = solr_manager.update_panther_document_publications(panther_id, publications_count_list)
    
    if success:
        print(f"Successfully updated publication counts for {panther_id}")
        print(f"Added {len(publications_count_list)} publication count entries")
    
    return success

def update_all_panther_publications_count(query: str = '*:*', start_from: int = 0) -> None:
    """Update publication counts for all Panther documents matching the query."""
    print("Updating publication counts for all Panther documents...")
    print(f"PG_PUBLICATIONS_URL: https://rest.uniprot.org/uniprotkb/stream")
    
    config = SolrConfig()
    solr_manager = SolrManager(config)
    publications_server = PublicationsServerWrapper()
    
    # Get all Panther documents
    documents = solr_manager.get_all_panther_documents(query)
    total_docs = len(documents)
    
    # Filter documents to start from a specific index if needed
    if start_from > 0:
        documents = documents[start_from:]
        print(f"Starting from document {start_from + 1} of {total_docs}")
    
    # Process each document with progress bar
    with tqdm(total=len(documents), desc="Processing Panther documents", unit="doc") as pbar:
        for i, doc in enumerate(documents):
            panther_id = doc.get('id')
            current_index = start_from + i + 1
            
            pbar.set_description(f"Processing: {panther_id} ({current_index}/{total_docs})")
            
            uniprot_ids = doc.get('uniprot_ids', [])
            if isinstance(uniprot_ids, str):
                uniprot_ids = [uniprot_ids]
            
            if not uniprot_ids:
                pbar.update(1)
                continue
            
            # Get publication counts for all UniProt IDs in this document
            publications_count_list = get_publications_count_for_uniprot_ids(uniprot_ids, publications_server)
            
            # Update the document
            success = solr_manager.update_panther_document_publications(panther_id, publications_count_list)
            
            if not success:
                print(f"Failed to update document: {panther_id}")
            
            # Show inner progress for UniProt IDs processing
            inner_progress = (len(publications_count_list) * 100) // len(uniprot_ids) if uniprot_ids else 100
            
            # Overall progress
            overall_progress = ((current_index) * 100) // total_docs
            print(f"\rProgress: {overall_progress}% ({current_index}/{total_docs})")
            
            pbar.update(1)
    
    print("Completed updating all Panther documents with publication counts")

def update_panther_publications_from_csv(csv_file_path: str = "solr_indexed_documents.csv") -> bool:
    """Update publication counts for Panther documents listed in a CSV file."""
    print(f"Updating publication counts from CSV file: {csv_file_path}")
    
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
    publications_server = PublicationsServerWrapper()
    
    # Process each document ID with progress bar
    successful_updates = 0
    failed_updates = 0
    
    # Skip first 82 IDs
    document_ids = document_ids[82:]
    print(f"Skipping first 82 IDs, processing remaining {len(document_ids)} IDs")
    
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
                
                # Get publication counts and update document
                publications_count_list = get_publications_count_for_uniprot_ids(uniprot_ids, publications_server)
                
                if solr_manager.update_panther_document_publications(panther_id, publications_count_list):
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

def analyze_uniprot_publications(uniprot_id: str) -> None:
    """Analyze publications for a specific UniProt ID."""
    print(f"\nAnalyzing publications for UniProt ID: {uniprot_id}")
    print("-" * 80)
    
    publications_server = PublicationsServerWrapper()
    publications = publications_server.get_publications_by_uniprot_id(uniprot_id)
    
    if publications is None:
        print(f"Error fetching publications for {uniprot_id}")
        return
    
    print(f"Found {len(publications)} publications for {uniprot_id}")
    
    if publications:
        print("\nPubMed IDs:")
        for i, pubmed_id in enumerate(publications[:10], 1):  # Show first 10
            print(f"{i}. {pubmed_id}")
        
        if len(publications) > 10:
            print(f"... and {len(publications) - 10} more")
    
    # Create Uniprot2PubMapping object
    uni2pub = Uniprot2PubMapping(uniprot_id, len(publications))
    print(f"\nUniprot2PubMapping JSON:")
    print(json.dumps(uni2pub.to_dict(), indent=2))

def test_uniprot_api_connection() -> None:
    """Test the connection to UniProt API."""
    print("Testing UniProt API connection...")
    print("-" * 50)
    
    publications_server = PublicationsServerWrapper()
    
    # Test with a known UniProt ID
    test_ids = ["P60981", "Q9SLA2", "A0A1U8BBT0"]
    
    for uniprot_id in test_ids:
        print(f"\nTesting with UniProt ID: {uniprot_id}")
        publications = publications_server.get_publications_by_uniprot_id(uniprot_id)
        
        if publications is None:
            print(f"❌ Failed to fetch publications for {uniprot_id}")
        else:
            print(f"✅ Successfully fetched {len(publications)} publications for {uniprot_id}")
            if publications:
                print(f"   Sample PubMed IDs: {publications[:3]}")

# ============================================================================
# MAIN FUNCTION
# Test UniProt API connection
# python process_publications.py --action test-api

# # Update all Panther documents
# python process_publications.py --action update-all

# # Update a single document
# python process_publications.py --action update-single --panther-id PTHR10263

# # Update from CSV file (default action)
# python process_publications.py --action update-from-csv --csv-file my_documents.csv

# ============================================================================

def main():
    """Main function to run various publication processing tasks."""
    parser = argparse.ArgumentParser(description='Process publications for Panther Solr collection')
    parser.add_argument('--action', choices=[
        'test-api', 'analyze-uniprot', 'update-single', 'update-all', 'update-from-csv'
    ], default='update-from-csv', help='Action to perform')
    parser.add_argument('--panther-id', type=str, help='Panther ID for single document update')
    parser.add_argument('--uniprot-id', type=str, help='UniProt ID for analysis')
    parser.add_argument('--csv-file', type=str, default='solr_indexed_documents.csv', 
                       help='CSV file containing document IDs to update')
    parser.add_argument('--start-from', type=int, default=0, 
                       help='Starting index for batch processing (0-based)')
    parser.add_argument('--query', type=str, default='*:*', 
                       help='Solr query for filtering documents')
    
    args = parser.parse_args()
    
    if args.action == 'test-api':
        test_uniprot_api_connection()
    
    elif args.action == 'analyze-uniprot':
        if not args.uniprot_id:
            print("Error: --uniprot-id is required for analyze-uniprot action")
            return
        analyze_uniprot_publications(args.uniprot_id)
    
    elif args.action == 'update-single':
        if not args.panther_id:
            print("Error: --panther-id is required for update-single action")
            return
        update_single_panther_publications_count(args.panther_id)
    
    elif args.action == 'update-all':
        update_all_panther_publications_count(args.query, args.start_from)
    
    elif args.action == 'update-from-csv':
        update_panther_publications_from_csv(args.csv_file)

if __name__ == "__main__":
    main() 