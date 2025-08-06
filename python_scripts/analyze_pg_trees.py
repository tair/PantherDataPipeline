import os
import csv
import pysolr
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv('.env.sandbox')

def generate_pg5_stats():
    """
    Query the Panther collection in Solr and generate pg5_stats.csv
    with total number of trees at the top and list of all IDs
    """
    
    # Configuration from environment variables
    SOLR_HOST = os.getenv('SOLR_HOST', 'http://52.37.99.223:8983')
    PANTHER_COLLECTION = os.getenv('PANTHER_COLLECTION', 'panther')
    PANTHER_SOLR_URL = f"{SOLR_HOST}/solr/{PANTHER_COLLECTION}"
    
    print(f"Connecting to Panther collection: {PANTHER_SOLR_URL}")
    
    try:
        # Initialize Solr client
        panther_solr = pysolr.Solr(PANTHER_SOLR_URL, timeout=60)
        
        # Query all documents to get their IDs and gene_ids
        print("Querying Panther collection for all IDs and gene counts...")
        results = panther_solr.search('*:*', fl='id,gene_ids,publications_count', sort='id asc', rows=1000000)
        
        total_trees = len(results)
        total_genes = 0
        
        print(f"Total number of trees found: {total_trees}")
        print("Calculating total genes...")
        
        # Calculate total genes with progress bar
        with tqdm(total=total_trees, desc="Counting genes", unit="tree") as pbar:
            for doc in results:
                gene_ids = doc.get('gene_ids', [])
                if isinstance(gene_ids, list):
                    total_genes += len(gene_ids)
                elif isinstance(gene_ids, str):
                    # If it's a single gene_id as string
                    total_genes += 1
                pbar.update(1)
        
        print(f"Total number of genes found: {total_genes}")
        
        # Generate CSV file
        csv_filename = 'pg5_stats.csv'
        print(f"Generating {csv_filename}...")
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write total counts at the top
            writer.writerow(['Total Trees', total_trees])
            writer.writerow(['Total Genes', total_genes])
            writer.writerow([])  # Empty row for separation
            
            # Write header for IDs and gene counts
            writer.writerow(['Panther ID', 'Gene Count', 'Publications Count'])
            
            # Write all IDs with their gene counts and progress bar
            with tqdm(total=total_trees, desc="Writing IDs to CSV", unit="id") as pbar:
                for doc in results:
                    panther_id = doc.get('id', '')
                    gene_ids = doc.get('gene_ids', [])
                    publications_count = doc.get('publications_count', [])
                    
                    # Count genes for this specific tree
                    gene_count = 0
                    if isinstance(gene_ids, list):
                        gene_count = len(gene_ids)
                    elif isinstance(gene_ids, str):
                        gene_count = 1
                    
                    # Count total publications for this specific tree
                    pub_count_total = 0
                    if isinstance(publications_count, list):
                        for pub_entry in publications_count:
                            try:
                                if isinstance(pub_entry, str):
                                    import json
                                    pub_data = json.loads(pub_entry)
                                    pub_count_total += pub_data.get('pub_count', 0)
                                elif isinstance(pub_entry, dict):
                                    pub_count_total += pub_entry.get('pub_count', 0)
                            except (json.JSONDecodeError, AttributeError):
                                continue
                    
                    writer.writerow([panther_id, gene_count, pub_count_total])
                    pbar.update(1)
        
        print(f"Successfully generated {csv_filename}")
        print(f"File contains {total_trees} Panther tree IDs and {total_genes} total genes")
        
    except Exception as e:
        print(f"Error occurred: {e}")
        raise

def main():
    """Main function to run the PG5 stats generation"""
    generate_pg5_stats()

if __name__ == "__main__":
    main()
