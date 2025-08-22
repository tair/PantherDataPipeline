"""
Ortholog Service - Handles ortholog mapping operations
Based on PruningController.callOrthologApi() functionality
"""

import json
import logging
import os
import sys
import requests
from typing import Dict, List, Optional, Any
from urllib.parse import quote
from dotenv import load_dotenv

# Add parent directory to path for utils import
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from utils.taxon_utils import get_ortholog_target_taxon_ids, get_organism_mapping

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class OrthologService:
    """Service for ortholog mapping operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.base_url = "https://pantherdb.org"
        self.ortho_url = f"{self.base_url}/services/oai/pantherdb/ortholog/matchortho?geneInputList="
        
        # Load mapping files
        self.locus_mapping = self._load_locus_mapping()
        self.org_mapping = get_organism_mapping()  # Use centralized utility
    
    def get_ortholog_mapping(self, uniprot_id: str, query_organism_id: int) -> str:
        """
        Get ortholog mapping for a gene
        Returns JSON array string exactly like Java implementation
        """
        try:
            # Build taxon filter using centralized utility
            taxon_list = get_ortholog_target_taxon_ids(exclude_taxon_id=query_organism_id)
            taxon_filters_param = "%2C".join(map(str, taxon_list))
            
            # Construct URL exactly like Java
            ortholog_url = (f"{self.ortho_url}{uniprot_id}&organism={query_organism_id}"
                          f"&targetOrganism={taxon_filters_param}&orthologType=all")
            
            self.logger.info(f"Ortholog URL: {ortholog_url}")
            
            # Make HTTP request
            response = requests.get(ortholog_url, timeout=30)
            response.raise_for_status()
            
            ortho_data = response.json()
            
            # Process the data using getAllMapped logic
            mapped_results = self._get_all_mapped(ortho_data)
            
            # Convert to JSON array string
            return json.dumps(mapped_results)
            
        except Exception as e:
            self.logger.error(f"Error in get_ortholog_mapping: {str(e)}")
            return "{}"  # Return empty object string like Java
    
    def _get_all_mapped(self, ortho_data: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Process ortholog data using getAllMapped logic from Java
        """
        result_list = []
        
        try:
            # Navigate to search.mapping.mapped
            if not ortho_data.get("search") or not ortho_data["search"].get("mapping"):
                self.logger.info("Search or mapping is null")
                return result_list
            
            mapped_list = ortho_data["search"]["mapping"].get("mapped", [])
            if not mapped_list:
                self.logger.info("No mapped results found")
                return result_list
            
            for mapped_item in mapped_list:
                target_gene = mapped_item.get("target_gene")
                if not target_gene:
                    continue
                
                # Split by | like Java: gene_id.split("\\|")
                parts = target_gene.split("|")
                if len(parts) < 2:
                    continue
                
                organism_code = parts[0]
                organism_name = organism_code
                
                # Map organism code to display name
                if self.org_mapping and organism_code in self.org_mapping:
                    organism_name = self.org_mapping[organism_code]
                
                extracted_gene_id = parts[1]
                
                # Process gene ID like Java
                gene_parts = extracted_gene_id.split("=", 1)  # Split with limit 2 in Java
                if len(gene_parts) < 2:
                    continue
                
                code = gene_parts[0]
                if code == "TAIR" and self.locus_mapping:
                    val = gene_parts[1]
                    # Handle nested = like Java
                    if "=" in val:
                        val = val.split("=", 1)[1]
                    
                    # Look up in locus mapping
                    updated_gene_id = self.locus_mapping.get(val)
                    if updated_gene_id:
                        extracted_gene_id = updated_gene_id
                    else:
                        extracted_gene_id = val
                else:
                    extracted_gene_id = gene_parts[1]
                
                # Extract UniProt ID like Java
                uniprot_id = ""
                if "UniProtKB=" in target_gene:
                    uniprot_id = target_gene.split("UniProtKB=")[1]
                
                # Create result object
                result_obj = {
                    "gene_id": extracted_gene_id,
                    "organism": organism_name,
                    "uniprot_id": uniprot_id,
                    "ortholog": mapped_item.get("ortholog", "")
                }
                
                result_list.append(result_obj)
            
            self.logger.info(f"Processed {len(result_list)} ortholog mappings")
            return result_list
            
        except Exception as e:
            self.logger.error(f"Error processing mapped data: {str(e)}")
            return result_list
    
    def _load_locus_mapping(self) -> Optional[Dict[str, str]]:
        """Load AGI locus ID mapping from CSV file"""
        try:
            # Get resources path from environment
            resources_path = os.getenv('RESOURCES_PATH')
            if not resources_path:
                self.logger.error("RESOURCES_PATH environment variable not set")
                return None
            
            mapping_file = os.path.join(resources_path, "AGI_locusId_mapping_20200410.csv")
            
            if not os.path.exists(mapping_file):
                self.logger.error(f"Locus mapping file not found: {mapping_file}")
                return None
            
            locus_mapping = {}
            with open(mapping_file, 'r') as f:
                # Skip header
                next(f)
                for line in f:
                    line = line.strip()
                    if line:
                        # Parse like Java: space-delimited, then split by comma
                        parts = line.split(',')
                        if len(parts) >= 2:
                            agi_id = parts[0]
                            locus_id = parts[1]
                            locus_mapping[locus_id] = agi_id
            
            self.logger.info(f"Loaded {len(locus_mapping)} locus mappings")
            return locus_mapping
            
        except Exception as e:
            self.logger.error(f"Error loading locus mapping: {str(e)}")
            return None
    

    
    def validate_ortholog_request(self, uniprot_id: str, query_organism_id: str) -> Dict[str, Any]:
        """Validate ortholog mapping request"""
        validation_result = {"is_valid": True, "errors": []}
        
        try:
            # Check uniprot_id
            if not uniprot_id or not uniprot_id.strip():
                validation_result["is_valid"] = False
                validation_result["errors"].append("UniProt ID is required")
            
            # Check query_organism_id
            if not query_organism_id or not query_organism_id.strip():
                validation_result["is_valid"] = False
                validation_result["errors"].append("Query organism ID is required")
            else:
                try:
                    int(query_organism_id)
                except ValueError:
                    validation_result["is_valid"] = False
                    validation_result["errors"].append("Query organism ID must be an integer")
            
            return validation_result
            
        except Exception as e:
            self.logger.error(f"Error validating ortholog request: {str(e)}")
            return {"is_valid": False, "errors": [f"Validation error: {str(e)}"]}