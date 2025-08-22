"""
Pruning Service - Handles tree pruning operations
Based on PruningController.getPrunedTree() and PantherServerWrapper.readPrunedPantherTreeById() functionality
"""

import json
import logging
import os
import requests
from typing import Dict, List, Optional, Any
from urllib.parse import quote
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class PruningService:
    """Service for tree pruning operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.base_url = "https://pantherdb.org/services/oai/pantherdb"
        self.treeinfo_url = f"{self.base_url}/treeinfo"
        
        # Load TAIR mapping file for post-processing
        self.locus_mapping = self._load_locus_mapping()
    
    def get_pruned_tree(self, tree_id: str, taxon_ids_to_show: List[str]) -> str:
        """
        Get pruned tree for specific taxon IDs
        Returns JSON string exactly like Java implementation
        """
        try:
            # Convert taxon IDs to integers and then back to comma-separated string
            taxon_array = [int(tid) for tid in taxon_ids_to_show]
            taxon_filters_param = ",".join(map(str, taxon_array))
            
            # Construct URL exactly like Java
            pruned_tree_url = f"{self.treeinfo_url}?family={tree_id}&taxonFltr={taxon_filters_param}"
            
            self.logger.info(f"Pruned tree URL: {pruned_tree_url}")
            
            # Make HTTP request to external Panther API
            response = requests.get(pruned_tree_url, timeout=30)
            response.raise_for_status()
            
            raw_tree_json = response.text
            
            # Process the tree using post-processing logic
            processed_tree = self._process_pruned_tree(raw_tree_json)
            
            return processed_tree
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error calling Panther API: {str(e)}")
            raise Exception(f"Panther API call failed: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error in get_pruned_tree: {str(e)}")
            raise Exception(f"Tree pruning failed: {str(e)}")
    
    def _process_pruned_tree(self, json_string: str) -> str:
        """
        Process pruned tree JSON string by updating values using local mapping files
        Based on PantherETLPipeline.processPrunedTree() logic
        """
        try:
            # Parse the JSON string into PantherData-like structure
            panther_data = json.loads(json_string)
            
            # Navigate to the annotation node
            if not panther_data.get("search") or not panther_data["search"].get("annotation_node"):
                self.logger.warning("No annotation_node found in tree data")
                return json_string  # Return original if no annotation node
            
            root_annotation = panther_data["search"]["annotation_node"]
            
            # Update the tree recursively with TAIR mappings
            updated_root = self._update_panther_tree(root_annotation, self.locus_mapping)
            
            # Update the structure with processed annotation
            panther_data["search"]["annotation_node"] = updated_root
            
            # Convert back to JSON string
            return json.dumps(panther_data)
            
        except Exception as e:
            self.logger.error(f"Error processing pruned tree: {str(e)}")
            return json_string  # Return original JSON if processing fails
    
    def _update_panther_tree(self, node: Dict[str, Any], mapping: Optional[Dict[str, str]]) -> Dict[str, Any]:
        """
        Update each node in the tree recursively
        Based on PantherETLPipeline.updatePantherTree() logic
        """
        try:
            # Update gene_id from mapping for current node
            if node.get("gene_id") and mapping:
                gene_id = node["gene_id"]
                
                # Check if this is a TAIR gene ID (format: "TAIR:...")
                if ":" in gene_id:
                    code = gene_id.split(":")[0]
                    if code == "TAIR":
                        val = gene_id.split(":", 1)[1]  # Split with max splits = 1
                        
                        # Handle nested equals (e.g., "TAIR=AT1G01010")
                        if "=" in val and len(val.split("=")) > 1:
                            val = val.split("=", 1)[1]  # Get the part after first =
                            
                            # Look up in mapping
                            updated_gene_id = mapping.get(val)
                            if updated_gene_id:
                                node["gene_id"] = f"{code}:{updated_gene_id}"
                                self.logger.debug(f"Updated gene_id: {gene_id} -> {node['gene_id']}")
            
            # Recursively update children
            if node.get("children") and node["children"].get("annotation_node"):
                children = node["children"]["annotation_node"]
                if isinstance(children, list):
                    for i, child_node in enumerate(children):
                        children[i] = self._update_panther_tree(child_node, mapping)
            
            return node
            
        except Exception as e:
            self.logger.error(f"Error updating tree node: {str(e)}")
            return node  # Return original node if update fails
    
    def _load_locus_mapping(self) -> Optional[Dict[str, str]]:
        """Load AGI locus ID mapping from CSV file (same as OrthologService)"""
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
            
            self.logger.info(f"Loaded {len(locus_mapping)} locus mappings for pruning")
            return locus_mapping
            
        except Exception as e:
            self.logger.error(f"Error loading locus mapping: {str(e)}")
            return None
    
    def validate_pruning_request(self, tree_id: str, taxon_ids_to_show: List[str]) -> Dict[str, Any]:
        """Validate tree pruning request"""
        validation_result = {"is_valid": True, "errors": []}
        
        try:
            # Check tree_id
            if not tree_id or not tree_id.strip():
                validation_result["is_valid"] = False
                validation_result["errors"].append("Tree ID is required")
            elif not tree_id.startswith('PTHR'):
                validation_result["is_valid"] = False
                validation_result["errors"].append("Tree ID must start with 'PTHR'")
            
            # Check taxon_ids_to_show
            if not isinstance(taxon_ids_to_show, list):
                validation_result["is_valid"] = False
                validation_result["errors"].append("taxonIdsToShow must be an array")
            else:
                # Validate each taxon ID can be converted to integer
                for i, taxon_id in enumerate(taxon_ids_to_show):
                    try:
                        int(taxon_id)
                    except (ValueError, TypeError):
                        validation_result["is_valid"] = False
                        validation_result["errors"].append(f"Invalid taxon ID at index {i}: {taxon_id}")
            
            return validation_result
            
        except Exception as e:
            self.logger.error(f"Error validating pruning request: {str(e)}")
            return {"is_valid": False, "errors": [f"Validation error: {str(e)}"]}