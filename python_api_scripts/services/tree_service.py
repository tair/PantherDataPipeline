"""
Tree Service - Handles phylogenetic tree data operations
Based on PhylogenesServerWrapper functionality with real S3 data
"""

import json
import logging
import os
from typing import Dict, List, Optional, Any
from .s3_service import S3Service

logger = logging.getLogger(__name__)

class TreeService:
    """Service for handling phylogenetic tree operations"""
    
    def __init__(self, s3_service: Optional[S3Service] = None):
        self.logger = logging.getLogger(__name__)
        self.s3_service = s3_service or S3Service()
    
    def get_tree_root_annotation(self, tree_id: str, taxon_filter: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Get tree root annotation data from S3 or mock data
        Based on PhylogenesServerWrapper.getPantherTreeRootById()
        """
        try:
            self.logger.info(f"Getting tree root annotation for {tree_id}")
            
            # Require S3 to be available
            if not self.s3_service.is_available():
                raise Exception("S3 service not available. Please configure valid AWS credentials")
            
            # Get real data from S3
            tree_data = self.s3_service.get_tree_data(tree_id)
            if not tree_data:
                raise Exception(f"Tree data not found in S3 for tree ID: {tree_id}")
            
            # Apply taxon filtering if specified
            if taxon_filter:
                tree_data = self._filter_tree_by_taxons(tree_data, taxon_filter)
            
            return tree_data
            
        except Exception as e:
            self.logger.error(f"Error getting tree root annotation for {tree_id}: {str(e)}")
            raise
    
    def map_persistent_ids(self, annotation_node: Dict[str, Any]) -> Dict[str, str]:
        """
        Map persistent IDs to FASTA headers
        Based on PantherLocalWrapper.mapPersistentIds()
        """
        try:
            self.logger.debug("Mapping persistent IDs to FASTA headers")
            
            persistent_id_map = {}
            self._iterate_node_map_persistent_ids(annotation_node, persistent_id_map)
            
            self.logger.info(f"Mapped {len(persistent_id_map)} persistent IDs")
            return persistent_id_map
            
        except Exception as e:
            self.logger.error(f"Error mapping persistent IDs: {str(e)}")
            raise
    
    def _iterate_node_map_persistent_ids(self, node: Dict[str, Any], persistent_id_map: Dict[str, str]) -> None:
        """
        Recursively iterate through tree nodes to map persistent IDs
        Based on iterate_node_mapPersistentIds() in Java
        """
        try:
            if node.get("children") and node["children"].get("annotation_node"):
                for child_node in node["children"]["annotation_node"]:
                    if child_node.get("tree_node_type") == "LEAF":
                        persistent_id = child_node.get("persistent_id")
                        
                        # Extract UniProt ID using exact Java logic: get_uniprotId()
                        # Java: if(node_name == null) return null; return node_name.split("UniProtKB=")[1];
                        node_name = child_node.get("node_name")
                        if node_name is None:
                            uniprot_id = None
                        else:
                            try:
                                uniprot_id = node_name.split("UniProtKB=")[1]
                            except IndexError:
                                uniprot_id = None
                        
                        organism = child_node.get("organism")
                        
                        # Extract gene ID using exact Java logic: get_extractedGeneId()
                        # Java: if(gene_id == null) return null; return gene_id.split(":")[1];
                        gene_id_field = child_node.get("gene_id")
                        if gene_id_field is None:
                            extracted_gene_id = None
                        else:
                            try:
                                extracted_gene_id = gene_id_field.split(":")[1]
                            except IndexError:
                                extracted_gene_id = None
                        
                        # Create FASTA header exactly like Java: get_uniprotId() + "|" + getOrganism() + "|" + get_extractedGeneId()
                        # Note: Java allows null values in the concatenation
                        if uniprot_id is not None and organism is not None and extracted_gene_id is not None:
                            fasta_header = f"{uniprot_id}|{organism}|{extracted_gene_id}"
                            persistent_id_map[persistent_id] = fasta_header
                            self.logger.debug(f"Mapped {persistent_id} -> {fasta_header}")
                        else:
                            self.logger.debug(f"Skipping {persistent_id} - missing required fields: uniprot_id={uniprot_id}, organism={organism}, gene_id={extracted_gene_id}")
                    
                    # Recurse for children
                    if child_node.get("children"):
                        self._iterate_node_map_persistent_ids(child_node, persistent_id_map)
                        
        except Exception as e:
            self.logger.error(f"Error in node iteration: {str(e)}")
            raise
    

    
    def _filter_tree_by_taxons(self, tree_data: Dict[str, Any], taxon_filter: List[str]) -> Dict[str, Any]:
        """
        Filter tree data by specified taxon IDs
        """
        if not tree_data.get("children") or not tree_data["children"].get("annotation_node"):
            return tree_data
        
        filtered_children = []
        for child in tree_data["children"]["annotation_node"]:
            if child.get("tree_node_type") == "LEAF":
                # For leaf nodes, check taxon_id
                if child.get("taxon_id") in taxon_filter:
                    filtered_children.append(child)
            else:
                # For non-leaf nodes, recursively filter
                filtered_child = self._filter_tree_by_taxons(child, taxon_filter)
                if self._has_matching_leaves(filtered_child, taxon_filter):
                    filtered_children.append(filtered_child)
        
        tree_data["children"]["annotation_node"] = filtered_children
        return tree_data
    
    def _has_matching_leaves(self, node: Dict[str, Any], taxon_filter: List[str]) -> bool:
        """
        Check if a node has any leaf descendants matching the taxon filter
        """
        if node.get("tree_node_type") == "LEAF":
            return node.get("taxon_id") in taxon_filter
        
        if not node.get("children") or not node["children"].get("annotation_node"):
            return False
        
        for child in node["children"]["annotation_node"]:
            if self._has_matching_leaves(child, taxon_filter):
                return True
        
        return False
    
    def validate_tree_id(self, tree_id: str) -> bool:
        """Validate tree ID format"""
        try:
            # Basic validation - should start with PTHR
            if not tree_id or not tree_id.startswith('PTHR'):
                return False
            
            # Additional validation could be added here
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating tree ID: {str(e)}")
            return False