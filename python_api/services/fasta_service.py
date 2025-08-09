"""
FASTA Service - Handles FASTA generation from tree and MSA data
Based on PhylogenesServerWrapper.get_processed_msa() functionality with real S3 data
"""

import json
import logging
import os
from typing import Dict, List, Optional, Any
from .tree_service import TreeService
from .s3_service import S3Service

logger = logging.getLogger(__name__)

class FastaService:
    """Service for generating FASTA sequences from phylogenetic data"""
    
    def __init__(self, tree_service: TreeService, s3_service: Optional[S3Service] = None):
        self.tree_service = tree_service
        self.s3_service = s3_service or S3Service()
        self.logger = logging.getLogger(__name__)
    
    def generate_fasta_from_tree(self, tree_id: str, taxon_filter: Optional[List[str]] = None) -> str:
        """
        Generate FASTA document from tree data
        Based on PhylogenesServerWrapper.get_processed_msa()
        """
        try:
            self.logger.info(f"Generating FASTA for tree {tree_id}")
            
            # Step 1: Get tree root annotation
            annotation_root = self.tree_service.get_tree_root_annotation(tree_id, taxon_filter)
            
            # Step 2: Map persistent IDs to FASTA headers
            persistent_id_map = self.tree_service.map_persistent_ids(annotation_root)
            
            # Step 3: Get MSA data (mock implementation)
            msa_data = self._get_msa_data(tree_id)
            
            # Step 4: Process MSA and generate FASTA
            fasta_content = self._process_msa_to_fasta(msa_data, persistent_id_map)
            
            self.logger.info(f"Generated FASTA with {len(fasta_content.split('>')) - 1} sequences")
            return fasta_content
            
        except Exception as e:
            self.logger.error(f"Error generating FASTA for tree {tree_id}: {str(e)}")
            raise
    
    def _get_msa_data(self, tree_id: str) -> Dict[str, Any]:
        """
        Get MSA data for tree from S3 or mock data
        Based on PhylogenesServerWrapper.get_processed_msa() S3 access
        """
        try:
            self.logger.debug(f"Getting MSA data for {tree_id}")
            
            # Require S3 to be available
            if not self.s3_service.is_available():
                raise Exception("S3 service not available. Please configure valid AWS credentials")
            
            # Get real MSA data from S3
            msa_data = self.s3_service.get_msa_data(tree_id)
            if not msa_data:
                raise Exception(f"MSA data not found in S3 for tree ID: {tree_id}")
            
            return msa_data
            
        except Exception as e:
            self.logger.error(f"Error getting MSA data for {tree_id}: {str(e)}")
            raise
    

    
    def _process_msa_to_fasta(self, msa_data: Dict[str, Any], persistent_id_map: Dict[str, str]) -> str:
        """
        Process MSA data into FASTA format
        Based on the main loop in PhylogenesServerWrapper.get_processed_msa()
        """
        try:
            self.logger.debug("Processing MSA data to FASTA format")
            
            fasta_content = ""
            sequence_count = 0
            
            # Process family data
            for family_item in msa_data.get("family_data", []):
                msa = family_item.get("msa_data", {})
                sequence_list = msa.get("sequence_list", [])
                
                for seq_info in sequence_list:
                    persistent_id = seq_info.get("persistent_id")
                    sequence = seq_info.get("sequence", "")
                    
                    # Check if this persistent ID should be included
                    if persistent_id in persistent_id_map:
                        sequence_count += 1
                        
                        # Add FASTA header (exact Java format)
                        fasta_header = persistent_id_map[persistent_id]
                        fasta_content += ">" + fasta_header + "\n"
                        
                        # Format sequence exactly like Java: sequence.replaceAll("(.{60})", "$1\n")
                        formatted_sequence = self._format_sequence_java_style(sequence)
                        fasta_content += formatted_sequence
                        fasta_content += "\n"
            
            self.logger.info(f"Processed {sequence_count} sequences into FASTA format")
            return fasta_content
            
        except Exception as e:
            self.logger.error(f"Error processing MSA to FASTA: {str(e)}")
            raise
    
    def _format_sequence_java_style(self, sequence: str) -> str:
        """
        Format sequence exactly like Java: sequence.replaceAll("(.{60})", "$1\n")
        This adds a newline after every 60 characters
        """
        try:
            if not sequence:
                return ""
            
            # Java regex (.{60}) matches exactly 60 characters and replaces with $1\n
            # This is equivalent to adding \n after every 60 chars
            formatted = ""
            for i in range(0, len(sequence), 60):
                chunk = sequence[i:i + 60]
                if len(chunk) == 60:  # Only add newline if we have exactly 60 chars
                    formatted += chunk + "\n"
                else:  # Last chunk (less than 60 chars)
                    formatted += chunk
            
            return formatted
            
        except Exception as e:
            self.logger.error(f"Error formatting sequence: {str(e)}")
            return sequence  # Return original if formatting fails
    
    def validate_fasta_request(self, tree_id: str, taxon_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """Validate FASTA generation request"""
        try:
            validation_result = {"is_valid": True, "errors": []}
            
            # Validate tree ID
            if not self.tree_service.validate_tree_id(tree_id):
                validation_result["is_valid"] = False
                validation_result["errors"].append(f"Invalid tree ID format: {tree_id}")
            
            # Validate taxon IDs if provided
            if taxon_ids:
                for taxon_id in taxon_ids:
                    if not isinstance(taxon_id, str) or not taxon_id.strip():
                        validation_result["is_valid"] = False
                        validation_result["errors"].append(f"Invalid taxon ID: {taxon_id}")
            
            return validation_result
            
        except Exception as e:
            self.logger.error(f"Error validating FASTA request: {str(e)}")
            return {"is_valid": False, "errors": [f"Validation error: {str(e)}"]}
    
    def get_fasta_stats(self, fasta_content: str) -> Dict[str, int]:
        """Get statistics about generated FASTA content"""
        try:
            lines = fasta_content.split('\n')
            sequence_count = len([line for line in lines if line.startswith('>')])
            total_residues = len([char for line in lines if not line.startswith('>') for char in line if char.isalpha()])
            
            return {
                "sequence_count": sequence_count,
                "total_residues": total_residues,
                "total_lines": len(lines)
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating FASTA stats: {str(e)}")
            return {"sequence_count": 0, "total_residues": 0, "total_lines": 0}