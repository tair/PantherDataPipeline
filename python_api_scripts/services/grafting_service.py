"""
Grafting Service - Handles sequence grafting operations
Based on PruningController.callGraftingApi() and getPrunedAndGraftedTree() functionality
"""

import json
import logging
import os
import requests
from typing import Dict, List, Optional, Any
from urllib.parse import quote

logger = logging.getLogger(__name__)

class GraftingService:
    """Service for sequence grafting operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.base_url = "https://pantherdb.org"
        self.graft_url = f"{self.base_url}/services/oai/pantherdb/graftsequence"
        
        # Panther 17.0 taxon filters (from Java code)
        self.taxon_filters_arr = [
            13333, 3702, 15368, 51351, 3055, 2711, 3659, 4155, 3847, 3635, 4232, 112509, 3880, 214687, 4097, 39947,
            105231, 3197, 3218, 3694, 3760, 3988, 4555, 4081, 4558, 3641, 4565, 29760, 4577, 29655, 3708, 4072, 71139,
            51240, 4236, 3983, 4432, 88036, 4113, 3562, 6239, 7955, 44689, 7227, 83333, 9606, 10090, 10116, 559292,
            284812
        ]
    
    def validate_grafting_request(self, sequence: str) -> Dict[str, Any]:
        """
        Validate grafting request parameters
        """
        errors = []
        
        if not sequence or not sequence.strip():
            errors.append("Sequence is required")
        elif len(sequence.strip()) < 10:
            errors.append("Sequence must be at least 10 amino acids long")
        else:
            # Basic protein sequence validation
            valid_amino_acids = set('ACDEFGHIKLMNPQRSTVWY')
            sequence_upper = sequence.strip().upper()
            invalid_chars = set(sequence_upper) - valid_amino_acids
            if invalid_chars:
                errors.append(f"Invalid amino acid characters: {', '.join(invalid_chars)}")
        
        return {
            'is_valid': len(errors) == 0,
            'errors': errors
        }
    
    def call_grafting_api(self, sequence: str, taxon_filters: Optional[List[int]] = None) -> str:
        """
        Call the external Panther grafting API
        Mirrors Java callGraftingApi() method exactly
        
        Args:
            sequence: Protein sequence to graft
            taxon_filters: Optional list of taxon IDs to filter by
            
        Returns:
            JSON string response from Panther API
        """
        try:
            # Use default taxon filters if none provided
            if taxon_filters is None:
                taxon_filters = self.taxon_filters_arr
            
            # Convert taxon filters to comma-separated string
            taxon_filters_param = ",".join(map(str, taxon_filters))
            
            # Construct URL exactly like Java implementation
            grafting_url = f"{self.graft_url}?sequence={quote(sequence)}&taxonFltr={taxon_filters_param}"
            
            self.logger.info(f"Got Grafting Request {grafting_url}")
            
            # Make HTTP request to external Panther API
            response = requests.get(grafting_url, timeout=60)  # Grafting can take longer
            response.raise_for_status()
            
            json_string = response.text
            
            # Validate that we got valid JSON
            try:
                json.loads(json_string)
            except json.JSONDecodeError:
                raise Exception(f"Invalid JSON response from Panther API: {json_string[:200]}...")
            
            return json_string
            
        except requests.exceptions.Timeout:
            error_msg = "Panther grafting API call timed out"
            self.logger.error(error_msg)
            raise Exception(error_msg)
        except requests.exceptions.RequestException as e:
            error_msg = f"Panther grafting API call failed: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
        except Exception as e:
            error_msg = f"Error calling Panther grafting API: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
    
    def get_grafted_tree(self, sequence: str) -> str:
        """
        Get grafted tree for a sequence using default taxon filters
        Mirrors Java getGrafterTree() method
        
        Args:
            sequence: Protein sequence to graft
            
        Returns:
            JSON string response from Panther API
        """
        return self.call_grafting_api(sequence, self.taxon_filters_arr)
    
    def get_pruned_and_grafted_tree(self, sequence: str, taxon_ids_to_show: List[str]) -> str:
        """
        Get grafted and then pruned tree for specific taxon IDs
        Mirrors Java getPrunedAndGraftedTree() method exactly
        
        Args:
            sequence: Protein sequence to graft
            taxon_ids_to_show: List of taxon IDs to show in pruned tree
            
        Returns:
            JSON string response from Panther API
        """
        try:
            # Convert taxon IDs to integers
            taxon_array = [int(tid) for tid in taxon_ids_to_show]
            
            # Convert to comma-separated string for URL parameter
            taxon_filters_param = ",".join(map(str, taxon_array))
            
            # Construct URL exactly like Java implementation
            grafting_url = f"{self.graft_url}?sequence={quote(sequence)}&taxonFltr={taxon_filters_param}"
            
            self.logger.info(f"Got Grafting Request {grafting_url}")
            
            # Make HTTP request to external Panther API
            response = requests.get(grafting_url, timeout=60)
            response.raise_for_status()
            
            json_string = response.text
            
            # Validate that we got valid JSON
            try:
                json.loads(json_string)
            except json.JSONDecodeError:
                raise Exception(f"Invalid JSON response from Panther API: {json_string[:200]}...")
            
            return json_string
            
        except requests.exceptions.Timeout:
            error_msg = "Panther grafting API call timed out"
            self.logger.error(error_msg)
            raise Exception(error_msg)
        except requests.exceptions.RequestException as e:
            error_msg = f"Panther grafting API call failed: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
        except Exception as e:
            error_msg = f"Error calling Panther grafting API: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
    
    def get_grafting_stats(self, json_response: str) -> Dict[str, Any]:
        """
        Extract statistics from grafting response
        
        Args:
            json_response: JSON response from grafting API
            
        Returns:
            Dictionary with grafting statistics
        """
        try:
            data = json.loads(json_response)
            
            stats = {
                'has_search_data': 'search' in data,
                'has_phyloxml': False,
                'has_msa': False,
                'sequence_count': 0,
                'tree_id': None
            }
            
            if 'search' in data:
                search_data = data['search']
                stats['has_phyloxml'] = 'phyloxml' in search_data and search_data['phyloxml']
                stats['has_msa'] = 'msa' in search_data and search_data['msa']
                stats['tree_id'] = search_data.get('treeId')
                
                if 'sequences' in search_data and isinstance(search_data['sequences'], list):
                    stats['sequence_count'] = len(search_data['sequences'])
            
            return stats
            
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            self.logger.warning(f"Could not parse grafting stats: {str(e)}")
            return {
                'has_search_data': False,
                'has_phyloxml': False,
                'has_msa': False,
                'sequence_count': 0,
                'tree_id': None,
                'parse_error': str(e)
            }