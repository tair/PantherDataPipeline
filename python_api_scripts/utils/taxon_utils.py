"""
Taxon Utilities - Centralized loading of taxon IDs and organism mappings
This module provides consistent access to organism data across all scripts and services.
"""

import csv
import logging
import os
from typing import Dict, List, Optional, Set
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class TaxonLoader:
    """Centralized loader for taxon data from organism_to_display.csv"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._organism_data = None
        self._load_organism_data()
    
    def _load_organism_data(self) -> None:
        """Load organism data from CSV file"""
        try:
            # Get resources path from environment
            resources_path = os.getenv('RESOURCES_PATH')
            if not resources_path:
                self.logger.error("RESOURCES_PATH environment variable not set")
                self._organism_data = []
                return
            
            csv_file = os.path.join(resources_path, "organism_to_display.csv")
            
            if not os.path.exists(csv_file):
                self.logger.error(f"Organism CSV file not found: {csv_file}")
                self._organism_data = []
                return
            
            organism_data = []
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Clean up the data
                    organism_data.append({
                        'organism': row.get('Organism', '').strip(),
                        'display_name': row.get('displayName', '').strip(),
                        'common_name': row.get('common name', '').strip(),
                        'abbrev_code': row.get('Abbrev name code', '').strip(),
                        'taxon_id': int(row.get('taxonID', 0)) if row.get('taxonID', '').strip().isdigit() else 0,
                        'is_plant': row.get('Is plant?', '').strip().lower() == 'y'
                    })
            
            self._organism_data = organism_data
            self.logger.info(f"Loaded {len(organism_data)} organisms from CSV")
            
        except Exception as e:
            self.logger.error(f"Error loading organism data: {str(e)}")
            self._organism_data = []
    
    def get_all_taxon_ids(self) -> List[int]:
        """Get all taxon IDs from the CSV file"""
        if not self._organism_data:
            return []
        
        taxon_ids = [org['taxon_id'] for org in self._organism_data if org['taxon_id'] > 0]
        return sorted(taxon_ids)
    
    def get_plant_taxon_ids(self) -> List[int]:
        """Get taxon IDs for plants only"""
        if not self._organism_data:
            return []
        
        plant_taxon_ids = [
            org['taxon_id'] for org in self._organism_data 
            if org['taxon_id'] > 0 and org['is_plant']
        ]
        return sorted(plant_taxon_ids)
    
    def get_non_plant_taxon_ids(self) -> List[int]:
        """Get taxon IDs for non-plants only"""
        if not self._organism_data:
            return []
        
        non_plant_taxon_ids = [
            org['taxon_id'] for org in self._organism_data 
            if org['taxon_id'] > 0 and not org['is_plant']
        ]
        return sorted(non_plant_taxon_ids)
    
    def get_organism_mapping(self) -> Dict[str, str]:
        """Get mapping from organism code to display name"""
        if not self._organism_data:
            return {}
        
        mapping = {}
        for org in self._organism_data:
            if org['abbrev_code'] and org['display_name']:
                # Build display name like Java services do
                display_name = org['display_name']
                if org['common_name']:
                    display_name += f" ({org['common_name']})"
                mapping[org['abbrev_code']] = display_name
        
        return mapping
    
    def get_taxon_by_code(self, abbrev_code: str) -> Optional[Dict]:
        """Get organism data by abbreviation code"""
        if not self._organism_data:
            return None
        
        for org in self._organism_data:
            if org['abbrev_code'] == abbrev_code:
                return org
        
        return None
    
    def get_taxon_by_id(self, taxon_id: int) -> Optional[Dict]:
        """Get organism data by taxon ID"""
        if not self._organism_data:
            return None
        
        for org in self._organism_data:
            if org['taxon_id'] == taxon_id:
                return org
        
        return None
    
    def is_valid_taxon_id(self, taxon_id: int) -> bool:
        """Check if a taxon ID is valid (exists in the CSV)"""
        return taxon_id in [org['taxon_id'] for org in self._organism_data or []]
    
    def get_ortholog_target_taxon_ids(self, exclude_taxon_id: Optional[int] = None) -> List[int]:
        """
        Get taxon IDs suitable for ortholog searches
        Excludes the query organism if specified
        """
        all_taxon_ids = self.get_all_taxon_ids()
        
        if exclude_taxon_id is not None:
            all_taxon_ids = [tid for tid in all_taxon_ids if tid != exclude_taxon_id]
        
        return all_taxon_ids


# Global instance for easy access
_taxon_loader = None

def get_taxon_loader() -> TaxonLoader:
    """Get the global TaxonLoader instance (singleton pattern)"""
    global _taxon_loader
    if _taxon_loader is None:
        _taxon_loader = TaxonLoader()
    return _taxon_loader


# Convenience functions for common use cases
def get_all_taxon_ids() -> List[int]:
    """Get all taxon IDs from organism CSV"""
    return get_taxon_loader().get_all_taxon_ids()


def get_plant_taxon_ids() -> List[int]:
    """Get plant taxon IDs only"""
    return get_taxon_loader().get_plant_taxon_ids()


def get_non_plant_taxon_ids() -> List[int]:
    """Get non-plant taxon IDs only"""
    return get_taxon_loader().get_non_plant_taxon_ids()


def get_organism_mapping() -> Dict[str, str]:
    """Get organism code to display name mapping"""
    return get_taxon_loader().get_organism_mapping()


def get_ortholog_target_taxon_ids(exclude_taxon_id: Optional[int] = None) -> List[int]:
    """Get taxon IDs for ortholog searches, optionally excluding one"""
    return get_taxon_loader().get_ortholog_target_taxon_ids(exclude_taxon_id)


def is_valid_taxon_id(taxon_id: int) -> bool:
    """Check if taxon ID is valid"""
    return get_taxon_loader().is_valid_taxon_id(taxon_id)


def reload_taxon_data() -> None:
    """Force reload of taxon data (useful for testing or after CSV updates)"""
    global _taxon_loader
    _taxon_loader = None
    get_taxon_loader()  # This will create a new instance


def get_agi_locus_mapping() -> Dict[str, str]:
    """
    Get AGI locus ID mapping (locus_id -> agi_id)
    This is used by gene ID processing scripts
    """
    try:
        import csv
        
        resources_path = os.getenv('RESOURCES_PATH')
        if not resources_path:
            logger.error("RESOURCES_PATH environment variable not set")
            return {}
        
        mapping_file = os.path.join(resources_path, "AGI_locusId_mapping_20200410.csv")
        
        if not os.path.exists(mapping_file):
            logger.error(f"AGI locus mapping file not found: {mapping_file}")
            return {}
        
        mapping = {}
        with open(mapping_file, 'r', encoding='utf-8') as f:
            # Try both CSV and TSV formats
            first_line = f.readline()
            f.seek(0)
            
            delimiter = '\t' if '\t' in first_line else ','
            reader = csv.DictReader(f, delimiter=delimiter)
            
            for row in reader:
                agi_id = row.get('AGI_id', '').strip()
                locus_id = row.get('locus_id', '').strip()
                if agi_id and locus_id:
                    mapping[locus_id] = agi_id
        
        logger.info(f"Loaded {len(mapping)} AGI locus mappings")
        return mapping
        
    except Exception as e:
        logger.error(f"Error loading AGI locus mapping: {str(e)}")
        return {}


if __name__ == "__main__":
    # Example usage and testing
    loader = get_taxon_loader()
    
    print(f"Total organisms: {len(loader.get_all_taxon_ids())}")
    print(f"Plant organisms: {len(loader.get_plant_taxon_ids())}")
    print(f"Non-plant organisms: {len(loader.get_non_plant_taxon_ids())}")
    
    print("\nFirst 10 taxon IDs:", loader.get_all_taxon_ids()[:10])
    print("First 5 plant taxon IDs:", loader.get_plant_taxon_ids()[:5])
    print("First 5 non-plant taxon IDs:", loader.get_non_plant_taxon_ids()[:5])
    
    # Test organism lookup
    arabidopsis = loader.get_taxon_by_id(3702)
    if arabidopsis:
        print(f"\nArabidopsis: {arabidopsis}")
    
    human = loader.get_taxon_by_code('HUMAN')
    if human:
        print(f"Human: {human}")
