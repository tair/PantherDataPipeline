"""
Panther API - Python Flask Server
Mirrors the client-side API endpoints with mock responses
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import time
import random
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
from services.tree_service import TreeService
from services.fasta_service import FastaService
from services.s3_service import S3Service
from services.ortholog_service import OrthologService
from services.pruning_service import PruningService

# Load environment variables
load_dotenv('config.env')

app = Flask(__name__)

# Configure logging
log_handlers = [logging.StreamHandler()]

# Try to add file handler, fall back to console only if permission denied
try:
    log_file = os.getenv('LOG_FILE', 'panther_api.log')
    # Ensure logs directory exists
    log_dir = os.path.dirname(log_file) if os.path.dirname(log_file) else '.'
    os.makedirs(log_dir, exist_ok=True)
    log_handlers.append(logging.FileHandler(log_file))
except (PermissionError, OSError) as e:
    print(f"Warning: Could not create log file, logging to console only: {e}")

logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=log_handlers
)
logger = logging.getLogger(__name__)

# Reduce AWS/boto3 logging verbosity
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# Enable CORS for all routes with credentials support
allowed_origins = os.getenv('ALLOWED_ORIGINS', 'http://localhost:8081').split(',')
CORS(app, 
     origins=allowed_origins,
     supports_credentials=True,
     allow_headers=["Content-Type", "Authorization"],
     methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])

# Initialize services
s3_service = S3Service()
tree_service = TreeService(s3_service)
fasta_service = FastaService(tree_service, s3_service)
ortholog_service = OrthologService()
pruning_service = PruningService()

# Mock data constants
MOCK_TREE_DATA = {
    "search": {
        "book": "PTHR12345",
        "treeId": "PTHR12345:SF123",
        "phyloxml": """<?xml version="1.0" encoding="UTF-8"?>
<phyloxml xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
          xmlns="http://www.phyloxml.org" 
          xsi:schemaLocation="http://www.phyloxml.org http://www.phyloxml.org/1.20/phyloxml.xsd">
  <phylogeny rooted="true">
    <name>Sample Tree</name>
    <clade>
      <name>Root</name>
      <confidence type="bootstrap">100</confidence>
      <clade>
        <name>Human</name>
        <taxonomy>
          <scientific_name>Homo sapiens</scientific_name>
          <id>9606</id>
        </taxonomy>
      </clade>
      <clade>
        <name>Mouse</name>
        <taxonomy>
          <scientific_name>Mus musculus</scientific_name>
          <id>10090</id>
        </taxonomy>
      </clade>
    </clade>
  </phylogeny>
</phyloxml>""",
        "msa": "MKVLWAALLVTFLAGCQAKVEQAVETEPEPELRQQTEWQSGQRWQRLASLIANTI...",
        "sequences": [
            {
                "id": "UniProtKB=P12345",
                "organism": "Homo sapiens",
                "taxonId": "9606",
                "sequence": "MKVLWAALLVTFLAGCQAKVEQAVETEPEPELRQQTEWQSGQRWQRLASLIANTI"
            },
            {
                "id": "UniProtKB=Q67890",
                "organism": "Mus musculus", 
                "taxonId": "10090",
                "sequence": "MKVLWAALLVTFLAGCQAKVEQAVETEPEPELRQQTEWQSGQRWQRLASLIANTI"
            }
        ]
    }
}

MOCK_ORTHOLOG_DATA = [
    {
        "uniprotId": "P12345",
        "geneSymbol": "GENE1",
        "organism": "Homo sapiens",
        "taxonId": "9606",
        "orthologs": [
            {
                "uniprotId": "Q67890",
                "geneSymbol": "Gene1",
                "organism": "Mus musculus",
                "taxonId": "10090",
                "orthologyType": "LDO"
            },
            {
                "uniprotId": "R11111", 
                "geneSymbol": "gene1",
                "organism": "Drosophila melanogaster",
                "taxonId": "7227",
                "orthologyType": "O"
            }
        ]
    }
]

MOCK_FASTA_DATA = """>UniProtKB=P12345|Homo sapiens
MKVLWAALLVTFLAGCQAKVEQAVETEPEPELRQQTEWQSGQRWQRLASLIANTI
PVSLEEFGVYPFNPFFPEESLLLEPTLGKLKHRGFPNLGEKGFGLYLTRSSQL
FPSDNSNGGTLSFMKFMNCNLRPLDEGVPFIHTKDSDDVDVYSNLNLGKRQDY

>UniProtKB=Q67890|Mus musculus  
MKVLWAALLVTFLAGCQAKVEQAVETEPEPELRQQTEWQSGQRWQRLASLIANTI
PVSLEEFGVYPFNPFFPEESLLLEPTLGKLKHRGFPNLGEKGFGLYLTRSSQL
FPSDNSNGGTLSFMKFMNCNLRPLDEGVPFIHTKDSDDVDVYSNLNLGKRQDY
"""

# Error responses
ERROR_RESPONSES = {
    "invalid_sequence": {
        "search": {
            "error": "Invalid sequence format or sequence too short"
        }
    },
    "tree_not_found": {
        "error": "Tree not found",
        "code": "TREE_NOT_FOUND"
    },
    "s3_unavailable": {
        "error": "S3 service unavailable. Please configure valid AWS credentials",
        "code": "S3_UNAVAILABLE"
    },
    "timeout": {
        "error": "Connection timed out",
        "code": "TIMEOUT"
    }
}

def create_error_response(error_type, status_code=400):
    """Create standardized error response"""
    error_data = ERROR_RESPONSES.get(error_type, {
        "error": "An unexpected error occurred",
        "code": "UNKNOWN_ERROR"
    })
    error_data["timestamp"] = datetime.now().isoformat()
    return jsonify(error_data), status_code

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    logger.debug("Health check requested")
    
    # Test S3 connection
    s3_status = s3_service.test_connection()
    
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "Panther API",
        "version": "1.0.0",
        "environment": os.getenv('FLASK_ENV', 'production'),
        "data_mode": "real_s3",
        "s3_connection": s3_status
    })

# =============================================================================
# PANTHER API ENDPOINTS
# =============================================================================

@app.route('/panther/grafting', methods=['POST'])
def graft_tree():
    """
    Tree grafting - Add sequence to phylogenetic tree
    """
    try:
        data = request.get_json()
        sequence = data.get('sequence', '').strip()
        
        # Validate input
        if not sequence:
            return create_error_response("invalid_sequence", 400)
        
        if len(sequence) < 10:
            return create_error_response("invalid_sequence", 400)
        

        
        # Simulate occasional errors for testing
        if random.random() < 0.1:  # 10% chance of error
            return create_error_response("timeout", 504)
        
        # Return mock tree data with grafted sequence (grafting doesn't use S3 data)
        response_data = MOCK_TREE_DATA.copy()
        response_data["search"]["graftedSequence"] = sequence
        response_data["search"]["graftPosition"] = random.randint(1, 100)
        
        return jsonify(response_data)
        
    except Exception as e:
        app.logger.error(f"Error in graft_tree: {str(e)}")
        return create_error_response("unknown", 500)

@app.route('/panther/pruning/<tree_id>', methods=['POST'])
def prune_tree(tree_id):
    """
    Regular tree pruning - Remove taxa from tree
    Based on PruningController.getPrunedTree() logic
    """
    try:
        logger.info(f"Tree pruning request for tree: {tree_id}")
        
        data = request.get_json()
        taxon_ids_to_show = data.get('taxonIdsToShow', [])
        
        logger.info(f"Tree pruning: treeId={tree_id}, taxonIds={taxon_ids_to_show}")
        
        # Validate request
        validation = pruning_service.validate_pruning_request(tree_id, taxon_ids_to_show)
        if not validation['is_valid']:
            logger.warning(f"Invalid pruning request: {validation['errors']}")
            return create_error_response("tree_not_found", 404)
        
        try:
            # Call pruning service (calls external Panther API)
            result = pruning_service.get_pruned_tree(tree_id, taxon_ids_to_show)
            
            # Parse result to return as JSON object (not string)
            tree_data = json.loads(result)
            
            # Log statistics
            logger.info(f"Successfully pruned tree {tree_id} for {len(taxon_ids_to_show)} taxa")
            
            return jsonify(tree_data)
            
        except Exception as service_error:
            error_msg = str(service_error)
            logger.error(f"Service error pruning tree {tree_id}: {error_msg}")
            
            # Return specific error responses based on error type
            if "Panther API call failed" in error_msg:
                return create_error_response("s3_unavailable", 503)  # External API unavailable
            elif "not found" in error_msg.lower():
                return create_error_response("tree_not_found", 404)
            else:
                return create_error_response("unknown", 500)
        
    except Exception as e:
        logger.error(f"Error in prune_tree: {str(e)}")
        return create_error_response("unknown", 500)

@app.route('/panther/grafting/prune', methods=['POST'])
def prune_grafted_tree():
    """
    Grafted tree pruning - Prune a previously grafted tree
    """
    try:
        data = request.get_json()
        sequence = data.get('sequence', '').strip()
        taxon_ids_to_show = data.get('taxonIdsToShow', [])
        
        # Validate input
        if not sequence:
            return create_error_response("invalid_sequence", 400)
        

        
        # Filter sequences based on taxon IDs
        filtered_sequences = []
        if taxon_ids_to_show:
            for seq in MOCK_TREE_DATA["search"]["sequences"]:
                if seq["taxonId"] in taxon_ids_to_show:
                    filtered_sequences.append(seq)
        else:
            filtered_sequences = MOCK_TREE_DATA["search"]["sequences"]
        
        response_data = MOCK_TREE_DATA.copy()
        response_data["search"]["sequences"] = filtered_sequences
        response_data["search"]["graftedSequence"] = sequence
        response_data["search"]["prunedTaxonIds"] = taxon_ids_to_show
        
        return jsonify(response_data)
        
    except Exception as e:
        app.logger.error(f"Error in prune_grafted_tree: {str(e)}")
        return create_error_response("unknown", 500)

@app.route('/panther/pruning/fastadoc/<tree_id>', methods=['POST'])
@app.route('/panther/fastadoc/<tree_id>', methods=['POST'])
def download_fasta(tree_id):
    """
    Download FASTA sequences for tree
    Based on PruningController.callFastaApi() logic
    """
    try:
        logger.info(f"FASTA download request for tree: {tree_id}")
        
        data = request.get_json() if request.is_json else {}
        taxon_ids_to_show = data.get('taxonIdsToShow', [])
        
        # Validate request
        validation = fasta_service.validate_fasta_request(tree_id, taxon_ids_to_show)
        if not validation['is_valid']:
            logger.warning(f"Invalid FASTA request: {validation['errors']}")
            return create_error_response("tree_not_found", 404)

        # Generate FASTA using service (mimics Java logic)
        # If taxon_array is null or empty -> full tree, else -> pruned tree
        taxon_filter = taxon_ids_to_show if taxon_ids_to_show else None
        
        try:
            fasta_content = fasta_service.generate_fasta_from_tree(tree_id, taxon_filter)
        except Exception as service_error:
            error_msg = str(service_error)
            logger.error(f"Service error generating FASTA for {tree_id}: {error_msg}")
            
            # Return specific error responses based on error type
            if "not found in S3" in error_msg:
                return create_error_response("tree_not_found", 404)
            elif "S3 service not available" in error_msg:
                return create_error_response("s3_unavailable", 503)
            else:
                return create_error_response("unknown", 500)
        
        # Log statistics
        stats = fasta_service.get_fasta_stats(fasta_content)
        logger.info(f"Generated FASTA: {stats['sequence_count']} sequences, {stats['total_residues']} residues")
        
        # Return as plain text with appropriate headers
        return fasta_content, 200, {
            'Content-Type': 'text/plain',
            'Content-Disposition': f'attachment; filename="{tree_id}.fasta"'
        }
        
    except Exception as e:
        logger.error(f"Error in download_fasta for {tree_id}: {str(e)}")
        return create_error_response("unknown", 500)

@app.route('/panther/orthomapping', methods=['POST'])
def get_ortholog_mapping():
    """
    Get ortholog mapping for a gene
    Based on PruningController.callOrthologApi() logic
    """
    try:
        logger.info("Ortholog mapping request received")
        
        data = request.get_json()
        uniprot_id = data.get('uniprotId', '').strip()
        query_organism_id = data.get('queryOrganismId', '').strip()
        
        logger.info(f"Ortholog mapping: uniprotId={uniprot_id}, queryOrganismId={query_organism_id}")
        
        # Handle null queryOrganismId like Java
        if not query_organism_id:
            return "", 200, {'Content-Type': 'text/plain'}
        
        # Validate request
        validation = ortholog_service.validate_ortholog_request(uniprot_id, query_organism_id)
        if not validation['is_valid']:
            logger.warning(f"Invalid ortholog request: {validation['errors']}")
            return "{}", 200, {'Content-Type': 'text/plain'}
        
        # Convert to integer
        try:
            query_id = int(query_organism_id)
        except ValueError:
            return "{}", 200, {'Content-Type': 'text/plain'}
        
        # Call ortholog service
        result = ortholog_service.get_ortholog_mapping(uniprot_id, query_id)
        
        # Return as plain text JSON string like Java
        return result, 200, {'Content-Type': 'text/plain'}
        
    except Exception as e:
        logger.error(f"Error in get_ortholog_mapping: {str(e)}")
        return "{}", 200, {'Content-Type': 'text/plain'}

# =============================================================================
# UTILITY ENDPOINTS
# =============================================================================

@app.route('/panther/validate/sequence', methods=['POST'])
def validate_sequence():
    """
    Validate protein sequence format
    """
    try:
        data = request.get_json()
        sequence = data.get('sequence', '').strip()
        
        # Basic validation
        valid_amino_acids = set('ACDEFGHIKLMNPQRSTVWY')
        sequence_upper = sequence.upper()
        
        is_valid = (
            len(sequence) >= 10 and
            all(aa in valid_amino_acids for aa in sequence_upper)
        )
        
        return jsonify({
            "isValid": is_valid,
            "length": len(sequence),
            "message": "Valid protein sequence" if is_valid else "Invalid protein sequence"
        })
        
    except Exception as e:
        app.logger.error(f"Error in validate_sequence: {str(e)}")
        return create_error_response("unknown", 500)

@app.route('/panther/trees/search', methods=['GET'])
def search_trees():
    """
    Search available trees by keyword
    """
    try:
        query = request.args.get('q', '').strip()
        limit = int(request.args.get('limit', 10))
        
        # Mock tree search results
        mock_trees = [
            {"id": "PTHR12345", "name": "Protein kinase family", "geneCount": 150},
            {"id": "PTHR67890", "name": "Transcription factor family", "geneCount": 89},
            {"id": "PTHR11111", "name": "Membrane transport family", "geneCount": 245},
        ]
        
        # Simple filtering by query
        if query:
            filtered_trees = [
                tree for tree in mock_trees 
                if query.lower() in tree["name"].lower() or query.upper() in tree["id"]
            ]
        else:
            filtered_trees = mock_trees
        
        return jsonify({
            "trees": filtered_trees[:limit],
            "total": len(filtered_trees),
            "query": query
        })
        
    except Exception as e:
        app.logger.error(f"Error in search_trees: {str(e)}")
        return create_error_response("unknown", 500)

# =============================================================================
# ERROR HANDLERS
# =============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error": "Endpoint not found",
        "code": "NOT_FOUND",
        "timestamp": datetime.now().isoformat()
    }), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        "error": "Method not allowed",
        "code": "METHOD_NOT_ALLOWED", 
        "timestamp": datetime.now().isoformat()
    }), 405

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "error": "Internal server error",
        "code": "INTERNAL_ERROR",
        "timestamp": datetime.now().isoformat()
    }), 500

# =============================================================================
# DEVELOPMENT UTILITIES
# =============================================================================

@app.route('/debug/endpoints', methods=['GET'])
def list_endpoints():
    """List all available endpoints (for development)"""
    endpoints = []
    for rule in app.url_map.iter_rules():
        endpoints.append({
            "endpoint": rule.endpoint,
            "methods": list(rule.methods),
            "url": str(rule)
        })
    return jsonify({"endpoints": endpoints})

@app.route('/debug/s3', methods=['GET'])
def test_s3_connection():
    """Test S3 connection and bucket access"""
    try:
        logger.info("Testing S3 connection")
        
        connection_test = s3_service.test_connection()
        
        # Add additional debug information
        debug_info = {
            "connection_test": connection_test,
            "s3_available": s3_service.is_available(),
            "data_mode": "real_s3",
            "tree_bucket": os.getenv('PG_TREE_BUCKET', 'phg-panther-data-19'),
            "msa_bucket": os.getenv('PG_MSA_BUCKET', 'phg-panther-msa-data-19'),
            "aws_region": os.getenv('AWS_REGION', 'us-west-2'),
            "has_aws_credentials": bool(os.getenv('AWS_ACCESS_KEY') and os.getenv('AWS_SECRET_KEY'))
        }
        
        return jsonify(debug_info)
        
    except Exception as e:
        logger.error(f"Error testing S3 connection: {str(e)}")
        return jsonify({
            "error": str(e),
            "s3_available": False
        }), 500

if __name__ == '__main__':
    # Development server configuration
    port = int(os.getenv('PORT', 8080))
    debug = os.getenv('DEBUG', 'True').lower() == 'true'
    
    logger.info(f"Starting Panther API on port {port} (debug={debug})")
    
    app.run(
        host='0.0.0.0',
        port=port,
        debug=debug,
        threaded=True
    )