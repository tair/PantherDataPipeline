# Panther Python API

A simplified Python implementation of the Panther phylogenetic tree API, based on the Java PruningController architecture.

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Edit `config.env` to customize settings:

```bash
# Basic configuration
FLASK_ENV=development
DEBUG=True
PORT=8080

# Mock data (set to false for real AWS S3 integration)
USE_MOCK_DATA=true

# Logging
LOG_LEVEL=INFO
LOG_FILE=panther_api.log

# CORS origins
ALLOWED_ORIGINS=http://localhost:8081,http://localhost:3000
```

### 3. Run the API

```bash
python panther_api.py
```

The API will be available at `http://localhost:8080`

## 📋 Architecture

### Services Layer

- **TreeService**: Handles phylogenetic tree operations and persistent ID mapping
- **FastaService**: Generates FASTA sequences from tree and MSA data

### Key Features

- ✅ **Proper separation of concerns** - Services handle business logic
- ✅ **Configuration via environment** - Easy deployment configuration
- ✅ **Comprehensive logging** - Debug and monitor API operations
- ✅ **Input validation** - Validate tree IDs and request parameters
- ✅ **Error handling** - Graceful error responses with proper HTTP codes
- ✅ **CORS support** - Ready for frontend integration

## 🔧 FASTA Generation

The FASTA generation closely mirrors the Java implementation:

1. **Tree Structure**: Get phylogenetic tree annotation data
2. **Persistent ID Mapping**: Map tree leaf nodes to FASTA headers (`UniProtID|Organism|GeneID`)
3. **MSA Data**: Retrieve Multiple Sequence Alignment data
4. **FASTA Assembly**: Combine headers and sequences with 60-character line wrapping

### Endpoints

- `POST /panther/fastadoc/{tree_id}` - Full tree FASTA
- `POST /panther/pruning/fastadoc/{tree_id}` - Filtered FASTA by taxon IDs

## 📊 Logging

Logs are written to both console and file (`panther_api.log`):

- Request/response information
- Service operations
- Error details
- Performance metrics

## 🔄 Mock vs Real S3 Data

The API supports both mock data and real AWS S3 data:

### Using Mock Data (Default)

- Set `USE_MOCK_DATA=true` in config.env
- No AWS credentials required
- Uses realistic mock data that matches S3 structure

### Using Real S3 Data

1. **Configure AWS credentials** in config.env:

   ```bash
   AWS_ACCESS_KEY=your_actual_access_key
   AWS_SECRET_KEY=your_actual_secret_key
   AWS_REGION=us-west-2
   USE_MOCK_DATA=false
   ```

2. **Ensure S3 bucket access** to:

   - `phg-panther-data-19` (tree data)
   - `phg-panther-msa-data-19` (MSA sequence data)

3. **Test connection**:
   ```bash
   curl http://localhost:8080/debug/s3
   ```

### Automatic Fallback

- If S3 is unavailable, the API automatically falls back to mock data
- Errors are logged and gracefully handled
- Health check endpoint shows S3 connection status

## 🧪 Testing

Import the Postman collection (`Panther_API_Postman_Collection.json`) for comprehensive API testing.
