# PHG-XXX - Process and Update MSA (Multiple Sequence Alignment) Files

## Overview

Process MSA files for all Panther tree IDs from Solr, generate JSON format files locally, upload to S3, and validate data integrity.

## Prerequisites

### Environment Setup

Open PantherDataPipeline: `https://github.com/tair/PantherDataPipeline.git`

Update following variables in `application.properties` file:

- `SOLR_HOST`: Solr server URL
  - Local Version: `http://localhost:8983`
  - Test Server: `http://[phylogenes-test-ec2-ip]:8983`
- `PANTHER_COLLECTION`: `panther`
- `MSA_LOCAL_PATH`: Local storage path for MSA JSON files
- `PANTHER_SERVER_URL`: `https://pantherdb.org/services/oai/pantherdb`
- `PG_MSA_BUCKET`: S3 bucket name for MSA data storage

### Required Dependencies

```bash
cd python_api_scripts
pip install -r requirements.txt
```

## Step 1: Generate MSA Files for All Panther Trees

### Process All MSA Files Locally

Run `process_all_msa_files()` in `process_all_msa.py`:

```bash
cd python_api_scripts/pipeline_scripts
python process_all_msa.py
```

**Process:**

1. Queries Solr to retrieve all Panther tree IDs from panther collection
2. For each tree ID:
   - Retrieves MSA data from Panther server API
   - Processes and formats data as JSON
   - Saves locally in `MSA_LOCAL_PATH` directory
3. Shows progress with progress bar
4. Logs processing statistics and failed tree IDs

### Process with S3 Upload

```bash
python process_all_msa.py --upload-s3
```

Saves MSA files locally AND uploads them to S3 bucket during processing.

### Process with Custom Local Path

```bash
python process_all_msa.py --local-path /custom/storage/path
```

## Step 2: Upload Existing MSA Files to S3

### Batch Upload Existing Local Files

Run `upload_all_msa_files()` in `MSABatchUploader`:

```bash
python process_all_msa.py --upload-existing-s3
```

**Process:**

1. Scans local MSA directory for existing PTHR\*.json files
2. Uploads each file to S3 bucket with proper naming convention
3. Stops on first upload failure for data integrity
4. Reports upload statistics

## Step 3: Data Validation

### Validate MSA Sequence Counts Against Solr

Run `validate_counts()` in `MSAValidator`:

```bash
python process_all_msa.py --validate-counts
```

**Process:**

1. Queries Solr for persistent_ids of all panther trees
2. Extracts persistent_ids from local MSA JSON files
3. Compares counts and identifies mismatches
4. Generates validation error report in CSV format
5. Saves results to `logs/msa_validation_errors.csv`

### Custom Validation Output

```bash
python process_all_msa.py --validate-counts --output-csv custom_validation.csv
```

### Validate FASTA API Responses

Run FASTA validation against Solr gene ID counts:

**Single Tree Validation:**

```bash
python process_all_msa.py --validate-fasta PTHR11913
```

**Batch FASTA Validation:**

```bash
python process_all_msa.py --validate-fasta-batch
python process_all_msa.py --validate-fasta-batch --output-csv fasta_validation.csv
```

## Step 4: Monitoring and Troubleshooting

### Log Files

- Main processing log: `python_api_scripts/logs/process_all_msa.log`
- Validation errors: `python_api_scripts/logs/msa_validation_errors.csv`
- FASTA validation: `python_api_scripts/logs/fasta_validation_results.csv`

### Verbose Logging

```bash
python process_all_msa.py --verbose
```

### Common Commands Summary

```bash
# Complete MSA processing with S3 upload
python process_all_msa.py --upload-s3

# Upload existing files to S3
python process_all_msa.py --upload-existing-s3

# Validate data integrity
python process_all_msa.py --validate-counts

# Full validation suite
python process_all_msa.py --validate-counts --validate-fasta-batch --verbose
```

## Expected Outputs

### Successful Processing

- Local JSON files: `{MSA_LOCAL_PATH}/PTHR*.json`
- S3 uploads: Files stored in configured S3 bucket
- Processing statistics logged with success/failure counts

### Validation Results

- **No mismatches**: All persistent_ids match between Solr and MSA files
- **Mismatches found**: Detailed CSV report with discrepancies
- **FASTA validation**: Sequence counts match Solr gene ID counts

### Error Handling

- Failed tree IDs logged and reported
- Partial success scenarios handled gracefully
- Validation errors saved to CSV for review
- Process stops on critical S3 upload failures

## Configuration Files

- Environment variables: `.env` file in project root
- Application settings: `application.properties`
- Dependencies: `requirements.txt`
- Logging configuration: Built into script with UTF-8 encoding support
