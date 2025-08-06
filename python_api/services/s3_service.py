"""
S3 Service - Handles AWS S3 operations for Panther data
Based on PhylogenesServerWrapper S3 functionality
"""

import json
import logging
import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class S3Service:
    """Service for AWS S3 operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._s3_client = None
        self._initialize_s3_client()
    
    def _initialize_s3_client(self):
        """Initialize S3 client with credentials"""
        try:
            aws_access_key = os.getenv('AWS_ACCESS_KEY')
            aws_secret_key = os.getenv('AWS_SECRET_KEY')
            aws_region = os.getenv('AWS_REGION', 'us-west-2')
            
            if not aws_access_key or not aws_secret_key:
                self.logger.warning("AWS credentials not found, S3 operations will be disabled")
                return
            
            self._s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=aws_region
            )
            
            self.logger.info("S3 client initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize S3 client: {str(e)}")
            self._s3_client = None
    
    def is_available(self) -> bool:
        """Check if S3 service is available"""
        return self._s3_client is not None
    
    def get_tree_data(self, tree_id: str) -> Optional[Dict[str, Any]]:
        """
        Get tree data from S3 bucket
        Based on PhylogenesServerWrapper.getPantherTreeRootById()
        """
        try:
            if not self.is_available():
                self.logger.error("S3 service not available")
                return None
            
            bucket_name = os.getenv('PG_TREE_BUCKET', 'phg-panther-data-19')
            key = f"{tree_id}.json"
            
            self.logger.info(f"Fetching tree data from S3: {bucket_name}/{key}")
            
            response = self._s3_client.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
            
            # Parse JSON content
            json_data = json.loads(content)
            
            # Iterate through keys like Java code does
            for key_name in json_data.keys():
                self.logger.debug(f"Processing key: {key_name}")
                
                if key_name == 'jsonString':
                    # Parse the jsonString value as nested JSON
                    json_string_value = json_data[key_name]
                    nested_json = json.loads(json_string_value)
                    
                    # Extract search.annotation_node from nested JSON
                    if 'search' in nested_json and 'annotation_node' in nested_json['search']:
                        return nested_json['search']['annotation_node']
                        
                elif key_name == 'search':
                    # Direct search object in root JSON
                    search_obj = json_data['search']
                    self.logger.debug(f"Search object keys: {list(search_obj.keys()) if isinstance(search_obj, dict) else 'not a dict'}")
                    
                    # Implement the same logic as Java SearchResult.getAnnotation_node()
                    # First try direct annotation_node
                    if 'annotation_node' in search_obj and search_obj['annotation_node'] is not None:
                        return search_obj['annotation_node']
                    
                    # Fallback to tree_topology.annotation_node
                    elif 'tree_topology' in search_obj and search_obj['tree_topology'] is not None:
                        tree_topology = search_obj['tree_topology']
                        if isinstance(tree_topology, dict) and 'annotation_node' in tree_topology:
                            return tree_topology['annotation_node']
                    
                    self.logger.warning(f"Neither 'annotation_node' nor 'tree_topology.annotation_node' found in search object. Available keys: {list(search_obj.keys()) if isinstance(search_obj, dict) else 'not a dict'}")
            
            # If we get here, neither expected structure was found
            self.logger.warning(f"No 'jsonString' or 'search' key found in {key}. Available keys: {list(json_data.keys())}")
            return None
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                self.logger.warning(f"Tree data not found in S3: {tree_id}")
            else:
                self.logger.error(f"S3 ClientError getting tree data: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON from S3: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Error getting tree data from S3: {str(e)}")
            return None
    
    def get_msa_data(self, tree_id: str) -> Optional[Dict[str, Any]]:
        """
        Get MSA data from S3 bucket
        Based on PhylogenesServerWrapper.get_processed_msa() S3 access
        """
        try:
            if not self.is_available():
                self.logger.error("S3 service not available")
                return None
            
            bucket_name = os.getenv('PG_MSA_BUCKET', 'phg-panther-msa-data-19')
            key = f"{tree_id}.json"
            
            self.logger.info(f"Fetching MSA data from S3: {bucket_name}/{key}")
            
            response = self._s3_client.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
            
            # Parse JSON content
            json_data = json.loads(content)
            
            # Iterate through keys like Java code does: while (keys.hasNext())
            msa_structure = {"family_data": []}
            
            for key_name, value in json_data.items():
                self.logger.debug(f"Processing MSA key: {key_name}, type: {type(value)}")
                
                # Check if value is a JSONArray (list in Python)
                if isinstance(value, list) and len(value) > 0:
                    self.logger.debug(f"Found array for key {key_name} with {len(value)} items")
                    
                    # Get first item from array: familyNames.getJSONObject(0)
                    first_item = value[0]
                    if isinstance(first_item, dict) and 'msa_data' in first_item:
                        # Extract msa_data string: (String) familyNames.getJSONObject(0).get("msa_data")
                        msa_data_string = first_item['msa_data']
                        
                        if isinstance(msa_data_string, str):
                            try:
                                # Parse msa_data string as JSON (like ObjectMapper.readValue)
                                msa_data_json = json.loads(msa_data_string)
                                
                                # Transform to expected structure for processing
                                # The msa_data_json should have search.MSA_list.sequence_info structure
                                if 'search' in msa_data_json and 'MSA_list' in msa_data_json['search']:
                                    sequence_info = msa_data_json['search']['MSA_list'].get('sequence_info', [])
                                    msa_structure["family_data"].append({
                                        "msa_data": {
                                            "sequence_list": sequence_info
                                        }
                                    })
                                    self.logger.debug(f"Extracted {len(sequence_info)} sequences from MSA data")
                                else:
                                    self.logger.warning(f"MSA data missing search.MSA_list structure for {tree_id}")
                                    
                            except json.JSONDecodeError as e:
                                self.logger.error(f"Could not parse msa_data string for {tree_id}: {str(e)}")
                                continue
                        else:
                            self.logger.warning(f"msa_data is not a string for {tree_id}, got: {type(msa_data_string)}")
            
            if not msa_structure["family_data"]:
                self.logger.warning(f"No valid MSA data found in {tree_id}")
                return None
            
            return msa_structure
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                self.logger.warning(f"MSA data not found in S3: {tree_id}")
            else:
                self.logger.error(f"S3 ClientError getting MSA data: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse MSA JSON from S3: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Error getting MSA data from S3: {str(e)}")
            return None
    
    def test_connection(self) -> Dict[str, Any]:
        """Test S3 connection and access to buckets"""
        try:
            if not self.is_available():
                return {
                    "status": "error",
                    "message": "S3 service not initialized"
                }
            
            # Test access to tree bucket
            tree_bucket = os.getenv('PG_TREE_BUCKET', 'phg-panther-data-19')
            msa_bucket = os.getenv('PG_MSA_BUCKET', 'phg-panther-msa-data-19')
            
            result = {
                "status": "success",
                "tree_bucket": tree_bucket,
                "msa_bucket": msa_bucket,
                "tree_bucket_access": False,
                "msa_bucket_access": False
            }
            
            # Test tree bucket access
            try:
                self._s3_client.head_bucket(Bucket=tree_bucket)
                result["tree_bucket_access"] = True
            except ClientError:
                self.logger.warning(f"Cannot access tree bucket: {tree_bucket}")
            
            # Test MSA bucket access
            try:
                self._s3_client.head_bucket(Bucket=msa_bucket)
                result["msa_bucket_access"] = True
            except ClientError:
                self.logger.warning(f"Cannot access MSA bucket: {msa_bucket}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error testing S3 connection: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }