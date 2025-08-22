from quixstreams.sinks import BatchingSink, SinkBatch
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time
import logging
import uuid
import os
from typing import List, Dict, Any
from datetime import datetime
import requests


TIMESTAMP_COL_MAPPER = {
    "year": lambda col: col.dt.year.astype(str),
    "month": lambda col: col.dt.month.astype(str).str.zfill(2),
    "day": lambda col: col.dt.day.astype(str).str.zfill(2),
    "hour": lambda col: col.dt.hour.astype(str).str.zfill(2)
}


class S3DirectSink(BatchingSink):
    """
    Writes Kafka batches directly to S3 as Hive-partitioned Parquet files,
    then optionally registers the table using the discover endpoint.
    """
    
    def __init__(
        self,
        s3_bucket: str,
        s3_prefix: str,
        table_name: str,
        hive_columns: List[str] = None,
        timestamp_column: str = "ts_ms",
        catalog_url: str = None,
        auto_discover: bool = True,
        namespace: str = "default",
        auto_create_bucket: bool = True
    ):
        """
        Initialize S3 Direct Sink
        
        Args:
            s3_bucket: S3 bucket name
            s3_prefix: S3 prefix/path for data files
            table_name: Table name for registration
            hive_columns: List of columns to use for Hive partitioning. Include 'year', 'month', 
                         'day', 'hour' to extract these from timestamp_column
            timestamp_column: Column containing timestamp to extract time partitions from
            catalog_url: Optional REST Catalog URL for table registration
            auto_discover: Whether to auto-register table on first write
            namespace: Catalog namespace (default: "default")
            auto_create_bucket: if True, create bucket in S3 if missing.
        """
        self._aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self._aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self._aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self._aws_endpoint_url = os.getenv("AWS_ENDPOINT_URL", None)
        print(self._aws_endpoint_url)
        self._credentials = {
            "region_name": self._aws_region,
            "aws_access_key_id": self._aws_access_key_id,
            "aws_secret_access_key": self._aws_secret_access_key,
            "endpoint_url": self._aws_endpoint_url,
        }
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table_name = table_name
        self.hive_columns = hive_columns or []
        self.timestamp_column = timestamp_column
        self.catalog_url = catalog_url.rstrip('/') if catalog_url else None
        self.auto_discover = auto_discover
        self.namespace = namespace
        self.table_registered = False
        
        self.logger = logging.getLogger(__name__)
        
        # S3 client will be initialized in setup()
        self.s3_client = None
        self._ts_hive_columns = {'year', 'month', 'day', 'hour'} & set(self.hive_columns)
        self._auto_create_bucket = auto_create_bucket
        
        super().__init__()
    
    def setup(self):
        """Initialize S3 client and test connection"""
        try:
            # Initialize S3 client
            self.s3_client = boto3.client(
                's3',
                **self._credentials
            )
            
            # Confirm bucket connection
            self._ensure_bucket()
            
            # Test Catalog connection if configured
            if self.catalog_url:
                try:
                    response = requests.get(f"{self.catalog_url}/health", timeout=5)
                    response.raise_for_status()
                    self.logger.info("Successfully connected to REST Catalog at %s", self.catalog_url)
                except Exception as e:
                    self.logger.warning("Could not connect to REST Catalog: %s. Table registration disabled.", e)
                    self.auto_discover = False
            
            # Check if table already exists in S3 and validate partition strategy
            self._validate_existing_table_structure()
                    
        except Exception as e:
            self.logger.error("Failed to setup S3 connection: %s", e)
            raise

    def _ensure_bucket(self):
        bucket = self.s3_bucket
        try:
            self.s3_client.head_bucket(Bucket=bucket)
        except boto3.ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404 and self._auto_create_bucket:
                # Bucket does not exist, create it
                print(f"⚠️ Bucket '{bucket}' not found. Creating it...")
                self.s3_client.create_bucket(Bucket=self.s3_bucket)
                print(f"✅ Bucket '{bucket}' created.")
            else:
                raise
        self.logger.info("Successfully connected to S3 bucket: %s", bucket)

    def write(self, batch: SinkBatch):
        """Write batch directly to S3"""
        # Register table before first write if auto-discover is enabled
        if self.auto_discover and not self.table_registered and self.catalog_url:
            self._register_table()
            
        attempts = 3
        while attempts:
            start = time.perf_counter()
            try:
                self._write_batch(batch)
                elapsed_ms = (time.perf_counter() - start) * 1000
                self.logger.info("✔ wrote %d rows to S3 in %.1f ms", batch.size, elapsed_ms)
                return
            except Exception as exc:
                attempts -= 1
                if attempts == 0:
                    raise
                self.logger.warning("Write failed (%s) – retrying …", exc)
                time.sleep(3)
    
    def _write_batch(self, batch: SinkBatch):
        """Convert batch to Parquet and write to S3 with Hive partitioning"""
        if not batch:
            return
        
        # Convert batch to list of dictionaries
        rows = []
        for item in batch:
            row = item.value.copy()
            # Add timestamp and key if not present
            if self.timestamp_column not in row:
                row[self.timestamp_column] = item.timestamp
            row["__key"] = item.key
            rows.append(row)
        
        # Convert to DataFrame
        df = pd.DataFrame(rows)
        
        # Add time-based columns if needed (but only use those specified in hive_columns)
        if self._ts_hive_columns:
            df = self._add_timestamp_columns(df)
        
        # Use only the explicitly specified partition columns
        partition_columns = self.hive_columns.copy()
        
        if partition_columns:
            # Group by partition columns and write each partition
            for group_values, group_df in df.groupby(partition_columns):
                if not isinstance(group_values, tuple):
                    group_values = (group_values,)
                
                # Build S3 key with Hive partitioning
                partition_parts = [f"{col}={val}" for col, val in zip(partition_columns, group_values)]
                s3_key = f"{self.s3_prefix}/{self.table_name}/" + "/".join(partition_parts) + f"/data_{uuid.uuid4().hex}.parquet"
                
                # Remove partition columns from data (Hive style)
                data_df = group_df.drop(columns=partition_columns, errors='ignore')
                
                # Write to S3
                self._write_parquet_to_s3(data_df, s3_key)
                
                # Register file in manifest if catalog is configured
                if self.catalog_url and self.table_registered:
                    self._register_file_in_manifest(s3_key, len(data_df), partition_columns, group_values)
        else:
            # No partitioning - write as single file
            s3_key = f"{self.s3_prefix}/{self.table_name}/data_{uuid.uuid4().hex}.parquet"
            self._write_parquet_to_s3(df, s3_key)
            
            # Register file in manifest if catalog is configured
            if self.catalog_url and self.table_registered:
                self._register_file_in_manifest(s3_key, len(df), [], [])

    def _write_parquet_to_s3(self, df: pd.DataFrame, s3_key: str):
        """Write DataFrame to S3 as Parquet"""
        # Convert to Arrow table
        table = pa.Table.from_pandas(df)
        
        # Write to buffer
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf)
        
        # Upload to S3
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=s3_key,
            Body=buf.getvalue().to_pybytes()
        )
        
        self.logger.debug("Wrote %d rows to s3://%s/%s", len(df), self.s3_bucket, s3_key)
    
    def _register_table(self):
        """Register the table in REST Catalog"""
        if not self.catalog_url:
            return
            
        try:
            # First check if table already exists
            check_response = requests.get(
                f"{self.catalog_url}/namespaces/{self.namespace}/tables/{self.table_name}",
                timeout=5
            )
            
            if check_response.status_code == 200:
                self.logger.info("Table '%s' already exists in catalog", self.table_name)
                self.table_registered = True
                # Validate partition strategy matches
                self._validate_partition_strategy(check_response.json())
                return
            
            # Table doesn't exist, create it
            s3_path = f"s3://{self.s3_bucket}/{self.s3_prefix}/{self.table_name}"
            
            # Define partition spec based on configuration
            partition_spec = self.hive_columns.copy()
            
            # Create table with minimal schema (will be inferred from data)
            create_response = requests.put(
                f"{self.catalog_url}/namespaces/{self.namespace}/tables/{self.table_name}",
                json={
                    "location": s3_path,
                    "partition_spec": partition_spec,
                    "properties": {
                        "created_by": "quix-lake-sink",
                        "auto_discovered": "false"
                    }
                },
                timeout=30
            )
            
            if create_response.status_code in [200, 201]:
                self.logger.info(
                    "Successfully created table '%s' in REST Catalog with partitions: %s",
                    self.table_name,
                    partition_spec
                )
                self.table_registered = True
            else:
                self.logger.warning(
                    "Failed to create table '%s': %s", 
                    self.table_name, 
                    create_response.text
                )
                
        except Exception as e:
            self.logger.warning("Failed to register table '%s': %s", self.table_name, e)
    
    def _add_timestamp_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add timestamp-based columns that can be used for partitioning"""

        # Convert to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df[self.timestamp_column]):
            # Assume milliseconds if numeric
            sample_value = float(df[self.timestamp_column].iloc[0] if not df[self.timestamp_column].empty else 0)
            
            if sample_value > 1e12:  # Milliseconds
                df[self.timestamp_column] = pd.to_datetime(df[self.timestamp_column], unit='ms')
            elif sample_value > 1e9:   # Seconds  
                df[self.timestamp_column] = pd.to_datetime(df[self.timestamp_column], unit='s')
            else:
                df[self.timestamp_column] = pd.to_datetime(df[self.timestamp_column])
        
        # Add time-based columns based on what's in hive_columns
        timestamp_col = df[self.timestamp_column]
        
        # Only add columns that are specified in ts_hive_columns
        for col in self._ts_hive_columns:
            df[col] = TIMESTAMP_COL_MAPPER[col](timestamp_col)

        return df
    
    def _validate_partition_strategy(self, table_metadata: Dict[str, Any]):
        """Validate that the sink's partition strategy matches the existing table"""
        existing_partition_spec = table_metadata.get("partition_spec", [])
        
        # Build expected partition spec from sink configuration
        expected_partition_spec = self.hive_columns.copy()
        
        # Check if partition strategies match
        if set(existing_partition_spec) != set(expected_partition_spec):
            error_msg = (
                f"Partition strategy mismatch for table '{self.table_name}'. "
                f"Existing table has partitions: {existing_partition_spec}, "
                f"but sink is configured with: {expected_partition_spec}. "
                "This would corrupt the folder structure. Please ensure the sink partition "
                "configuration matches the existing table."
            )
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Also check the order of partitions
        if existing_partition_spec != expected_partition_spec:
            warning_msg = (
                f"Partition column order differs for table '{self.table_name}'. "
                f"Existing: {existing_partition_spec}, Configured: {expected_partition_spec}. "
                "While this won't corrupt data, it may lead to suboptimal query performance."
            )
            self.logger.warning(warning_msg)
    
    def _validate_existing_table_structure(self):
        """Check if table already exists in S3 and validate partition structure"""
        table_prefix = f"{self.s3_prefix}/{self.table_name}/"
        
        try:
            # List objects to see if table exists
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=table_prefix,
                MaxKeys=100
            )
            
            if 'Contents' not in response:
                # Table doesn't exist yet, no validation needed
                return
            
            # Detect existing partition columns from S3 structure
            detected_partition_columns = []
            for obj in response['Contents']:
                if obj['Key'].endswith('.parquet'):
                    # Extract path after table prefix
                    relative_path = obj['Key'][len(table_prefix):]
                    path_parts = relative_path.split('/')
                    
                    # Look for Hive-style partitions (col=value)
                    for part in path_parts[:-1]:  # Exclude filename
                        if '=' in part:
                            col_name = part.split('=')[0]
                            if col_name not in detected_partition_columns:
                                detected_partition_columns.append(col_name)
            
            if detected_partition_columns:
                # Build expected partition spec from sink configuration
                expected_partition_spec = self.hive_columns.copy()
                
                # Check if partition strategies match
                if set(detected_partition_columns) != set(expected_partition_spec):
                    error_msg = (
                        f"Partition strategy mismatch for table '{self.table_name}'. "
                        f"Existing table in S3 has partitions: {detected_partition_columns}, "
                        f"but sink is configured with: {expected_partition_spec}. "
                        "This would corrupt the folder structure. Please ensure the sink partition "
                        "configuration matches the existing table."
                    )
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)
                
                self.logger.info(
                    "Validated partition strategy for existing table '%s'. Partitions: %s",
                    self.table_name,
                    detected_partition_columns
                )
                
        except self.s3_client.exceptions.NoSuchBucket:
            raise
        except ValueError:
            raise
        except Exception as e:
            self.logger.warning(
                "Could not validate existing table structure: %s. Proceeding with caution.", e
            )
    
    def _register_file_in_manifest(self, s3_key: str, row_count: int, 
                                  partition_columns: List[str], partition_values: tuple):
        """Register a newly written file in the catalog manifest"""
        try:
            # Build S3 URL
            file_path = f"s3://{self.s3_bucket}/{s3_key}"
            
            # Get file size
            try:
                response = self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
                file_size = response['ContentLength']
            except:
                file_size = 0
            
            # Build partition values dict
            partition_dict = {}
            if partition_columns and partition_values:
                for col, val in zip(partition_columns, partition_values):
                    partition_dict[col] = str(val)
            
            # Create file entry
            file_entry = {
                "file_path": file_path,
                "file_size": file_size,
                "last_modified": datetime.utcnow().isoformat(),
                "partition_values": partition_dict,
                "row_count": row_count
            }
            
            # Send to catalog
            response = requests.post(
                f"{self.catalog_url}/namespaces/{self.namespace}/tables/{self.table_name}/manifest/add-files",
                json={"files": [file_entry]},
                timeout=5
            )
            
            if response.status_code == 200:
                self.logger.debug("Registered file in manifest: %s", file_path)
            else:
                self.logger.warning("Failed to register file in manifest: %s", response.text)
                
        except Exception as e:
            # Don't fail the write if manifest registration fails
            self.logger.warning("Failed to register file in manifest: %s", e)