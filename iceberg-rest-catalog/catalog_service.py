import os
import json
import boto3
import duckdb
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
import uuid
import logging

logger = logging.getLogger(__name__)

class IcebergCatalogService:
    """
    Lightweight Iceberg-compatible REST catalog service for DuckDB/DuckLake.
    Manages table metadata while data remains in S3 as Hive-partitioned Parquet.
    """
    
    def __init__(self):
        self.s3_bucket = os.getenv('S3_BUCKET', 'quixlake-data')
        self.s3_prefix = os.getenv('S3_PREFIX', 'data')
        self.catalog_prefix = os.getenv('CATALOG_PREFIX', 'catalog')
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        
        # Initialize S3 client
        self.s3_client = boto3.client('s3', region_name=self.aws_region)
        
        # Initialize DuckDB for schema inference
        self.con = duckdb.connect(":memory:")
        self._setup_duckdb()
        
        # In-memory cache for catalog metadata
        self._catalog_cache = {}
        self._load_catalog()
        
        # In-memory cache for manifests with TTL
        self._manifest_cache = {}
        self._manifest_cache_ttl = 300  # 5 minutes
        self._manifest_last_modified = {}
        
        # Batch update settings
        self._pending_manifest_updates = {}
        self._last_manifest_flush = datetime.utcnow()
    
    def _setup_duckdb(self):
        """Setup DuckDB with necessary extensions"""
        self.con.execute("INSTALL httpfs")
        self.con.execute("LOAD httpfs")
        
        # Configure S3 settings
        if os.getenv('AWS_ACCESS_KEY_ID'):
            self.con.execute(f"SET s3_access_key_id='{os.getenv('AWS_ACCESS_KEY_ID')}';")
        if os.getenv('AWS_SECRET_ACCESS_KEY'):
            self.con.execute(f"SET s3_secret_access_key='{os.getenv('AWS_SECRET_ACCESS_KEY')}';")
        self.con.execute(f"SET s3_region='{self.aws_region}';")
    
    def _load_catalog(self):
        """Load catalog metadata from S3"""
        try:
            catalog_key = f"{self.catalog_prefix}/catalog.json"
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=catalog_key)
            self._catalog_cache = json.loads(response['Body'].read())
            logger.info(f"Loaded catalog with {len(self._catalog_cache.get('tables', {}))} tables")
        except self.s3_client.exceptions.NoSuchKey:
            # Initialize empty catalog
            self._catalog_cache = {
                "version": "1.0",
                "tables": {},
                "namespaces": ["default"]
            }
            self._save_catalog()
        except Exception as e:
            logger.error(f"Error loading catalog: {e}")
            self._catalog_cache = {
                "version": "1.0",
                "tables": {},
                "namespaces": ["default"]
            }
    
    def _save_catalog(self):
        """Save catalog metadata to S3"""
        try:
            catalog_key = f"{self.catalog_prefix}/catalog.json"
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=catalog_key,
                Body=json.dumps(self._catalog_cache, indent=2),
                ContentType='application/json'
            )
        except Exception as e:
            logger.error(f"Error saving catalog: {e}")
    
    def list_namespaces(self) -> List[str]:
        """List all namespaces"""
        return self._catalog_cache.get("namespaces", ["default"])
    
    def list_tables(self, namespace: str) -> List[str]:
        """List all tables in a namespace"""
        tables = []
        for table_id, metadata in self._catalog_cache.get("tables", {}).items():
            if metadata.get("namespace") == namespace:
                tables.append(metadata["name"])
        return sorted(tables)
    
    def get_table(self, namespace: str, table_name: str) -> Dict[str, Any]:
        """Get table metadata"""
        table_id = f"{namespace}.{table_name}"
        if table_id not in self._catalog_cache.get("tables", {}):
            raise ValueError(f"Table {table_id} not found")
        
        return self._catalog_cache["tables"][table_id]
    
    def create_or_update_table(self, namespace: str, table_name: str, 
                              location: str, schema: Optional[Dict[str, Any]] = None,
                              partition_spec: List[str] = None, 
                              properties: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create or update table metadata"""
        table_id = f"{namespace}.{table_name}"
        
        # If schema not provided, infer from data
        if not schema:
            schema = self._infer_schema(location)
        
        # Detect partition columns if not provided
        if partition_spec is None:
            partition_spec = self._detect_partition_columns(location)
        
        # Create table metadata
        table_metadata = {
            "name": table_name,
            "namespace": namespace,
            "location": location,
            "schema": schema,
            "partition_spec": partition_spec,
            "properties": properties or {},
            "format_version": 2,
            "last_updated": datetime.utcnow().isoformat(),
            "metadata_location": f"s3://{self.s3_bucket}/{self.catalog_prefix}/tables/{namespace}/{table_name}/metadata.json",
            "manifest_location": f"s3://{self.s3_bucket}/{self.catalog_prefix}/tables/{namespace}/{table_name}/manifest.json"
        }
        
        # Update cache and save
        self._catalog_cache["tables"][table_id] = table_metadata
        self._save_catalog()
        
        # Also save individual table metadata
        self._save_table_metadata(namespace, table_name, table_metadata)
        
        # Initialize empty manifest
        self._initialize_manifest(namespace, table_name)
        
        return table_metadata
    
    def drop_table(self, namespace: str, table_name: str):
        """Drop table from catalog (does not delete data)"""
        table_id = f"{namespace}.{table_name}"
        if table_id in self._catalog_cache.get("tables", {}):
            del self._catalog_cache["tables"][table_id]
            self._save_catalog()
            
            # Remove table metadata file
            try:
                metadata_key = f"{self.catalog_prefix}/tables/{namespace}/{table_name}/metadata.json"
                self.s3_client.delete_object(Bucket=self.s3_bucket, Key=metadata_key)
            except Exception as e:
                logger.warning(f"Could not delete table metadata file: {e}")
    
    def refresh_table(self, namespace: str, table_name: str) -> Dict[str, Any]:
        """Refresh table metadata by re-scanning the data location"""
        table_id = f"{namespace}.{table_name}"
        if table_id not in self._catalog_cache.get("tables", {}):
            raise ValueError(f"Table {table_id} not found")
        
        table_metadata = self._catalog_cache["tables"][table_id]
        location = table_metadata["location"]
        
        # Re-infer schema and partitions
        schema = self._infer_schema(location)
        partition_spec = self._detect_partition_columns(location)
        
        # Update metadata
        table_metadata["schema"] = schema
        table_metadata["partition_spec"] = partition_spec
        table_metadata["last_updated"] = datetime.utcnow().isoformat()
        
        # Save updates
        self._catalog_cache["tables"][table_id] = table_metadata
        self._save_catalog()
        self._save_table_metadata(namespace, table_name, table_metadata)
        
        # Also refresh the manifest
        manifest_result = self.refresh_manifest(namespace, table_name)
        table_metadata["manifest_refreshed"] = manifest_result
        
        return table_metadata
    
    def discover_and_register(self, namespace: str, table_name: str, s3_path: str) -> Dict[str, Any]:
        """Discover table from S3 path and register in catalog"""
        # Validate path exists
        if not self._path_exists(s3_path):
            raise ValueError(f"S3 path does not exist: {s3_path}")
        
        # Infer schema and partitions
        schema = self._infer_schema(s3_path)
        partition_spec = self._detect_partition_columns(s3_path)
        
        # Count files and build manifest
        file_count = 0
        total_size_mb = 0
        manifest_entries = []
        
        path_parts = s3_path.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        prefix = path_parts[1] + '/' if len(path_parts) > 1 else ''
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.parquet'):
                        file_path = f"s3://{bucket}/{obj['Key']}"
                        file_count += 1
                        total_size_mb += obj['Size'] / (1024 * 1024)
                        
                        # Extract partition values from path
                        partition_values = {}
                        relative_path = obj['Key'].replace(prefix, '')
                        path_parts = relative_path.split('/')
                        
                        for part in path_parts[:-1]:  # Exclude filename
                            if '=' in part:
                                key, value = part.split('=', 1)
                                if key in partition_spec:
                                    partition_values[key] = value
                        
                        entry = {
                            "file_path": file_path,
                            "file_size": obj['Size'],
                            "last_modified": obj['LastModified'].isoformat(),
                            "partition_values": partition_values
                        }
                        manifest_entries.append(entry)
        
        # Register table
        table_metadata = self.create_or_update_table(
            namespace=namespace,
            table_name=table_name,
            location=s3_path,
            schema=schema,
            partition_spec=partition_spec,
            properties={
                "file_count": str(file_count),
                "total_size_mb": str(round(total_size_mb, 2))
            }
        )
        
        # Save the manifest with all discovered files
        manifest = {
            "version": 1,
            "table": f"{namespace}.{table_name}",
            "entries": manifest_entries,
            "last_updated": datetime.utcnow().isoformat(),
            "file_count": file_count,
            "total_size_bytes": int(total_size_mb * 1024 * 1024)
        }
        self._save_manifest(namespace, table_name, manifest)
        
        return {
            "table_metadata": table_metadata,
            "discovery_result": {
                "file_count": file_count,
                "total_size_mb": round(total_size_mb, 2),
                "partition_columns": partition_spec
            }
        }
    
    def _save_table_metadata(self, namespace: str, table_name: str, metadata: Dict[str, Any]):
        """Save individual table metadata file"""
        try:
            metadata_key = f"{self.catalog_prefix}/tables/{namespace}/{table_name}/metadata.json"
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2),
                ContentType='application/json'
            )
        except Exception as e:
            logger.error(f"Error saving table metadata: {e}")
    
    def _infer_schema(self, location: str) -> Dict[str, Any]:
        """Infer schema from Parquet files"""
        try:
            # Find first parquet file
            path_parts = location.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] + '/' if len(path_parts) > 1 else ''
            
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=10)
            first_file = None
            for obj in response.get('Contents', []):
                if obj['Key'].endswith('.parquet'):
                    first_file = f"s3://{bucket}/{obj['Key']}"
                    break
            
            if not first_file:
                raise ValueError("No Parquet files found to infer schema")
            
            # Use DuckDB to get schema
            schema_df = self.con.execute(f"""
                SELECT * FROM read_parquet('{first_file}') 
                WHERE 1=0
            """).fetch_df()
            
            # Convert to Iceberg-style schema
            fields = []
            for col_name, dtype in zip(schema_df.columns, schema_df.dtypes):
                field = {
                    "id": len(fields) + 1,
                    "name": col_name,
                    "type": self._pandas_to_iceberg_type(str(dtype)),
                    "required": False
                }
                fields.append(field)
            
            return {
                "type": "struct",
                "fields": fields
            }
            
        except Exception as e:
            logger.error(f"Error inferring schema: {e}")
            return {"type": "struct", "fields": []}
    
    def _pandas_to_iceberg_type(self, pandas_type: str) -> str:
        """Convert pandas dtype to Iceberg type"""
        type_mapping = {
            "int64": "long",
            "int32": "int",
            "float64": "double",
            "float32": "float",
            "object": "string",
            "bool": "boolean",
            "datetime64[ns]": "timestamp",
            "timedelta64[ns]": "long"
        }
        
        for pandas_t, iceberg_t in type_mapping.items():
            if pandas_type.startswith(pandas_t):
                return iceberg_t
        
        return "string"  # Default fallback
    
    def _detect_partition_columns(self, location: str) -> List[str]:
        """Detect Hive-style partition columns from directory structure"""
        partition_columns = []
        
        try:
            path_parts = location.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] + '/' if len(path_parts) > 1 else ''
            
            # List some objects to detect partitioning pattern
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=100)
            
            for obj in response.get('Contents', []):
                key = obj['Key']
                # Look for Hive-style partitions (col=value)
                parts = key.split('/')
                for part in parts:
                    if '=' in part and not part.endswith('.parquet'):
                        col_name = part.split('=')[0]
                        if col_name not in partition_columns:
                            partition_columns.append(col_name)
            
        except Exception as e:
            logger.error(f"Error detecting partitions: {e}")
        
        return partition_columns
    
    def _path_exists(self, path: str) -> bool:
        """Check if S3 path exists"""
        if path.startswith('s3://'):
            path_parts = path.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ''
            try:
                response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
                return 'Contents' in response
            except Exception:
                return False
        return False
    
    def _initialize_manifest(self, namespace: str, table_name: str):
        """Initialize empty manifest for a table"""
        manifest = {
            "version": 1,
            "table": f"{namespace}.{table_name}",
            "entries": [],
            "last_updated": datetime.utcnow().isoformat()
        }
        self._save_manifest(namespace, table_name, manifest)
    
    def _save_manifest(self, namespace: str, table_name: str, manifest: Dict[str, Any]):
        """Save manifest to S3 and update cache"""
        try:
            manifest_key = f"{self.catalog_prefix}/tables/{namespace}/{table_name}/manifest.json"
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=manifest_key,
                Body=json.dumps(manifest, indent=2),
                ContentType='application/json'
            )
            
            # Update cache and last modified
            cache_key = f"{namespace}.{table_name}"
            self._manifest_cache[cache_key] = (manifest, datetime.utcnow())
            self._manifest_last_modified[cache_key] = datetime.utcnow()
            
        except Exception as e:
            logger.error(f"Error saving manifest: {e}")
    
    def _load_manifest(self, namespace: str, table_name: str) -> Dict[str, Any]:
        """Load manifest from S3 with caching"""
        cache_key = f"{namespace}.{table_name}"
        now = datetime.utcnow()
        
        # Check cache first
        if cache_key in self._manifest_cache:
            cached_manifest, cached_time = self._manifest_cache[cache_key]
            age_seconds = (now - cached_time).total_seconds()
            
            # Return cached version if still fresh
            if age_seconds < self._manifest_cache_ttl:
                logger.debug(f"Returning cached manifest for {cache_key} (age: {age_seconds:.1f}s)")
                return cached_manifest
        
        try:
            manifest_key = f"{self.catalog_prefix}/tables/{namespace}/{table_name}/manifest.json"
            
            # Check if we need to reload based on S3 last modified
            try:
                head_response = self.s3_client.head_object(Bucket=self.s3_bucket, Key=manifest_key)
                s3_last_modified = head_response['LastModified']
                
                # If we have a cached version and S3 hasn't changed, extend cache
                if cache_key in self._manifest_last_modified:
                    if s3_last_modified <= self._manifest_last_modified[cache_key]:
                        if cache_key in self._manifest_cache:
                            # Update cache timestamp to extend TTL
                            cached_manifest, _ = self._manifest_cache[cache_key]
                            self._manifest_cache[cache_key] = (cached_manifest, now)
                            return cached_manifest
                
                self._manifest_last_modified[cache_key] = s3_last_modified
            except:
                pass
            
            # Load from S3
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=manifest_key)
            manifest = json.loads(response['Body'].read())
            
            # Cache the manifest
            self._manifest_cache[cache_key] = (manifest, now)
            
            return manifest
            
        except self.s3_client.exceptions.NoSuchKey:
            # Return empty manifest if not found
            empty_manifest = {
                "version": 1,
                "table": f"{namespace}.{table_name}",
                "entries": [],
                "last_updated": datetime.utcnow().isoformat()
            }
            # Cache empty manifest too
            self._manifest_cache[cache_key] = (empty_manifest, now)
            return empty_manifest
            
        except Exception as e:
            logger.error(f"Error loading manifest: {e}")
            empty_manifest = {
                "version": 1,
                "table": f"{namespace}.{table_name}",
                "entries": [],
                "last_updated": datetime.utcnow().isoformat()
            }
            return empty_manifest
    
    def add_files_to_manifest(self, namespace: str, table_name: str, file_entries: List[Dict[str, Any]]):
        """Add files to table manifest with batching"""
        cache_key = f"{namespace}.{table_name}"
        
        # Add to pending updates
        if cache_key not in self._pending_manifest_updates:
            self._pending_manifest_updates[cache_key] = []
        
        self._pending_manifest_updates[cache_key].extend(file_entries)
        
        # Check if we should flush (every 10 seconds or 100 files)
        should_flush = False
        if len(self._pending_manifest_updates[cache_key]) >= 100:
            should_flush = True
        elif (datetime.utcnow() - self._last_manifest_flush).total_seconds() > 10:
            should_flush = True
        
        if should_flush:
            self._flush_manifest_updates(namespace, table_name)
    
    def _flush_manifest_updates(self, namespace: str = None, table_name: str = None):
        """Flush pending manifest updates to S3"""
        if namespace and table_name:
            # Flush specific table
            cache_key = f"{namespace}.{table_name}"
            if cache_key in self._pending_manifest_updates:
                self._flush_single_manifest(namespace, table_name)
        else:
            # Flush all pending updates
            for cache_key in list(self._pending_manifest_updates.keys()):
                parts = cache_key.split('.')
                if len(parts) == 2:
                    self._flush_single_manifest(parts[0], parts[1])
        
        self._last_manifest_flush = datetime.utcnow()
    
    def _flush_single_manifest(self, namespace: str, table_name: str):
        """Flush a single manifest to S3"""
        cache_key = f"{namespace}.{table_name}"
        if cache_key not in self._pending_manifest_updates:
            return
        
        pending_entries = self._pending_manifest_updates[cache_key]
        if not pending_entries:
            return
        
        try:
            # Load current manifest (from cache if possible)
            manifest = self._load_manifest(namespace, table_name)
            
            # Create a map for faster lookups
            existing_files = {entry["file_path"]: i for i, entry in enumerate(manifest["entries"])}
            
            # Add new entries efficiently
            for entry in pending_entries:
                file_path = entry["file_path"]
                if file_path in existing_files:
                    # Update existing entry
                    manifest["entries"][existing_files[file_path]] = entry
                else:
                    # Add new entry
                    manifest["entries"].append(entry)
                    existing_files[file_path] = len(manifest["entries"]) - 1
            
            manifest["last_updated"] = datetime.utcnow().isoformat()
            
            # Save to S3
            self._save_manifest(namespace, table_name, manifest)
            
            # Update cache
            self._manifest_cache[cache_key] = (manifest, datetime.utcnow())
            
            # Clear pending updates
            del self._pending_manifest_updates[cache_key]
            
            logger.info(f"Flushed {len(pending_entries)} entries to manifest {cache_key}")
            
        except Exception as e:
            logger.error(f"Error flushing manifest {cache_key}: {e}")
    
    def get_manifest(self, namespace: str, table_name: str) -> Dict[str, Any]:
        """Get table manifest"""
        return self._load_manifest(namespace, table_name)
    
    def get_files_for_query(self, namespace: str, table_name: str, 
                           partition_filters: Dict[str, Any] = None) -> List[str]:
        """Get list of files matching partition filters (optimized)"""
        # Ensure pending updates are included
        cache_key = f"{namespace}.{table_name}"
        if cache_key in self._pending_manifest_updates and self._pending_manifest_updates[cache_key]:
            # If we have pending updates, flush them first to ensure query accuracy
            self._flush_single_manifest(namespace, table_name)
        
        manifest = self._load_manifest(namespace, table_name)
        
        if not partition_filters:
            # Return all files
            return [entry["file_path"] for entry in manifest["entries"]]
        
        # Filter files based on partition values
        matching_files = []
        for entry in manifest["entries"]:
            partition_values = entry.get("partition_values", {})
            
            # Check if all filters match
            match = True
            for key, value in partition_filters.items():
                entry_value = str(partition_values.get(key, ""))
                
                if isinstance(value, list):
                    # Multiple values - treat as OR condition
                    if not any(entry_value == str(v) for v in value):
                        match = False
                        break
                else:
                    # Single value
                    if entry_value != str(value):
                        match = False
                        break
            
            if match:
                matching_files.append(entry["file_path"])
        
        return matching_files
    
    def refresh_manifest(self, namespace: str, table_name: str) -> Dict[str, Any]:
        """Refresh manifest by scanning S3"""
        table_id = f"{namespace}.{table_name}"
        if table_id not in self._catalog_cache.get("tables", {}):
            raise ValueError(f"Table {table_id} not found")
        
        table_metadata = self._catalog_cache["tables"][table_id]
        location = table_metadata["location"]
        partition_spec = table_metadata.get("partition_spec", [])
        
        # Scan S3 for all parquet files
        manifest_entries = []
        file_count = 0
        total_size = 0
        
        path_parts = location.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        prefix = path_parts[1] + '/' if len(path_parts) > 1 else ''
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.parquet'):
                        file_path = f"s3://{bucket}/{obj['Key']}"
                        file_count += 1
                        total_size += obj['Size']
                        
                        # Extract partition values from path
                        partition_values = {}
                        relative_path = obj['Key'].replace(prefix, '')
                        path_parts = relative_path.split('/')
                        
                        for part in path_parts[:-1]:  # Exclude filename
                            if '=' in part:
                                key, value = part.split('=', 1)
                                if key in partition_spec:
                                    partition_values[key] = value
                        
                        entry = {
                            "file_path": file_path,
                            "file_size": obj['Size'],
                            "last_modified": obj['LastModified'].isoformat(),
                            "partition_values": partition_values
                        }
                        manifest_entries.append(entry)
        
        # Save updated manifest
        manifest = {
            "version": 1,
            "table": f"{namespace}.{table_name}",
            "entries": manifest_entries,
            "last_updated": datetime.utcnow().isoformat(),
            "file_count": file_count,
            "total_size_bytes": total_size
        }
        self._save_manifest(namespace, table_name, manifest)
        
        return {
            "files_discovered": file_count,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "manifest_updated": True
        }