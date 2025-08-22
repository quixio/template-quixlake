import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
import duckdb
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
import uuid
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class PostgresCatalogService:
    """
    PostgreSQL-based Iceberg-compatible REST catalog service.
    Stores catalog metadata and manifests in PostgreSQL instead of S3.
    """
    
    def __init__(self):
        # Database configuration
        self.db_host = os.environ['POSTGRES_HOST']
        self.db_port = int(os.getenv('POSTGRES_PORT', '5432'))
        self.db_name = os.getenv('POSTGRES_DB', 'iceberg_catalog')
        self.db_user = os.getenv('POSTGRES_USER', 'postgres')
        self.db_password = os.environ['POSTGRES_PASSWORD']
        
        # S3 configuration for data files (catalog metadata in PostgreSQL, data files still in S3)
        self.s3_bucket = os.getenv('S3_BUCKET', 'quixlake-data')
        self.s3_prefix = os.getenv('S3_PREFIX', 'data')
        self._aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self._aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self._aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self._aws_endpoint_url = os.getenv("AWS_ENDPOINT_URL", None)
        if (self._aws_endpoint_url or '').startswith('http://'):
            self._duckdb_aws_ssl = "false"
        else:
            self._duckdb_aws_ssl = "true"
        self._credentials = {
            "region_name": self._aws_region,
            "aws_access_key_id": self._aws_access_key_id,
            "aws_secret_access_key": self._aws_secret_access_key,
            "endpoint_url": self._aws_endpoint_url,
        }
        
        # Ensure database exists
        self._ensure_database_exists()
        
        # Initialize connection pool
        self.pool = ThreadedConnectionPool(
            minconn=2,
            maxconn=20,
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
            user=self.db_user,
            password=self.db_password
        )
        
        # Initialize DuckDB for schema inference
        self.con = duckdb.connect(":memory:")
        self._setup_duckdb()
        
        # Initialize database schema
        self._init_database()
        
        # Log successful initialization
        logger.info("PostgresCatalogService initialized successfully")
        
        # In-memory cache for performance
        self._table_cache = {}
        self._cache_ttl = 60  # 1 minute
        
    def _setup_duckdb(self):
        """Setup DuckDB with necessary extensions"""
        self.con.execute("INSTALL httpfs")
        self.con.execute("LOAD httpfs")
        
        # Configure S3 settings
        if url := self._aws_endpoint_url:  # non-AWS server, like MinIO
            # httpfs ALWAYS appends a scheme, based on s3_use_ssl (=true by default)
            if url.startswith('http'):
                url = url.split("//", 1)[1]
            self.con.execute(f"SET s3_endpoint='{url}';")
            self.con.execute("SET s3_url_style='path';")
            self.con.execute(f"SET s3_use_ssl={self._duckdb_aws_ssl}")
        if self._aws_access_key_id:
            self.con.execute(f"SET s3_access_key_id='{self._aws_access_key_id}';")
            self.con.execute(f"SET s3_secret_access_key='{self._aws_secret_access_key}';")
        self.con.execute(f"SET s3_region='{self._aws_region}';")
    
    def _ensure_database_exists(self):
        """Create the database if it doesn't exist"""
        conn = None
        try:
            # Connect to the default 'postgres' database to create our target database
            conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database='postgres',  # Connect to default database
                user=self.db_user,
                password=self.db_password
            )
            conn.autocommit = True  # Required for CREATE DATABASE
            
            with conn.cursor() as cur:
                # Check if database exists
                cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (self.db_name,)
                )
                
                if not cur.fetchone():
                    # Database doesn't exist, create it
                    logger.info(f"Creating database '{self.db_name}'...")
                    cur.execute(f'CREATE DATABASE "{self.db_name}"')
                    logger.info(f"Database '{self.db_name}' created successfully")
                else:
                    logger.info(f"Database '{self.db_name}' already exists")
                    
        except psycopg2.OperationalError as e:
            if "could not connect to server" in str(e):
                logger.error(f"Cannot connect to PostgreSQL server at {self.db_host}:{self.db_port}")
                logger.error("Please ensure PostgreSQL is running and accessible")
            elif "password authentication failed" in str(e):
                logger.error(f"Authentication failed for user '{self.db_user}'")
                logger.error("Please check your PostgreSQL credentials")
            else:
                logger.error(f"Operational error: {e}")
            raise
        except psycopg2.ProgrammingError as e:
            if "permission denied to create database" in str(e):
                logger.error(f"User '{self.db_user}' does not have permission to create databases")
                logger.error("Please grant CREATE DATABASE permission or create the database manually")
            else:
                logger.error(f"Programming error: {e}")
            raise
        except psycopg2.Error as e:
            logger.error(f"Error ensuring database exists: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    @contextmanager
    def get_db_connection(self):
        """Get a database connection from the pool"""
        conn = self.pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            self.pool.putconn(conn)
    
    def _init_database(self):
        """Initialize database schema"""
        try:
            logger.info("Initializing database schema...")
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Create namespaces table
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS namespaces (
                            id SERIAL PRIMARY KEY,
                            name VARCHAR(255) UNIQUE NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # Create tables table
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS tables (
                            id SERIAL PRIMARY KEY,
                            namespace_id INTEGER REFERENCES namespaces(id) ON DELETE CASCADE,
                            name VARCHAR(255) NOT NULL,
                            location TEXT NOT NULL,
                            schema JSONB,
                            partition_spec JSONB,
                            properties JSONB,
                            format_version INTEGER DEFAULT 2,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(namespace_id, name)
                        )
                    """)
                    
                    # Create manifest_entries table
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS manifest_entries (
                            id SERIAL PRIMARY KEY,
                            table_id INTEGER REFERENCES tables(id) ON DELETE CASCADE,
                            file_path TEXT NOT NULL,
                            file_size BIGINT,
                            row_count INTEGER,
                            partition_values JSONB,
                            last_modified TIMESTAMP,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(table_id, file_path)
                        )
                    """)
                    
                    # Create indexes for performance
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_manifest_entries_table_id 
                        ON manifest_entries(table_id)
                    """)
                    
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_manifest_entries_partition_values 
                        ON manifest_entries USING GIN (partition_values)
                    """)
                    
                    # Additional indexes for performance
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_manifest_entries_table_partition 
                        ON manifest_entries(table_id, partition_values)
                    """)
                    
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_tables_namespace_name 
                        ON tables(namespace_id, name)
                    """)
                    
                    # Insert default namespace if not exists
                    cur.execute("""
                        INSERT INTO namespaces (name) 
                        VALUES ('default') 
                        ON CONFLICT (name) DO NOTHING
                    """)
                    
                    # Create a trigger to update the updated_at column
                    cur.execute("""
                        CREATE OR REPLACE FUNCTION update_updated_at_column()
                        RETURNS TRIGGER AS $$
                        BEGIN
                            NEW.updated_at = CURRENT_TIMESTAMP;
                            RETURN NEW;
                        END;
                        $$ language 'plpgsql';
                    """)
                    
                    cur.execute("""
                        DROP TRIGGER IF EXISTS update_tables_updated_at ON tables;
                        CREATE TRIGGER update_tables_updated_at 
                        BEFORE UPDATE ON tables 
                        FOR EACH ROW 
                        EXECUTE FUNCTION update_updated_at_column();
                    """)
                
            logger.info("Database schema initialized successfully")
            
            # Verify tables were created
            self._verify_database_schema()
            
        except Exception as e:
            logger.error(f"Error initializing database schema: {e}")
            raise
    
    def _verify_database_schema(self):
        """Verify that all required tables exist"""
        required_tables = ['namespaces', 'tables', 'manifest_entries']
        
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                """)
                
                existing_tables = [row[0] for row in cur.fetchall()]
                logger.info(f"Existing tables in database: {existing_tables}")
                
                missing_tables = [t for t in required_tables if t not in existing_tables]
                if missing_tables:
                    raise RuntimeError(f"Required tables missing: {missing_tables}")
                
                logger.info("All required tables verified successfully")
    
    def list_namespaces(self) -> List[str]:
        """List all namespaces"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM namespaces ORDER BY name")
                return [row[0] for row in cur.fetchall()]
    
    def list_tables(self, namespace: str) -> List[str]:
        """List all tables in a namespace"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT t.name 
                    FROM tables t
                    JOIN namespaces n ON t.namespace_id = n.id
                    WHERE n.name = %s
                    ORDER BY t.name
                """, (namespace,))
                return [row[0] for row in cur.fetchall()]
    
    def get_table(self, namespace: str, table_name: str) -> Dict[str, Any]:
        """Get table metadata"""
        # Check cache first
        cache_key = f"{namespace}.{table_name}"
        if cache_key in self._table_cache:
            cached_data, cached_time = self._table_cache[cache_key]
            if (datetime.utcnow() - cached_time).total_seconds() < self._cache_ttl:
                return cached_data
        
        with self.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT t.*, n.name as namespace_name
                    FROM tables t
                    JOIN namespaces n ON t.namespace_id = n.id
                    WHERE n.name = %s AND t.name = %s
                """, (namespace, table_name))
                
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"Table {namespace}.{table_name} not found")
                
                # Format metadata
                metadata = {
                    "name": row["name"],
                    "namespace": row["namespace_name"],
                    "location": row["location"],
                    "schema": row["schema"],
                    "partition_spec": row["partition_spec"] or [],
                    "properties": row["properties"] or {},
                    "format_version": row["format_version"],
                    "last_updated": row["updated_at"].isoformat(),
                    "metadata_location": f"postgres://catalog/tables/{namespace}/{table_name}/metadata",
                    "manifest_location": f"postgres://catalog/tables/{namespace}/{table_name}/manifest"
                }
                
                # Cache the result
                self._table_cache[cache_key] = (metadata, datetime.utcnow())
                
                return metadata
    
    def create_or_update_table(self, namespace: str, table_name: str, 
                              location: str, schema: Optional[Dict[str, Any]] = None,
                              partition_spec: List[str] = None, 
                              properties: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create or update table metadata"""
        # If schema not provided, infer from data
        if not schema:
            schema = self._infer_schema(location)
        
        # Detect partition columns if not provided
        if partition_spec is None:
            partition_spec = self._detect_partition_columns(location)
        
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get namespace id
                cur.execute("SELECT id FROM namespaces WHERE name = %s", (namespace,))
                namespace_row = cur.fetchone()
                if not namespace_row:
                    # Create namespace if it doesn't exist
                    cur.execute("INSERT INTO namespaces (name) VALUES (%s) RETURNING id", (namespace,))
                    namespace_id = cur.fetchone()[0]
                else:
                    namespace_id = namespace_row[0]
                
                # Upsert table
                cur.execute("""
                    INSERT INTO tables (namespace_id, name, location, schema, partition_spec, properties)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (namespace_id, name) 
                    DO UPDATE SET 
                        location = EXCLUDED.location,
                        schema = EXCLUDED.schema,
                        partition_spec = EXCLUDED.partition_spec,
                        properties = EXCLUDED.properties,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                """, (
                    namespace_id, 
                    table_name, 
                    location,
                    json.dumps(schema),
                    json.dumps(partition_spec),
                    json.dumps(properties or {})
                ))
                
                table_id = cur.fetchone()[0]
        
        # Clear cache
        cache_key = f"{namespace}.{table_name}"
        if cache_key in self._table_cache:
            del self._table_cache[cache_key]
        
        # Return the metadata
        return self.get_table(namespace, table_name)
    
    def drop_table(self, namespace: str, table_name: str):
        """Drop table from catalog (does not delete data)"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM tables 
                    WHERE namespace_id = (SELECT id FROM namespaces WHERE name = %s)
                    AND name = %s
                """, (namespace, table_name))
                
                if cur.rowcount == 0:
                    raise ValueError(f"Table {namespace}.{table_name} not found")
        
        # Clear cache
        cache_key = f"{namespace}.{table_name}"
        if cache_key in self._table_cache:
            del self._table_cache[cache_key]
    
    def refresh_table(self, namespace: str, table_name: str) -> Dict[str, Any]:
        """Refresh table metadata by re-scanning the data location"""
        # Get current table metadata
        table_metadata = self.get_table(namespace, table_name)
        location = table_metadata["location"]
        
        # Re-infer schema and partitions
        schema = self._infer_schema(location)
        partition_spec = self._detect_partition_columns(location)
        
        # Update table
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE tables 
                    SET schema = %s, partition_spec = %s
                    WHERE namespace_id = (SELECT id FROM namespaces WHERE name = %s)
                    AND name = %s
                """, (
                    json.dumps(schema),
                    json.dumps(partition_spec),
                    namespace,
                    table_name
                ))
        
        # Clear cache
        cache_key = f"{namespace}.{table_name}"
        if cache_key in self._table_cache:
            del self._table_cache[cache_key]
        
        # Also refresh the manifest
        manifest_result = self.refresh_manifest(namespace, table_name)
        
        # Get updated metadata
        updated_metadata = self.get_table(namespace, table_name)
        updated_metadata["manifest_refreshed"] = manifest_result
        
        return updated_metadata
    
    def discover_and_register(self, namespace: str, table_name: str, s3_path: str) -> Dict[str, Any]:
        """Discover table from S3 path and register in catalog"""
        import boto3
        
        # Initialize S3 client
        s3_client = boto3.client('s3', **self._credentials)
        
        # Validate path exists
        if not self._path_exists(s3_path, s3_client):
            raise ValueError(f"S3 path does not exist: {s3_path}")
        
        # Infer schema and partitions
        schema = self._infer_schema(s3_path)
        partition_spec = self._detect_partition_columns(s3_path, s3_client)
        
        # Count files and prepare manifest entries
        file_count = 0
        total_size_mb = 0
        manifest_entries = []
        
        path_parts = s3_path.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        prefix = path_parts[1] + '/' if len(path_parts) > 1 else ''
        
        paginator = s3_client.get_paginator('list_objects_v2')
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
                        
                        manifest_entries.append({
                            "file_path": file_path,
                            "file_size": obj['Size'],
                            "last_modified": obj['LastModified'],
                            "partition_values": partition_values
                        })
        
        # Register table
        properties = {
            "file_count": str(file_count),
            "total_size_mb": str(round(total_size_mb, 2))
        }
        
        table_metadata = self.create_or_update_table(
            namespace=namespace,
            table_name=table_name,
            location=s3_path,
            schema=schema,
            partition_spec=partition_spec,
            properties=properties
        )
        
        # Get table id and insert manifest entries
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get table id
                cur.execute("""
                    SELECT t.id 
                    FROM tables t
                    JOIN namespaces n ON t.namespace_id = n.id
                    WHERE n.name = %s AND t.name = %s
                """, (namespace, table_name))
                
                table_id = cur.fetchone()[0]
                
                # Insert manifest entries
                for entry in manifest_entries:
                    cur.execute("""
                        INSERT INTO manifest_entries 
                        (table_id, file_path, file_size, partition_values, last_modified)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (table_id, file_path) DO UPDATE SET
                            file_size = EXCLUDED.file_size,
                            partition_values = EXCLUDED.partition_values,
                            last_modified = EXCLUDED.last_modified
                    """, (
                        table_id,
                        entry["file_path"],
                        entry["file_size"],
                        json.dumps(entry["partition_values"]),
                        entry["last_modified"]
                    ))
        
        return {
            "table_metadata": table_metadata,
            "discovery_result": {
                "file_count": file_count,
                "total_size_mb": round(total_size_mb, 2),
                "partition_columns": partition_spec
            }
        }
    
    def add_files_to_manifest(self, namespace: str, table_name: str, file_entries: List[Dict[str, Any]]):
        """Add files to table manifest"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get table id and current properties
                cur.execute("""
                    SELECT t.id, t.properties
                    FROM tables t
                    JOIN namespaces n ON t.namespace_id = n.id
                    WHERE n.name = %s AND t.name = %s
                """, (namespace, table_name))
                
                result = cur.fetchone()
                if not result:
                    raise ValueError(f"Table {namespace}.{table_name} not found")
                
                table_id = result[0]
                current_properties = result[1] or {}
                
                # Insert or update entries
                for entry in file_entries:
                    cur.execute("""
                        INSERT INTO manifest_entries 
                        (table_id, file_path, file_size, row_count, partition_values, last_modified)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (table_id, file_path) DO UPDATE SET
                            file_size = EXCLUDED.file_size,
                            row_count = EXCLUDED.row_count,
                            partition_values = EXCLUDED.partition_values,
                            last_modified = EXCLUDED.last_modified
                    """, (
                        table_id,
                        entry["file_path"],
                        entry.get("file_size"),
                        entry.get("row_count"),
                        json.dumps(entry.get("partition_values", {})),
                        entry.get("last_modified", datetime.utcnow())
                    ))
                
                # Update table file_count in properties
                cur.execute("""
                    SELECT COUNT(*) as total_count, 
                           COALESCE(SUM(file_size), 0) as total_size
                    FROM manifest_entries
                    WHERE table_id = %s
                """, (table_id,))
                
                stats = cur.fetchone()
                new_file_count = stats[0]
                total_size_mb = stats[1] / (1024 * 1024) if stats[1] else 0
                
                # Update properties with new file count
                current_properties['file_count'] = str(new_file_count)
                current_properties['total_size_mb'] = str(round(total_size_mb, 2))
                
                cur.execute("""
                    UPDATE tables 
                    SET properties = %s
                    WHERE id = %s
                """, (json.dumps(current_properties), table_id))
                
                # Clear cache for this table
                cache_key = f"{namespace}.{table_name}"
                if cache_key in self._table_cache:
                    del self._table_cache[cache_key]
    
    def get_manifest(self, namespace: str, table_name: str, 
                     limit: int = None, offset: int = 0,
                     partition_filter: Dict[str, Any] = None) -> Dict[str, Any]:
        """Get table manifest with optional pagination and filtering"""
        with self.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get table id
                cur.execute("""
                    SELECT t.id, t.partition_spec
                    FROM tables t
                    JOIN namespaces n ON t.namespace_id = n.id
                    WHERE n.name = %s AND t.name = %s
                """, (namespace, table_name))
                
                result = cur.fetchone()
                if not result:
                    raise ValueError(f"Table {namespace}.{table_name} not found")
                
                table_id = result["id"]
                partition_spec = result["partition_spec"] or []
                
                # Build query with optional partition filtering
                query_parts = ["SELECT file_path, file_size, row_count, partition_values, last_modified",
                              "FROM manifest_entries",
                              "WHERE table_id = %s"]
                params = [table_id]
                
                # Add partition filtering if specified
                if partition_filter:
                    for key, value in partition_filter.items():
                        if isinstance(value, list):
                            # Handle multiple values (IN clause)
                            placeholders = ','.join(['%s'] * len(value))
                            query_parts.append(f"AND partition_values->'{key}' IN ({placeholders})")
                            params.extend([str(v) for v in value])
                        else:
                            query_parts.append(f"AND partition_values->>'{key}' = %s")
                            params.append(str(value))
                
                # Get total count for pagination
                count_query = query_parts.copy()
                count_query[0] = "SELECT COUNT(*)"
                cur.execute(" ".join(count_query), params)
                total_count = cur.fetchone()["count"]
                
                # Add ordering and pagination
                query_parts.append("ORDER BY file_path")
                if limit:
                    query_parts.append(f"LIMIT {limit} OFFSET {offset}")
                
                # Execute main query
                cur.execute(" ".join(query_parts), params)
                
                entries = []
                total_size = 0
                for row in cur.fetchall():
                    entry = {
                        "file_path": row["file_path"],
                        "file_size": row["file_size"],
                        "row_count": row["row_count"],
                        "partition_values": row["partition_values"] or {},
                        "last_modified": row["last_modified"].isoformat() if row["last_modified"] else None
                    }
                    entries.append(entry)
                    if row["file_size"]:
                        total_size += row["file_size"]
                
                # For full manifest (no limit), get total stats from database
                if not limit and total_count > 1000:
                    cur.execute("""
                        SELECT COUNT(*) as count, COALESCE(SUM(file_size), 0) as total_size
                        FROM manifest_entries
                        WHERE table_id = %s
                    """, (table_id,))
                    stats = cur.fetchone()
                    total_count = stats["count"]
                    total_size = stats["total_size"]
                
                return {
                    "version": 1,
                    "table": f"{namespace}.{table_name}",
                    "entries": entries,
                    "last_updated": datetime.utcnow().isoformat(),
                    "file_count": len(entries) if limit else total_count,
                    "total_size_bytes": total_size,
                    "pagination": {
                        "total": total_count,
                        "limit": limit,
                        "offset": offset
                    } if limit else None,
                    "partition_spec": partition_spec
                }
    
    def get_partitions(self, namespace: str, table_name: str) -> List[Dict[str, Any]]:
        """Get unique partition values for lazy loading (optimized)"""
        with self.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get table id and partition spec
                cur.execute("""
                    SELECT t.id, t.partition_spec
                    FROM tables t
                    JOIN namespaces n ON t.namespace_id = n.id
                    WHERE n.name = %s AND t.name = %s
                """, (namespace, table_name))
                
                result = cur.fetchone()
                if not result:
                    raise ValueError(f"Table {namespace}.{table_name} not found")
                
                table_id = result["id"]
                partition_spec = result["partition_spec"] or []
                
                if not partition_spec:
                    return []
                
                # Get distinct partition values using PostgreSQL JSONB features
                cur.execute("""
                    SELECT DISTINCT partition_values
                    FROM manifest_entries
                    WHERE table_id = %s
                    AND partition_values IS NOT NULL
                    AND partition_values != '{}'::jsonb
                    ORDER BY partition_values
                """, (table_id,))
                
                partitions = []
                for row in cur.fetchall():
                    partitions.append(row["partition_values"])
                
                return partitions
    
    def remove_files_from_manifest(self, namespace: str, table_name: str, file_paths: List[str]):
        """Remove specific files from table manifest"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get table id
                cur.execute("""
                    SELECT t.id 
                    FROM tables t
                    JOIN namespaces n ON t.namespace_id = n.id
                    WHERE n.name = %s AND t.name = %s
                """, (namespace, table_name))
                
                result = cur.fetchone()
                if not result:
                    raise ValueError(f"Table {namespace}.{table_name} not found")
                
                table_id = result[0]
                
                # Delete entries for the specified file paths
                for file_path in file_paths:
                    cur.execute("""
                        DELETE FROM manifest_entries 
                        WHERE table_id = %s AND file_path = %s
                    """, (table_id, file_path))
                
                # Update table properties with new file count and size
                cur.execute("""
                    WITH stats AS (
                        SELECT 
                            COUNT(*) as file_count,
                            COALESCE(SUM(file_size), 0) as total_size
                        FROM manifest_entries
                        WHERE table_id = %s
                    )
                    UPDATE tables
                    SET properties = properties || 
                        jsonb_build_object(
                            'file_count', stats.file_count::text,
                            'total_size_mb', (stats.total_size / 1048576.0)::text
                        )
                    FROM stats
                    WHERE id = %s
                """, (table_id, table_id))
    
    def get_partition_hierarchy(self, namespace: str, table_name: str, path: str = '', lazy: bool = True) -> Dict[str, Any]:
        """Get partition hierarchy for UI tree view (optimized for lazy loading)"""
        with self.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get table id and partition spec
                cur.execute("""
                    SELECT t.id, t.partition_spec
                    FROM tables t
                    JOIN namespaces n ON t.namespace_id = n.id
                    WHERE n.name = %s AND t.name = %s
                """, (namespace, table_name))
                
                result = cur.fetchone()
                if not result:
                    raise ValueError(f"Table {namespace}.{table_name} not found")
                
                table_id = result["id"]
                partition_spec = result["partition_spec"] or []
                
                if not partition_spec:
                    return {"partitions": [], "has_data": True}
                
                # Parse the path to get current partition filters
                current_filters = {}
                if path:
                    parts = path.split('/')
                    for part in parts:
                        if '=' in part:
                            key, value = part.split('=', 1)
                            current_filters[key] = value
                
                # Determine the next partition column to query
                current_depth = len(current_filters)
                if current_depth >= len(partition_spec):
                    # We're at the leaf level
                    return {"partitions": [], "has_data": True}
                
                next_column = partition_spec[current_depth]
                
                # Build query to get distinct values for the next partition level
                query = """
                    SELECT DISTINCT partition_values->>%s as value
                    FROM manifest_entries
                    WHERE table_id = %s
                    AND partition_values IS NOT NULL
                """
                params = [next_column, table_id]
                
                # Add filters for parent partitions
                for key, value in current_filters.items():
                    query += " AND partition_values->>%s = %s"
                    params.extend([key, value])
                
                query += " ORDER BY value"
                
                cur.execute(query, params)
                
                partitions = []
                for row in cur.fetchall():
                    if row["value"] is not None:
                        # Build the full path for this partition
                        new_path = path + '/' if path else ''
                        new_path += f"{next_column}={row['value']}"
                        
                        partition_info = {
                            "name": f"{next_column}={row['value']}",
                            "path": new_path,
                            "has_children": current_depth + 1 < len(partition_spec)
                        }
                        
                        # Always count files for this partition path
                        count_query = """
                            SELECT COUNT(*) as file_count
                            FROM manifest_entries
                            WHERE table_id = %s
                        """
                        count_params = [table_id]
                        
                        # Add all filters including the current one
                        all_filters = current_filters.copy()
                        all_filters[next_column] = row["value"]
                        
                        for k, v in all_filters.items():
                            count_query += " AND partition_values->>%s = %s"
                            count_params.extend([k, v])
                        
                        cur.execute(count_query, count_params)
                        file_count = cur.fetchone()["file_count"]
                        partition_info["file_count"] = file_count
                        partition_info["has_data"] = file_count > 0
                        
                        partitions.append(partition_info)
                
                return {
                    "partitions": partitions,
                    "partition_spec": partition_spec,
                    "current_depth": current_depth
                }
    
    def get_files_for_query(self, namespace: str, table_name: str, 
                           partition_filters: Dict[str, Any] = None) -> List[str]:
        """Get list of files matching partition filters (optimized for PostgreSQL)"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                # Base query
                query = """
                    SELECT file_path
                    FROM manifest_entries me
                    JOIN tables t ON me.table_id = t.id
                    JOIN namespaces n ON t.namespace_id = n.id
                    WHERE n.name = %s AND t.name = %s
                """
                params = [namespace, table_name]
                
                # Add partition filters using JSONB operators
                if partition_filters:
                    filter_conditions = []
                    for key, value in partition_filters.items():
                        if isinstance(value, list):
                            # Multiple values - use OR condition with ANY
                            or_conditions = []
                            for v in value:
                                or_conditions.append("me.partition_values->%s = %s")
                                params.extend([key, json.dumps(str(v))])
                            filter_conditions.append(f"({' OR '.join(or_conditions)})")
                        else:
                            # Single value
                            filter_conditions.append("me.partition_values->%s = %s")
                            params.extend([key, json.dumps(str(value))])
                    
                    if filter_conditions:
                        query += " AND " + " AND ".join(filter_conditions)
                
                query += " ORDER BY file_path"
                
                cur.execute(query, params)
                return [row[0] for row in cur.fetchall()]
    
    def refresh_manifest(self, namespace: str, table_name: str) -> Dict[str, Any]:
        """Refresh manifest by scanning S3"""
        import boto3
        
        # Get table metadata
        table_metadata = self.get_table(namespace, table_name)
        location = table_metadata["location"]
        partition_spec = table_metadata.get("partition_spec", [])
        
        # Initialize S3 client
        s3_client = boto3.client('s3', **self._credentials)
        
        # Scan S3 for all parquet files
        manifest_entries = []
        file_count = 0
        total_size = 0
        
        path_parts = location.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        prefix = path_parts[1] + '/' if len(path_parts) > 1 else ''
        
        paginator = s3_client.get_paginator('list_objects_v2')
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
                        
                        manifest_entries.append({
                            "file_path": file_path,
                            "file_size": obj['Size'],
                            "last_modified": obj['LastModified'],
                            "partition_values": partition_values
                        })
        
        # Update manifest in database
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get table id
                cur.execute("""
                    SELECT t.id 
                    FROM tables t
                    JOIN namespaces n ON t.namespace_id = n.id
                    WHERE n.name = %s AND t.name = %s
                """, (namespace, table_name))
                
                table_id = cur.fetchone()[0]
                
                # Clear existing entries
                cur.execute("DELETE FROM manifest_entries WHERE table_id = %s", (table_id,))
                
                # Insert new entries
                for entry in manifest_entries:
                    cur.execute("""
                        INSERT INTO manifest_entries 
                        (table_id, file_path, file_size, partition_values, last_modified)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        table_id,
                        entry["file_path"],
                        entry["file_size"],
                        json.dumps(entry["partition_values"]),
                        entry["last_modified"]
                    ))
        
        return {
            "files_discovered": file_count,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "manifest_updated": True
        }
    
    def _infer_schema(self, location: str) -> Dict[str, Any]:
        """Infer schema from Parquet files"""
        import boto3
        
        try:
            # Initialize S3 client
            s3_client = boto3.client('s3', **self._credentials)
            
            # Find first parquet file
            path_parts = location.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] + '/' if len(path_parts) > 1 else ''
            
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=10)
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
    
    def _detect_partition_columns(self, location: str, s3_client=None) -> List[str]:
        """Detect Hive-style partition columns from directory structure"""
        import boto3
        
        if not s3_client:
            s3_client = boto3.client('s3', **self._credentials)
        
        partition_columns = []
        
        try:
            path_parts = location.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] + '/' if len(path_parts) > 1 else ''
            
            # List some objects to detect partitioning pattern
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=100)
            
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
    
    def _path_exists(self, path: str, s3_client=None) -> bool:
        """Check if S3 path exists"""
        import boto3
        
        if not s3_client:
            s3_client = boto3.client('s3', **self._credentials)
        
        if path.startswith('s3://'):
            path_parts = path.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ''
            try:
                response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
                return 'Contents' in response
            except Exception:
                return False
        return False
    
    def __del__(self):
        """Cleanup connection pool on deletion"""
        if hasattr(self, 'pool'):
            self.pool.closeall()