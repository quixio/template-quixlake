import requests
import logging
from typing import List, Dict, Any, Optional
import os
import time

logger = logging.getLogger(__name__)

class CatalogClient:
    """Client for interacting with the REST Catalog service"""
    
    def __init__(self, catalog_url: str = None):
        self.catalog_url = catalog_url or os.getenv("CATALOG_URL", "http://iceberg-rest-catalog:5001")
        self.catalog_url = self.catalog_url.rstrip('/')
        
        # Simple cache for table metadata to avoid repeated calls
        self._table_cache = {}
        self._cache_ttl = 60  # 1 minute cache
        
    def list_tables(self, namespace: str = "default") -> List[str]:
        """List all tables in a namespace"""
        try:
            response = requests.get(f"{self.catalog_url}/namespaces/{namespace}/tables")
            response.raise_for_status()
            return response.json().get("tables", [])
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []
    
    def get_table(self, table_name: str, namespace: str = "default") -> Optional[Dict[str, Any]]:
        """Get table metadata from catalog with caching"""
        cache_key = f"{namespace}.{table_name}"
        
        # Check cache first
        if cache_key in self._table_cache:
            cached_data, cached_time = self._table_cache[cache_key]
            if (time.time() - cached_time) < self._cache_ttl:
                return cached_data
        
        try:
            response = requests.get(f"{self.catalog_url}/namespaces/{namespace}/tables/{table_name}")
            response.raise_for_status()
            table_data = response.json()
            
            # Cache the result
            self._table_cache[cache_key] = (table_data, time.time())
            
            return table_data
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                # Cache None result too
                self._table_cache[cache_key] = (None, time.time())
                return None
            logger.error(f"Error getting table {table_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error getting table {table_name}: {e}")
            return None
    
    def register_table(self, table_name: str, location: str, schema: Dict[str, Any] = None,
                      partition_spec: List[str] = None, namespace: str = "default") -> bool:
        """Register a table in the catalog"""
        try:
            data = {
                "location": location,
                "schema": schema,
                "partition_spec": partition_spec
            }
            response = requests.put(
                f"{self.catalog_url}/namespaces/{namespace}/tables/{table_name}",
                json=data
            )
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Error registering table {table_name}: {e}")
            return False
    
    def discover_table(self, table_name: str, s3_path: str, namespace: str = "default") -> Dict[str, Any]:
        """Discover and register a table from S3 path"""
        try:
            response = requests.post(
                f"{self.catalog_url}/discover",
                params={
                    "namespace": namespace,
                    "table": table_name,
                    "s3_path": s3_path
                }
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error discovering table {table_name}: {e}")
            raise
    
    def refresh_table(self, table_name: str, namespace: str = "default") -> Dict[str, Any]:
        """Refresh table metadata"""
        try:
            response = requests.post(
                f"{self.catalog_url}/namespaces/{namespace}/tables/{table_name}/refresh"
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error refreshing table {table_name}: {e}")
            raise
    
    def drop_table(self, table_name: str, namespace: str = "default") -> bool:
        """Drop table from catalog (does not delete data)"""
        try:
            response = requests.delete(
                f"{self.catalog_url}/namespaces/{namespace}/tables/{table_name}"
            )
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Error dropping table {table_name}: {e}")
            return False
    
    def get_files_for_query(self, table_name: str, partition_filters: Dict[str, Any] = None, 
                           namespace: str = "default") -> List[str]:
        """Get list of files for query based on partition filters"""
        try:
            params = partition_filters or {}
            logger.info(f"Requesting files for {table_name} with filters: {params}")
            response = requests.get(
                f"{self.catalog_url}/namespaces/{namespace}/tables/{table_name}/files",
                params=params
            )
            response.raise_for_status()
            result = response.json()
            logger.info(f"Got {result.get('count', 0)} files from catalog")
            return result.get("files", [])
        except Exception as e:
            logger.error(f"Error getting files for query: {e}")
            # Fallback to wildcard if catalog fails
            return []
    
    def refresh_manifest(self, table_name: str, namespace: str = "default") -> Dict[str, Any]:
        """Refresh table manifest by scanning S3"""
        try:
            response = requests.post(
                f"{self.catalog_url}/namespaces/{namespace}/tables/{table_name}/manifest/refresh"
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error refreshing manifest: {e}")
            raise
    
    def add_files_to_manifest(self, table_name: str, files: List[Dict[str, Any]], 
                             namespace: str = "default") -> bool:
        """Add files to table manifest"""
        try:
            response = requests.post(
                f"{self.catalog_url}/namespaces/{namespace}/tables/{table_name}/manifest/add-files",
                json={"files": files}
            )
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Error adding files to manifest: {e}")
            return False
    
    def get_manifest(self, table_name: str, namespace: str = "default") -> Dict[str, Any]:
        """Get table manifest"""
        try:
            response = requests.get(
                f"{self.catalog_url}/namespaces/{namespace}/tables/{table_name}/manifest"
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error getting manifest: {e}")
            return {}
    
    def get_partition_hierarchy(self, table_name: str, path: str = "", lazy: bool = True, 
                               namespace: str = "default") -> Dict[str, Any]:
        """Get partition hierarchy for lazy loading"""
        try:
            params = {
                "path": path,
                "lazy": "true" if lazy else "false"
            }
            response = requests.get(
                f"{self.catalog_url}/namespaces/{namespace}/tables/{table_name}/partition-hierarchy",
                params=params
            )
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            if e.response.status_code == 501:
                # Fallback to getting full manifest if hierarchy endpoint not supported
                logger.info("Partition hierarchy endpoint not supported, falling back to manifest")
                return self.get_manifest(table_name, namespace)
            logger.error(f"Error getting partition hierarchy: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error getting partition hierarchy: {e}")
            return {}
    
    def remove_files_from_manifest(self, table_name: str, file_paths: List[str], 
                                  namespace: str = "default") -> bool:
        """Remove files from table manifest (used after partial deletions)"""
        try:
            response = requests.post(
                f"{self.catalog_url}/namespaces/{namespace}/tables/{table_name}/manifest/remove-files",
                json={"files": file_paths}
            )
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Error removing files from manifest: {e}")
            return False