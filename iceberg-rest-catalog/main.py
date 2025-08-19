from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import logging
from dotenv import load_dotenv
import threading
import time
import atexit

load_dotenv(".env")

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize catalog service based on backend type
catalog_backend = os.getenv('CATALOG_BACKEND', 's3').lower()

if catalog_backend == 'postgres':
    from postgres_catalog_service import PostgresCatalogService
    catalog_service = PostgresCatalogService()
    logger.info("Using PostgreSQL catalog backend")
else:
    from catalog_service import IcebergCatalogService
    catalog_service = IcebergCatalogService()
    logger.info("Using S3 catalog backend")

# Background thread for periodic manifest flushing (only for S3 backend)
if catalog_backend == 's3':
    def periodic_flush():
        """Periodically flush pending manifest updates"""
        while True:
            try:
                time.sleep(5)  # Check every 5 seconds
                catalog_service._flush_manifest_updates()
            except Exception as e:
                logger.error(f"Error in periodic flush: {e}")

    # Start background thread
    flush_thread = threading.Thread(target=periodic_flush, daemon=True)
    flush_thread.start()

    # Ensure all manifests are flushed on shutdown
    def shutdown_handler():
        """Flush all pending updates before shutdown"""
        logger.info("Flushing all pending manifest updates...")
        catalog_service._flush_manifest_updates()

    atexit.register(shutdown_handler)

@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "iceberg-rest-catalog"})

@app.route("/cache-status", methods=["GET"])
def cache_status():
    """Get cache status information"""
    try:
        if catalog_backend == 's3':
            status = {
                "backend": "s3",
                "catalog_tables_cached": len(catalog_service._catalog_cache.get("tables", {})),
                "manifests_cached": len(catalog_service._manifest_cache),
                "pending_manifest_updates": {
                    k: len(v) for k, v in catalog_service._pending_manifest_updates.items()
                },
                "manifest_cache_ttl_seconds": catalog_service._manifest_cache_ttl
            }
        else:
            status = {
                "backend": "postgres",
                "tables_cached": len(catalog_service._table_cache),
                "cache_ttl_seconds": catalog_service._cache_ttl,
                "connection_pool_size": catalog_service.pool.maxconn
            }
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error getting cache status: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/namespaces", methods=["GET"])
def list_namespaces():
    """List all namespaces in the catalog"""
    try:
        namespaces = catalog_service.list_namespaces()
        return jsonify({"namespaces": namespaces})
    except Exception as e:
        logger.error(f"Error listing namespaces: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/namespaces/<namespace>/tables", methods=["GET"])
def list_tables(namespace):
    """List all tables in a namespace"""
    try:
        tables = catalog_service.list_tables(namespace)
        return jsonify({"tables": tables})
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/namespaces/<namespace>/tables/<table_name>", methods=["GET"])
def get_table(namespace, table_name):
    """Get table metadata"""
    try:
        table_metadata = catalog_service.get_table(namespace, table_name)
        return jsonify(table_metadata)
    except Exception as e:
        logger.error(f"Error getting table {namespace}.{table_name}: {e}")
        return jsonify({"error": str(e)}), 404

@app.route("/namespaces/<namespace>/tables/<table_name>", methods=["PUT", "POST"])
def create_or_update_table(namespace, table_name):
    """Create or update a table in the catalog"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request body is required"}), 400
            
        # Extract table properties
        location = data.get("location")
        schema = data.get("schema")
        partition_spec = data.get("partition_spec", [])
        properties = data.get("properties", {})
        
        if not location:
            return jsonify({"error": "Table location is required"}), 400
            
        result = catalog_service.create_or_update_table(
            namespace=namespace,
            table_name=table_name,
            location=location,
            schema=schema,
            partition_spec=partition_spec,
            properties=properties
        )
        
        return jsonify(result), 201 if request.method == "POST" else 200
        
    except Exception as e:
        logger.error(f"Error creating/updating table {namespace}.{table_name}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/namespaces/<namespace>/tables/<table_name>", methods=["DELETE"])
def drop_table(namespace, table_name):
    """Drop a table from the catalog"""
    try:
        catalog_service.drop_table(namespace, table_name)
        return "", 204
    except Exception as e:
        logger.error(f"Error dropping table {namespace}.{table_name}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/namespaces/<namespace>/tables/<table_name>/refresh", methods=["POST"])
def refresh_table(namespace, table_name):
    """Refresh table metadata by scanning the data location"""
    try:
        result = catalog_service.refresh_table(namespace, table_name)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error refreshing table {namespace}.{table_name}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/discover", methods=["POST"])
def discover_table():
    """Discover and register a table from S3 path"""
    try:
        namespace = request.args.get("namespace", "default")
        table_name = request.args.get("table")
        s3_path = request.args.get("s3_path")
        
        if not table_name or not s3_path:
            return jsonify({"error": "table and s3_path parameters are required"}), 400
            
        result = catalog_service.discover_and_register(namespace, table_name, s3_path)
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error discovering table: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/namespaces/<namespace>/tables/<table_name>/manifest", methods=["GET"])
def get_manifest(namespace, table_name):
    """Get table manifest with optional pagination and filtering"""
    try:
        # Extract query parameters for PostgreSQL optimization
        if catalog_backend == 'postgres':
            limit = request.args.get('limit', type=int)
            offset = request.args.get('offset', 0, type=int)
            
            # Extract partition filters from query params
            partition_filter = {}
            for key in request.args:
                if key not in ['limit', 'offset']:
                    values = request.args.getlist(key)
                    if len(values) == 1:
                        partition_filter[key] = values[0]
                    else:
                        partition_filter[key] = values
            
            manifest = catalog_service.get_manifest(
                namespace, table_name, 
                limit=limit, 
                offset=offset,
                partition_filter=partition_filter if partition_filter else None
            )
        else:
            # S3 backend doesn't support pagination/filtering yet
            manifest = catalog_service.get_manifest(namespace, table_name)
        
        return jsonify(manifest)
    except Exception as e:
        logger.error(f"Error getting manifest: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/namespaces/<namespace>/tables/<table_name>/manifest/refresh", methods=["POST"])
def refresh_manifest(namespace, table_name):
    """Refresh table manifest by scanning S3"""
    try:
        result = catalog_service.refresh_manifest(namespace, table_name)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error refreshing manifest: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/namespaces/<namespace>/tables/<table_name>/files", methods=["GET"])
def get_query_files(namespace, table_name):
    """Get files for query based on partition filters"""
    try:
        # Parse partition filters from query params
        # Use getlist to handle multiple values for the same key
        partition_filters = {}
        for key in request.args.keys():
            if key not in ['namespace', 'table']:
                values = request.args.getlist(key)
                if len(values) == 1:
                    partition_filters[key] = values[0]
                else:
                    # Multiple values for same key - treat as OR condition
                    partition_filters[key] = values
        
        files = catalog_service.get_files_for_query(namespace, table_name, partition_filters)
        return jsonify({"files": files, "count": len(files)})
    except Exception as e:
        logger.error(f"Error getting query files: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/namespaces/<namespace>/tables/<table_name>/partitions", methods=["GET"])
def get_partitions(namespace, table_name):
    """Get distinct partition values for lazy loading"""
    try:
        # Check if this is a hierarchy request (has path parameter)
        path = request.args.get('path', '')
        lazy = request.args.get('lazy', 'false').lower() == 'true'
        
        if catalog_backend == 'postgres' and hasattr(catalog_service, 'get_partition_hierarchy'):
            # Use optimized partition hierarchy method
            result = catalog_service.get_partition_hierarchy(namespace, table_name, path, lazy)
            return jsonify(result)
        elif catalog_backend == 'postgres' and hasattr(catalog_service, 'get_partitions'):
            partitions = catalog_service.get_partitions(namespace, table_name)
            return jsonify({"partitions": partitions, "count": len(partitions)})
        else:
            # For S3 backend, fall back to getting from manifest
            manifest = catalog_service.get_manifest(namespace, table_name)
            # Extract unique partition values
            partition_values = set()
            for entry in manifest.get("entries", []):
                pv = entry.get("partition_values", {})
                if pv:
                    partition_values.add(tuple(sorted(pv.items())))
            
            # Convert back to list of dicts
            partitions = [dict(items) for items in partition_values]
            return jsonify({"partitions": sorted(partitions, key=str), "count": len(partitions)})
    except Exception as e:
        logger.error(f"Error getting partitions: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/namespaces/<namespace>/tables/<table_name>/partition-hierarchy", methods=["GET"])
def get_partition_hierarchy(namespace, table_name):
    """Get partition hierarchy optimized for UI tree view"""
    try:
        path = request.args.get('path', '')
        lazy = request.args.get('lazy', 'true').lower() == 'true'
        
        if catalog_backend == 'postgres' and hasattr(catalog_service, 'get_partition_hierarchy'):
            result = catalog_service.get_partition_hierarchy(namespace, table_name, path, lazy)
            return jsonify(result)
        else:
            # Fallback for S3 backend
            return jsonify({"error": "Partition hierarchy not supported for S3 backend"}), 501
    except Exception as e:
        logger.error(f"Error getting partition hierarchy: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/namespaces/<namespace>/tables/<table_name>/manifest/add-files", methods=["POST"])
def add_files_to_manifest(namespace, table_name):
    """Add files to manifest (used by sink)"""
    try:
        data = request.get_json()
        if not data or "files" not in data:
            return jsonify({"error": "files array is required in request body"}), 400
        
        catalog_service.add_files_to_manifest(namespace, table_name, data["files"])
        return jsonify({"message": "Files added to manifest"}), 200
        
    except Exception as e:
        logger.error(f"Error adding files to manifest: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/namespaces/<namespace>/tables/<table_name>/manifest/remove-files", methods=["POST"])
def remove_files_from_manifest(namespace, table_name):
    """Remove files from manifest (used after partial deletions)"""
    try:
        data = request.get_json()
        if not data or "files" not in data:
            return jsonify({"error": "files array is required in request body"}), 400
        
        if catalog_backend == 'postgres' and hasattr(catalog_service, 'remove_files_from_manifest'):
            catalog_service.remove_files_from_manifest(namespace, table_name, data["files"])
            return jsonify({"message": "Files removed from manifest"}), 200
        else:
            return jsonify({"error": "Remove files not supported for S3 backend"}), 501
        
    except Exception as e:
        logger.error(f"Error removing files from manifest: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)