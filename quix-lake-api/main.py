from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from duck_db_service import DuckDbService
from task_manager import TaskManager, TaskType, TaskStatus
import re
import io
import os
import pandas as pd
from dotenv import load_dotenv
import queue
import threading
import json
import time
import uuid
load_dotenv(".env")

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Initialize the service
db_service = DuckDbService()

# Initialize task manager
task_manager = TaskManager(state_dir=os.getenv("TASK_STATE_DIR", "/app/state"))

@app.route("/grafana/metrics", methods=["POST"])
def metrics():
    """Get available metrics for Grafana"""
    metrics_data = db_service.get_grafana_metrics()
    return jsonify(metrics_data)

@app.route("/query", methods=["POST"])
def query():
    """Execute SQL query endpoint with DuckLake tables"""
    raw_sql = request.get_data(as_text=True)
    
    # Check for explain parameter
    explain_analyze = request.args.get('explain', 'false').lower() == 'true'
    
    # Check for union_by_name parameter (default false for strict schema checking)
    union_by_name = request.args.get('union_by_name', 'false').lower() == 'true'
    
    # Replace FROM table_name with optimized file list from manifest
    from_pattern = r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b"
    def replace_table(match):
        table_name = match.group(1).lower()
        
        # Skip system tables and functions
        if table_name in ['sqlite_temp_master', 'information_schema']:
            return match.group(0)
        
        # Extract partition filters from WHERE clause
        partition_filters = db_service._extract_partition_filters(raw_sql, table_name)
        print(f"Extracted partition filters: {partition_filters}")
        
        # Get specific files from catalog manifest
        files = db_service.catalog_client.get_files_for_query(table_name, partition_filters)
        
        if files:
            # Use specific file list for optimal performance
            file_list = "[" + ", ".join(f"'{f}'" for f in files) + "]"
            print(f"Using {len(files)} specific files from manifest for table {table_name}")
            # Always include hive_partitioning=true to get partition columns from paths
            return f"FROM read_parquet({file_list}, union_by_name={str(union_by_name).lower()}, hive_partitioning=true)"
        else:
            # Fallback to wildcard pattern if no manifest or no files
            data_path = db_service._get_data_path(table_name)
            print(f"Falling back to wildcard pattern for table {table_name}")
            return f"FROM read_parquet('{data_path}/**/*.parquet', hive_partitioning=true, union_by_name={str(union_by_name).lower()})"
    
    print(f"Original SQL: {raw_sql}")
    raw_sql = re.sub(from_pattern, replace_table, raw_sql, flags=re.IGNORECASE)
    print(f"Modified SQL: {raw_sql[:200]}..." if len(raw_sql) > 200 else f"Modified SQL: {raw_sql}")
    
    # Debug: Log explain parameter
    if explain_analyze:
        print(f"[QUERY ENDPOINT] Explain requested: explain_analyze={explain_analyze}")
    
    try:
        # Execute query (may return explain output too)
        query_result = db_service.execute_query(raw_sql, explain_analyze=explain_analyze)
        
        if explain_analyze and isinstance(query_result, tuple):
            # Got both DataFrame and explain output
            df, explain_output = query_result
            
            # Return JSON response with both data and explain
            response = {
                "data": df.to_csv(index=False),
                "explain": explain_output,
                "format": "csv"
            }
            return jsonify(response)
        else:
            # Normal query without explain - return CSV as before
            df = query_result
            return df.to_csv(index=False)
            
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/grafana/query', methods=['POST'])
def grafana_query():
    """Handle Grafana query requests"""
    req = request.get_json()
    print(req)
    
    folder = req["targets"][0]["target"]
    query = req["targets"][0]["payload"]["query"]
    
    # Grafana queries always use union_by_name for schema flexibility
    union_by_name = True
    # Enable partition pruning for Grafana queries by default
    enable_partition_pruning = True
    
    # Replace FROM table_name with optimized file list from manifest
    from_pattern = r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b"
    def replace_table(match):
        table_name = match.group(1).lower()
        
        # Skip system tables and functions
        if table_name in ['sqlite_temp_master', 'information_schema']:
            return match.group(0)
        
        # Extract partition filters from WHERE clause
        partition_filters = db_service._extract_partition_filters(query, table_name)
        
        # Get specific files from catalog manifest
        files = db_service.catalog_client.get_files_for_query(table_name, partition_filters)
        
        if files:
            # Use specific file list for optimal performance
            file_list = "[" + ", ".join(f"'{f}'" for f in files) + "]"
            # Always include hive_partitioning=true to get partition columns from paths
            return f"FROM read_parquet({file_list}, union_by_name={str(union_by_name).lower()}, hive_partitioning=true)"
        else:
            # Fallback to wildcard pattern if no manifest or no files
            data_path = db_service._get_data_path(table_name)
            return f"FROM read_parquet('{data_path}/**/*.parquet', hive_partitioning=true, union_by_name={str(union_by_name).lower()})"
    query = re.sub(from_pattern, replace_table, query, flags=re.IGNORECASE)
    
    try:
        # Grafana queries don't need explain analyze by default for performance
        df = db_service.execute_query(query, explain_analyze=False)
        # Convert DataFrame to Grafana time series format
        grafana_result = db_service.convert_dataframe_to_grafana_format(df, folder)
        return jsonify(grafana_result)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    #{'app': 'dashboard', 'requestId': 'SQR152', 'timezone': 'browser', 'range': {'to': '2025-07-02T08:39:07.260Z', 'from': '2025-07-02T02:39:07.260Z', 'raw': {'from': 'now-6h', 'to': 'now'}}, 'interval': '30s', 'intervalMs': 30000, 'targets': [{'datasource': {'type': 'simpod-json-datasource', 'uid': 'feqlj691gnrpca'}, 'refId': 'A', 'editorMode': 'builder', 'payload': {'query': 'SELECT * FROM :selected: LIMIT 10'}, 'target': 'machine=3D_PRINTER_1/experiment_name=experiment-beta'}], 'maxDataPoints': 500, 'scopedVars': {'__sceneObject': {'text': '__sceneObject'}, '__interval': {'text': '30s', 'value': '30s'}, '__interval_ms': {'text': '30000', 'value': 30000}}, 'startTime': 1751445576019, 'rangeRaw': {'from': 'now-6h', 'to': 'now'}, 'dashboardUID': None, 'panelId': 1, 'panelName': 'Panel Title', 'panelPluginId': 'timeseries', 'scopes': []}v
    

@app.route("/tables", methods=["GET"])
def tables():
    """List available tables in the database"""
    try:
        # Check if detailed metadata is requested
        include_metadata = request.args.get('include_metadata', 'false').lower() == 'true'
        
        if include_metadata:
            result = db_service.get_tables_with_metadata()
            return jsonify({"tables": result})
        else:
            # Default behavior - just return table names for backward compatibility
            result = db_service.get_tables()
            return jsonify({"tables": result})
    except Exception as e:
        print(f"Error in tables: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/schema", methods=["GET"])
def schema():
    """Get table schema without scanning data"""
    try:
        table_name = request.args.get('table', None)
        if not table_name:
            return jsonify({"error": "Table name is required as query parameter"}), 400
        
        # Get schema from DuckDB catalog without scanning data
        schema_info = db_service.get_table_schema(table_name)
        return jsonify(schema_info)
    except Exception as e:
        print(f"Error getting schema: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/partitions", methods=["GET"])
def partitions():
    """Get partition tree structure for a table (supports lazy loading)"""
    try:
        table_name = request.args.get('table', None)
        if not table_name:
            return jsonify({"error": "Table name is required as query parameter"}), 400
        
        # Check for lazy loading parameters
        path = request.args.get('path', '')  # Path within the table to explore
        include_sizes = request.args.get('include_sizes', 'false').lower() == 'true'
        
        result = db_service.get_partition_level(table_name, path, include_sizes)
        
        response_data = {
            "table": table_name,
            "path": path
        }
        
        response_data["partitions"] = result
        
        return jsonify(response_data)
    except Exception as e:
        print(f"Error getting partitions: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/partition-info", methods=["GET"])
def partition_info():
    """Get partition structure information for a table"""
    try:
        table_name = request.args.get('table', None)
        if not table_name:
            return jsonify({"error": "Table name is required as query parameter"}), 400
        
        result = db_service.get_table_partition_info(table_name)
        return jsonify(result)
    except Exception as e:
        print(f"Error getting partition info: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/hive-folders", methods=["GET"])
def hive_folders():
    """Get folder structure for hive tables (compatible with sql-query-ui)"""
    try:
        parent_path = request.args.get('parent', '')
        
        if not parent_path:
            # Root level - return list of available tables
            tables = db_service.get_tables()
            return jsonify({"folders": tables})
        
        # Parse table name and path from parent parameter
        # Expected format: "table_name" or "table_name/partition_path"
        path_parts = parent_path.split('/', 1)
        table_name = path_parts[0]
        partition_path = path_parts[1] if len(path_parts) > 1 else ''
        
        # Get partition tree for the table
        partition_tree = db_service.get_partition_tree(table_name)
        
        if partition_path:
            # Navigate to specific partition
            folders = db_service.get_partition_folders(table_name, partition_path)
        else:
            # Return top-level partitions for the table
            folders = [child["name"] for child in partition_tree["tree"]["children"] 
                      if child["type"] == "directory"]
        
        return jsonify({"folders": folders})
        
    except Exception as e:
        print(f"Error getting hive folders: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/discover", methods=["POST"])
def discover():
    """Discover and register existing Parquet files in S3 as a table"""
    try:
        table_name = request.args.get('table', None)
        if not table_name:
            return jsonify({"error": "Table name is required as query parameter"}), 400
        
        s3_path = request.args.get('s3_path', None)
        if not s3_path:
            return jsonify({"error": "S3 path is required as query parameter"}), 400
        
        # Validate S3 path format
        if not s3_path.startswith('s3://'):
            return jsonify({"error": "S3 path must start with 's3://'"}), 400
        
        result = db_service.discover_and_register_table(table_name, s3_path)
        
        return jsonify({
            "message": result["message"],
            "discovery_result": {
                "table_name": result["table_name"],
                "s3_path": result["s3_path"],
                "file_count": result["file_count"],
                "total_size_mb": result["total_size_mb"],
                "partition_columns": result["partition_columns"]
            }
        })
        
    except Exception as e:
        print(f"Error in discover endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/repartition", methods=["POST"])
def repartition():
    """Repartition table data with new partition scheme"""
    try:
        table_name = request.args.get('table', None)
        if not table_name:
            return jsonify({"error": "Table name is required as query parameter"}), 400
        
        # Check if async mode is requested
        async_mode = request.args.get('async', 'false').lower() == 'true'
        
        # Partition configuration
        hive_columns = request.args.get('hive_columns', '').split(',') if request.args.get('hive_columns') else []
        hive_columns = [col.strip() for col in hive_columns if col.strip()]
        
        timestamp_column = request.args.get('timestamp_column', None)
        timestamp_format = request.args.get('timestamp_format', 'day')
        target_file_size_mb = int(request.args.get('target_file_size_mb', '128'))
        
        if async_mode:
            # Use task manager for async execution
            task_id = task_manager.create_task(
                TaskType.REPARTITION,
                {
                    "table_name": table_name,
                    "hive_columns": hive_columns,
                    "timestamp_column": timestamp_column,
                    "timestamp_format": timestamp_format,
                    "target_file_size_mb": target_file_size_mb
                }
            )
            
            # Start the task
            task_manager.start_task(task_id)
            
            return jsonify({
                "task_id": task_id,
                "message": "Repartition started in background",
                "progress_url": f"/tasks/{task_id}/progress"
            })
        else:
            # Synchronous mode (existing behavior)
            result = db_service.repartition_table_v3(
                table_name,
                hive_columns=hive_columns,
                timestamp_column=timestamp_column,
                timestamp_format=timestamp_format,
                target_file_size_mb=target_file_size_mb
            )
            
            return jsonify({
                "message": f"Successfully repartitioned table '{table_name}'",
                "repartition_result": result
            })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/compact", methods=["POST"])
def compact():
    """Compact table files and fix schema inconsistencies while preserving partition structure"""
    try:
        table_name = request.args.get('table', None)
        if not table_name:
            return jsonify({"error": "Table name is required as query parameter"}), 400
        
        # Check if async mode is requested
        async_mode = request.args.get('async', 'false').lower() == 'true'
        
        # Only parameter needed is target file size
        target_file_size_mb = int(request.args.get('target_file_size_mb', '128'))
        
        if async_mode:
            # Use task manager for async execution
            task_id = task_manager.create_task(
                TaskType.COMPACT,
                {
                    "table_name": table_name,
                    "target_file_size_mb": target_file_size_mb
                }
            )
            
            # Start the task
            task_manager.start_task(task_id)
            
            return jsonify({
                "task_id": task_id,
                "message": "Compaction started in background",
                "progress_url": f"/tasks/{task_id}/progress"
            })
        else:
            # Synchronous mode (existing behavior)
            result = db_service.compact_table(
                table_name, 
                target_file_size_mb=target_file_size_mb
            )
            
            return jsonify({
                "message": f"Successfully compacted table '{table_name}'",
                "compaction_result": result
            })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/delete", methods=["DELETE"])
def delete():
    """Delete data from table with optional WHERE conditions"""
    try:
        table_name = request.args.get('table', None)
        if not table_name:
            return jsonify({"error": "Table name is required as query parameter"}), 400
        
        # Optional WHERE clause for conditional deletion
        where_clause = request.args.get('where', None)
        
        # Optional: delete entire table/partitions
        delete_table = request.args.get('delete_table', 'false').lower() == 'true'
        
        # Optional: specific partitions to delete
        partitions = request.args.get('partitions', None)
        
        result = db_service.delete_data(
            table_name=table_name,
            where_clause=where_clause,
            delete_table=delete_table,
            partitions=partitions.split(',') if partitions else None
        )
        
        return jsonify({
            "message": f"Successfully deleted data from table '{table_name}'",
            "deletion_result": result
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/refresh-manifest", methods=["POST"])
def refresh_manifest():
    """Refresh table manifest by scanning S3"""
    try:
        table_name = request.args.get('table', None)
        if not table_name:
            return jsonify({"error": "Table name is required as query parameter"}), 400
        
        result = db_service.catalog_client.refresh_manifest(table_name)
        
        return jsonify({
            "message": f"Successfully refreshed manifest for table '{table_name}'",
            "result": result
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/insert", methods=["POST"])
def insert():
    """Insert batch of rows from CSV data with Hive partitioning"""
    try:
        table_name = request.args.get('table', None)
        if not table_name:
            return jsonify({"error": "Table name is required as query parameter"}), 400
        
        # Get partitioning configuration
        hive_columns = request.args.get('hive_columns', '').split(',') if request.args.get('hive_columns') else []
        hive_columns = [col.strip() for col in hive_columns if col.strip()]
        
        timestamp_column = request.args.get('timestamp_column', None)
        timestamp_format = request.args.get('timestamp_format', 'day')  # day, hour, month
        
        csv_data = request.get_data(as_text=True)
        if not csv_data:
            return jsonify({"error": "CSV data is required in request body"}), 400
        
        df = pd.read_csv(io.StringIO(csv_data))
        
        if df.empty:
            return jsonify({"error": "CSV data is empty"}), 400
        
        # Validate hive columns exist in DataFrame (except time partitions that will be generated)
        time_partition_names = ['year', 'month', 'day', 'hour']
        for col in hive_columns:
            if col not in df.columns and col not in time_partition_names:
                return jsonify({"error": f"Hive column '{col}' not found in CSV data"}), 400
        
        # Validate timestamp column if specified
        if timestamp_column and timestamp_column not in df.columns:
            return jsonify({"error": f"Timestamp column '{timestamp_column}' not found in CSV data"}), 400
        
        rows_inserted = db_service.insert_dataframe_partitioned(
            table_name, 
            df, 
            hive_columns=hive_columns,
            timestamp_column=timestamp_column,
            timestamp_format=timestamp_format
        )
        
        return jsonify({
            "message": f"Successfully inserted {rows_inserted} rows into partitioned table '{table_name}'",
            "rows_inserted": rows_inserted,
            "hive_columns": hive_columns,
            "timestamp_column": timestamp_column,
            "timestamp_format": timestamp_format
        })
        
    except pd.errors.EmptyDataError:
        return jsonify({"error": "Invalid CSV format"}), 400
    except pd.errors.ParserError as e:
        return jsonify({"error": f"CSV parsing error: {str(e)}"}), 400
    except ValueError as e:
        # Check if it's a partition structure mismatch
        if "Partition structure mismatch" in str(e):
            return jsonify({
                "error": str(e),
                "error_type": "partition_mismatch",
                "suggested_actions": [
                    "Use the Repartition feature to change the table's partition structure",
                    "Modify your insert to match the existing partition columns",
                    f"Get current partition info at: /partition-info?table={table_name}"
                ]
            }), 409  # Conflict status code
        else:
            return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Task Management Endpoints
@app.route("/tasks", methods=["GET"])
def list_tasks():
    """List all tasks"""
    try:
        tasks = task_manager.get_all_tasks()
        return jsonify({
            "tasks": [task.to_dict() for task in tasks.values()]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/tasks/<task_id>", methods=["GET"])
def get_task(task_id):
    """Get task details"""
    try:
        task = task_manager.get_task(task_id)
        if not task:
            return jsonify({"error": "Task not found"}), 404
        return jsonify(task.to_dict())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/tasks/<task_id>/start", methods=["POST"])
def start_task(task_id):
    """Start or resume a task"""
    try:
        success = task_manager.start_task(task_id)
        if not success:
            return jsonify({"error": "Failed to start task"}), 400
        return jsonify({"message": "Task started", "task_id": task_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/tasks/<task_id>/pause", methods=["POST"])
def pause_task(task_id):
    """Pause a running task"""
    try:
        success = task_manager.pause_task(task_id)
        if not success:
            return jsonify({"error": "Failed to pause task"}), 400
        return jsonify({"message": "Task paused", "task_id": task_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/tasks/<task_id>/resume", methods=["POST"])
def resume_task(task_id):
    """Resume a paused task"""
    try:
        success = task_manager.resume_task(task_id)
        if not success:
            return jsonify({"error": "Failed to resume task"}), 400
        return jsonify({"message": "Task resumed", "task_id": task_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/tasks/<task_id>/cancel", methods=["POST"])
def cancel_task(task_id):
    """Cancel a task"""
    try:
        success = task_manager.cancel_task(task_id)
        if not success:
            return jsonify({"error": "Failed to cancel task"}), 400
        return jsonify({"message": "Task cancelled", "task_id": task_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/tasks/<task_id>/delete", methods=["DELETE"])
def delete_task(task_id):
    """Delete a completed task"""
    try:
        success = task_manager.delete_task(task_id)
        if not success:
            return jsonify({"error": "Cannot delete active task"}), 400
        return jsonify({"message": "Task deleted", "task_id": task_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/tasks/<task_id>/progress")
def task_progress(task_id):
    """SSE endpoint for task progress"""
    def generate():
        progress_queue = task_manager.get_progress_queue(task_id)
        if not progress_queue:
            yield f"data: {json.dumps({'status': 'error', 'error': 'Task not found'})}\n\n"
            return
            
        task = task_manager.get_task(task_id)
        # Send current state
        yield f"data: {json.dumps(task.to_dict())}\n\n"
        
        # Stream updates
        while True:
            try:
                update = progress_queue.get(timeout=30)
                yield f"data: {json.dumps(update)}\n\n"
                
                if update.get("status") in ["completed", "error", "cancelled"]:
                    break
            except queue.Empty:
                # Send keepalive
                yield f": keepalive\n\n"
    
    return Response(generate(), mimetype="text/event-stream")

# Register task executors
def repartition_executor(params, progress_callback):
    """Execute repartition task"""
    return db_service.repartition_table_v3(
        params["table_name"],
        hive_columns=params.get("hive_columns", []),
        timestamp_column=params.get("timestamp_column"),
        timestamp_format=params.get("timestamp_format", "day"),
        target_file_size_mb=params.get("target_file_size_mb", 128),
        progress_callback=progress_callback
    )

def compact_executor(params, progress_callback):
    """Execute compaction task"""
    return db_service.compact_table(
        params["table_name"],
        target_file_size_mb=params.get("target_file_size_mb", 128),
        progress_callback=progress_callback
    )

# Register executors with task manager
task_manager.register_executor(TaskType.REPARTITION, repartition_executor)
task_manager.register_executor(TaskType.COMPACT, compact_executor)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)