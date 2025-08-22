import duckdb
import os
import time
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import uuid
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from catalog_client import CatalogClient
import re
import logging
import requests

logger = logging.getLogger(__name__)


class DuckDbService:
    def __init__(self):
        self.con = None
        self.s3_bucket = os.getenv('S3_BUCKET', 'quixlake-data')
        self.s3_prefix = os.getenv('S3_PREFIX', 'data')
        self._aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self._aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self._aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self._aws_endpoint_url = os.getenv("AWS_ENDPOINT_URL", None)
        self._credentials = {
            "region_name": self._aws_region,
            "aws_access_key_id": self._aws_access_key_id,
            "aws_secret_access_key": self._aws_secret_access_key,
            "endpoint_url": self._aws_endpoint_url,
        }

        # Initialize catalog client
        self.catalog_client = CatalogClient()
        
        # Always initialize S3 client for file operations
        try:
            self.s3_client = boto3.client(
                's3',
                **self._credentials
            )
            # Test S3 connection
            self.s3_client.head_bucket(Bucket=self.s3_bucket)
            print(f"✅ S3 connection established: s3://{self.s3_bucket}/{self.s3_prefix}")
        except (ClientError, NoCredentialsError) as e:
            print(f"❌ S3 connection failed: {e}")
            raise RuntimeError(f"S3 connection required but failed: {e}")
        
        self._setup_connection()
    
    def _setup_connection(self):
        """Initialize DuckDB connection for S3 Parquet data access"""
        # Create state directory for temporary operations
        os.makedirs("state", exist_ok=True)
        
        # Use in-memory database (no persistent state needed with REST catalog)
        self.con = duckdb.connect(":memory:")
        
        # Install required extensions (httpfs for S3 data)
        self.con.execute("INSTALL httpfs")
        self.con.execute("LOAD httpfs")
        
        # Configure S3 settings for data access
        if self._aws_endpoint_url:
            self.con.execute(f"SET s3_endpoint='{self._aws_endpoint_url}';")
            self.con.execute(f"SET s3_url_style='path';")
        if self._aws_access_key_id:
            self.con.execute(f"SET s3_access_key_id='{self._aws_access_key_id}';")
            self.con.execute(f"SET s3_secret_access_key='{self._aws_secret_access_key}';")
        self.con.execute(f"SET s3_region='{self._aws_region}';")
        
        # Performance settings
        self.con.execute("SET memory_limit = '4GB';")
        # Create temp directory if it doesn't exist
        os.makedirs("state/duckdb_swap", exist_ok=True)
        self.con.execute("SET temp_directory = 'state/duckdb_swap';")
        self.con.execute("SET max_temp_directory_size = '100GB';")
        self.con.execute("SET threads=32;")
        
        # IMPORTANT: Forcefully disable ALL profiling to prevent explain output
        # DuckDB has persistent settings that need to be overridden
        self.con.execute("SET enable_profiling = 'no_output';")
        self.con.execute("SET enable_progress_bar = false;")
        self.con.execute("SET explain_output = 'physical_only';")
        # Note: profiling_mode only supports 'standard' or 'detailed', not 'disabled'
        self.con.execute("SET profiling_mode = 'standard';")
        # Clear profiling output destination
        self.con.execute("SET profiling_output = '';")
        print("DuckDB initialized for S3 data access with REST Catalog")

    def _get_data_path(self, table_name: str) -> str:
        """Get the data path for a table (always S3)"""
        return f"s3://{self.s3_bucket}/{self.s3_prefix}/{table_name}"
    
    def _path_exists(self, path: str) -> bool:
        """Check if S3 path exists"""
        if path.startswith('s3://'):
            # Extract bucket and key from S3 path
            path_parts = path.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ''
            try:
                # List objects with the prefix to check if any exist
                response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
                return 'Contents' in response
            except ClientError:
                return False
        else:
            # Fallback for local paths (should not be used for data)
            return os.path.exists(path)
    
    def _list_directory(self, path: str) -> List[str]:
        """List directory contents (always S3 for data)"""
        if path.startswith('s3://'):
            path_parts = path.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] + '/' if len(path_parts) > 1 else ''
            
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=bucket, 
                    Prefix=prefix, 
                    Delimiter='/'
                )
                
                # Get "folders" (common prefixes)
                folders = []
                if 'CommonPrefixes' in response:
                    for obj in response['CommonPrefixes']:
                        folder_name = obj['Prefix'].replace(prefix, '').rstrip('/')
                        if folder_name:
                            folders.append(folder_name)
                
                # Get files
                files = []
                if 'Contents' in response:
                    for obj in response['Contents']:
                        file_name = obj['Key'].replace(prefix, '')
                        if file_name and '/' not in file_name:  # Only direct children
                            files.append(file_name)
                
                return folders + files
            except ClientError:
                return []
        else:
            # Fallback for local paths (should not be used for data)
            return os.listdir(path) if os.path.exists(path) else []
    
    def _is_directory(self, path: str) -> bool:
        """Check if path is a directory (always S3 for data)"""
        if path.startswith('s3://'):
            # In S3, directories are represented by common prefixes
            path_parts = path.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] + '/' if len(path_parts) > 1 else ''
            
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=bucket, 
                    Prefix=prefix, 
                    MaxKeys=1
                )
                return 'Contents' in response
            except ClientError:
                return False
        else:
            # Fallback for local paths (should not be used for data)
            return os.path.isdir(path)
    
    def _remove_directory(self, path: str):
        """Remove directory and contents (always S3 for data)"""
        if path.startswith('s3://'):
            path_parts = path.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] + '/' if len(path_parts) > 1 else ''
            
            # List all objects with the prefix
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if 'Contents' in response:
                # Delete all objects
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                if objects_to_delete:
                    self.s3_client.delete_objects(
                        Bucket=bucket,
                        Delete={'Objects': objects_to_delete}
                    )
        else:
            # Fallback for local paths (should not be used for data)
            import shutil
            shutil.rmtree(path)

    def _remove_file(self, path: str):
        """Remove individual file (S3 compatible)"""
        if path.startswith('s3://'):
            path_parts = path.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ''
            self.s3_client.delete_object(Bucket=bucket, Key=key)
        else:
            # Fallback for local paths
            import os
            os.remove(path)

    def _get_file_size(self, path: str) -> int:
        """Get file size in bytes (S3 compatible)"""
        if path.startswith('s3://'):
            path_parts = path.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ''
            try:
                response = self.s3_client.head_object(Bucket=bucket, Key=key)
                return response['ContentLength']
            except ClientError:
                return 0
        else:
            # Fallback for local paths
            import os
            return os.path.getsize(path) if os.path.exists(path) else 0

    def _extract_partition_filters(self, sql: str, table_name: str) -> Dict[str, Any]:
        """Extract partition column filters from WHERE clause"""
        try:
            # Get table metadata to know partition columns
            table_metadata = self.catalog_client.get_table(table_name)
            if not table_metadata:
                return {}
            
            partition_cols = table_metadata.get("partition_spec", [])
            if not partition_cols:
                return {}
            
            # Simple regex to find partition column filters
            # This is a basic implementation - could be enhanced with SQL parser
            filters = {}
            where_match = re.search(r'WHERE\s+(.*?)(?:GROUP|ORDER|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
            if where_match:
                where_clause = where_match.group(1)
                
                # Check if we have time-based partitions and ts_ms filters
                has_time_partitions = any(col in ['year', 'month', 'day', 'hour'] for col in partition_cols)
                if has_time_partitions:
                    # Try to extract ts_ms filters and convert to partition filters
                    ts_filters = self._extract_ts_ms_filters(where_clause)
                    if ts_filters:
                        filters.update(ts_filters)
                
                for col in partition_cols:
                    values_found = []
                    
                    # First check for IN clause: col IN ('value1', 'value2')
                    in_pattern = rf"{col}\s+IN\s*\(([^)]+)\)"
                    in_match = re.search(in_pattern, where_clause, re.IGNORECASE)
                    if in_match:
                        # Extract values from IN clause
                        values_str = in_match.group(1)
                        # Parse comma-separated values, handling quotes
                        for value in re.findall(r"'([^']*)'|\"([^\"]*)\"|([^,\s]+)", values_str):
                            # value is a tuple, take the non-empty one
                            val = next(v for v in value if v)
                            values_found.append(val.strip())
                    
                    # Also check for OR conditions: col = 'value1' OR col = 'value2'
                    # Find all occurrences of col = value patterns
                    # Handle both quoted and unquoted values
                    or_pattern = rf"{col}\s*=\s*(?:'([^']*)'|\"([^\"]*)\"|([^\s)]+))"
                    for match in re.finditer(or_pattern, where_clause, re.IGNORECASE):
                        # Get the non-empty group (1 for single quotes, 2 for double quotes, 3 for unquoted)
                        val = match.group(1) or match.group(2) or match.group(3)
                        if val and val not in values_found:
                            values_found.append(val)
                    
                    # Set the filter value(s)
                    if len(values_found) == 1:
                        filters[col] = values_found[0]
                    elif len(values_found) > 1:
                        filters[col] = values_found
            
            return filters
        except Exception as e:
            logger.error(f"Error extracting partition filters: {e}")
            return {}
    
    def _extract_ts_ms_filters(self, where_clause: str) -> Dict[str, Any]:
        """Extract ts_ms filters and convert to year/month/day partition filters"""
        filters = {}
        
        try:
            # Look for ts_ms comparisons with both numeric and string timestamps
            # Pattern for numeric: ts_ms > 123456789
            # Pattern for string: ts_ms > '2025-01-20 00:00:00'
            comparison_pattern = r"ts_ms\s*([><=]+)\s*(?:(\d+)|'([^']+)'|\"([^\"]+)\")"
            matches = re.findall(comparison_pattern, where_clause, re.IGNORECASE)
            
            min_ts = None
            max_ts = None
            min_inclusive = True
            max_inclusive = True
            
            for op, numeric_val, single_quote_val, double_quote_val in matches:
                # Get the actual value (numeric or string)
                value = numeric_val or single_quote_val or double_quote_val
                
                # Convert string timestamps to numeric
                if not numeric_val and value:
                    # Parse date string
                    from datetime import datetime
                    try:
                        # Try common formats
                        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S']:
                            try:
                                dt = datetime.strptime(value, fmt)
                                ts_value = int(dt.timestamp() * 1000)  # Convert to milliseconds
                                break
                            except ValueError:
                                continue
                        else:
                            # If no format matched, skip this comparison
                            continue
                    except Exception:
                        continue
                else:
                    ts_value = int(value)
                
                if op == '>':
                    if min_ts is None or ts_value >= min_ts:
                        min_ts = ts_value
                        min_inclusive = False
                elif op == '>=':
                    if min_ts is None or ts_value > min_ts:
                        min_ts = ts_value
                        min_inclusive = True
                elif op == '<':
                    if max_ts is None or ts_value <= max_ts:
                        max_ts = ts_value
                        max_inclusive = False
                elif op == '<=':
                    if max_ts is None or ts_value < max_ts:
                        max_ts = ts_value
                        max_inclusive = True
                elif op == '=':
                    min_ts = max_ts = ts_value
                    min_inclusive = max_inclusive = True
            
            # Check for BETWEEN clause with both numeric and string values
            between_pattern = r"ts_ms\s+BETWEEN\s+(?:(\d+)|'([^']+)'|\"([^\"]+)\")\s+AND\s+(?:(\d+)|'([^']+)'|\"([^\"]+)\")"
            between_match = re.search(between_pattern, where_clause, re.IGNORECASE)
            if between_match:
                # Extract start value
                start_val = between_match.group(1) or between_match.group(2) or between_match.group(3)
                end_val = between_match.group(4) or between_match.group(5) or between_match.group(6)
                
                # Convert string timestamps if needed
                if not between_match.group(1):  # Start is not numeric
                    from datetime import datetime
                    try:
                        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S']:
                            try:
                                dt = datetime.strptime(start_val, fmt)
                                min_ts = int(dt.timestamp() * 1000)
                                break
                            except ValueError:
                                continue
                    except Exception:
                        pass
                else:
                    min_ts = int(start_val)
                
                if not between_match.group(4):  # End is not numeric
                    from datetime import datetime
                    try:
                        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S']:
                            try:
                                dt = datetime.strptime(end_val, fmt)
                                max_ts = int(dt.timestamp() * 1000)
                                break
                            except ValueError:
                                continue
                    except Exception:
                        pass
                else:
                    max_ts = int(end_val)
                
                min_inclusive = max_inclusive = True
            
            # Convert timestamps to date parts
            if min_ts or max_ts:
                # Import datetime here to avoid circular imports
                from datetime import datetime, timedelta
                
                # Determine if values are in milliseconds or seconds
                sample_ts = min_ts if min_ts else max_ts
                if sample_ts > 1e12:  # Milliseconds
                    if min_ts:
                        min_date = datetime.fromtimestamp(min_ts / 1000)
                        if not min_inclusive:
                            min_date = min_date + timedelta(milliseconds=1)
                    if max_ts:
                        max_date = datetime.fromtimestamp(max_ts / 1000)
                        if not max_inclusive:
                            max_date = max_date - timedelta(milliseconds=1)
                else:  # Seconds
                    if min_ts:
                        min_date = datetime.fromtimestamp(min_ts)
                        if not min_inclusive:
                            min_date = min_date + timedelta(seconds=1)
                    if max_ts:
                        max_date = datetime.fromtimestamp(max_ts)
                        if not max_inclusive:
                            max_date = max_date - timedelta(seconds=1)
                
                # Extract year/month/day filters based on the range
                if min_ts and max_ts:
                    # If both bounds exist, check if they're in the same partition
                    if min_date.year == max_date.year:
                        filters['year'] = str(min_date.year)
                        if min_date.month == max_date.month:
                            filters['month'] = str(min_date.month).zfill(2)
                            if min_date.day == max_date.day:
                                filters['day'] = str(min_date.day).zfill(2)
                                # Check if we need hour-level filtering
                                # Only add hour filter if it actually narrows down the search
                                time_span_hours = (max_date - min_date).total_seconds() / 3600
                                
                                # If filtering the whole day (or close to it), don't add hour filter
                                # Also check if we're spanning from hour 0 to hour 23
                                if time_span_hours >= 23 or (min_date.hour == 0 and max_date.hour == 23):
                                    # Don't add hour filter - we're essentially filtering the whole day
                                    pass
                                elif min_date.hour == max_date.hour:
                                    # Same hour
                                    filters['hour'] = str(min_date.hour).zfill(2)
                                else:
                                    # Multiple hours but less than a full day
                                    hours = []
                                    current = min_date.replace(minute=0, second=0, microsecond=0)
                                    end = max_date.replace(minute=59, second=59, microsecond=999999)
                                    while current <= end:
                                        hours.append(str(current.hour).zfill(2))
                                        current = current + timedelta(hours=1)
                                        if current.hour == 0:  # Wrapped to next day
                                            break
                                    # Only add hour filter if it's beneficial (less than 12 hours)
                                    if len(hours) <= 12:
                                        filters['hour'] = hours
                            else:
                                # Multiple days in same month
                                days = []
                                current = min_date.replace(hour=0, minute=0, second=0, microsecond=0)
                                end = max_date.replace(hour=23, minute=59, second=59, microsecond=999999)
                                while current <= end:
                                    days.append(str(current.day).zfill(2))
                                    current = current + timedelta(days=1)
                                if len(days) <= 31:  # Reasonable limit
                                    filters['day'] = days
                        else:
                            # Multiple months in same year
                            months = []
                            current = datetime(min_date.year, min_date.month, 1)
                            end = datetime(max_date.year, max_date.month, 1)
                            while current <= end:
                                months.append(str(current.month).zfill(2))
                                if current.month == 12:
                                    current = datetime(current.year + 1, 1, 1)
                                else:
                                    current = datetime(current.year, current.month + 1, 1)
                            if len(months) <= 12:
                                filters['month'] = months
                elif min_ts:
                    # Only lower bound - filter from this point forward
                    filters['year'] = str(min_date.year)
                    filters['month'] = str(min_date.month).zfill(2)
                    filters['day'] = str(min_date.day).zfill(2)
                    # Only add hour if it's not the beginning of the day
                    # If it's hour 0, we want all hours of this day and beyond
                    if min_date.hour > 0:
                        # List remaining hours of this day
                        hours = [str(h).zfill(2) for h in range(min_date.hour, 24)]
                        if len(hours) <= 12:  # Only if it narrows the search significantly
                            filters['hour'] = hours
                elif max_ts:
                    # Only upper bound - filter up to this point
                    filters['year'] = str(max_date.year)
                    filters['month'] = str(max_date.month).zfill(2)
                    filters['day'] = str(max_date.day).zfill(2)
                    # Only add hour if it's not the end of the day
                    # If it's hour 23, we want all hours up to this day
                    if max_date.hour < 23:
                        # List hours from start of day up to this hour
                        hours = [str(h).zfill(2) for h in range(0, max_date.hour + 1)]
                        if len(hours) <= 12:  # Only if it narrows the search significantly
                            filters['hour'] = hours
                
                logger.info(f"Extracted ts_ms filters: min={min_ts}, max={max_ts}, partition_filters={filters}")
            
        except Exception as e:
            logger.error(f"Error extracting ts_ms filters: {e}")
        
        return filters
    
    def execute_query(self, raw_sql: str, explain_analyze: bool = False):
        """Execute a SQL query and return results as DataFrame (and optionally explain plan)"""
        if not raw_sql.strip().lower().startswith("select"):
            raise ValueError("only SELECT allowed")
        
        explain_output = None
        
        # Optionally run explain analyze and capture it
        if explain_analyze:
            print(f"\n[EXPLAIN REQUESTED] Running EXPLAIN ANALYZE for query")
            
            try:
                # Set detailed explain output and enable profiling for stats
                original_explain_output = None
                original_profiling = None
                try:
                    # Save current settings
                    result = self.con.execute("SELECT value FROM duckdb_settings() WHERE name = 'explain_output'").fetchone()
                    original_explain_output = result[0] if result else 'physical_only'
                    
                    result = self.con.execute("SELECT value FROM duckdb_settings() WHERE name = 'enable_profiling'").fetchone()
                    original_profiling = result[0] if result else 'no_output'
                    
                    # Set to 'all' for maximum detail and enable profiling
                    self.con.execute("SET explain_output = 'all'")
                    self.con.execute("SET enable_profiling = 'query_tree'")
                    self.con.execute("SET profiling_output = ''")
                except Exception as e:
                    print(f"Note: Could not set explain settings: {e}")
                    pass
                
                # Use EXPLAIN (ANALYZE) for detailed execution stats including file counts
                try:
                    # Try EXPLAIN ANALYZE first for more detailed stats
                    explain_result = self.con.execute(f"EXPLAIN (ANALYZE) {raw_sql}").fetchall()
                    explain_type = "EXPLAIN (ANALYZE)"
                except:
                    # Fallback to regular EXPLAIN
                    explain_result = self.con.execute(f"EXPLAIN {raw_sql}").fetchall()
                    explain_type = "EXPLAIN"
                
                # Restore original settings
                try:
                    if original_explain_output:
                        self.con.execute(f"SET explain_output = '{original_explain_output}'")
                    if original_profiling:
                        self.con.execute(f"SET enable_profiling = '{original_profiling}'")
                except:
                    pass
                
                # Capture explain output for response
                explain_lines = []
                
                if explain_result and len(explain_result) > 0:
                    # DuckDB EXPLAIN ANALYZE returns ('analyzed_plan', '<plan content>')
                    for row in explain_result:
                        if len(row) >= 2:
                            key = str(row[0])
                            value = str(row[1])
                            
                            # The plan is a multi-line string with query tree
                            if value.strip():  # Make sure we have content
                                explain_lines.append(f"{key}:")
                                for line in value.split('\n'):
                                    explain_lines.append(line)
                            else:
                                # If value is empty, the whole row might be the plan
                                explain_lines.append(str(row))
                        else:
                            # Single column result
                            explain_lines.append(str(row))
                
                explain_output = '\n'.join(explain_lines)
                
                # Also print to console for debugging
                print(f"\n══════════════════════════════════════════════════════")
                print(f"  DuckDB {explain_type} OUTPUT")
                print("══════════════════════════════════════════════════════")
                print(explain_output if explain_output else "[Empty explain output]")
                print("══════════════════════════════════════════════════════\n")
                
            except Exception as e:
                explain_output = f"Failed to run EXPLAIN ANALYZE: {str(e)}"
                print(f"[EXPLAIN ERROR] {explain_output}")
        
        # Execute the query and return DataFrame
        result = self.con.execute(raw_sql).fetch_df()
        
        # Return both result and explain output if requested
        if explain_analyze:
            return result, explain_output
        else:
            return result

    def convert_dataframe_to_grafana_format(self, df, target_name: str = "query_result") -> List[Dict[str, Any]]:
        """Convert DataFrame to Grafana time series format"""
        try:
            # Look for timestamp columns (common names)
            timestamp_cols = []
            for col in df.columns:
                col_lower = col.lower()
                if any(ts_name in col_lower for ts_name in ['ts_ms']):
                    timestamp_cols.append(col)
            
            # If no timestamp column found, create index-based timestamps
            if not timestamp_cols:
                import time
                now = int(time.time() * 1000)
                timestamps = [now - i * 60_000 for i in range(len(df))]  # 1 minute intervals
            else:
                # Use the first timestamp column found
                timestamp_col = timestamp_cols[0]
                timestamps = df[timestamp_col].tolist()
                
                # Convert to milliseconds if needed
                if df[timestamp_col].dtype in ['int64', 'float64'] and df[timestamp_col].max() < 1e12:
                    # Likely seconds, convert to milliseconds
                    timestamps = [int(ts * 1000) if ts else 0 for ts in timestamps]
                elif df[timestamp_col].dtype == 'datetime64[ns]':
                    # Convert datetime to milliseconds
                    timestamps = [int(ts.timestamp() * 1000) if pd.notna(ts) else 0 for ts in df[timestamp_col]]
            
            # Get numeric columns for values
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            # Remove timestamp columns from numeric columns
            numeric_cols = [col for col in numeric_cols if col not in timestamp_cols]
            
            results = []
            
            # If no numeric columns, return the first non-timestamp column
            if not numeric_cols:
                value_cols = [col for col in df.columns if col not in timestamp_cols]
                if value_cols:
                    numeric_cols = [value_cols[0]]
            
            # Create a series for each numeric column
            for col in numeric_cols:
                datapoints = []
                for i, (timestamp, value) in enumerate(zip(timestamps, df[col])):
                    if pd.notna(value):
                        datapoints.append([float(value), timestamp])
                    else:
                        datapoints.append([0, timestamp])
                
                results.append({
                    "target": f"{target_name}_{col}",
                    "datapoints": datapoints
                })
            
            # If no results, create a default series
            if not results:
                datapoints = [[0, ts] for ts in timestamps[:10]]  # Limit to 10 points
                results.append({
                    "target": target_name,
                    "datapoints": datapoints
                })
            
            return results
            
        except Exception as e:
            print(f"Error converting DataFrame to Grafana format: {e}")
            # Return empty result on error
            return [{
                "target": target_name,
                "datapoints": []
            }]

    def delete_data(self, table_name: str, where_clause: str = None, 
                   delete_table: bool = False, partitions: List[str] = None) -> Dict[str, Any]:
        """Delete data from table with various deletion modes (S3 compatible)"""
        try:
            table_path = self._get_data_path(table_name)
            if not self._path_exists(table_path):
                raise ValueError(f"Table '{table_name}' does not exist")
            
            deletion_stats = {
                "rows_deleted": 0,
                "partitions_deleted": 0,
                "files_deleted": 0,
                "deletion_mode": "conditional"
            }
            
            # Track deleted files for manifest cleanup
            deleted_files = []
            
            if delete_table:
                # Delete entire table data and schema
                size_before_mb = self._get_directory_size_mb(table_path)
                self._remove_directory(table_path)
                
                # Drop table from catalog
                try:
                    self.catalog_client.drop_table(table_name)
                    print(f"Dropped table {table_name} from catalog")
                except Exception as e:
                    print(f"Warning: Could not drop table from catalog: {e}")
                
                # Remove from DuckLake catalog if using local catalog
                # Note: catalog unregistration not implemented yet
                
                deletion_stats["deletion_mode"] = "entire_table"
                deletion_stats["size_deleted_mb"] = size_before_mb
                return deletion_stats
            
            if partitions:
                # Delete specific partitions
                deletion_stats["deletion_mode"] = "partition_based"
                for partition in partitions:
                    partition_path = f"{table_path}/{partition}"
                    if self._path_exists(partition_path) and self._is_directory(partition_path):
                        # Collect files before deletion
                        deleted_files.extend(self._collect_parquet_files(partition_path))
                        self._remove_directory(partition_path)
                        deletion_stats["partitions_deleted"] += 1
                
                # Update manifest to remove deleted files
                if deleted_files and self.catalog_client:
                    self.catalog_client.remove_files_from_manifest(table_name, deleted_files)
                
                return deletion_stats
            
            if where_clause:
                # Conditional deletion with WHERE clause
                deletion_stats["deletion_mode"] = "conditional"
                
                # Check if table is partitioned
                items = self._list_directory(table_path)
                is_partitioned = any(self._is_directory(f"{table_path}/{item}") 
                                   for item in items 
                                   if not item.startswith('.'))
                
                if is_partitioned:
                    # Handle partitioned table - recursively find all partition directories
                    total_rows_deleted = 0
                    
                    def process_directory(current_path: str):
                        items = self._list_directory(current_path)
                        has_parquet = any(item.endswith('.parquet') for item in items)
                        
                        if has_parquet:
                            # This directory has parquet files, process it
                            rows_del, files_del = self._delete_from_partition(current_path, where_clause)
                            deleted_files.extend(files_del)
                            return rows_del
                        else:
                            # Recurse into subdirectories
                            total_deleted = 0
                            for item in items:
                                if not item.startswith('.'):
                                    subdir_path = f"{current_path}/{item}"
                                    if self._is_directory(subdir_path):
                                        total_deleted += process_directory(subdir_path)
                            return total_deleted
                    
                    total_rows_deleted = process_directory(table_path)
                    deletion_stats["rows_deleted"] = total_rows_deleted
                else:
                    # Handle non-partitioned table
                    rows_del, files_del = self._delete_from_partition(table_path, where_clause)
                    deletion_stats["rows_deleted"] = rows_del
                    deleted_files.extend(files_del)
                
                # Update manifest to remove deleted files
                if deleted_files and self.catalog_client:
                    self.catalog_client.remove_files_from_manifest(table_name, deleted_files)
                
                return deletion_stats
            else:
                # Delete all data but preserve table structure (truncate)
                deletion_stats["deletion_mode"] = "truncate_table"
                
                # Collect all files before deletion
                deleted_files = self._collect_parquet_files(table_path)
                
                # Delete all parquet files recursively
                def count_and_delete_files(current_path: str) -> int:
                    files_deleted = 0
                    items = self._list_directory(current_path)
                    
                    for item in items:
                        if not item.startswith('.'):
                            item_path = f"{current_path}/{item}"
                            if self._is_directory(item_path):
                                files_deleted += count_and_delete_files(item_path)
                            elif item.endswith('.parquet'):
                                self._remove_file(item_path)
                                files_deleted += 1
                    return files_deleted
                
                files_deleted = count_and_delete_files(table_path)
                deletion_stats["files_deleted"] = files_deleted
                deletion_stats["rows_deleted"] = "all"
                
                # Update manifest to remove all files
                if deleted_files and self.catalog_client:
                    self.catalog_client.remove_files_from_manifest(table_name, deleted_files)
                
                return deletion_stats
                
        except Exception as e:
            print(f"Error deleting data: {e}")
            raise

    def _delete_from_partition(self, partition_path: str, where_clause: str) -> Tuple[int, List[str]]:
        """Delete rows from a partition based on WHERE clause (S3 compatible)
        Returns: (rows_deleted, list_of_deleted_files)
        """
        try:
            # Get all parquet files in partition
            items = self._list_directory(partition_path)
            parquet_files = [f for f in items if f.endswith('.parquet')]
            
            if not parquet_files:
                return 0, []
            
            # Read all files with schema unification
            full_paths = [f"{partition_path}/{f}" for f in parquet_files]
            temp_table = f"temp_delete_{uuid.uuid4().hex}"
            kept_table = f"temp_kept_{uuid.uuid4().hex}"
            deleted_files = []
            
            try:
                # Create temporary table - use proper list syntax for DuckDB
                print(f"Delete: Processing {len(parquet_files)} files in partition {partition_path}")
                print(f"Delete: Files = {parquet_files}")
                
                # Use DuckDB list syntax properly
                if len(full_paths) == 1:
                    # Single file
                    sql_query = f"""
                        CREATE TEMP TABLE {temp_table} AS 
                        SELECT * FROM read_parquet('{full_paths[0]}', union_by_name=True)
                    """
                else:
                    # Multiple files - use list syntax
                    file_list_str = "['" + "', '".join(full_paths) + "']"
                    sql_query = f"""
                        CREATE TEMP TABLE {temp_table} AS 
                        SELECT * FROM read_parquet({file_list_str}, union_by_name=True)
                    """
                
                print(f"Delete: Executing SQL = {sql_query}")
                self.con.execute(sql_query)
                
                # Count rows before deletion
                rows_before = self.con.execute(f"SELECT COUNT(*) FROM {temp_table}").fetchone()[0]
                print(f"Delete: Found {rows_before} rows before deletion")
                
                if rows_before == 0:
                    return 0, []
                
                # Create table with rows to keep (inverse of WHERE clause)
                keep_sql = f"""
                    CREATE TEMP TABLE {kept_table} AS 
                    SELECT * FROM {temp_table} WHERE NOT ({where_clause})
                """
                print(f"Delete: Keep query = {keep_sql}")
                self.con.execute(keep_sql)
                
                # Count rows after deletion
                rows_after = self.con.execute(f"SELECT COUNT(*) FROM {kept_table}").fetchone()[0]
                rows_deleted = rows_before - rows_after
                print(f"Delete: Keeping {rows_after} rows, deleting {rows_deleted} rows")
                
                if rows_deleted > 0:
                    # Remove old files
                    print(f"Delete: Removing {len(parquet_files)} old files")
                    for f in parquet_files:
                        file_path = f"{partition_path}/{f}"
                        print(f"Delete: Removing file {file_path}")
                        self._remove_file(file_path)
                        deleted_files.append(file_path)
                    
                    # Write back the kept data
                    if rows_after > 0:
                        output_path = f"{partition_path}/data_{uuid.uuid4().hex}.parquet"
                        print(f"Delete: Writing {rows_after} kept rows to {output_path}")
                        self.con.execute(f"""
                            COPY (SELECT * FROM {kept_table})
                            TO '{output_path}' (FORMAT PARQUET)
                        """)
                    else:
                        print("Delete: No rows to keep, partition will be empty")
                
                return rows_deleted, deleted_files
                
            finally:
                # Clean up temp tables
                self.con.execute(f"DROP TABLE IF EXISTS {temp_table}")
                self.con.execute(f"DROP TABLE IF EXISTS {kept_table}")
                
        except Exception as e:
            print(f"Error deleting from partition {partition_path}: {e}")
            raise

    def _get_directory_size_mb(self, directory_path: str) -> float:
        """Get directory size in MB (S3 compatible)"""
        if directory_path.startswith('s3://'):
            path_parts = directory_path.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] + '/' if len(path_parts) > 1 else ''
            
            total_size = 0
            try:
                paginator = self.s3_client.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            total_size += obj['Size']
                return total_size / (1024 * 1024)
            except ClientError:
                return 0
        else:
            # Fallback for local paths
            import os
            total_size = 0
            for root, _, files in os.walk(directory_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        total_size += os.path.getsize(file_path)
                    except (OSError, IOError):
                        pass
            return total_size / (1024 * 1024)

    def _validate_partition_structure(self, table_name: str, new_hive_columns: List[str]):
        """Validate that new partition structure matches existing table structure"""
        table_path = self._get_data_path(table_name)
        
        # If table doesn't exist yet, no validation needed
        if not self._path_exists(table_path):
            return
        
        # Get existing partition structure
        existing_structure = self._detect_partition_structure(table_path)
        
        if existing_structure and existing_structure != new_hive_columns:
            raise ValueError(
                f"Partition structure mismatch! Table '{table_name}' has existing partition structure: "
                f"{existing_structure} but trying to insert with: {new_hive_columns}. "
                f"Please use repartition to change the structure or insert with matching partitions."
            )

    def _collect_parquet_files(self, path: str) -> List[str]:
        """Recursively collect all parquet file paths under a directory"""
        files = []
        try:
            items = self._list_directory(path)
            for item in items:
                if not item.startswith('.'):
                    item_path = f"{path}/{item}"
                    if self._is_directory(item_path):
                        files.extend(self._collect_parquet_files(item_path))
                    elif item.endswith('.parquet'):
                        files.append(item_path)
        except Exception as e:
            logger.error(f"Error collecting parquet files from {path}: {e}")
        return files
    
    def _detect_partition_structure(self, table_path: str) -> List[str]:
        """Detect the partition structure from existing files (S3 compatible)"""
        partition_columns = []
        
        try:
            # Find a sample partition path by recursively exploring directories
            def explore_directory(current_path: str, depth: int = 0, max_depth: int = 10) -> List[str]:
                if depth > max_depth:
                    return []
                
                if not self._path_exists(current_path):
                    return []
                
                items = self._list_directory(current_path)
                
                # Look for parquet files at current level
                has_parquet = any(item.endswith('.parquet') for item in items)
                
                if has_parquet:
                    # Found parquet files, extract partition structure from path
                    if table_path.endswith('/'):
                        table_path_clean = table_path[:-1]
                    else:
                        table_path_clean = table_path
                    
                    if current_path != table_path_clean:
                        # Extract relative path
                        rel_path = current_path.replace(table_path_clean + '/', '')
                        if rel_path and '=' in rel_path:
                            # Parse partition folders in order
                            # e.g., "experiment_name=test/machine=3D_PRINTER_0/year=2025/month=08/day=12"
                            parts = rel_path.split('/')
                            found_columns = []
                            for part in parts:
                                if '=' in part:
                                    col_name = part.split('=')[0]
                                    found_columns.append(col_name)
                            return found_columns
                    return []
                else:
                    # No parquet files here, explore subdirectories
                    # First, check if any items look like partition directories
                    partition_dirs = [item for item in items if not item.startswith('.') and '=' in item]
                    
                    if partition_dirs:
                        # Explore partition directories
                        for item in partition_dirs:
                            subdir_path = f"{current_path}/{item}"
                            if self._is_directory(subdir_path):
                                result = explore_directory(subdir_path, depth + 1, max_depth)
                                if result:
                                    # If we found a complete path, return it
                                    return result
                    else:
                        # No partition dirs at this level, check all subdirectories
                        for item in items:
                            if not item.startswith('.'):
                                subdir_path = f"{current_path}/{item}"
                                if self._is_directory(subdir_path):
                                    result = explore_directory(subdir_path, depth + 1, max_depth)
                                    if result:
                                        return result
                    return []
            
            partition_columns = explore_directory(table_path)
            print(f"Detected partition structure for {table_path}: {partition_columns}")
            
        except Exception as e:
            print(f"Error detecting partition structure: {e}")
            partition_columns = []
        
        return partition_columns

    def get_tables(self) -> List[str]:
        """Get list of tables from REST Catalog"""
        try:
            # Get tables from REST catalog
            tables = self.catalog_client.list_tables()
            return sorted(tables)
            
        except Exception as e:
            print(f"Error getting tables from catalog: {e}")
            return []
    
    def get_tables_with_metadata(self) -> List[Dict[str, Any]]:
        """Get list of tables with metadata including file count and size"""
        try:
            # Get tables from REST catalog
            table_names = self.catalog_client.list_tables()
            tables_info = []
            
            for table_name in sorted(table_names):
                try:
                    # Get table metadata
                    table_metadata = self.catalog_client.get_table(table_name)
                    if table_metadata:
                        # Extract properties that might contain file count and size
                        properties = table_metadata.get("properties", {})
                        
                        table_info = {
                            "name": table_name,
                            "file_count": int(properties.get("file_count", 0)),
                            "total_size_mb": float(properties.get("total_size_mb", 0))
                        }
                        
                        # Don't fetch manifest here as it's slow - rely on properties only
                        # The properties should be updated when tables are discovered or refreshed
                        
                        tables_info.append(table_info)
                    else:
                        tables_info.append({
                            "name": table_name,
                            "file_count": 0,
                            "total_size_mb": 0
                        })
                except Exception as e:
                    logger.error(f"Error getting metadata for table {table_name}: {e}")
                    tables_info.append({
                        "name": table_name,
                        "file_count": 0,
                        "total_size_mb": 0
                    })
            
            return tables_info
            
        except Exception as e:
            print(f"Error getting tables with metadata: {e}")
            return []
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get table schema from REST Catalog"""
        try:
            # First try to get from REST catalog
            table_metadata = self.catalog_client.get_table(table_name)
            
            if table_metadata:
                # Convert Iceberg schema to our format
                columns = []
                for field in table_metadata.get("schema", {}).get("fields", []):
                    columns.append({
                        "name": field["name"],
                        "type": field["type"],
                        "nullable": not field.get("required", False)
                    })
                
                return {
                    "table": table_name,
                    "columns": columns,
                    "source": "catalog",
                    "location": table_metadata.get("location"),
                    "partitions": table_metadata.get("partition_spec", [])
                }
            else:
                # Table not in catalog, try to infer from data
                data_path = self._get_data_path(table_name)
                if self._path_exists(data_path):
                    # Get first parquet file to infer schema
                    files = self._list_parquet_files(data_path)
                    if files:
                        first_file = files[0]
                        # Use DESCRIBE to get schema without reading data
                        schema_result = self.con.execute(f"""
                            DESCRIBE SELECT * FROM read_parquet('{first_file}', file_row_number=true) LIMIT 0
                        """).fetchall()
                        
                        columns = []
                        for row in schema_result:
                            if row[0] != 'file_row_number':  # Skip internal column
                                columns.append({
                                    "name": row[0],
                                    "type": str(row[1]),
                                    "nullable": row[2] == 'YES' if len(row) > 2 else True
                                })
                        
                        return {
                            "table": table_name,
                            "columns": columns,
                            "source": "inferred"
                        }
                else:
                    return {"error": f"Table '{table_name}' not found"}
            
        except Exception as e:
            print(f"Error getting table schema: {e}")
            return {"error": str(e)}
    
    def _list_parquet_files(self, path: str, limit: int = 1) -> List[str]:
        """List parquet files in path (for schema inference)"""
        files = []
        if path.startswith('s3://'):
            # S3 path
            path_parts = path.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] if len(path_parts) > 1 else ''
            
            try:
                paginator = self.s3_client.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    for obj in page.get('Contents', []):
                        if obj['Key'].endswith('.parquet'):
                            files.append(f"s3://{bucket}/{obj['Key']}")
                            if len(files) >= limit:
                                return files
            except Exception as e:
                print(f"Error listing S3 files: {e}")
        
        return files

    def get_table_partition_info(self, table_name: str) -> Dict[str, Any]:
        """Get partition structure information for a table (optimized version)"""
        try:
            table_path = self._get_data_path(table_name)
            if not self._path_exists(table_path):
                raise ValueError(f"Table '{table_name}' does not exist")
            
            # Get partition structure without loading full tree
            partition_structure = self._detect_partition_structure(table_path)
            
            # Get basic stats without loading full tree
            # Just count top-level partitions and estimate counts
            top_level_items = self._list_directory(table_path)
            partition_count = sum(1 for item in top_level_items 
                                 if not item.startswith('.') and '=' in item)
            
            # For file count and size, we can't get exact numbers without scanning
            # Return estimates or skip these expensive operations
            return {
                "table_name": table_name,
                "partition_columns": partition_structure,
                "is_partitioned": len(partition_structure) > 0,
                "partition_count": partition_count,  # Top-level partition count
                "total_files": "Unknown (use partitions endpoint for details)",
                "total_size_mb": "Unknown (use partitions endpoint for details)",
                "note": "File counts and sizes not calculated for performance. Use /partitions endpoint with specific paths for detailed information."
            }
            
        except Exception as e:
            print(f"Error getting partition info: {e}")
            raise

    def _build_partition_tree_from_manifest(self, table_name: str) -> Dict[str, Dict[str, Any]]:
        """Build partition tree structure from manifest (fast, no S3 scanning)"""
        try:
            # Get manifest from catalog
            manifest = self.catalog_client.get_manifest(table_name)
            if not manifest or not manifest.get("entries"):
                return {}
            
            # Get partition columns ONCE from table metadata
            table_metadata = self.catalog_client.get_table(table_name)
            partition_spec = table_metadata.get("partition_spec", []) if table_metadata else []
            
            # Build tree structure from file paths
            tree = {}
            for entry in manifest["entries"]:
                file_path = entry["file_path"]
                partition_values = entry.get("partition_values", {})
                
                # Build partition path from values
                if partition_values and partition_spec:
                    parts = []
                    # Use cached partition spec
                    for col in partition_spec:
                        if col in partition_values:
                            parts.append(f"{col}={partition_values[col]}")
                    
                    if parts:
                        path = "/".join(parts)
                        if path not in tree:
                            tree[path] = {
                                "files": [],
                                "size": 0,
                                "partition_values": partition_values
                            }
                        tree[path]["files"].append(file_path)
                        tree[path]["size"] += entry.get("file_size", 0)
            
            return tree
        except Exception as e:
            logger.error(f"Error building partition tree from manifest: {e}")
            return {}
    
    def _get_partition_level_from_manifest(self, table_name: str, partition_path: str = "", 
                                          include_sizes: bool = False) -> Optional[List[Dict[str, Any]]]:
        """Get one partition level directly from manifest without building full tree (efficient for lazy loading)"""
        try:
            # Try to use optimized partition hierarchy endpoint first
            hierarchy = self.catalog_client.get_partition_hierarchy(table_name, partition_path, lazy=True)
            
            # Check if we got hierarchy response (new format)
            if hierarchy and "partitions" in hierarchy:
                # Convert hierarchy format to expected format
                items = []
                for partition in hierarchy["partitions"]:
                    item = {
                        "name": partition["name"],
                        "path": partition["path"],
                        "type": "directory",
                        "has_children": partition.get("has_children", False),
                        "file_count": partition.get("file_count", 0),
                        "size_mb": 0  # Size calculation can be added if needed
                    }
                    if "file_count" in partition:
                        item["direct_file_count"] = partition["file_count"]
                    items.append(item)
                return items
            
            # Fallback to old manifest-based approach if hierarchy endpoint not available
            manifest = hierarchy if hierarchy and "entries" in hierarchy else self.catalog_client.get_manifest(table_name)
            if not manifest or not manifest.get("entries"):
                return None
            
            # Get partition columns ONCE
            table_metadata = self.catalog_client.get_table(table_name)
            partition_spec = table_metadata.get("partition_spec", []) if table_metadata else []
            if not partition_spec:
                return []
            
            # Calculate depth of requested path
            path_depth = len(partition_path.split('/')) if partition_path else 0
            
            # Track items at this level
            level_items = {}  # name -> stats
            
            # Process only relevant entries
            for entry in manifest["entries"]:
                partition_values = entry.get("partition_values", {})
                if not partition_values:
                    continue
                
                # Build partition path for this entry
                entry_parts = []
                for col in partition_spec:
                    if col in partition_values:
                        entry_parts.append(f"{col}={partition_values[col]}")
                
                entry_path = "/".join(entry_parts)
                
                # Check if this entry is relevant to our current path
                if partition_path:
                    if not entry_path.startswith(partition_path + '/') and entry_path != partition_path:
                        continue
                
                # Extract the item at current depth
                path_parts = entry_path.split('/')
                if len(path_parts) > path_depth:
                    item_name = path_parts[path_depth]
                    
                    if item_name not in level_items:
                        level_items[item_name] = {
                            "file_count": 0,
                            "size": 0,
                            "is_leaf": len(path_parts) == path_depth + 1
                        }
                    
                    level_items[item_name]["file_count"] += 1
                    level_items[item_name]["size"] += entry.get("file_size", 0)
            
            # Convert to result format
            items = []
            for name, stats in level_items.items():
                item_path = f"{partition_path}/{name}" if partition_path else name
                items.append({
                    "name": name,
                    "type": "directory",
                    "path": item_path,
                    "has_children": not stats["is_leaf"],
                    "direct_file_count": stats["file_count"] if stats["is_leaf"] else 0,
                    "file_count": stats["file_count"],
                    "size_mb": stats["size"] / (1024 * 1024) if include_sizes else 0
                })
            
            return sorted(items, key=lambda x: x["name"])
            
        except Exception as e:
            logger.error(f"Error getting partition level from manifest: {e}")
            return None
    
    def _get_level_from_manifest_tree(self, partition_tree: Dict[str, Dict[str, Any]], 
                                     partition_path: str = "", include_sizes: bool = False) -> List[Dict[str, Any]]:
        """Extract one level of partitions from manifest tree"""
        items = []
        path_depth = len(partition_path.split('/')) if partition_path else 0
        
        # Find all paths at the next level
        seen_items = set()
        for full_path in partition_tree.keys():
            # Check if this path is under our current path
            if partition_path and not full_path.startswith(partition_path + '/'):
                if partition_path != full_path:
                    continue
            
            # Split path and get the item at current depth
            parts = full_path.split('/')
            if len(parts) > path_depth:
                item_name = parts[path_depth]
                
                if item_name not in seen_items:
                    seen_items.add(item_name)
                    
                    # Check if this is a leaf (has files) or directory (has children)
                    item_path = f"{partition_path}/{item_name}" if partition_path else item_name
                    
                    # Check if exact match (leaf)
                    if item_path in partition_tree:
                        # This is a leaf with files
                        info = partition_tree[item_path]
                        items.append({
                            "name": item_name,
                            "type": "directory",
                            "path": item_path,
                            "has_children": False,
                            "direct_file_count": len(info["files"]),
                            "file_count": len(info["files"]),
                            "size_mb": info["size"] / (1024 * 1024) if include_sizes else 0
                        })
                    else:
                        # This is a directory with subdirectories
                        # Count children
                        has_children = any(p.startswith(item_path + '/') for p in partition_tree.keys())
                        
                        # Calculate size if requested
                        size = 0
                        file_count = 0
                        if include_sizes:
                            for p, info in partition_tree.items():
                                if p.startswith(item_path + '/') or p == item_path:
                                    size += info["size"]
                                    file_count += len(info["files"])
                        
                        items.append({
                            "name": item_name,
                            "type": "directory",
                            "path": item_path,
                            "has_children": has_children,
                            "direct_file_count": 0,
                            "file_count": file_count,
                            "size_mb": size / (1024 * 1024) if include_sizes else 0
                        })
        
        return sorted(items, key=lambda x: x["name"])
    
    def get_partition_level(self, table_name: str, partition_path: str = "", include_sizes: bool = False) -> List[Dict[str, Any]]:
        """Get one level of partition structure (lazy loading for performance)"""
        try:
            # Try to get just one level from manifest without building full tree
            manifest_level = self._get_partition_level_from_manifest(table_name, partition_path, include_sizes)
            if manifest_level is not None:
                return manifest_level
            
            # Fallback to S3 scanning if no manifest
            table_path = self._get_data_path(table_name)
            if not self._path_exists(table_path):
                raise ValueError(f"Table '{table_name}' does not exist")
            
            # Build full path to explore
            if partition_path:
                full_path = f"{table_path}/{partition_path}"
            else:
                full_path = table_path
            
            if not self._path_exists(full_path):
                return []
            
            # Get items at this level only
            items = []
            try:
                dir_items = sorted(self._list_directory(full_path))
                for item in dir_items:
                    if item.startswith('.'):
                        continue  # Skip hidden files
                    
                    item_path = f"{full_path}/{item}"
                    is_dir = self._is_directory(item_path)
                    
                    item_info = {
                        "name": item,
                        "type": "directory" if is_dir else "file",
                        "path": f"{partition_path}/{item}" if partition_path else item
                    }
                    
                    if is_dir:
                        # For directories, check if it has children (for UI expand indicator)
                        try:
                            child_items = self._list_directory(item_path)
                            has_children = any(not c.startswith('.') for c in child_items)
                            item_info["has_children"] = has_children
                            
                            # Only count direct files in this directory (no recursion for performance)
                            direct_file_count = sum(1 for c in child_items if c.endswith('.parquet'))
                            item_info["direct_file_count"] = direct_file_count
                            
                            # Only calculate sizes if explicitly requested (for performance)
                            if include_sizes:
                                # IMPORTANT: Only calculate direct children sizes, not recursive
                                # Recursive calculation is too slow on S3 with deep paths
                                total_size = 0
                                file_count = 0
                                
                                # Only scan direct children, no recursion
                                items = self._list_directory(item_path)
                                for subitem in items:
                                    full_subitem_path = f"{item_path}/{subitem}"
                                    if not self._is_directory(full_subitem_path) and subitem.endswith('.parquet'):
                                        file_count += 1
                                        try:
                                            total_size += self._get_file_size(full_subitem_path)
                                        except:
                                            pass
                                
                                item_info["file_count"] = file_count
                                item_info["size_bytes"] = total_size
                                item_info["size_mb"] = total_size / (1024 * 1024) if total_size else 0
                            else:
                                # Fast mode - no recursive size calculation
                                item_info["file_count"] = direct_file_count  # Only direct files
                                item_info["size_mb"] = 0
                        except:
                            item_info["has_children"] = False
                            item_info["direct_file_count"] = 0
                            item_info["size_mb"] = 0
                    else:
                        # For files, get size info
                        try:
                            file_size = self._get_file_size(item_path)
                            item_info["size_bytes"] = file_size
                            item_info["size_mb"] = file_size / (1024 * 1024) if file_size else 0
                            
                            # Add file extension
                            if '.' in item:
                                item_info["extension"] = item.split('.')[-1].lower()
                        except:
                            item_info["size_bytes"] = 0
                            item_info["size_mb"] = 0
                    
                    items.append(item_info)
                    
            except Exception as e:
                print(f"Error listing partition level: {e}")
                return []
            
            return items
            
        except Exception as e:
            print(f"Error getting partition level: {e}")
            raise
    
    def get_partition_tree(self, table_name: str) -> Dict[str, Any]:
        """Get the partition tree structure for a table (S3 compatible)"""
        try:
            # Try to use manifest first for better performance
            partition_tree = self._build_partition_tree_from_manifest(table_name)
            if partition_tree:
                # Build tree structure from manifest
                tree = {
                    "type": "directory",
                    "name": table_name,
                    "path": "",
                    "size_mb": 0,
                    "file_count": 0,
                    "children": []
                }
                
                # Convert flat partition tree to hierarchical structure
                for path, info in partition_tree.items():
                    parts = path.split('/')
                    current = tree
                    
                    for i, part in enumerate(parts):
                        # Find or create child
                        child = None
                        for c in current["children"]:
                            if c["name"] == part:
                                child = c
                                break
                        
                        if not child:
                            child = {
                                "type": "directory",
                                "name": part,
                                "path": "/".join(parts[:i+1]),
                                "size_mb": 0,
                                "file_count": 0,
                                "children": []
                            }
                            current["children"].append(child)
                        
                        # Update stats
                        child["size_mb"] += info["size"] / (1024 * 1024)
                        child["file_count"] += len(info["files"])
                        
                        current = child
                
                # Update root stats
                tree["size_mb"] = sum(info["size"] for info in partition_tree.values()) / (1024 * 1024)
                tree["file_count"] = sum(len(info["files"]) for info in partition_tree.values())
                
                # Add summary
                summary = {
                    "total_size_mb": tree["size_mb"],
                    "total_files": tree["file_count"],
                    "is_partitioned": len(tree["children"]) > 0,
                    "partition_count": len(partition_tree),
                    "direct_files": 0
                }
                
                return {
                    "tree": tree,
                    "summary": summary
                }
            
            # Fallback to S3 scanning if no manifest
            table_path = self._get_data_path(table_name)
            if not self._path_exists(table_path):
                raise ValueError(f"Table '{table_name}' does not exist")
            
            def build_tree(path: str, relative_path: str = "") -> Dict[str, Any]:
                """Recursively build partition tree structure"""
                is_directory = self._is_directory(path)
                
                tree = {
                    "type": "directory" if is_directory else "file",
                    "name": path.split('/')[-1] if relative_path else table_name,
                    "path": relative_path,
                    "size_mb": 0,
                    "file_count": 0,
                    "children": []
                }
                
                if is_directory:
                    # Directory - recurse into children
                    try:
                        items = sorted(self._list_directory(path))
                        for item in items:
                            if item.startswith('.'):
                                continue  # Skip hidden files
                            
                            item_path = f"{path}/{item}"
                            child_relative_path = f"{relative_path}/{item}" if relative_path else item
                            child_tree = build_tree(item_path, child_relative_path)
                            
                            # Aggregate statistics
                            tree["size_mb"] += child_tree["size_mb"]
                            tree["file_count"] += child_tree["file_count"]
                            tree["children"].append(child_tree)
                            
                    except Exception as e:
                        tree["error"] = f"Cannot read directory: {e}"
                else:
                    # File - get file info
                    try:
                        file_size = self._get_file_size(path)
                        tree["size_mb"] = file_size / (1024 * 1024) if file_size else 0
                        tree["file_count"] = 1
                        tree["size_bytes"] = file_size or 0
                        
                        # Add file extension info
                        filename = path.split('/')[-1]
                        if '.' in filename:
                            tree["extension"] = filename.split('.')[-1].lower()
                            
                    except Exception as e:
                        tree["error"] = f"Cannot read file: {e}"
                
                return tree
            
            partition_tree = build_tree(table_path)
            
            # Add summary statistics
            summary = {
                "total_size_mb": partition_tree["size_mb"],
                "total_files": partition_tree["file_count"],
                "is_partitioned": len(partition_tree["children"]) > 0 and 
                                any(child["type"] == "directory" for child in partition_tree["children"]),
                "partition_count": len([child for child in partition_tree["children"] 
                                      if child["type"] == "directory"]),
                "direct_files": len([child for child in partition_tree["children"] 
                                   if child["type"] == "file"])
            }
            
            return {
                "tree": partition_tree,
                "summary": summary
            }
            
        except Exception as e:
            print(f"Error getting partition tree: {e}")
            raise

    def get_partition_folders(self, table_name: str, partition_path: str) -> List[str]:
        """Get folder names at a specific partition path"""
        try:
            # Try to use manifest first for better performance
            partition_tree = self._build_partition_tree_from_manifest(table_name)
            if partition_tree:
                # Extract folders at the specified level
                folders = set()
                path_depth = len(partition_path.split('/')) if partition_path else 0
                
                for full_path in partition_tree.keys():
                    # Check if this path is under our current path
                    if partition_path and not full_path.startswith(partition_path + '/'):
                        continue
                    
                    # Get the folder at the next level
                    parts = full_path.split('/')
                    if len(parts) > path_depth:
                        folders.add(parts[path_depth])
                
                return sorted(list(folders))
            
            # Fallback to S3 scanning if no manifest
            table_path = self._get_data_path(table_name)
            if not self._path_exists(table_path):
                raise ValueError(f"Table '{table_name}' does not exist")
            
            # Navigate to the specific partition path
            full_path = f"{table_path}/{partition_path}"
            if not self._path_exists(full_path) or not self._is_directory(full_path):
                return []
            
            # Get immediate subdirectories
            folders = []
            for item in sorted(self._list_directory(full_path)):
                if item.startswith('.'):
                    continue
                item_path = f"{full_path}/{item}"
                if self._is_directory(item_path):
                    folders.append(item)
            
            return folders
            
        except Exception as e:
            print(f"Error getting partition folders: {e}")
            return []

    def get_grafana_metrics(self) -> List[Dict[str, Any]]:
        """Get available metrics for Grafana - returns all tables as metrics"""
        try:
            tables = self.get_tables()
            
            metrics = []
            for table in tables:
                metrics.append({
                    "label": table,
                    "value": table,
                    "payloads": [
                        {
                             "name": "query",
                             "type": "textarea",             
                             "placeholder": f"SELECT * FROM {table} LIMIT 100"}
                    ]
                })
            
            return metrics
            
        except Exception as e:
            print(f"Error getting metrics: {e}")
            # Return default metrics on error
            return [
                {
                    "label": "No tables found",
                    "value": "default_table"
                }
            ]

    def insert_dataframe(self, table_name: str, df: pd.DataFrame) -> int:
        """Insert DataFrame into Hive-partitioned data structure"""
        try:
            # Use the partitioned insert method without partitioning for simple inserts
            return self.insert_dataframe_partitioned(table_name, df, [], None, 'day')
            
        except Exception as e:
            print(f"Error inserting DataFrame: {e}")
            raise

    def insert_dataframe_partitioned(self, table_name: str, df: pd.DataFrame, 
                                   hive_columns: List[str] = None, 
                                   timestamp_column: str = None,
                                   timestamp_format: str = 'day') -> int:
        """Insert DataFrame with DuckLake metadata + Hive Parquet data"""
        print(f"DEBUG: insert_dataframe_partitioned called for table: {table_name}")
        try:
            if hive_columns is None:
                hive_columns = []
            
            # Validate partition structure if table already exists
            if hive_columns:
                self._validate_partition_structure(table_name, hive_columns)
            
            # 1. Process timestamp partitioning for columns that need it
            original_df = df.copy()  # Keep original for schema registration
            time_partition_columns = []
            
            # Check which hive_columns are time-based and need special processing
            for col in hive_columns:
                if col in ['year', 'month', 'day', 'hour'] and timestamp_column:
                    time_partition_columns.append(col)
            
            # Add timestamp partitions to DataFrame if needed
            if time_partition_columns and timestamp_column and timestamp_column in df.columns:
                df = self._add_timestamp_partitions(df, timestamp_column, timestamp_format)
            
            # 2. Store as Hive-partitioned Parquet files using specified column order
            result = self._store_partitioned_data(table_name, df, hive_columns)
            
            # 3. Register table schema in DuckLake catalog (after data is stored)
            self._register_table_schema_with_data(table_name, original_df)
            
            return result
            
        except Exception as e:
            print(f"Error inserting partitioned DataFrame: {e}")
            raise

    def _register_table_schema(self, table_name: str, df: pd.DataFrame):
        """Register table schema in REST Catalog"""
        try:
            data_path = self._get_data_path(table_name)
            
            # Convert DataFrame schema to Iceberg-style schema
            fields = []
            for col_name, dtype in zip(df.columns, df.dtypes):
                field = {
                    "id": len(fields) + 1,
                    "name": col_name,
                    "type": self._pandas_to_iceberg_type(str(dtype)),
                    "required": False
                }
                fields.append(field)
            
            schema = {
                "type": "struct",
                "fields": fields
            }
            
            # Register in catalog
            self.catalog_client.register_table(
                table_name=table_name,
                location=data_path,
                schema=schema
            )
            
            print(f"Registered table schema in REST Catalog: {table_name}")
            
        except Exception as e:
            print(f"Warning: Could not register table schema: {e}")

    def _register_table_schema_with_data(self, table_name: str, df: pd.DataFrame):
        """Register table schema in REST Catalog using actual stored data"""
        print(f"DEBUG: _register_table_schema_with_data called for table: {table_name}")
        try:
            data_path = self._get_data_path(table_name)
            
            # Check if data directory exists
            if self._path_exists(data_path):
                # Discover and register in catalog
                self.catalog_client.discover_table(
                    table_name=table_name,
                    s3_path=data_path
                )
                print(f"Registered table with data in REST Catalog: {table_name}")
            else:
                print(f"Warning: Data directory does not exist: {data_path}")
            
        except Exception as e:
            print(f"Warning: Could not register table with data: {e}")

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
    
    def _add_timestamp_partitions(self, df: pd.DataFrame, timestamp_column: str, timestamp_format: str) -> pd.DataFrame:
        """Add timestamp-based partition columns"""
        # Convert to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df[timestamp_column]):
            sample_value = float(df[timestamp_column].dropna().iloc[0] if not df[timestamp_column].dropna().empty else 0)
            
            if sample_value > 1e12:  # Milliseconds
                df[timestamp_column] = pd.to_datetime(df[timestamp_column], unit='ms')
            elif sample_value > 1e9:   # Seconds  
                df[timestamp_column] = pd.to_datetime(df[timestamp_column], unit='s')
            else:
                df[timestamp_column] = pd.to_datetime(df[timestamp_column])
        
        # Add partition columns
        if timestamp_format == 'day':
            df['year'] = df[timestamp_column].dt.year.astype(str)
            df['month'] = df[timestamp_column].dt.month.astype(str).str.zfill(2)
            df['day'] = df[timestamp_column].dt.day.astype(str).str.zfill(2)
        elif timestamp_format == 'hour':
            df['year'] = df[timestamp_column].dt.year.astype(str)
            df['month'] = df[timestamp_column].dt.month.astype(str).str.zfill(2)
            df['day'] = df[timestamp_column].dt.day.astype(str).str.zfill(2)
            df['hour'] = df[timestamp_column].dt.hour.astype(str).str.zfill(2)
        elif timestamp_format == 'month':
            df['year'] = df[timestamp_column].dt.year.astype(str)
            df['month'] = df[timestamp_column].dt.month.astype(str).str.zfill(2)
            
        return df

    def _get_time_partition_columns(self, timestamp_format: str) -> List[str]:
        """Get list of time partition column names"""
        if timestamp_format == 'day':
            return ['year', 'month', 'day']
        elif timestamp_format == 'hour':
            return ['year', 'month', 'day', 'hour']
        elif timestamp_format == 'month':
            return ['year', 'month']
        return []
    

    def _store_partitioned_data(self, table_name: str, df: pd.DataFrame, partition_columns: List[str]) -> int:
        """Store DataFrame as Hive-partitioned Parquet files (always S3)"""
        data_path = self._get_data_path(table_name)
        
        if not partition_columns:
            # No partitioning - store as single file
            file_path = f"{data_path}/data_{uuid.uuid4().hex}.parquet"
            
            # Use DuckDB's COPY TO with S3 path
            temp_table = f"temp_insert_{uuid.uuid4().hex}"
            self.con.register(temp_table, df)
            self.con.execute(f"COPY (SELECT * FROM {temp_table}) TO '{file_path}' (FORMAT PARQUET)")
            self.con.unregister(temp_table)
            return len(df)
        
        total_rows = 0
        # Group by partition columns
        for group_values, group_df in df.groupby(partition_columns):
            if not isinstance(group_values, tuple):
                group_values = (group_values,)
            
            # Build partition path
            partition_parts = [f"{col}={val}" for col, val in zip(partition_columns, group_values)]
            partition_path = f"{data_path}/" + "/".join(partition_parts)
            
            # Remove partition columns from data
            data_df = group_df.drop(columns=partition_columns, errors='ignore')
            
            # Save as Parquet using DuckDB's COPY TO with S3 path
            file_path = f"{partition_path}/data_{uuid.uuid4().hex}.parquet"
            temp_table = f"temp_partition_{uuid.uuid4().hex}"
            self.con.register(temp_table, data_df)
            self.con.execute(f"COPY (SELECT * FROM {temp_table}) TO '{file_path}' (FORMAT PARQUET)")
            self.con.unregister(temp_table)
                
            total_rows += len(data_df)
        
        return total_rows

    def repartition_table_v3(self, table_name: str, hive_columns: List[str] = None,
                         timestamp_column: str = None, timestamp_format: str = 'day',
                         target_file_size_mb: int = 128, progress_callback=None) -> Dict[str, Any]:
        """Memory-efficient repartition - process data in chunks"""
        print(f"\n=== Starting memory-efficient repartition for table '{table_name}' ===")
        if progress_callback:
            progress_callback("Starting repartition...", 0)
        
        data_path = self._get_data_path(table_name)
        temp_data_path = f"{data_path}_temp_{uuid.uuid4().hex[:8]}"
        
        if not self._path_exists(data_path):
            raise ValueError(f"Table '{table_name}' does not exist")
        
        # Ensure temp directory doesn't exist
        if self._path_exists(temp_data_path):
            print(f"Cleaning up existing temp directory: {temp_data_path}")
            self._remove_directory(temp_data_path)
        
        try:
            # Step 1: Get list of all parquet files
            print("Step 1: Scanning for parquet files...")
            if progress_callback:
                progress_callback("Scanning for parquet files...", 5)
            all_files = []
            
            if data_path.startswith('s3://'):
                # For S3, use boto3 to list files
                bucket, prefix = data_path.replace('s3://', '').split('/', 1)
                s3_client = boto3.client('s3')
                paginator = s3_client.get_paginator('list_objects_v2')
                
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            if obj['Key'].endswith('.parquet'):
                                all_files.append(f"s3://{bucket}/{obj['Key']}")
            else:
                # For local files
                import glob
                pattern = os.path.join(data_path, '**', '*.parquet')
                all_files = glob.glob(pattern, recursive=True)
            
            print(f"Found {len(all_files)} parquet files")
            if progress_callback:
                progress_callback(f"Found {len(all_files)} parquet files", 10)
            
            if not all_files:
                raise ValueError("No parquet files found in table")
            
            # Check for mixed partition structures
            print("\nChecking partition structure consistency...")
            partition_depths = {}
            for file_path in all_files[:100]:  # Sample first 100 files
                # Count partition depth (number of = signs in path)
                partition_count = file_path.count('=')
                if partition_count > 0:
                    # Extract partition structure
                    parts = file_path.split('/')
                    partition_parts = [p for p in parts if '=' in p]
                    partition_structure = '/'.join([p.split('=')[0] for p in partition_parts])
                    
                    if partition_structure not in partition_depths:
                        partition_depths[partition_structure] = 0
                    partition_depths[partition_structure] += 1
            
            if len(partition_depths) > 1:
                print("\n⚠️  WARNING: Mixed partition structures detected!")
                print("Found the following partition structures:")
                for structure, count in partition_depths.items():
                    print(f"  - {structure}: {count} files")
                print("\nRepartitioning will handle these files individually, which may be slower.")
                print("Consider using consistent partition structures for better performance.")
            
            # Step 2: Process files in batches
            batch_size = 50  # Process 50 files at a time
            total_rows = 0
            
            # First pass: determine schema and validate columns
            print("\nStep 2: Determining schema and validating columns...")
            if progress_callback:
                progress_callback("Determining schema and validating columns...", 15)
            sample_df = self.con.execute(f"""
                SELECT * FROM read_parquet('{all_files[0]}', 
                                         union_by_name=true, 
                                         hive_partitioning=true)
                LIMIT 1
            """).fetch_df()
            
            base_columns = list(sample_df.columns)
            print(f"Base columns: {base_columns}")
            
            # Validate partition columns
            final_partition_columns = []
            if hive_columns:
                print(f"\nValidating partition columns: {hive_columns}")
                for col in hive_columns:
                    if col not in base_columns and col not in ['year', 'month', 'day', 'hour']:
                        raise ValueError(f"Partition column '{col}' not found in data. Available: {base_columns}")
                    final_partition_columns.append(col)
            
            # Step 3: Create temp directory structure
            print(f"\nStep 3: Creating temp directory with partitions: {final_partition_columns or 'none'}")
            
            # Create a temporary view for processing
            temp_view = f"repartition_view_{uuid.uuid4().hex[:8]}"
            
            # Step 4: Process files in batches
            print(f"\nStep 4: Processing files in batches of {batch_size}...")
            if progress_callback:
                progress_callback(f"Processing {len(all_files)} files in batches...", 20)
            
            for batch_idx in range(0, len(all_files), batch_size):
                batch_files = all_files[batch_idx:batch_idx + batch_size]
                batch_num = batch_idx // batch_size + 1
                total_batches = (len(all_files) + batch_size - 1) // batch_size
                
                # Calculate progress percentage for this batch
                batch_progress = 20 + (batch_idx / len(all_files)) * 60  # 20-80% for processing
                
                print(f"\nProcessing batch {batch_num}/{total_batches} ({len(batch_files)} files)...")
                if progress_callback:
                    progress_callback(f"Processing batch {batch_num}/{total_batches}...", int(batch_progress))
                
                # Create file list for DuckDB
                file_list = "['" + "', '".join(batch_files) + "']"
                
                # Try to create view with hive_partitioning, but handle mixed partition structures
                try:
                    # Create a view for this batch with timestamp processing if needed
                    if timestamp_column and timestamp_column in base_columns:
                        # Process with timestamp columns
                        timestamp_expr = self._get_timestamp_expression(timestamp_column, timestamp_format)
                        
                        if timestamp_expr:
                            self.con.execute(f"""
                                CREATE OR REPLACE VIEW {temp_view} AS
                                SELECT *,
                                    {timestamp_expr}
                                FROM read_parquet({file_list}, 
                                                union_by_name=true, 
                                                hive_partitioning=true)
                            """)
                        else:
                            # No timestamp columns to add
                            self.con.execute(f"""
                                CREATE OR REPLACE VIEW {temp_view} AS
                                SELECT * 
                                FROM read_parquet({file_list}, 
                                                union_by_name=true, 
                                                hive_partitioning=true)
                            """)
                    else:
                        # Process without timestamp columns
                        self.con.execute(f"""
                            CREATE OR REPLACE VIEW {temp_view} AS
                            SELECT * 
                            FROM read_parquet({file_list}, 
                                            union_by_name=true, 
                                            hive_partitioning=true)
                        """)
                except Exception as e:
                    if "Hive partition mismatch" in str(e) or "key" in str(e) and "not found" in str(e):
                        print(f"  Warning: Mixed partition structures detected, processing files individually...")
                        
                        # Process files one by one to handle mixed structures
                        combined_dfs = []
                        for single_file in batch_files:
                            try:
                                if timestamp_column and timestamp_column in base_columns:
                                    timestamp_expr = self._get_timestamp_expression(timestamp_column, timestamp_format)
                                    if timestamp_expr:
                                        single_df = self.con.execute(f"""
                                            SELECT *,
                                                {timestamp_expr}
                                            FROM read_parquet('{single_file}', 
                                                            union_by_name=true, 
                                                            hive_partitioning=true)
                                        """).fetch_df()
                                    else:
                                        single_df = self.con.execute(f"""
                                            SELECT * 
                                            FROM read_parquet('{single_file}', 
                                                            union_by_name=true, 
                                                            hive_partitioning=true)
                                        """).fetch_df()
                                else:
                                    single_df = self.con.execute(f"""
                                        SELECT * 
                                        FROM read_parquet('{single_file}', 
                                                        union_by_name=true, 
                                                        hive_partitioning=true)
                                    """).fetch_df()
                                
                                combined_dfs.append(single_df)
                            except Exception as file_error:
                                print(f"    Error reading file {single_file}: {file_error}")
                                # Skip problematic files
                                continue
                        
                        if combined_dfs:
                            # Combine all dataframes using pandas concat with proper handling
                            import pandas as pd
                            combined_df = pd.concat(combined_dfs, ignore_index=True)
                            
                            # Ensure partition columns are strings
                            partition_cols_to_check = ['year', 'month', 'day', 'hour']
                            for col in partition_cols_to_check:
                                if col in combined_df.columns:
                                    combined_df[col] = combined_df[col].astype(str)
                                    # Ensure proper padding for month/day/hour
                                    if col in ['month', 'day', 'hour']:
                                        combined_df[col] = combined_df[col].str.zfill(2)
                            
                            # Save to a temporary parquet file instead of using register
                            temp_parquet = f"{temp_data_path}/batch_{uuid.uuid4().hex[:8]}.parquet"
                            combined_df.to_parquet(temp_parquet, index=False)
                            
                            # Create view from the temporary parquet file
                            if timestamp_column and timestamp_column in base_columns:
                                timestamp_expr = self._get_timestamp_expression(timestamp_column, timestamp_format)
                                if timestamp_expr:
                                    self.con.execute(f"""
                                        CREATE OR REPLACE VIEW {temp_view} AS
                                        SELECT *,
                                            {timestamp_expr}
                                        FROM read_parquet('{temp_parquet}')
                                    """)
                                else:
                                    self.con.execute(f"""
                                        CREATE OR REPLACE VIEW {temp_view} AS
                                        SELECT * FROM read_parquet('{temp_parquet}')
                                    """)
                            else:
                                self.con.execute(f"""
                                    CREATE OR REPLACE VIEW {temp_view} AS
                                    SELECT * FROM read_parquet('{temp_parquet}')
                                """)
                        else:
                            raise Exception("No files could be processed in this batch")
                    else:
                        # Re-raise other errors
                        raise
                
                # Get row count for this batch
                batch_row_count = self.con.execute(f"SELECT COUNT(*) FROM {temp_view}").fetchone()[0]
                total_rows += batch_row_count
                print(f"  Processing {batch_row_count:,} rows...")
                
                # Write this batch to temp location
                if final_partition_columns:
                    # Write with partitioning
                    # For the first batch, we can overwrite. For subsequent batches, we append
                    overwrite_option = "OVERWRITE_OR_IGNORE" if batch_idx == 0 else "APPEND"
                    
                    # Build a SELECT statement that ensures partition columns are strings
                    select_parts = []
                    for col in self.con.execute(f"DESCRIBE {temp_view}").fetchall():
                        col_name = col[0]
                        if col_name in final_partition_columns and col_name in ['year', 'month', 'day', 'hour']:
                            # Explicitly cast time partition columns to VARCHAR
                            select_parts.append(f"CAST({col_name} AS VARCHAR) AS {col_name}")
                        else:
                            select_parts.append(col_name)
                    
                    select_clause = ", ".join(select_parts)
                    
                    self.con.execute(f"""
                        COPY (SELECT {select_clause} FROM {temp_view})
                        TO '{temp_data_path}'
                        (FORMAT PARQUET, 
                         PARTITION_BY ({', '.join(final_partition_columns)}),
                         {overwrite_option})
                    """)
                else:
                    # Write without partitioning - create unique file for each batch
                    batch_file = f"{temp_data_path}/data_{batch_num:04d}.parquet"
                    self.con.execute(f"""
                        COPY (SELECT * FROM {temp_view})
                        TO '{batch_file}'
                        (FORMAT PARQUET)
                    """)
                
                print(f"  Batch {batch_num} written successfully")
            
            # Clean up the view
            self.con.execute(f"DROP VIEW IF EXISTS {temp_view}")
            
            print(f"\nProcessed {total_rows:,} total rows")
            
            # Step 5: Swap directories (atomic operation)
            print(f"\nStep 5: Swapping directories...")
            if progress_callback:
                progress_callback("Swapping directories...", 85)
            backup_path = f"{data_path}_old_{uuid.uuid4().hex[:8]}"
            
            # Move current to backup
            if data_path.startswith('s3://'):
                # For S3, rename by copying
                self._copy_s3_directory(data_path, backup_path)
                self._remove_directory(data_path)
            else:
                import shutil
                shutil.move(data_path, backup_path)
            
            # Move temp to current
            if temp_data_path.startswith('s3://'):
                self._copy_s3_directory(temp_data_path, data_path)
                self._remove_directory(temp_data_path)
            else:
                import shutil
                shutil.move(temp_data_path, data_path)
            
            # Remove backup
            self._remove_directory(backup_path)
            
            # Step 6: Update catalog with new partition structure
            print(f"\nStep 6: Updating catalog with new partition structure...")
            if progress_callback:
                progress_callback("Updating catalog...", 95)
            # First refresh to get latest file list
            self.catalog_client.refresh_table(table_name)
            
            # Then update the partition spec in the catalog
            self._update_table_partition_spec(table_name, final_partition_columns)
            
            print(f"\n=== Repartition completed successfully ===")
            if progress_callback:
                progress_callback("Repartition completed successfully!", 100)
            
            return {
                "rows_processed": total_rows,
                "new_hive_columns": final_partition_columns,
                "timestamp_column": timestamp_column,
                "timestamp_format": timestamp_format,
                "target_file_size_mb": target_file_size_mb
            }
            
        except Exception as e:
            print(f"\n!!! Repartition failed: {e}")
            
            # Clean up temp directory if it exists
            if self._path_exists(temp_data_path):
                print("Cleaning up temp directory...")
                self._remove_directory(temp_data_path)
            
            raise
    
    def _get_timestamp_expression(self, timestamp_column: str, timestamp_format: str) -> str:
        """Generate SQL expression for extracting timestamp parts"""
        expressions = []
        
        # Convert timestamp to proper datetime first
        ts_expr = f"CAST({timestamp_column} AS TIMESTAMP)"
        
        # Add year/month/day/hour based on format
        # Explicitly cast everything to VARCHAR to ensure string type
        if timestamp_format in ['day', 'hour', 'month']:
            expressions.append(f"CAST(CAST(YEAR({ts_expr}) AS VARCHAR) AS VARCHAR) AS year")
        if timestamp_format in ['day', 'hour', 'month']:
            expressions.append(f"CAST(LPAD(CAST(MONTH({ts_expr}) AS VARCHAR), 2, '0') AS VARCHAR) AS month")
        if timestamp_format in ['day', 'hour']:
            expressions.append(f"CAST(LPAD(CAST(DAY({ts_expr}) AS VARCHAR), 2, '0') AS VARCHAR) AS day")
        if timestamp_format == 'hour':
            expressions.append(f"CAST(LPAD(CAST(HOUR({ts_expr}) AS VARCHAR), 2, '0') AS VARCHAR) AS hour")
        
        return ',\n            '.join(expressions) if expressions else ''
    
    def _copy_s3_directory(self, source_path: str, dest_path: str):
        """Copy S3 directory using boto3"""
        # Parse bucket and prefix
        source_parts = source_path.replace('s3://', '').split('/', 1)
        bucket = source_parts[0]
        source_prefix = source_parts[1] + '/' if len(source_parts) > 1 else ''
        
        dest_parts = dest_path.replace('s3://', '').split('/', 1)
        dest_prefix = dest_parts[1] + '/' if len(dest_parts) > 1 else ''
        
        # List and copy all objects
        paginator = self.s3_client.get_paginator('list_objects_v2')
        copied_count = 0
        
        for page in paginator.paginate(Bucket=bucket, Prefix=source_prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    source_key = obj['Key']
                    dest_key = source_key.replace(source_prefix, dest_prefix, 1)
                    
                    # Copy object
                    copy_source = {'Bucket': bucket, 'Key': source_key}
                    self.s3_client.copy_object(
                        CopySource=copy_source,
                        Bucket=bucket,
                        Key=dest_key
                    )
                    copied_count += 1
        
        return copied_count
    
    def _update_table_partition_spec(self, table_name: str, partition_columns: List[str]):
        """Update the partition specification in the catalog"""
        try:
            # Get current table metadata
            table_metadata = self.catalog_client.get_table(table_name)
            if not table_metadata:
                print(f"Warning: Could not find table {table_name} in catalog")
                return
            
            # Update the table with new partition spec through the catalog API
            # Use PUT request to the create_or_update_table endpoint
            url = f"{self.catalog_url}/namespaces/{self.catalog_namespace}/tables/{table_name}"
            payload = {
                "location": table_metadata.get('location'),
                "schema": table_metadata.get('schema'),
                "partition_spec": partition_columns,
                "properties": table_metadata.get('properties', {})
            }
            
            response = requests.put(url, json=payload)
            response.raise_for_status()
            
            print(f"Updated partition spec for table {table_name}: {partition_columns}")
                    
        except Exception as e:
            print(f"Warning: Failed to update partition spec in catalog: {e}")
    
    def _get_table_partition_spec(self, table_name: str) -> List[str]:
        """Get the partition specification from the catalog"""
        try:
            table_metadata = self.catalog_client.get_table(table_name)
            if table_metadata and 'partition_spec' in table_metadata:
                return table_metadata.get('partition_spec', [])
            return []
        except Exception as e:
            print(f"Warning: Failed to get partition spec from catalog: {e}")
            return []
    
    def repartition_table_v2(self, table_name: str, hive_columns: List[str] = None,
                         timestamp_column: str = None, timestamp_format: str = 'day',
                         target_file_size_mb: int = 128) -> Dict[str, Any]:
        """Completely new repartition implementation"""
        print(f"\n=== Starting repartition_v2 for table '{table_name}' ===")
        data_path = self._get_data_path(table_name)
        backup_path = f"{data_path}_backup_{uuid.uuid4().hex[:8]}"
        
        if not self._path_exists(data_path):
            raise ValueError(f"Table '{table_name}' does not exist")
        
        try:
            # Step 1: Move entire directory to backup (simple rename, no reading)
            print(f"Step 1: Moving data to backup location...")
            if data_path.startswith('s3://'):
                # For S3, use boto3 to copy objects
                print("Using boto3 to copy S3 objects...")
                
                # Parse bucket and prefix
                data_parts = data_path.replace('s3://', '').split('/', 1)
                bucket = data_parts[0]
                data_prefix = data_parts[1] + '/' if len(data_parts) > 1 else ''
                
                backup_parts = backup_path.replace('s3://', '').split('/', 1)
                backup_prefix = backup_parts[1] + '/' if len(backup_parts) > 1 else ''
                
                # List and copy all objects
                paginator = self.s3_client.get_paginator('list_objects_v2')
                copied_count = 0
                
                for page in paginator.paginate(Bucket=bucket, Prefix=data_prefix):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            source_key = obj['Key']
                            # Replace data prefix with backup prefix
                            dest_key = source_key.replace(data_prefix, backup_prefix, 1)
                            
                            # Copy object
                            copy_source = {'Bucket': bucket, 'Key': source_key}
                            self.s3_client.copy_object(
                                CopySource=copy_source,
                                Bucket=bucket,
                                Key=dest_key
                            )
                            copied_count += 1
                            
                            if copied_count % 100 == 0:
                                print(f"Copied {copied_count} files...")
                
                print(f"Backup created successfully - copied {copied_count} files")
            else:
                # Local filesystem - simple rename
                import shutil
                shutil.move(data_path, backup_path)
            
            # Step 2: Read all data from backup location
            print(f"\nStep 2: Reading data from backup...")
            
            # List all parquet files in backup
            all_files = self._collect_parquet_files(backup_path)
            print(f"Found {len(all_files)} parquet files")
            
            if not all_files:
                raise ValueError("No parquet files found in table")
            
            # Read files in batches to handle large datasets
            batch_size = 100
            all_data = []
            
            # First, detect what partition columns exist in the directory structure
            print("Detecting partition columns from directory structure...")
            sample_file = all_files[0] if all_files else None
            detected_partition_cols = []
            
            if sample_file and '=' in sample_file:
                # Extract partition columns from path
                # e.g., .../experiment_name=test/machine=3D_PRINTER_0/year=2025/...
                path_parts = sample_file.split('/')
                for part in path_parts:
                    if '=' in part:
                        col_name = part.split('=')[0]
                        if col_name not in detected_partition_cols:
                            detected_partition_cols.append(col_name)
                print(f"Detected partition columns in path: {detected_partition_cols}")
            
            # Read data - try with hive_partitioning first, fall back if it fails
            try:
                print("Attempting to read with hive_partitioning=true...")
                for i in range(0, len(all_files), batch_size):
                    batch_files = all_files[i:i+batch_size]
                    print(f"Reading batch {i//batch_size + 1}/{(len(all_files) + batch_size - 1)//batch_size}")
                    
                    file_list = "['" + "', '".join(batch_files) + "']"
                    batch_df = self.con.execute(f"""
                        SELECT * FROM read_parquet({file_list}, union_by_name=true, hive_partitioning=true)
                    """).fetch_df()
                    
                    all_data.append(batch_df)
                print("Successfully read with hive_partitioning")
            except Exception as e:
                print(f"Failed to read with hive_partitioning: {e}")
                print("Falling back to reading without hive_partitioning...")
                
                # Clear any partial data
                all_data = []
                
                # Read without hive partitioning
                for i in range(0, len(all_files), batch_size):
                    batch_files = all_files[i:i+batch_size]
                    print(f"Reading batch {i//batch_size + 1}/{(len(all_files) + batch_size - 1)//batch_size}")
                    
                    file_list = "['" + "', '".join(batch_files) + "']"
                    batch_df = self.con.execute(f"""
                        SELECT * FROM read_parquet({file_list}, union_by_name=true)
                    """).fetch_df()
                    
                    # Manually add partition columns from file paths
                    if detected_partition_cols:
                        print(f"Adding partition columns from paths: {detected_partition_cols}")
                        for idx, file_path in enumerate(batch_files[0:len(batch_df)]):
                            path_parts = file_path.split('/')
                            for part in path_parts:
                                if '=' in part:
                                    col_name, col_value = part.split('=', 1)
                                    if col_name in detected_partition_cols:
                                        if col_name not in batch_df.columns:
                                            batch_df[col_name] = None
                                        # This is a simplified approach - in reality we'd need to map rows to files
                                        batch_df.loc[idx:idx, col_name] = col_value
                    
                    all_data.append(batch_df)
            
            # Combine all data
            print("\nStep 3: Combining all data...")
            df = pd.concat(all_data, ignore_index=True) if len(all_data) > 1 else all_data[0]
            print(f"Total rows: {len(df):,}")
            print(f"Columns: {list(df.columns)}")
            
            # Step 4: Add timestamp columns if needed
            if timestamp_column and timestamp_column in df.columns:
                print(f"\nStep 4: Adding timestamp columns from '{timestamp_column}'...")
                
                # Convert to datetime
                if df[timestamp_column].dtype.name not in ['datetime64[ns]', 'datetime64[ms]']:
                    if pd.api.types.is_numeric_dtype(df[timestamp_column]):
                        # Assume milliseconds if numeric
                        df['_ts'] = pd.to_datetime(df[timestamp_column], unit='ms')
                    else:
                        df['_ts'] = pd.to_datetime(df[timestamp_column])
                else:
                    df['_ts'] = df[timestamp_column]
                
                # Add time columns
                if 'year' not in df.columns and timestamp_format in ['day', 'hour']:
                    df['year'] = df['_ts'].dt.year.astype(str)
                if 'month' not in df.columns and timestamp_format in ['day', 'hour', 'month']:
                    df['month'] = df['_ts'].dt.month.astype(str).str.zfill(2)
                if 'day' not in df.columns and timestamp_format in ['day', 'hour']:
                    df['day'] = df['_ts'].dt.day.astype(str).str.zfill(2)
                if 'hour' not in df.columns and timestamp_format == 'hour':
                    df['hour'] = df['_ts'].dt.hour.astype(str).str.zfill(2)
                
                # Drop temp column
                df = df.drop('_ts', axis=1)
                print(f"Added timestamp columns. New columns: {list(df.columns)}")
            
            # Step 5: Validate partition columns
            final_partition_columns = []
            if hive_columns:
                print(f"\nStep 5: Validating partition columns: {hive_columns}")
                for col in hive_columns:
                    if col not in df.columns:
                        raise ValueError(f"Partition column '{col}' not found in data. Available: {list(df.columns)}")
                    final_partition_columns.append(col)
            
            # Step 6: Write data with new partitioning
            print(f"\nStep 6: Writing data with partitions: {final_partition_columns or 'none'}")
            
            # Register dataframe
            write_table = f"repartition_write_{uuid.uuid4().hex[:8]}"
            self.con.register(write_table, df)
            
            try:
                if final_partition_columns:
                    # Write with partitioning
                    self.con.execute(f"""
                        COPY (SELECT * FROM {write_table})
                        TO '{data_path}'
                        (FORMAT PARQUET, PARTITION_BY ({', '.join(final_partition_columns)}), OVERWRITE_OR_IGNORE)
                    """)
                else:
                    # Write without partitioning
                    self.con.execute(f"""
                        COPY (SELECT * FROM {write_table})
                        TO '{data_path}/data.parquet'
                        (FORMAT PARQUET, OVERWRITE_OR_IGNORE)
                    """)
                
                print("Data written successfully")
                
            finally:
                self.con.unregister(write_table)
            
            # Step 7: Clean up backup
            print(f"\nStep 7: Cleaning up backup...")
            self._remove_directory(backup_path)
            
            # Step 8: Refresh catalog
            print(f"\nStep 8: Refreshing catalog...")
            self.catalog_client.refresh_table(table_name)
            
            print(f"\n=== Repartition completed successfully ===")
            
            return {
                "rows_processed": len(df),
                "new_hive_columns": final_partition_columns,
                "timestamp_column": timestamp_column,
                "timestamp_format": timestamp_format,
                "target_file_size_mb": target_file_size_mb
            }
            
        except Exception as e:
            print(f"\n!!! Repartition failed: {e}")
            
            # Restore backup
            if self._path_exists(backup_path):
                print(f"Restoring from backup...")
                
                # Remove any partial data
                if self._path_exists(data_path):
                    self._remove_directory(data_path)
                
                # Move backup back
                if data_path.startswith('s3://'):
                    # Use boto3 to copy objects back
                    print("Restoring backup using boto3...")
                    
                    # Parse bucket and prefix
                    backup_parts = backup_path.replace('s3://', '').split('/', 1)
                    bucket = backup_parts[0]
                    backup_prefix = backup_parts[1] + '/' if len(backup_parts) > 1 else ''
                    
                    data_parts = data_path.replace('s3://', '').split('/', 1)
                    data_prefix = data_parts[1] + '/' if len(data_parts) > 1 else ''
                    
                    # List and copy all objects back
                    paginator = self.s3_client.get_paginator('list_objects_v2')
                    restored_count = 0
                    
                    for page in paginator.paginate(Bucket=bucket, Prefix=backup_prefix):
                        if 'Contents' in page:
                            for obj in page['Contents']:
                                source_key = obj['Key']
                                # Replace backup prefix with data prefix
                                dest_key = source_key.replace(backup_prefix, data_prefix, 1)
                                
                                # Copy object
                                copy_source = {'Bucket': bucket, 'Key': source_key}
                                self.s3_client.copy_object(
                                    CopySource=copy_source,
                                    Bucket=bucket,
                                    Key=dest_key
                                )
                                restored_count += 1
                    
                    print(f"Restored {restored_count} files from backup")
                    self._remove_directory(backup_path)
                else:
                    import shutil
                    shutil.move(backup_path, data_path)
                
                print("Backup restored")
            
            raise
    
    def repartition_table(self, table_name: str, hive_columns: List[str] = None,
                         timestamp_column: str = None, timestamp_format: str = 'day',
                         target_file_size_mb: int = 128) -> Dict[str, Any]:
        """Repartition existing table data with new partition scheme"""
        try:
            data_path = self._get_data_path(table_name)
            if not self._path_exists(data_path):
                raise ValueError(f"Table '{table_name}' does not exist")
            
            # Create backup path
            backup_suffix = f"_backup_{uuid.uuid4().hex}"
            backup_path = data_path + backup_suffix
            temp_table = f"{table_name}_temp_{uuid.uuid4().hex[:8]}"
            
            print(f"Starting repartition of table '{table_name}'")
            
            # Clean up any leftover restored.parquet files from previous failed attempts
            restored_file = f"{data_path}/restored.parquet"
            if self._path_exists(restored_file):
                print(f"Removing leftover restored.parquet from previous failed attempt")
                self._remove_file(restored_file)
            
            # Also check for data.parquet in root which shouldn't be there for partitioned tables
            data_file = f"{data_path}/data.parquet"
            if self._path_exists(data_file):
                print(f"Removing leftover data.parquet from previous operations")
                self._remove_file(data_file)
            
            # Step 1: Create backup
            print("Creating backup...")
            # Detect existing partition structure
            existing_hive_columns = self._detect_partition_structure(data_path)
            print(f"Detected existing partitions: {existing_hive_columns}")
            
            # Create backup - handle potential partition mismatches
            try:
                # First try with hive_partitioning
                print("Attempting to create backup with hive partitioning...")
                self.con.execute(f"""
                    COPY (SELECT * FROM read_parquet('{data_path}/**/*.parquet', union_by_name=true, hive_partitioning=true))
                    TO '{backup_path}/backup.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE)
                """)
                print("Backup created successfully with hive partitioning")
            except Exception as backup_error:
                print(f"Failed to create backup with hive partitioning: {backup_error}")
                # If hive partitioning fails, try without it
                print("Attempting to create backup without hive partitioning...")
                try:
                    self.con.execute(f"""
                        COPY (SELECT * FROM read_parquet('{data_path}/**/*.parquet', union_by_name=true))
                        TO '{backup_path}/backup.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE)
                    """)
                    print("Backup created successfully without hive partitioning")
                except Exception as e2:
                    print(f"Failed to create backup: {e2}")
                    raise Exception(f"Unable to create backup: {backup_error} / {e2}")
            
            try:
                # Step 2: Read all data into a temporary table (not view)
                # This loads the data into memory/temp storage before we delete the files
                # Use hive_partitioning=true to include partition columns from directory structure
                self.con.execute(f"""
                    CREATE TEMP TABLE {temp_table} AS 
                    SELECT * FROM read_parquet('{data_path}/**/*.parquet', union_by_name=true, hive_partitioning=true)
                """)
                
                # Get row count
                row_count = self.con.execute(f"SELECT COUNT(*) FROM {temp_table}").fetchone()[0]
                print(f"Processing {row_count:,} rows")
                
                # Debug: Check columns in temp table
                temp_columns = self.con.execute(f"""
                    SELECT column_name 
                    FROM (DESCRIBE {temp_table})
                """).fetchall()
                temp_column_names = [col[0] for col in temp_columns]
                print(f"DEBUG: Columns in temp table after reading parquet: {temp_column_names}")
                
                # Step 3: Delete original data (now safe because data is in temp table)
                self._remove_directory(data_path)
                
                # Step 4: Re-create with new partitioning
                # Prepare partition columns
                final_partition_columns = []
                if hive_columns:
                    final_partition_columns.extend(hive_columns)
                
                # Get existing columns in the temp table
                existing_columns = self.con.execute(f"""
                    SELECT column_name 
                    FROM (DESCRIBE {temp_table})
                """).fetchall()
                existing_column_names = [col[0].lower() for col in existing_columns]
                print(f"DEBUG: Existing columns in table: {existing_column_names}")
                
                # Determine if we need to add timestamp columns
                need_timestamp_columns = False
                if timestamp_column:
                    # Get the time columns that would be added for this format
                    time_cols_for_format = []
                    if timestamp_format == 'hour':
                        time_cols_for_format = ['year', 'month', 'day', 'hour']
                    elif timestamp_format == 'day':
                        time_cols_for_format = ['year', 'month', 'day']
                    elif timestamp_format == 'month':
                        time_cols_for_format = ['year', 'month']
                    
                    # Check if any time columns need to be created (not in existing columns)
                    for col in time_cols_for_format:
                        if col not in existing_column_names:
                            need_timestamp_columns = True
                            if col not in final_partition_columns:
                                final_partition_columns.append(col)
                    
                    print(f"DEBUG: Time columns needed: {time_cols_for_format}")
                    print(f"DEBUG: Need timestamp columns: {need_timestamp_columns}")
                    print(f"DEBUG: Final partition columns: {final_partition_columns}")
                
                # Read all data into pandas DataFrame first (like compaction does)
                print("Reading all data into memory...")
                df = self.con.execute(f"SELECT * FROM {temp_table}").fetch_df()
                print(f"DEBUG: DataFrame shape: {df.shape}")
                print(f"DEBUG: DataFrame columns: {list(df.columns)}")
                
                # Check if the requested partition columns might be existing partition columns
                # that are not in the data but in the file paths
                print(f"DEBUG: Requested hive_columns: {hive_columns}")
                print(f"DEBUG: Existing partition structure: {existing_hive_columns}")
                
                # Validate that non-timestamp partition columns exist
                if hive_columns:
                    missing_columns = []
                    for col in hive_columns:
                        if col not in df.columns and col not in ['year', 'month', 'day', 'hour']:
                            # Check if it's an existing partition column
                            if col not in existing_hive_columns:
                                missing_columns.append(col)
                            else:
                                print(f"WARNING: Column '{col}' is a partition column but not in data. "
                                      f"It needs to be reconstructed from file paths.")
                    
                    if missing_columns:
                        raise ValueError(f"Partition columns not found in table: {missing_columns}. "
                                       f"Available columns: {list(df.columns)}. "
                                       f"Existing partitions: {existing_hive_columns}")
                
                # Add timestamp columns if needed
                if need_timestamp_columns and timestamp_column and timestamp_column in df.columns:
                    print(f"Adding timestamp columns from {timestamp_column}")
                    
                    # Convert timestamp column to datetime if needed
                    if df[timestamp_column].dtype.name not in ['datetime64[ns]', 'datetime64[ms]']:
                        # Try to convert - handle milliseconds vs seconds
                        sample_val = df[timestamp_column].iloc[0] if len(df) > 0 else 0
                        try:
                            if isinstance(sample_val, (int, float)):
                                if sample_val > 1e12:  # Milliseconds
                                    df['_ts'] = pd.to_datetime(df[timestamp_column], unit='ms')
                                elif sample_val > 1e9:  # Seconds
                                    df['_ts'] = pd.to_datetime(df[timestamp_column], unit='s')
                                else:
                                    df['_ts'] = pd.to_datetime(df[timestamp_column])
                            else:
                                df['_ts'] = pd.to_datetime(df[timestamp_column])
                        except:
                            print(f"Warning: Could not convert {timestamp_column} to datetime")
                            df['_ts'] = pd.to_datetime(df[timestamp_column], errors='coerce')
                    else:
                        df['_ts'] = df[timestamp_column]
                    
                    # Add the time columns based on format
                    if timestamp_format in ['day', 'hour'] and 'year' not in df.columns:
                        df['year'] = df['_ts'].dt.year.astype(str)
                    if timestamp_format in ['day', 'hour', 'month'] and 'month' not in df.columns:
                        df['month'] = df['_ts'].dt.month.astype(str).str.zfill(2)
                    if timestamp_format in ['day', 'hour'] and 'day' not in df.columns:
                        df['day'] = df['_ts'].dt.day.astype(str).str.zfill(2)
                    if timestamp_format == 'hour' and 'hour' not in df.columns:
                        df['hour'] = df['_ts'].dt.hour.astype(str).str.zfill(2)
                    
                    # Drop the temporary timestamp column
                    if '_ts' in df.columns:
                        df = df.drop('_ts', axis=1)
                
                # Register the dataframe as a new temp table (like compaction does)
                final_table = f"{table_name}_final_{uuid.uuid4().hex[:8]}"
                self.con.register(final_table, df)
                
                # Debug: Check what columns are in the registered table
                final_columns = self.con.execute(f"""
                    SELECT column_name 
                    FROM (DESCRIBE {final_table})
                """).fetchall()
                final_column_names = [col[0] for col in final_columns]
                print(f"DEBUG: Columns in final registered table: {final_column_names}")
                
                try:
                    # Now write the data with partitioning
                    if final_partition_columns:
                        # Write with partitioning
                        print(f"Writing data with partitions: {final_partition_columns}")
                        self.con.execute(f"""
                            COPY (SELECT * FROM {final_table})
                            TO '{data_path}'
                            (FORMAT PARQUET, PARTITION_BY ({', '.join(final_partition_columns)}), OVERWRITE_OR_IGNORE)
                        """)
                    else:
                        # No partitioning
                        print("Writing data without partitions")
                        self.con.execute(f"""
                            COPY (SELECT * FROM {final_table})
                            TO '{data_path}/data.parquet'
                            (FORMAT PARQUET, OVERWRITE_OR_IGNORE)
                        """)
                finally:
                    # Clean up the registered table
                    self.con.unregister(final_table)
                
                # Clean up
                self.con.execute(f"DROP TABLE IF EXISTS {temp_table}")
                self._remove_directory(backup_path)
                
                # Refresh the catalog with new partition structure
                self.catalog_client.refresh_table(table_name)
                
                print(f"Repartition completed successfully")
                
                return {
                    "rows_processed": row_count,
                    "new_hive_columns": final_partition_columns,
                    "timestamp_column": timestamp_column,
                    "timestamp_format": timestamp_format,
                    "target_file_size_mb": target_file_size_mb
                }
                
            except Exception as e:
                # Restore backup on failure
                print(f"Repartition failed: {e}")
                print("Restoring from backup...")
                
                # Clean up temp table
                self.con.execute(f"DROP TABLE IF EXISTS {temp_table}")
                
                # Restore backup
                if self._path_exists(backup_path):
                    if self._path_exists(data_path):
                        self._remove_directory(data_path)
                    
                    print("Restoring from backup...")
                    
                    # First, check what partition columns exist in the backed up data
                    check_columns = self.con.execute(f"""
                        SELECT column_name 
                        FROM (DESCRIBE (SELECT * FROM read_parquet('{backup_path}/**/*.parquet', hive_partitioning=true) LIMIT 1))
                    """).fetchall()
                    all_columns = [col[0] for col in check_columns]
                    
                    # Identify partition columns (those that match pattern of existing partitions or were requested)
                    potential_partition_cols = ['experiment_name', 'machine', 'year', 'month', 'day', 'hour']
                    partition_cols_in_data = [col for col in potential_partition_cols if col in all_columns]
                    
                    if partition_cols_in_data and existing_hive_columns:
                        # Restore with the original partition structure
                        print(f"Restoring backup with partition columns found in data: {partition_cols_in_data}")
                        print(f"Using original partition order: {existing_hive_columns}")
                        # Use the original order but only columns that exist in data
                        restore_partitions = [col for col in existing_hive_columns if col in partition_cols_in_data]
                        if restore_partitions:
                            self.con.execute(f"""
                                COPY (SELECT * FROM read_parquet('{backup_path}/**/*.parquet', hive_partitioning=true))
                                TO '{data_path}'
                                (FORMAT PARQUET, PARTITION_BY ({', '.join(restore_partitions)}), OVERWRITE_OR_IGNORE)
                            """)
                        else:
                            # Data has no partition columns, restore as single file
                            print("No partition columns found in data, restoring as single file")
                            self.con.execute(f"""
                                COPY (SELECT * FROM read_parquet('{backup_path}/**/*.parquet', hive_partitioning=true))
                                TO '{data_path}/data.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE)
                            """)
                    else:
                        # No partitioning detected, restore as single file
                        print("No partitions detected, restoring as single file")
                        self.con.execute(f"""
                            COPY (SELECT * FROM read_parquet('{backup_path}/**/*.parquet'))
                            TO '{data_path}/data.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE)
                        """)
                    
                    self._remove_directory(backup_path)
                    print("Backup restored successfully")
                
                raise e
                
        except Exception as e:
            print(f"Error repartitioning table: {e}")
            raise

    def compact_table(self, table_name: str, target_file_size_mb: int = 128, progress_callback=None) -> Dict[str, Any]:
        """
        Compact table by reorganizing parquet files within each partition.
        This implementation works directly with S3 files without creating backup tables.
        """
        try:
            if progress_callback:
                progress_callback("Starting compaction...", 0)
                
            data_path = self._get_data_path(table_name)
            if not self._path_exists(data_path):
                raise ValueError(f"Table '{table_name}' does not exist")
            
            # Get partition structure from catalog (not from file system)
            existing_hive_columns = self._get_table_partition_spec(table_name)
            if not existing_hive_columns:
                # Fallback to detection if not in catalog
                existing_hive_columns = self._detect_partition_structure(data_path)
            print(f"Compaction: Table has partition columns: {existing_hive_columns}")
            
            if progress_callback:
                progress_callback("Analyzing table structure...", 5)
            
            compaction_stats = {
                "partitions_processed": 0,
                "files_before": 0,
                "files_after": 0,
                "rows_processed": 0,
                "bytes_before": 0,
                "bytes_after": 0
            }
            
            if existing_hive_columns:
                # Partitioned table - compact each partition separately
                def compact_partition(partition_path: str) -> Dict[str, Any]:
                    """Compact a single partition"""
                    items = self._list_directory(partition_path)
                    parquet_files = [f for f in items if f.endswith('.parquet')]
                    
                    if not parquet_files:
                        return {"files": 0, "rows": 0, "bytes": 0}
                    
                    # If only one file and it's reasonably sized, skip
                    if len(parquet_files) == 1:
                        file_size = self._get_file_size(f"{partition_path}/{parquet_files[0]}")
                        if file_size < target_file_size_mb * 1024 * 1024:
                            print(f"Compaction: Partition {partition_path} already optimal")
                            return {"files": 1, "rows": 0, "bytes": file_size}
                    
                    # Read all files in the partition
                    print(f"Compaction: Processing partition {partition_path} with {len(parquet_files)} files")
                    
                    # Use DuckDB to read and merge files
                    partition_df = self.con.execute(f"""
                        SELECT * FROM read_parquet('{partition_path}/*.parquet', union_by_name=true)
                    """).fetch_df()
                    
                    row_count = len(partition_df)
                    if row_count == 0:
                        # Remove empty files
                        for f in parquet_files:
                            self._remove_file(f"{partition_path}/{f}")
                        return {"files": 0, "rows": 0, "bytes": 0}
                    
                    # Calculate total size before
                    total_size_before = sum(
                        self._get_file_size(f"{partition_path}/{f}") 
                        for f in parquet_files
                    )
                    
                    # Remove old files
                    for f in parquet_files:
                        self._remove_file(f"{partition_path}/{f}")
                    
                    # Write compacted data directly using DuckDB
                    compacted_file = f"{partition_path}/data_{uuid.uuid4().hex}.parquet"
                    temp_table = f"compact_temp_{uuid.uuid4().hex}"
                    
                    try:
                        self.con.register(temp_table, partition_df)
                        self.con.execute(f"""
                            COPY (SELECT * FROM {temp_table}) 
                            TO '{compacted_file}' (FORMAT PARQUET)
                        """)
                        self.con.unregister(temp_table)
                    except Exception as e:
                        print(f"Error writing compacted file: {e}")
                        raise
                    
                    # Get size after
                    size_after = self._get_file_size(compacted_file)
                    
                    return {
                        "files": 1,
                        "rows": row_count,
                        "bytes": size_after,
                        "files_before": len(parquet_files),
                        "bytes_before": total_size_before
                    }
                
                # Count total partitions first for progress tracking
                total_partitions = []
                
                def count_partitions(path: str):
                    items = self._list_directory(path)
                    has_parquet = any(item.endswith('.parquet') for item in items)
                    if has_parquet:
                        total_partitions.append(path)
                    else:
                        for item in items:
                            if not item.startswith('.'):
                                subdir_path = f"{path}/{item}"
                                if self._is_directory(subdir_path):
                                    count_partitions(subdir_path)
                
                if progress_callback:
                    progress_callback("Scanning for partitions...", 10)
                
                count_partitions(data_path)
                total_partition_count = len(total_partitions)
                
                if progress_callback:
                    progress_callback(f"Found {total_partition_count} partitions to process", 15)
                
                # Recursively find and compact all partitions
                def process_directory(current_path: str):
                    items = self._list_directory(current_path)
                    has_parquet = any(item.endswith('.parquet') for item in items)
                    
                    if has_parquet:
                        # This is a leaf partition with data files
                        stats = compact_partition(current_path)
                        compaction_stats["partitions_processed"] += 1
                        
                        # Calculate progress
                        if progress_callback and total_partition_count > 0:
                            progress_pct = 15 + int((compaction_stats["partitions_processed"] / total_partition_count) * 70)
                            progress_callback(f"Processed partition {compaction_stats['partitions_processed']}/{total_partition_count}", progress_pct)
                        compaction_stats["files_before"] += stats.get("files_before", stats["files"])
                        compaction_stats["files_after"] += stats["files"]
                        compaction_stats["rows_processed"] += stats["rows"]
                        compaction_stats["bytes_before"] += stats.get("bytes_before", stats["bytes"])
                        compaction_stats["bytes_after"] += stats["bytes"]
                    else:
                        # Explore subdirectories
                        for item in items:
                            if not item.startswith('.'):
                                subdir_path = f"{current_path}/{item}"
                                if self._is_directory(subdir_path):
                                    process_directory(subdir_path)
                
                # Process all partitions
                process_directory(data_path)
                
            else:
                # Non-partitioned table - compact all files in root
                def compact_partition(partition_path: str) -> Dict[str, Any]:
                    """Compact a single partition"""
                    items = self._list_directory(partition_path)
                    parquet_files = [f for f in items if f.endswith('.parquet')]
                    
                    if not parquet_files:
                        return {"files": 0, "rows": 0, "bytes": 0}
                    
                    # If only one file and it's reasonably sized, skip
                    if len(parquet_files) == 1:
                        file_size = self._get_file_size(f"{partition_path}/{parquet_files[0]}")
                        if file_size < target_file_size_mb * 1024 * 1024:
                            print(f"Compaction: Partition {partition_path} already optimal")
                            return {"files": 1, "rows": 0, "bytes": file_size}
                    
                    # Read all files in the partition
                    print(f"Compaction: Processing partition {partition_path} with {len(parquet_files)} files")
                    
                    # Use DuckDB to read and merge files
                    partition_df = self.con.execute(f"""
                        SELECT * FROM read_parquet('{partition_path}/*.parquet', union_by_name=true)
                    """).fetch_df()
                    
                    row_count = len(partition_df)
                    if row_count == 0:
                        # Remove empty files
                        for f in parquet_files:
                            self._remove_file(f"{partition_path}/{f}")
                        return {"files": 0, "rows": 0, "bytes": 0}
                    
                    # Calculate total size before
                    total_size_before = sum(
                        self._get_file_size(f"{partition_path}/{f}") 
                        for f in parquet_files
                    )
                    
                    # Remove old files
                    for f in parquet_files:
                        self._remove_file(f"{partition_path}/{f}")
                    
                    # Write compacted data directly using DuckDB
                    compacted_file = f"{partition_path}/data_{uuid.uuid4().hex}.parquet"
                    temp_table = f"compact_temp_{uuid.uuid4().hex}"
                    
                    try:
                        self.con.register(temp_table, partition_df)
                        self.con.execute(f"""
                            COPY (SELECT * FROM {temp_table}) 
                            TO '{compacted_file}' (FORMAT PARQUET)
                        """)
                        self.con.unregister(temp_table)
                    except Exception as e:
                        print(f"Error writing compacted file: {e}")
                        raise
                    
                    # Get size after
                    size_after = self._get_file_size(compacted_file)
                    
                    return {
                        "files": 1,
                        "rows": row_count,
                        "bytes": size_after,
                        "files_before": len(parquet_files),
                        "bytes_before": total_size_before
                    }
                
                stats = compact_partition(data_path)
                compaction_stats["files_before"] = stats.get("files_before", stats["files"])
                compaction_stats["files_after"] = stats["files"]
                compaction_stats["rows_processed"] = stats["rows"]
                compaction_stats["bytes_before"] = stats.get("bytes_before", stats["bytes"])
                compaction_stats["bytes_after"] = stats["bytes"]
            
            # Calculate final metrics
            size_before_mb = compaction_stats["bytes_before"] / (1024 * 1024)
            size_after_mb = compaction_stats["bytes_after"] / (1024 * 1024)
            
            print(f"Compaction completed: {compaction_stats['files_before']} files -> {compaction_stats['files_after']} files")
            
            if progress_callback:
                progress_callback("Refreshing catalog...", 90)
            
            # Refresh the catalog
            try:
                self.catalog_client.refresh_table(table_name)
                print(f"Catalog refreshed for table {table_name}")
            except Exception as e:
                print(f"Warning: Could not refresh catalog for {table_name}: {e}")
            
            if progress_callback:
                progress_callback("Compaction completed successfully!", 100)
            
            return {
                "rows_processed": compaction_stats["rows_processed"],
                "files_before": compaction_stats["files_before"],
                "files_after": compaction_stats["files_after"],
                "size_before_mb": size_before_mb,
                "size_after_mb": size_after_mb,
                "compression_ratio": size_before_mb / size_after_mb if size_after_mb > 0 else 1.0,
                "target_file_size_mb": target_file_size_mb,
                "existing_partition_structure": existing_hive_columns,
                "partitions_preserved": bool(existing_hive_columns),
                "partitions_processed": compaction_stats["partitions_processed"]
            }
                
        except Exception as e:
            print(f"Error compacting table: {e}")
            raise
    
    
    
    
    
    def discover_and_register_table(self, table_name: str, s3_path: str) -> Dict[str, Any]:
        """Discover Parquet files in S3 and register in REST Catalog"""
        try:
            print(f"Discovering data at {s3_path} for table {table_name}")
            
            # Use catalog client to discover and register
            result = self.catalog_client.discover_table(
                table_name=table_name,
                s3_path=s3_path
            )
            
            return {
                "table_name": table_name,
                "s3_path": s3_path,
                "file_count": result["discovery_result"]["file_count"],
                "total_size_mb": result["discovery_result"]["total_size_mb"],
                "partition_columns": result["discovery_result"]["partition_columns"],
                "message": f"Successfully discovered and registered table '{table_name}' in REST Catalog"
            }
            
        except Exception as e:
            print(f"Error discovering table: {e}")
            raise

