#!/usr/bin/env python3
"""
Example of how to implement time-based file filtering for faster queries.
This demonstrates different approaches to speed up queries using file naming and filtering.
"""

def example_1_current_approach():
    """Current approach - relies on DuckDB's partition pruning"""
    # DuckDB automatically optimizes this when WHERE clause matches partition columns
    query = """
    SELECT * FROM read_parquet('s3://bucket/table/**/*.parquet', hive_partitioning=true)
    WHERE year = 2025 AND month = 1 AND day = 15
    """
    # DuckDB will only read: s3://bucket/table/year=2025/month=01/day=15/*.parquet
    print("Partition pruning happens automatically with hive_partitioning=true")


def example_2_filename_filtering():
    """Advanced: Use timestamp in filenames for even faster filtering"""
    
    # When writing files, name them with timestamps:
    # data_2025-01-15_00-00-00_2025-01-15_23-59-59.parquet
    # Format: data_STARTDATE_STARTTIME_ENDDATE_ENDTIME.parquet
    
    # Then query specific time ranges using glob patterns:
    queries = {
        "specific_day": "read_parquet('s3://bucket/table/**/data_2025-01-15*.parquet')",
        "date_range": "read_parquet('s3://bucket/table/**/data_2025-01-{15,16,17}*.parquet')",
        "month": "read_parquet('s3://bucket/table/**/data_2025-01*.parquet')",
    }
    return queries


def example_3_smart_path_building(table_name: str, where_clause: str) -> str:
    """Build optimized path based on WHERE clause analysis"""
    import re
    
    base_path = f"s3://bucket/{table_name}"
    
    # Extract year, month, day from WHERE clause
    year_match = re.search(r"year\s*=\s*(\d{4})", where_clause)
    month_match = re.search(r"month\s*=\s*(\d{1,2})", where_clause)
    day_match = re.search(r"day\s*=\s*(\d{1,2})", where_clause)
    
    # Build specific path to reduce file scanning
    if year_match:
        path = f"{base_path}/year={year_match.group(1)}"
        if month_match:
            path += f"/month={int(month_match.group(1)):02d}"
            if day_match:
                path += f"/day={int(day_match.group(1)):02d}"
        path += "/**/*.parquet"
    else:
        path = f"{base_path}/**/*.parquet"
    
    return f"read_parquet('{path}', hive_partitioning=true)"


def example_4_file_naming_best_practices():
    """Best practices for file naming to enable fast filtering"""
    
    best_practices = """
    1. PARTITION STRUCTURE (Current - Good):
       /year=2025/month=01/day=15/hour=14/data.parquet
       - Enables partition pruning
       - Works with WHERE year=2025 AND month=1
    
    2. TIMESTAMP IN FILENAME (Better for range queries):
       /year=2025/month=01/day=15/data_20250115_140000_20250115_145959.parquet
       - Format: data_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS.parquet
       - Enables: read_parquet('**/data_20250115_14*.parquet')
    
    3. COMPACT NAMING (Best for specific time queries):
       /2025/01/15/14/data_1736949600_1736953199.parquet
       - Uses Unix timestamps for start/end
       - Smaller paths = faster S3 listing
    
    4. FILE SIZE OPTIMIZATION:
       - Target 100-500 MB per file
       - Too many small files = slow S3 listing
       - Too large files = slow reads
    """
    return best_practices


def example_5_advanced_implementation():
    """Advanced implementation for the main.py replace_table function"""
    
    code = '''
    def replace_table(match):
        table_name = match.group(1).lower()
        
        if table_name in ['sqlite_temp_master', 'information_schema']:
            return match.group(0)
        
        data_path = db_service._get_data_path(table_name)
        
        # Parse the full query to find WHERE conditions
        # This is a simplified example - real implementation would need proper SQL parsing
        where_match = re.search(r'WHERE\\s+(.+?)(?:GROUP|ORDER|LIMIT|$)', raw_sql, re.IGNORECASE)
        
        if where_match:
            where_clause = where_match.group(1)
            
            # Check for time-based filters
            year = re.search(r"year\\s*=\\s*(\\d{4})", where_clause)
            month = re.search(r"month\\s*=\\s*(\\d{1,2})", where_clause)
            day = re.search(r"day\\s*=\\s*(\\d{1,2})", where_clause)
            
            # Build optimized path
            if year:
                path_parts = [data_path]
                path_parts.append(f"year={year.group(1)}")
                if month:
                    path_parts.append(f"month={int(month.group(1)):02d}")
                    if day:
                        path_parts.append(f"day={int(day.group(1)):02d}")
                
                optimized_path = "/".join(path_parts) + "/**/*.parquet"
                print(f"Optimized path for {table_name}: {optimized_path}")
                return f"FROM read_parquet('{optimized_path}', hive_partitioning=true, union_by_name={str(union_by_name).lower()})"
        
        # Default: read all files
        return f"FROM read_parquet('{data_path}/**/*.parquet', hive_partitioning=true, union_by_name={str(union_by_name).lower()})"
    '''
    return code


if __name__ == "__main__":
    print("=" * 60)
    print("TIME-BASED FILE FILTERING FOR QUERY OPTIMIZATION")
    print("=" * 60)
    
    print("\n1. CURRENT APPROACH (Automatic Partition Pruning):")
    example_1_current_approach()
    
    print("\n2. FILENAME FILTERING:")
    for query_type, query in example_2_filename_filtering().items():
        print(f"   {query_type}: {query}")
    
    print("\n3. SMART PATH EXAMPLE:")
    path = example_3_smart_path_building("my_table", "WHERE year = 2025 AND month = 1")
    print(f"   Optimized: {path}")
    
    print("\n4. FILE NAMING BEST PRACTICES:")
    print(example_4_file_naming_best_practices())
    
    print("\n5. ADVANCED IMPLEMENTATION:")
    print("   See code in example_5_advanced_implementation()")
    
    print("\n" + "=" * 60)
    print("KEY BENEFITS:")
    print("- Reduce S3 API calls by limiting directory scanning")
    print("- DuckDB reads only relevant files")
    print("- Queries on recent data become much faster")
    print("- Works especially well with time-series data")
    print("=" * 60)