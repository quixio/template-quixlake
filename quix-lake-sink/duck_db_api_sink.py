from quixstreams.sinks import BatchingSink, SinkBatch
import requests
import pandas as pd
import time
import logging
from typing import List
from io import StringIO


class DuckDbApiSink(BatchingSink):
    """
    Writes Kafka batches to DuckDB via HTTP API with Hive partitioning.
    """
    
    def __init__(self, 
                 api_url: str,
                 table_name: str,
                 hive_columns: List[str] = None,
                 timestamp_column: str = "ts_ms",
                 timestamp_format: str = "day"):
        self.api_url = api_url.rstrip('/')
        self.table_name = table_name
        self.hive_columns = hive_columns or []
        self.timestamp_column = timestamp_column
        self.timestamp_format = timestamp_format
        
        self.logger = logging.getLogger(__name__)
        
        super().__init__()
    
    def setup(self):
        """Test connection to DuckDB API"""
        try:
            response = requests.get(f"{self.api_url}/tables")
            response.raise_for_status()
            self.logger.info("Successfully connected to DuckDB API at %s", self.api_url)
        except Exception as e:
            self.logger.error("Failed to connect to DuckDB API: %s", e)
            raise
    
    def write(self, batch: SinkBatch):
        """Write batch to DuckDB via API"""
        attempts = 3
        while attempts:
            start = time.perf_counter()
            try:
                self._write_batch(batch)
                elapsed_ms = (time.perf_counter() - start) * 1000
                self.logger.info("✔ wrote %d rows in %.1f ms", batch.size, elapsed_ms)
                return
            except Exception as exc:
                attempts -= 1
                if attempts == 0:
                    raise
                self.logger.warning("Write failed (%s) – retrying …", exc)
                time.sleep(3)
    
    def _write_batch(self, batch: SinkBatch):
        """Convert batch to CSV and send to insert endpoint"""
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
        
        # Convert to DataFrame and then CSV
        df = pd.DataFrame(rows)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        # Build request parameters
        params = {
            'table': self.table_name
        }
        
        if self.hive_columns:
            params['hive_columns'] = ','.join(self.hive_columns)
        
        if self.timestamp_column:
            params['timestamp_column'] = self.timestamp_column
            params['timestamp_format'] = self.timestamp_format
        
        # Send to insert endpoint
        response = requests.post(
            f"{self.api_url}/insert",
            params=params,
            data=csv_data,
            headers={'Content-Type': 'text/csv'}
        )
        
        if response.status_code != 200:
            error_msg = f"API request failed with status {response.status_code}: {response.text}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
        
        result = response.json()
        self.logger.info("Successfully inserted %d rows into table '%s'", 
                        result.get('rows_inserted', len(rows)), self.table_name)