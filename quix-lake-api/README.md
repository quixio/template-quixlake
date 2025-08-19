# QuixLake API

REST API service for querying and managing data in QuixLake - a high-performance data lake built on DuckDB and S3.

## Overview

QuixLake API provides:
- SQL query execution over S3-stored Parquet files
- Table discovery and registration from existing S3 data
- Partition management and exploration
- Schema evolution support
- Data maintenance operations (compact, repartition, delete)
- Grafana datasource integration

## Features

- **High Performance**: Direct query execution on S3 data using DuckDB
- **Schema Flexibility**: Automatic schema evolution with `union_by_name`
- **Partition Awareness**: Hive-style partitioning with efficient pruning
- **Table Discovery**: Register existing S3 data as queryable tables
- **Data Management**: Compaction, repartitioning, and selective deletion
- **Monitoring**: Query explain plans and performance metrics

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables:
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
export S3_BUCKET=your-data-bucket
export S3_PREFIX=data  # Optional, defaults to 'data'
```

3. Run the service:
```bash
python main.py
```

The API will be available at `http://localhost:80`

## API Endpoints

### Query Endpoints

#### `POST /query`
Execute SQL queries against registered tables.

```bash
curl -X POST http://localhost:80/query \
  -H "Content-Type: text/plain" \
  -d "SELECT COUNT(*) FROM sensor_data WHERE day='2025-01-06'"
```

Parameters:
- `explain=true` - Include query execution plan
- `union_by_name=true` - Enable flexible schema matching (default: false)

#### `POST /grafana/query`
Grafana-compatible query endpoint for time series data.

### Table Management

#### `GET /tables`
List all registered tables.

```bash
curl http://localhost:80/tables
```

#### `GET /schema`
Get table schema without scanning data.

```bash
curl "http://localhost:80/schema?table=sensor_data"
```

#### `POST /discover`
Discover and register existing Parquet files in S3 as a table.

```bash
curl -X POST "http://localhost:80/discover?table=my_data&s3_path=s3://bucket/path/to/data"
```

### Partition Management

#### `GET /partitions`
Get partition tree structure with lazy loading support.

```bash
# Get root partitions
curl "http://localhost:80/partitions?table=sensor_data"

# Get specific level
curl "http://localhost:80/partitions?table=sensor_data&path=location=factory1"

# Include file sizes
curl "http://localhost:80/partitions?table=sensor_data&include_sizes=true"
```

#### `GET /partition-info`
Get partition structure information for a table.

```bash
curl "http://localhost:80/partition-info?table=sensor_data"
```

### Data Operations

#### `POST /insert`
Insert data with automatic partitioning.

```bash
curl -X POST "http://localhost:80/insert?table=sensor_data&hive_columns=location,sensor_type&timestamp_column=ts_ms&timestamp_format=day" \
  -H "Content-Type: text/csv" \
  --data-binary @data.csv
```

#### `POST /compact`
Compact small files within partitions for better performance.

```bash
curl -X POST "http://localhost:80/compact?table=sensor_data&target_file_size_mb=128"
```

#### `POST /repartition`
Change table partitioning structure.

```bash
curl -X POST "http://localhost:80/repartition?table=sensor_data&hive_columns=location,year,month&timestamp_column=ts_ms&timestamp_format=day"
```

#### `DELETE /delete`
Delete data with various options.

```bash
# Delete with WHERE clause
curl -X DELETE "http://localhost:80/delete?table=sensor_data&where=day='2025-01-01'"

# Delete specific partitions
curl -X DELETE "http://localhost:80/delete?table=sensor_data&partitions=location=factory1/day=2025-01-01"

# Delete entire table
curl -X DELETE "http://localhost:80/delete?table=sensor_data&delete_table=true"
```

### Grafana Integration

#### `POST /grafana/metrics`
Get available metrics for Grafana.

#### `GET /hive-folders`
Browse folder structure for Grafana file browser.

## Data Organization

Tables are stored in S3 with Hive-style partitioning:

```
s3://bucket/prefix/table_name/
├── partition1=value1/
│   ├── partition2=value2/
│   │   ├── year=2025/month=01/day=06/
│   │   │   ├── data_uuid1.parquet
│   │   │   └── data_uuid2.parquet
```

## Configuration

### Environment Variables

- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key
- `AWS_REGION` - AWS region (default: us-east-1)
- `S3_BUCKET` - S3 bucket for data storage
- `S3_PREFIX` - S3 prefix for data organization (default: data)
- `PORT` - API port (default: 80)
- `LOG_LEVEL` - Logging level (default: INFO)

### DuckDB Configuration

The service automatically configures DuckDB for optimal performance:
- Memory limit: 4GB
- Thread count: 32
- Temp directory with 100GB limit
- S3 access configuration

## Performance Optimization

### Query Performance

1. **Use Partition Filters**: Always include partition columns in WHERE clauses
   ```sql
   SELECT * FROM sensor_data 
   WHERE location='factory1' AND day='2025-01-06'
   ```

2. **Enable Union by Name**: For tables with evolving schemas
   ```bash
   curl -X POST "http://localhost:80/query?union_by_name=true" ...
   ```

3. **Compact Small Files**: Run compaction regularly
   ```bash
   curl -X POST "http://localhost:80/compact?table=sensor_data"
   ```

### Memory Management

- Adjust DuckDB memory limit in `duck_db_service.py`
- Monitor temp directory usage
- Use appropriate batch sizes for inserts

## Troubleshooting

### Common Issues

1. **S3 Access Denied**
   - Check AWS credentials
   - Verify bucket permissions
   - Ensure correct region

2. **Table Not Found**
   - Verify table is registered with `/tables`
   - Check S3 path exists
   - Run discovery if needed

3. **Schema Mismatch**
   - Use `union_by_name=true`
   - Check partition structure
   - Consider repartitioning

4. **Slow Queries**
   - Add partition filters
   - Run compaction
   - Check file sizes

### Debug Mode

Enable debug logging:
```bash
export LOG_LEVEL=DEBUG
python main.py
```

## Development

### Project Structure
```
quix-lake-api/
├── main.py              # Flask application and endpoints
├── duck_db_service.py   # DuckDB integration and S3 operations
├── requirements.txt     # Python dependencies
├── dockerfile          # Container definition
├── app.yaml           # Quix platform configuration
└── icon.png          # Service icon
```

### Adding New Endpoints

1. Define endpoint in `main.py`
2. Implement logic in `duck_db_service.py`
3. Add documentation
4. Write tests

### Testing

Run tests:
```bash
python -m pytest tests/
```

Integration tests:
```bash
python test_integration.py
```

## Deployment

### Docker

```bash
docker build -t quixlake-api .
docker run -p 80:80 \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -e S3_BUCKET=your-bucket \
  quixlake-api
```

### Kubernetes

See `k8s/api-deployment.yaml` for Kubernetes manifests.

### Quix Platform

Deploy using the included `app.yaml` configuration.

## API Reference

See the [API Documentation](docs/api.md) for detailed endpoint documentation.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Submit pull request

## License

[Your License Here]