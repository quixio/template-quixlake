# QuixLake Template

A complete data lake solution built on DuckDB, S3, and Apache Iceberg for high-performance streaming data analytics.

## Overview

QuixLake is a production-ready data lake platform that enables real-time ingestion, storage, and querying of streaming data. It combines Apache Kafka for streaming, S3 for object storage, Apache Iceberg for table format, and DuckDB for blazing-fast SQL analytics.

This template deploys a fully configured QuixLake instance with:
- Pre-built, production-ready container images for core services
- Example data pipeline with Time Series Benchmark Suite (TSBS) data
- Interactive query UI for data exploration
- S3-compatible MinIO storage for local development

## Architecture

### DataLake Infrastructure

```
┌──────────────────────────────────────────────────────────┐
│                    QuixLake DataLake                     │
│                                                          │
│  ┌─────────────────┐      ┌─────────────────────────┐  │
│  │  Iceberg Catalog│◄────►│  PostgreSQL Database    │  │
│  │  (REST API)     │      │  - Table metadata       │  │
│  └────────┬────────┘      │  - File manifests       │  │
│           │               │  - Partition info       │  │
│           │               └─────────────────────────┘  │
│           │                                            │
│           ▼                                            │
│  ┌─────────────────┐                                  │
│  │   MinIO (S3)    │                                  │
│  │ Object Storage  │                                  │
│  │                 │                                  │
│  │ Parquet Files:  │                                  │
│  │ ├── table_1/    │                                  │
│  │ │   └── data/   │                                  │
│  │ ├── table_2/    │                                  │
│  │     └── data/   │                                  │
│  └────────▲────────┘                                  │
│           │                                            │
│           │                                            │
│  ┌────────┴────────┐                                  │
│  │  QuixLake API   │  Query Engine                    │
│  │    (DuckDB)     │  - SQL execution                 │
│  │                 │  - Table discovery               │
│  │                 │  - Partition pruning             │
│  └────────┬────────┘                                  │
│           │                                            │
│           ▼                                            │
│  ┌─────────────────┐                                  │
│  │    Query UI     │  Interactive SQL Interface       │
│  │ (Data Explorer) │  - Query editor                  │
│  │                 │  - Results viewer                │
│  └─────────────────┘                                  │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### Ingestion Pipeline (Example)

```
┌──────────────────────────────────────────────────────────┐
│              TSBS Example Data Pipeline                  │
│                                                          │
│  ┌─────────────────┐                                    │
│  │ TSBS Data Gen   │  Generate sample time-series data  │
│  │   (Job)         │  - CPU metrics                     │
│  │                 │  - DevOps data                     │
│  └────────┬────────┘  - IoT sensor data                 │
│           │                                              │
│           v                                              │
│      Kafka Topic                                         │
│      (tsbs_data)                                         │
│           │                                              │
│           v                                              │
│  ┌─────────────────┐                                    │
│  │ TSBS Transformer│  Transform and enrich              │
│  │   (Service)     │  - Add metadata                    │
│  │                 │  - Format timestamps               │
│  └────────┬────────┘  - Normalize schema                │
│           │                                              │
│           v                                              │
│      Kafka Topic                                         │
│  (tsbs_data_transformed)                                 │
│           │                                              │
│           v                                              │
│  ┌─────────────────────────┐                            │
│  │ Quix TS Datalake Sink   │  Write to DataLake        │
│  │      (Service)          │                            │
│  │                         │  - Batch messages          │
│  │  ┌──────────────────┐   │  - Partition data         │
│  │  │ Partition by:    │   │  - Write Parquet          │
│  │  │ - region         │   │  - Register in catalog    │
│  │  │ - datacenter     │   │                            │
│  │  │ - hostname       │   │                            │
│  │  └──────────────────┘   │                            │
│  └────────┬────────────────┘                            │
│           │                                              │
│           v                                              │
│     [To DataLake]                                        │
│      MinIO (S3)                                          │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

## Components

### Core Services (Pre-built Images)

#### 1. **Quix TS Datalake API**
DuckDB-based REST API for querying S3 data with:
- SQL query execution over Parquet files
- Table discovery and automatic registration
- Partition management and pruning
- Schema evolution support
- Grafana datasource integration

#### 2. **Quix TS Query UI**
Interactive web interface with:
- SQL query editor with syntax highlighting
- Real-time query results
- Table and partition browsing
- Embedded in Quix platform as "Data Explorer"

#### 3. **Quix TS Datalake Catalog**
Metadata catalog service featuring:
- PostgreSQL backend for reliability
- Manifest management to avoid expensive S3 ListObjects
- Query optimization with partition filtering
- Automatic table discovery

### Data Pipeline Components

#### 4. **Quix TS Datalake Sink**
Kafka-to-S3 sink connector that:
- Writes streaming data as Hive-partitioned Parquet files
- Supports time-based and custom partitioning
- Automatically registers tables in catalog
- Handles schema evolution

[View detailed README](./quix-ts-datalake-sink/README.md)

#### 5. **TSBS Data Generator**
Generates realistic time-series benchmark data for testing:
- Configurable data types (cpu-only, devops, iot)
- Adjustable scale and time ranges
- Deterministic generation with seed support

[View detailed README](./tsbs-quix-data-generator/README.md)

#### 6. **TSBS Transformer**
Transforms raw TSBS data into optimized format for the data lake.

[View detailed README](./tsbs-transformer/README.md)

### Infrastructure Services

#### 7. **PostgreSQL**
Database backend for the Iceberg Catalog, storing:
- Table metadata and schemas
- File manifests
- Partition information

[View detailed README](./postgresql/README.md)

#### 8. **MinIO**
S3-compatible object storage for development and testing.

[View detailed README](./minio/README.md)

#### 9. **MinIO Proxy**
Public access proxy for MinIO in Quix platform.

## Features

- **Real-time Ingestion**: Stream data from Kafka directly to S3 as Parquet
- **High-Performance Queries**: DuckDB provides sub-second analytical queries
- **Schema Evolution**: Automatic schema detection and evolution support
- **Partition Pruning**: Efficient queries using Hive-style partitioning
- **Table Discovery**: Automatically discover and register existing S3 data
- **Scalable Storage**: S3-based storage scales to petabytes
- **Standard Formats**: Parquet files compatible with any analytics tool
- **Production Ready**: Pre-built, tested container images
- **Embedded UI**: Query interface integrated in Quix platform

## Getting Started

### Prerequisites

- Quix account (sign up at [https://portal.platform.quix.io/signup](https://portal.platform.quix.io/signup))
- AWS account (optional - template includes MinIO for local testing)

### Quick Start

1. **Deploy to Quix**
   - Log in to your Quix account
   - Navigate to Templates → "QuixLake Template"
   - Click "Deploy template"

2. **Configure Secrets**

   Set the following secrets in your Quix environment:
   ```
   minio_user: admin
   minio_password: <your-secure-password>
   postgres_password: <your-secure-password>
   config_ui_auth_token: <your-auth-token>
   ```

3. **Start the Pipeline**

   The template deploys with:
   - All core services (API, UI, Catalog) automatically running
   - MinIO storage ready for data
   - PostgreSQL catalog initialized
   - Example pipeline in "Example pipeline" group

4. **Generate Sample Data**

   Start the TSBS Data Generator job to produce sample CPU metrics data.

5. **Query Your Data**

   Access the Query UI through:
   - Public URL: `https://query-ui-<your-workspace>.deployments.quix.io`
   - Or via the "Data Explorer" sidebar in Quix platform

   Try this query:
   ```sql
   SELECT * FROM sensordata LIMIT 10;
   ```

## Configuration

### Storage Configuration

Configure S3 storage (or use included MinIO):

```yaml
S3_BUCKET: quixdatalaketest          # Your bucket name
S3_PREFIX: ts_test                   # Data folder prefix
AWS_REGION: eu-west-2                # AWS region
AWS_ENDPOINT_URL: http://minio:9000  # For MinIO; remove for AWS S3
```

### Catalog Configuration

PostgreSQL backend configuration:

```yaml
CATALOG_BACKEND: postgres
POSTGRES_HOST: postgresql
POSTGRES_PORT: 80
POSTGRES_DB: iceberg_catalog
POSTGRES_USER: admin
```

### Sink Configuration

Configure how data is written to the lake:

```yaml
BATCH_SIZE: 1000                     # Messages per batch
COMMIT_INTERVAL: 30                  # Commit interval (seconds)
HIVE_COLUMNS: region,datacenter,hostname  # Partition columns
TIMESTAMP_COLUMN: ts_ms              # Time column for partitioning
AUTO_DISCOVER: true                  # Auto-register in catalog
```

See `quix.yaml` for complete configuration options.

## Data Flow

1. **Data Generation**: TSBS generator produces time-series metrics
2. **Transformation**: Transformer enriches and formats the data
3. **Streaming**: Data flows through Kafka topics
4. **Storage**: Sink writes batches to S3 as partitioned Parquet files
5. **Registration**: Tables automatically register in Iceberg Catalog
6. **Query**: API uses DuckDB to query Parquet files from S3
7. **Visualization**: Query UI provides interactive data exploration

## Data Organization

Data is organized in S3 using Hive-style partitioning:

```
s3://bucket/prefix/table_name/
├── region=us-east/
│   ├── datacenter=dc1/
│   │   ├── hostname=server01/
│   │   │   ├── batch_001_uuid.parquet
│   │   │   └── batch_002_uuid.parquet
```

For time-based partitioning:

```
s3://bucket/prefix/table_name/
├── year=2025/month=01/day=20/hour=10/
│   ├── batch_001_uuid.parquet
│   └── batch_002_uuid.parquet
```

This structure enables:
- **Partition Pruning**: Only scan relevant files based on WHERE clauses
- **Time-based Queries**: Efficiently filter by time ranges
- **Dimensional Analysis**: Group and filter by business dimensions
- **Cost Optimization**: Minimize data scanned and transfer costs

## Example Queries

### Basic Query
```sql
SELECT COUNT(*) as total_records
FROM sensordata
WHERE hostname = 'host_0';
```

### Time Series Analysis
```sql
SELECT
  ts_ms as timestamp,
  hostname,
  AVG(usage_user) as avg_cpu
FROM sensordata
GROUP BY ts_ms, hostname
ORDER BY ts_ms DESC
LIMIT 100;
```

### Partition-Aware Query
```sql
SELECT
  region,
  datacenter,
  COUNT(*) as record_count,
  AVG(usage_system) as avg_system_cpu
FROM sensordata
WHERE region = 'us-east-1'
  AND datacenter = 'dc1'
GROUP BY region, datacenter;
```

### Aggregation Query
```sql
SELECT
  hostname,
  MIN(usage_idle) as min_idle,
  MAX(usage_idle) as max_idle,
  AVG(usage_idle) as avg_idle
FROM sensordata
GROUP BY hostname
ORDER BY avg_idle ASC;
```

## API Documentation

### Query Endpoints

**Execute SQL Query**
```bash
POST /query
Content-Type: text/plain

SELECT * FROM sensordata LIMIT 10
```

**List Tables**
```bash
GET /tables
```

**Get Table Schema**
```bash
GET /schema?table=sensordata
```

**Get Partitions**
```bash
GET /partitions?table=sensordata
```

**Discover Table from S3**
```bash
POST /discover?table=my_table&s3_path=s3://bucket/path
```

### Grafana Integration

QuixLake API includes built-in Grafana datasource support:

```bash
POST /grafana/query
POST /grafana/metrics
GET /hive-folders
```

Configure Grafana datasource with your QuixLake API URL.

## Performance Tuning

### Query Performance

1. **Use Partition Filters**
   ```sql
   SELECT * FROM sensordata
   WHERE region = 'us-east-1'  -- Partition column
     AND datacenter = 'dc1';   -- Partition column
   ```

2. **Optimize Batch Size**
   - Larger batches = fewer, larger files = faster queries
   - Target 128-256MB Parquet files for optimal performance

3. **Choose Appropriate Partitioning**
   - High cardinality: Avoid (e.g., user_id with millions of values)
   - Low to medium cardinality: Good (e.g., region, sensor_type, date)
   - Time-based: Excellent for time-series data

### Storage Optimization

1. **Compact Small Files**: Use the API's `/compact` endpoint
2. **Repartition if Needed**: Change partitioning strategy with `/repartition`
3. **Monitor Storage**: Check file sizes and distribution in MinIO console

### Memory Management

- QuixLake API configures DuckDB with optimized memory settings
- Adjust deployment resources in `quix.yaml` if needed
- Monitor query performance through logs

## Monitoring

### Service Health

Check service status:
- QuixLake API: `GET https://quixlake-<workspace>.deployments.quix.io/health`
- Query UI: Access via public URL
- MinIO Console: Access via MinIO proxy public URL
- Catalog: `GET https://iceberg-catalog-<workspace>.deployments.quix.io/cache-status`

### Metrics

Monitor through Quix platform:
- Message throughput in Kafka topics
- CPU and memory usage per service
- Storage size in MinIO
- Query execution times in logs

## Using Your Own Data

To ingest your own data instead of sample data:

1. **Prepare Your Data Source**
   - Ensure data is in JSON format
   - Publish to a Kafka topic in your Quix environment

2. **Configure the Sink**
   Update the `quix-ts-datalake-sink` deployment:
   ```yaml
   input: your-topic-name
   TABLE_NAME: your-table-name
   HIVE_COLUMNS: your,partition,columns
   TIMESTAMP_COLUMN: your_timestamp_field
   ```

3. **Adjust Partitioning**
   Choose partition columns based on your query patterns:
   - Frequently filtered columns
   - Low to medium cardinality
   - Time-based for time-series data

4. **Deploy and Test**
   - Start publishing data
   - Query via the UI to verify
   - Monitor file sizes and adjust `BATCH_SIZE` if needed

## Troubleshooting

### Common Issues

**1. Tables Not Appearing**
- Check if sink is running: View deployment logs
- Verify data is flowing: Check Kafka topic messages
- Check catalog registration: `GET /tables` from API

**2. S3 Access Denied**
- Verify MinIO credentials in secrets
- Check bucket exists in MinIO
- Ensure endpoint URL is correct

**3. Slow Queries**
- Add partition filters to WHERE clause
- Check file sizes (target 128-256MB)
- Verify partition strategy matches query patterns

**4. Schema Errors**
- Ensure consistent data types in source
- Check partition column values are valid
- View table schema: `GET /schema?table=your_table`

### Debug Logging

Enable debug logging:
```yaml
# In quix-ts-datalake-sink
LOGLEVEL: DEBUG

# In Query UI
DEBUG_LOG_LEVEL: DEBUG
```

View logs in Quix platform deployment pages.

## Use Cases

### IoT Analytics
- Ingest sensor data from thousands of devices
- Partition by device location, type, and time
- Query historical trends and detect anomalies

### Application Monitoring
- Stream application logs and metrics to the data lake
- Partition by service, environment, and time
- Query for debugging and performance analysis

### Business Analytics
- Capture clickstream and event data
- Partition by user segments, campaigns, and time
- Run ad-hoc analytical queries for insights

### Time Series Monitoring
- Store metrics from distributed systems
- Partition by metric type and time windows
- Query for dashboards, alerts, and reports

## Advanced Configuration

### Custom Partitioning Strategies

**Daily partitions with region:**
```yaml
HIVE_COLUMNS: region,year,month,day
TIMESTAMP_COLUMN: ts_ms
```

**Hourly partitions:**
```yaml
HIVE_COLUMNS: year,month,day,hour
TIMESTAMP_COLUMN: event_time
```

**Multi-dimensional:**
```yaml
HIVE_COLUMNS: customer_id,product_category,year,month
TIMESTAMP_COLUMN: purchase_date
```

### Using AWS S3 Instead of MinIO

1. **Create S3 Bucket** in your AWS account

2. **Update Configuration** in all deployments:
   ```yaml
   S3_BUCKET: your-aws-bucket-name
   AWS_REGION: us-east-1  # Your region
   AWS_ENDPOINT_URL: ""   # Remove this line
   ```

3. **Configure Secrets**:
   ```
   minio_user: <your-aws-access-key-id>
   minio_password: <your-aws-secret-access-key>
   ```

4. **Remove MinIO Deployments** if not needed (optional)

### Scaling Considerations

**For High Throughput:**
- Increase `MAX_WRITE_WORKERS` in sink (default: 10)
- Increase sink CPU/memory resources
- Use larger `BATCH_SIZE` (1000-5000)

**For Many Tables:**
- Consider multiple sink deployments per table
- Adjust catalog cache settings
- Monitor PostgreSQL performance

**For Large Queries:**
- Increase API CPU/memory resources
- Use partition filters aggressively
- Consider compacting small files

## Development

### Project Structure
```
template-quixlake/
├── minio/                      # MinIO application
├── minio-proxy/                # MinIO proxy application
├── postgresql/                 # PostgreSQL application
├── quix-ts-datalake-sink/     # Sink application (source included)
├── tsbs-quix-data-generator/  # Data generator application
├── tsbs-transformer/          # Transformer application
├── quix.yaml                  # Quix deployment configuration
├── template.json              # Template metadata
└── README.md                  # This file
```

Note: Core services (API, UI, Catalog) use pre-built container images from Quix container registry.

### Customizing the Sink

The sink source code is included in `quix-ts-datalake-sink/`. To customize:

1. Edit `quixlake_sink.py` or `main.py`
2. Update `dockerfile` if needed
3. Build and push to your own registry
4. Update `image:` in `quix.yaml`

### Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Submit a pull request

## Resources

- **Quix Platform**: [https://quix.io/](https://quix.io/)
- **Documentation**: [https://docs.quix.io/](https://docs.quix.io/)
- **DuckDB**: [https://duckdb.org/](https://duckdb.org/)
- **Apache Iceberg**: [https://iceberg.apache.org/](https://iceberg.apache.org/)
- **Apache Parquet**: [https://parquet.apache.org/](https://parquet.apache.org/)

## Support

- **GitHub Issues**: [https://github.com/quixio/template-quixlake/issues](https://github.com/quixio/template-quixlake/issues)
- **Quix Community**: [https://quix.io/slack-invite](https://quix.io/slack-invite)
- **Email**: support@quix.io

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Built with open source technologies:
- [DuckDB](https://duckdb.org/) - Fast analytical database
- [Apache Kafka](https://kafka.apache.org/) - Streaming platform
- [Apache Parquet](https://parquet.apache.org/) - Columnar storage format
- [MinIO](https://min.io/) - S3-compatible object storage
- [PostgreSQL](https://www.postgresql.org/) - Metadata database
- [TSBS](https://github.com/timescale/tsbs) - Time series benchmarking suite
