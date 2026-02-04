# Quix TS Datalake Sink

This connector consumes time-series data from a Kafka topic and writes it to blob storage (AWS S3, Azure, GCP, MinIO) as Hive-partitioned Parquet files, with optional REST Catalog registration for data lake query API.

## Features

- **Multi-Cloud Storage**: Supports AWS S3, Azure Blob Storage, GCP Cloud Storage, and MinIO
- **Hive Partitioning**: Automatically partition data by any columns (e.g., location, sensor type, year/month/day/hour)
- **Time-based Partitioning**: Extract year/month/day/hour from timestamp columns for efficient time-based queries
- **REST Catalog Integration**: Optional table registration in a REST Catalog for seamless integration with analytics tools
- **Efficient Batching**: Configurable batch sizes and parallel uploads for high throughput
- **Schema Evolution**: Automatic schema detection from data
- **Partition Validation**: Prevents data corruption by validating partition strategies against existing tables

## How to Run

Create a [Quix](https://portal.platform.quix.io/signup?xlink=github) account or log in and visit the `Connectors` tab to use this connector.

Clicking `Set up connector` allows you to enter your connection details and runtime parameters.

Then either:
* Click `Test connection & deploy` to deploy the pre-built and configured container into Quix
* Or click `Customise connector` to inspect or alter the code before deployment

## Configuration

### Blob Storage Connection

Storage credentials are provided via the `Quix__BlobStorage__Connection__Json` environment variable. This is automatically configured when using Quix platform with `blobStorage: bind: true` in app.yaml.

#### MinIO / S3-Compatible Example
```json
{
  "provider": "Minio",
  "S3Compatible": {
    "BucketName": "my-bucket",
    "AccessKeyId": "access-key",
    "SecretAccessKey": "secret-key",
    "ServiceUrl": "http://minio:9000"
  }
}
```

#### AWS S3 Example
```json
{
  "provider": "S3",
  "S3Compatible": {
    "BucketName": "my-bucket",
    "AccessKeyId": "AKIAIOSFODNN7EXAMPLE",
    "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "ServiceUrl": "https://s3.us-east-1.amazonaws.com"
  }
}
```

#### Azure Blob Storage Example
```json
{
  "provider": "Azure",
  "AzureBlobStorage": {
    "ContainerName": "my-container",
    "AccountName": "mystorageaccount",
    "AccountKey": "your-account-key"
  }
}
```

#### GCP Cloud Storage Example
```json
{
  "provider": "GCP",
  "S3Compatible": {
    "BucketName": "my-bucket",
    "AccessKeyId": "your-access-key",
    "SecretAccessKey": "your-secret-key",
    "ServiceUrl": "https://storage.googleapis.com"
  }
}
```

## Environment Variables

### Required

| Variable | Description | Default |
|----------|-------------|---------|
| `input` | Name of the Kafka input topic to consume from | `tsbs_data_transformed` |

### Data Organization

| Variable | Description | Default |
|----------|-------------|---------|
| `S3_PREFIX` | Path prefix for data files within the bucket | `data` |
| `TABLE_NAME` | Table name for data organization and registration | Uses topic name |
| `HIVE_COLUMNS` | Comma-separated list of columns for Hive partitioning | `region,datacenter,hostname` |
| `TIMESTAMP_COLUMN` | Column containing timestamp values for time extraction | `ts_ms` |

### Catalog Integration (Optional)

| Variable | Description | Default |
|----------|-------------|---------|
| `CATALOG_URL` | REST Catalog URL for table registration (leave empty to skip) | (empty) |
| `CATALOG_AUTH_TOKEN` | Auth token for REST Catalog access | (empty) |
| `AUTO_DISCOVER` | Automatically register table on first write | `true` |
| `CATALOG_NAMESPACE` | Catalog namespace for table registration | `default` |

### Kafka Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `CONSUMER_GROUP` | Kafka consumer group name | `s3_direct_sink_v1.0` |
| `AUTO_OFFSET_RESET` | Where to start consuming if no offset exists | `earliest` |
| `KAFKA_KEY_DESERIALIZER` | Key deserializer type | `str` |
| `KAFKA_VALUE_DESERIALIZER` | Value deserializer type | `json` |

### Performance Tuning

| Variable | Description | Default |
|----------|-------------|---------|
| `BATCH_SIZE` | Number of messages to batch before writing | `1000` |
| `COMMIT_INTERVAL` | Kafka commit interval in seconds | `30` |
| `MAX_WRITE_WORKERS` | Parallel upload workers | `10` |

### Application Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `LOGLEVEL` | Application logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL) | `INFO` |

## Partitioning Strategy Examples

### Example 1: Time-based partitioning
```bash
HIVE_COLUMNS=year,month,day
TIMESTAMP_COLUMN=ts_ms
```
Creates: `s3://bucket/prefix/table/year=2024/month=01/day=15/data_*.parquet`

### Example 2: Multi-dimensional partitioning
```bash
HIVE_COLUMNS=location,sensor_type,year,month
TIMESTAMP_COLUMN=timestamp
```
Creates: `s3://bucket/prefix/table/location=NYC/sensor_type=temp/year=2024/month=01/data_*.parquet`

### Example 3: No partitioning
```bash
HIVE_COLUMNS=
```
Creates: `s3://bucket/prefix/table/data_*.parquet`

## Architecture

The sink uses a batching architecture for high throughput:

1. **Consume**: Messages are consumed from Kafka in batches
2. **Transform**: Time-based columns are extracted if needed
3. **Partition**: Data is grouped by partition columns
4. **Upload**: Multiple files are uploaded to storage in parallel
5. **Register**: Files are registered in the catalog (if configured)

## Requirements

- Blob storage access (AWS S3, Azure, GCP, or MinIO)
- Optional: REST Catalog endpoint for data catalog integration

## Contribute

Submit forked projects to the Quix [GitHub](https://github.com/quixio/quix-samples) repo. Any new project that we accept will be attributed to you and you'll receive $200 in Quix credit.

## Open Source

This project is open source under the Apache 2.0 license and available in our [GitHub](https://github.com/quixio/quix-samples) repo. Please star us and mention us on social to show your appreciation.
