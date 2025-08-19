# Iceberg REST Catalog Service

A lightweight REST catalog service for managing table metadata with support for both S3 and PostgreSQL backends.

## Features

- **Dual Backend Support**: Choose between S3 or PostgreSQL for storing catalog metadata
- **Manifest Management**: Track all Parquet files to avoid expensive S3 ListObjects operations
- **Query Optimization**: Use partition filters to return only relevant files
- **Caching**: In-memory caching for improved performance
- **Batch Updates**: Efficient batch processing for file registrations (S3 backend)

## Configuration

### Environment Variables

#### Common Settings
```bash
# Catalog backend type: 's3' or 'postgres' (default: 's3')
CATALOG_BACKEND=postgres

# S3 settings for data files (required for both backends)
S3_BUCKET=quixlake-data
S3_PREFIX=data
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key

# Service port
PORT=5001
```

#### S3 Backend Specific
```bash
# Catalog metadata prefix in S3
CATALOG_PREFIX=catalog
```

#### PostgreSQL Backend Specific
```bash
# PostgreSQL connection settings
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=iceberg_catalog
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

## Running with PostgreSQL

### 1. Set up PostgreSQL

Using Docker:
```bash
docker run -d \
  --name postgres-catalog \
  -e POSTGRES_DB=iceberg_catalog \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:15
```

### 2. Configure Environment

Create a `.env` file:
```bash
CATALOG_BACKEND=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=iceberg_catalog
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# S3 settings for data files
S3_BUCKET=your-bucket
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
```

### 3. Run the Service

```bash
python main.py
```

The service will automatically create the required database schema on startup.

## Database Schema (PostgreSQL)

The PostgreSQL backend uses the following tables:

- **namespaces**: Catalog namespaces
- **tables**: Table metadata including location, schema, and partition spec
- **manifest_entries**: Individual file entries with partition values

### Key Features of PostgreSQL Backend

1. **Connection Pooling**: Efficient database connection management
2. **JSONB Storage**: Flexible schema and partition value storage
3. **Indexed Queries**: Optimized partition filtering using GIN indexes
4. **Transactional Updates**: Atomic manifest updates

## API Endpoints

### Catalog Operations
- `GET /namespaces` - List all namespaces
- `GET /namespaces/{namespace}/tables` - List tables in namespace
- `GET /namespaces/{namespace}/tables/{table}` - Get table metadata
- `PUT /namespaces/{namespace}/tables/{table}` - Create/update table
- `DELETE /namespaces/{namespace}/tables/{table}` - Drop table

### Manifest Operations
- `GET /namespaces/{namespace}/tables/{table}/manifest` - Get manifest
- `POST /namespaces/{namespace}/tables/{table}/manifest/refresh` - Refresh manifest
- `POST /namespaces/{namespace}/tables/{table}/manifest/add-files` - Add files

### Query Optimization
- `GET /namespaces/{namespace}/tables/{table}/files?partition=value` - Get files for query

### Discovery
- `POST /discover?table=name&s3_path=s3://bucket/path` - Discover and register table

## Performance Considerations

### S3 Backend
- Batches manifest updates to reduce S3 API calls
- 5-minute TTL cache for manifests
- Background thread flushes updates periodically

### PostgreSQL Backend
- Connection pooling (2-20 connections)
- 1-minute table metadata cache
- GIN indexes on partition values for fast filtering
- No batching needed - direct database writes

## Monitoring

Check cache and backend status:
```bash
curl http://localhost:5001/cache-status
```

Response for PostgreSQL:
```json
{
  "backend": "postgres",
  "tables_cached": 5,
  "cache_ttl_seconds": 60,
  "connection_pool_size": 20
}
```

## Migration from S3 to PostgreSQL

To migrate existing catalog data from S3 to PostgreSQL:

1. Export tables from S3 backend
2. Switch `CATALOG_BACKEND=postgres`
3. Use the discover API to re-register tables

## Docker Support

Build and run with Docker:
```bash
docker build -t iceberg-rest-catalog .
docker run -p 5001:5001 --env-file .env iceberg-rest-catalog
```

## Development

Install dependencies:
```bash
pip install -r requirements.txt
```

Run tests:
```bash
python -m pytest tests/
```