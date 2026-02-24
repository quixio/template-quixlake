"""
Shared pytest fixtures for quix-ts-datalake-sink tests.
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
import pandas as pd


# ============================================================================
# Mock Quixportal Configuration
# ============================================================================

@pytest.fixture
def mock_s3_config():
    """Mock S3/MinIO blob storage configuration."""
    config = MagicMock()
    config.provider = MagicMock()
    config.provider.value = "S3"
    config.s3_compatible = MagicMock()
    config.s3_compatible.bucket_name = "test-bucket"
    config.s3_compatible.access_key_id = "test-access-key"
    config.s3_compatible.secret_access_key = "test-secret-key"
    config.s3_compatible.service_url = "http://localhost:9000"
    config.azure_blob_storage = None
    config.local_storage = None
    return config


@pytest.fixture
def mock_azure_config():
    """Mock Azure blob storage configuration."""
    config = MagicMock()
    config.provider = MagicMock()
    config.provider.value = "Azure"
    config.s3_compatible = None
    config.azure_blob_storage = MagicMock()
    config.azure_blob_storage.container_name = "test-container"
    config.azure_blob_storage.account_name = "testaccount"
    config.local_storage = None
    return config


@pytest.fixture
def mock_local_config():
    """Mock local storage configuration."""
    config = MagicMock()
    config.provider = MagicMock()
    config.provider.value = "Local"
    config.s3_compatible = None
    config.azure_blob_storage = None
    config.local_storage = MagicMock()
    config.local_storage.directory_path = "/tmp/test-storage"
    return config


# ============================================================================
# Mock Filesystem
# ============================================================================

@pytest.fixture
def mock_filesystem():
    """Mock quixportal filesystem."""
    fs = MagicMock()
    fs.exists.return_value = True
    fs.size.return_value = 1024
    fs.ls.return_value = []
    fs.glob.return_value = []
    return fs


@pytest.fixture
def mock_filesystem_with_files():
    """Mock filesystem with existing files."""
    fs = MagicMock()
    fs.exists.return_value = True
    fs.size.return_value = 1024
    fs.ls.return_value = ["file1.parquet", "file2.parquet"]
    fs.glob.return_value = [
        "data-lake/time-series/test_table/region=us-east/year=2025/data_001.parquet",
        "data-lake/time-series/test_table/region=us-west/year=2025/data_002.parquet",
    ]
    return fs


# ============================================================================
# Mock Catalog Responses
# ============================================================================

@pytest.fixture
def mock_catalog_health_response():
    """Mock healthy catalog response."""
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"status": "healthy"}
    response.raise_for_status.return_value = None
    return response


@pytest.fixture
def mock_catalog_table_exists_response():
    """Mock response when table exists."""
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "table_name": "test_table",
        "partition_spec": ["region", "year"],
        "location": "s3://test-bucket/data-lake/time-series/test_table"
    }
    return response


@pytest.fixture
def mock_catalog_table_not_found_response():
    """Mock response when table doesn't exist."""
    response = MagicMock()
    response.status_code = 404
    response.json.return_value = {"error": "Table not found"}
    return response


@pytest.fixture
def mock_catalog_create_success_response():
    """Mock successful table creation response."""
    response = MagicMock()
    response.status_code = 201
    response.json.return_value = {"table_name": "test_table", "created": True}
    return response


# ============================================================================
# Sample Data
# ============================================================================

@pytest.fixture
def sample_records():
    """Sample records as list of dicts."""
    return [
        {
            "region": "us-east",
            "datacenter": "dc1",
            "hostname": "host_0",
            "ts_ms": 1704067200000,  # 2024-01-01 00:00:00 UTC in milliseconds
            "usage_user": 45.5,
            "usage_system": 12.3,
        },
        {
            "region": "us-east",
            "datacenter": "dc1",
            "hostname": "host_1",
            "ts_ms": 1704067260000,  # 2024-01-01 00:01:00 UTC
            "usage_user": 52.1,
            "usage_system": 15.7,
        },
        {
            "region": "us-west",
            "datacenter": "dc2",
            "hostname": "host_2",
            "ts_ms": 1704067320000,  # 2024-01-01 00:02:00 UTC
            "usage_user": 38.9,
            "usage_system": 8.2,
        },
    ]


@pytest.fixture
def sample_dataframe(sample_records):
    """Sample DataFrame from records."""
    return pd.DataFrame(sample_records)


@pytest.fixture
def sample_records_with_empty_dicts():
    """Sample records containing empty dictionaries."""
    return [
        {"id": 1, "data": {"key": "value"}, "metadata": {}},
        {"id": 2, "data": {}, "metadata": {"status": "ok"}},
        {"id": 3, "data": {"nested": {"a": 1}}, "metadata": {}},
    ]


@pytest.fixture
def sample_records_timestamps_seconds():
    """Sample records with Unix timestamps in seconds."""
    return [
        {"ts_ms": 1704067200, "value": 1},  # 2024-01-01 00:00:00 UTC
        {"ts_ms": 1704153600, "value": 2},  # 2024-01-02 00:00:00 UTC
    ]


@pytest.fixture
def sample_records_timestamps_milliseconds():
    """Sample records with timestamps in milliseconds."""
    return [
        {"ts_ms": 1704067200000, "value": 1},
        {"ts_ms": 1704153600000, "value": 2},
    ]


@pytest.fixture
def sample_records_timestamps_microseconds():
    """Sample records with timestamps in microseconds."""
    return [
        {"ts_ms": 1704067200000000, "value": 1},
        {"ts_ms": 1704153600000000, "value": 2},
    ]


@pytest.fixture
def sample_records_timestamps_nanoseconds():
    """Sample records with timestamps in nanoseconds."""
    return [
        {"ts_ms": 1704067200000000000, "value": 1},
        {"ts_ms": 1704153600000000000, "value": 2},
    ]


# ============================================================================
# Mock SinkBatch
# ============================================================================

class MockSinkItem:
    """Mock item in a SinkBatch."""
    def __init__(self, value: dict, key: str = "test-key", timestamp: int = 1704067200000):
        self.value = value
        self.key = key
        self.timestamp = timestamp


class MockSinkBatch:
    """Mock SinkBatch for testing."""
    def __init__(self, items: list):
        self._items = [
            MockSinkItem(item) if isinstance(item, dict) else item
            for item in items
        ]

    def __iter__(self):
        return iter(self._items)

    def __bool__(self):
        return len(self._items) > 0

    @property
    def size(self):
        return len(self._items)


@pytest.fixture
def mock_sink_batch(sample_records):
    """Create a mock SinkBatch from sample records."""
    return MockSinkBatch(sample_records)


@pytest.fixture
def mock_empty_batch():
    """Create an empty mock SinkBatch."""
    return MockSinkBatch([])


# ============================================================================
# Environment Patches
# ============================================================================

@pytest.fixture
def mock_env_vars():
    """Mock environment variables."""
    env = {
        "input": "test_topic",
        "TABLE_NAME": "test_table",
        "HIVE_COLUMNS": "region,year,month",
        "TIMESTAMP_COLUMN": "ts_ms",
        "CATALOG_URL": "http://localhost:5001",
        "CATALOG_AUTH_TOKEN": "test-token",
        "AUTO_DISCOVER": "true",
        "CATALOG_NAMESPACE": "default",
        "CONSUMER_GROUP": "test-consumer-group",
        "AUTO_OFFSET_RESET": "earliest",
        "COMMIT_INTERVAL": "30",
        "MAX_WRITE_WORKERS": "5",
        "LOGLEVEL": "DEBUG",
        "Quix__Workspace__Id": "test-workspace-id",
    }
    with patch.dict("os.environ", env):
        yield env
