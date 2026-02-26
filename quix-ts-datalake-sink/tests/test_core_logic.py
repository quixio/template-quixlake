"""
Core logic tests for quix-ts-datalake-sink.

These tests verify the core transformation and validation logic
without requiring complex module mocking. They test the functions
and classes in isolation using dependency injection.
"""
import pytest
import pandas as pd
from datetime import datetime


# ============================================================================
# Test: parse_hive_columns logic
# ============================================================================

def parse_hive_columns(columns_str: str) -> list:
    """Parse comma-separated list of partition columns."""
    if not columns_str or columns_str.strip() == "":
        return []
    return [col.strip() for col in columns_str.split(",") if col.strip()]


class TestParseHiveColumns:
    """Tests for parse_hive_columns function."""

    def test_empty_string(self):
        assert parse_hive_columns("") == []

    def test_whitespace_only(self):
        assert parse_hive_columns("   ") == []

    def test_none(self):
        assert parse_hive_columns(None) == []

    def test_single_column(self):
        assert parse_hive_columns("region") == ["region"]

    def test_multiple_columns(self):
        assert parse_hive_columns("region,year,month") == ["region", "year", "month"]

    def test_with_spaces(self):
        assert parse_hive_columns("region , year , month") == ["region", "year", "month"]

    def test_trailing_comma(self):
        assert parse_hive_columns("region,year,") == ["region", "year"]

    def test_leading_comma(self):
        assert parse_hive_columns(",region,year") == ["region", "year"]

    def test_multiple_commas(self):
        assert parse_hive_columns("region,,year") == ["region", "year"]

    def test_time_based_columns(self):
        assert parse_hive_columns("year,month,day,hour") == ["year", "month", "day", "hour"]


# ============================================================================
# Test: Timestamp column mapping logic
# ============================================================================

TIMESTAMP_COL_MAPPER = {
    "year": lambda col: col.dt.year.astype(str),
    "month": lambda col: col.dt.month.astype(str).str.zfill(2),
    "day": lambda col: col.dt.day.astype(str).str.zfill(2),
    "hour": lambda col: col.dt.hour.astype(str).str.zfill(2)
}


def add_timestamp_columns(df: pd.DataFrame, timestamp_column: str, ts_hive_columns: set) -> pd.DataFrame:
    """
    Add timestamp-based columns (year/month/day/hour) for time-based partitioning.
    Extracted from QuixLakeSink._add_timestamp_columns for testing.
    """
    # Convert to datetime if needed (handles numeric timestamps)
    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_column]):
        sample_value = float(df[timestamp_column].iloc[0] if not df[timestamp_column].empty else 0)

        if sample_value > 1e17:
            # Nanoseconds
            df[timestamp_column] = pd.to_datetime(df[timestamp_column], unit='ns')
        elif sample_value > 1e14:
            # Microseconds
            df[timestamp_column] = pd.to_datetime(df[timestamp_column], unit='us')
        elif sample_value > 1e11:
            # Milliseconds
            df[timestamp_column] = pd.to_datetime(df[timestamp_column], unit='ms')
        else:
            # Seconds
            df[timestamp_column] = pd.to_datetime(df[timestamp_column], unit='s')

    timestamp_col = df[timestamp_column]

    for col in ts_hive_columns:
        df[col] = TIMESTAMP_COL_MAPPER[col](timestamp_col)

    return df


class TestTimestampColumnMapping:
    """Tests for timestamp column extraction logic."""

    def test_seconds_conversion(self):
        """Unix timestamps in seconds should be converted correctly."""
        df = pd.DataFrame({
            "ts_ms": [1704067200],  # 2024-01-01 00:00:00 UTC
            "value": [1],
        })

        result = add_timestamp_columns(df, "ts_ms", {"year", "month", "day", "hour"})

        assert result["year"].iloc[0] == "2024"
        assert result["month"].iloc[0] == "01"
        assert result["day"].iloc[0] == "01"
        assert result["hour"].iloc[0] == "00"

    def test_milliseconds_conversion(self):
        """Timestamps in milliseconds should be converted correctly."""
        df = pd.DataFrame({
            "ts_ms": [1704067200000],
            "value": [1],
        })

        result = add_timestamp_columns(df, "ts_ms", {"year", "month", "day"})

        assert result["year"].iloc[0] == "2024"
        assert result["month"].iloc[0] == "01"
        assert result["day"].iloc[0] == "01"

    def test_microseconds_conversion(self):
        """Timestamps in microseconds should be converted correctly."""
        df = pd.DataFrame({
            "ts_ms": [1704067200000000],
            "value": [1],
        })

        result = add_timestamp_columns(df, "ts_ms", {"year", "month"})

        assert result["year"].iloc[0] == "2024"
        assert result["month"].iloc[0] == "01"

    def test_nanoseconds_conversion(self):
        """Timestamps in nanoseconds should be converted correctly."""
        df = pd.DataFrame({
            "ts_ms": [1704067200000000000],
            "value": [1],
        })

        result = add_timestamp_columns(df, "ts_ms", {"year"})

        assert result["year"].iloc[0] == "2024"

    def test_already_datetime(self):
        """Should handle columns that are already datetime."""
        df = pd.DataFrame({
            "ts_ms": pd.to_datetime(["2024-06-15 14:30:00"]),
            "value": [1],
        })

        result = add_timestamp_columns(df, "ts_ms", {"year", "month", "day", "hour"})

        assert result["year"].iloc[0] == "2024"
        assert result["month"].iloc[0] == "06"
        assert result["day"].iloc[0] == "15"
        assert result["hour"].iloc[0] == "14"

    def test_zero_padding_month(self):
        """Month should be zero-padded to 2 digits."""
        df = pd.DataFrame({
            "ts_ms": [1704067200000],  # January
            "value": [1],
        })

        result = add_timestamp_columns(df, "ts_ms", {"month"})

        assert result["month"].iloc[0] == "01"
        assert len(result["month"].iloc[0]) == 2

    def test_zero_padding_day(self):
        """Day should be zero-padded to 2 digits."""
        df = pd.DataFrame({
            "ts_ms": [1704067200000],  # 1st
            "value": [1],
        })

        result = add_timestamp_columns(df, "ts_ms", {"day"})

        assert result["day"].iloc[0] == "01"
        assert len(result["day"].iloc[0]) == 2

    def test_zero_padding_hour(self):
        """Hour should be zero-padded to 2 digits."""
        df = pd.DataFrame({
            "ts_ms": [1704067200000],  # 00:00
            "value": [1],
        })

        result = add_timestamp_columns(df, "ts_ms", {"hour"})

        assert result["hour"].iloc[0] == "00"
        assert len(result["hour"].iloc[0]) == 2

    def test_partial_columns(self):
        """Should only add requested columns."""
        df = pd.DataFrame({
            "ts_ms": [1704067200000],
            "value": [1],
        })

        result = add_timestamp_columns(df, "ts_ms", {"year", "month"})

        assert "year" in result.columns
        assert "month" in result.columns
        assert "day" not in result.columns
        assert "hour" not in result.columns


# ============================================================================
# Test: Empty dictionary nullification logic
# ============================================================================

def null_empty_dicts(df: pd.DataFrame) -> None:
    """
    Convert empty dictionaries to null values before writing to Parquet.
    Extracted from QuixLakeSink._null_empty_dicts for testing.
    """
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            df[col] = df[col].apply(lambda x: x or None)


class TestNullEmptyDicts:
    """Tests for empty dictionary handling."""

    def test_replaces_empty_dicts(self):
        """Should replace empty dicts with None."""
        df = pd.DataFrame({
            "id": [1, 2],
            "data": [{}, {"key": "value"}],
        })

        null_empty_dicts(df)

        assert df["data"].iloc[0] is None
        assert df["data"].iloc[1] == {"key": "value"}

    def test_keeps_non_empty_dicts(self):
        """Should keep non-empty dicts unchanged."""
        df = pd.DataFrame({
            "id": [1],
            "data": [{"nested": {"a": 1}}],
        })

        null_empty_dicts(df)

        assert df["data"].iloc[0] == {"nested": {"a": 1}}

    def test_handles_no_dict_columns(self):
        """Should not modify DataFrame with no dict columns."""
        df = pd.DataFrame({
            "id": [1, 2],
            "value": [10, 20],
        })
        original = df.copy()

        null_empty_dicts(df)

        pd.testing.assert_frame_equal(df, original)

    def test_multiple_dict_columns(self):
        """Should handle multiple columns with dicts."""
        df = pd.DataFrame({
            "id": [1],
            "data1": [{}],
            "data2": [{"key": "value"}],
            "data3": [{}],
        })

        null_empty_dicts(df)

        assert df["data1"].iloc[0] is None
        assert df["data2"].iloc[0] == {"key": "value"}
        assert df["data3"].iloc[0] is None


# ============================================================================
# Test: Partition validation logic
# ============================================================================

def validate_partition_strategy(hive_columns: list, table_metadata: dict) -> None:
    """
    Validate that the sink's partition strategy matches the existing table.
    Extracted from QuixLakeSink._validate_partition_strategy for testing.
    """
    existing_partition_spec = table_metadata.get("partition_spec", [])
    expected_partition_spec = hive_columns.copy()

    if not existing_partition_spec:
        return  # Empty spec will be set on first write

    if set(existing_partition_spec) != set(expected_partition_spec):
        raise ValueError(
            f"Partition strategy mismatch. "
            f"Existing: {existing_partition_spec}, "
            f"Configured: {expected_partition_spec}"
        )


class TestPartitionValidation:
    """Tests for partition strategy validation."""

    def test_matching_partitions(self):
        """Should not raise when partitions match."""
        validate_partition_strategy(
            ["region", "year"],
            {"partition_spec": ["region", "year"]}
        )

    def test_matching_partitions_different_order(self):
        """Should not raise when partitions match but order differs."""
        validate_partition_strategy(
            ["year", "region"],
            {"partition_spec": ["region", "year"]}
        )

    def test_mismatched_partitions(self):
        """Should raise ValueError when partitions don't match."""
        with pytest.raises(ValueError, match="Partition strategy mismatch"):
            validate_partition_strategy(
                ["region", "year"],
                {"partition_spec": ["datacenter", "month"]}
            )

    def test_empty_existing_spec(self):
        """Should not raise when existing spec is empty."""
        validate_partition_strategy(
            ["region", "year"],
            {"partition_spec": []}
        )

    def test_missing_partition_spec(self):
        """Should not raise when partition_spec key is missing."""
        validate_partition_strategy(
            ["region", "year"],
            {}
        )


# ============================================================================
# Test: Hive partition path extraction logic
# ============================================================================

def extract_partition_columns_from_path(path: str, table_prefix: str) -> list:
    """
    Extract partition column names from a Hive-style path.
    Extracted from QuixLakeSink._validate_existing_table_structure for testing.
    """
    if not path.endswith('.parquet'):
        return []

    relative_path = path[len(table_prefix):] if path.startswith(table_prefix) else path
    path_parts = relative_path.split('/')

    partition_columns = []
    for part in path_parts[:-1]:  # Exclude filename
        if '=' in part:
            col_name = part.split('=')[0]
            if col_name not in partition_columns:
                partition_columns.append(col_name)

    return partition_columns


class TestHivePathExtraction:
    """Tests for Hive partition path extraction."""

    def test_simple_partition_path(self):
        """Should extract partition columns from simple path."""
        result = extract_partition_columns_from_path(
            "data-lake/table/region=us-east/data.parquet",
            "data-lake/table/"
        )
        assert result == ["region"]

    def test_multiple_partition_columns(self):
        """Should extract multiple partition columns."""
        result = extract_partition_columns_from_path(
            "data-lake/table/region=us-east/year=2024/month=01/data.parquet",
            "data-lake/table/"
        )
        assert result == ["region", "year", "month"]

    def test_no_partitions(self):
        """Should return empty list for non-partitioned path."""
        result = extract_partition_columns_from_path(
            "data-lake/table/data.parquet",
            "data-lake/table/"
        )
        assert result == []

    def test_non_parquet_file(self):
        """Should return empty list for non-parquet file."""
        result = extract_partition_columns_from_path(
            "data-lake/table/region=us-east/data.json",
            "data-lake/table/"
        )
        assert result == []

    def test_maintains_order(self):
        """Should maintain partition column order."""
        result = extract_partition_columns_from_path(
            "prefix/table/year=2024/month=01/day=15/hour=10/data.parquet",
            "prefix/table/"
        )
        assert result == ["year", "month", "day", "hour"]


# ============================================================================
# Test: Hive partition path construction logic
# ============================================================================

def build_hive_partition_path(
    s3_prefix: str,
    table_name: str,
    partition_columns: list,
    partition_values: tuple,
    file_id: str
) -> str:
    """
    Build a Hive-style partition path.
    Extracted from QuixLakeSink._write_batch for testing.
    """
    if partition_columns:
        partition_parts = [
            f"{col}={val}"
            for col, val in zip(partition_columns, partition_values)
        ]
        return f"{s3_prefix}/{table_name}/" + "/".join(partition_parts) + f"/data_{file_id}.parquet"
    else:
        return f"{s3_prefix}/{table_name}/data_{file_id}.parquet"


class TestHivePathConstruction:
    """Tests for Hive partition path construction."""

    def test_no_partitions(self):
        """Should create path without partitions."""
        result = build_hive_partition_path(
            "data-lake",
            "my_table",
            [],
            (),
            "abc123"
        )
        assert result == "data-lake/my_table/data_abc123.parquet"

    def test_single_partition(self):
        """Should create path with single partition."""
        result = build_hive_partition_path(
            "data-lake",
            "my_table",
            ["region"],
            ("us-east",),
            "abc123"
        )
        assert result == "data-lake/my_table/region=us-east/data_abc123.parquet"

    def test_multiple_partitions(self):
        """Should create path with multiple partitions."""
        result = build_hive_partition_path(
            "data-lake",
            "my_table",
            ["region", "year", "month"],
            ("us-east", "2024", "01"),
            "abc123"
        )
        assert result == "data-lake/my_table/region=us-east/year=2024/month=01/data_abc123.parquet"

    def test_time_partitions(self):
        """Should create path with time-based partitions."""
        result = build_hive_partition_path(
            "prefix",
            "events",
            ["year", "month", "day", "hour"],
            ("2024", "06", "15", "14"),
            "xyz789"
        )
        assert result == "prefix/events/year=2024/month=06/day=15/hour=14/data_xyz789.parquet"


# ============================================================================
# Test: S3 URI construction logic
# ============================================================================

def build_s3_uri(bucket: str, workspace_id: str, storage_key: str) -> str:
    """
    Build full S3 URI for catalog registration.
    Extracted from QuixLakeSink._register_files_in_manifest for testing.
    """
    if workspace_id:
        return f"s3://{bucket}/{workspace_id}/{storage_key}"
    else:
        return f"s3://{bucket}/{storage_key}"


class TestS3UriConstruction:
    """Tests for S3 URI construction."""

    def test_with_workspace_id(self):
        """Should include workspace_id in path."""
        result = build_s3_uri(
            "my-bucket",
            "workspace-123",
            "data-lake/table/data.parquet"
        )
        assert result == "s3://my-bucket/workspace-123/data-lake/table/data.parquet"

    def test_without_workspace_id(self):
        """Should omit workspace_id when empty."""
        result = build_s3_uri(
            "my-bucket",
            "",
            "data-lake/table/data.parquet"
        )
        assert result == "s3://my-bucket/data-lake/table/data.parquet"

    def test_with_partitioned_path(self):
        """Should handle partitioned paths."""
        result = build_s3_uri(
            "bucket",
            "ws",
            "prefix/table/region=us/data.parquet"
        )
        assert result == "s3://bucket/ws/prefix/table/region=us/data.parquet"
