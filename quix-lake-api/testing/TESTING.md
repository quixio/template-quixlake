# QuixLake API Integration Testing

This directory contains comprehensive integration tests for the QuixLake API with S3 storage.

## Test Coverage

The integration tests cover all major functionality:

### ✅ **Core Operations**
- **Insert Operations**: Basic and partitioned data insertion
- **Query Operations**: Simple queries and complex aggregations with filters
- **Partition Management**: Hive partitioning with multiple columns
- **Data Compaction**: File compaction while preserving data integrity
- **Delete Operations**: Conditional and table-level deletion

### ✅ **S3 Integration**
- **S3 Storage**: All data operations use S3 for file storage
- **Partition Tree**: S3 directory structure visualization
- **File Management**: S3-compatible file operations and cleanup

### ✅ **Error Handling**
- **Invalid Data**: Proper error responses for malformed CSV
- **Missing Tables**: Graceful handling of non-existent tables
- **API Errors**: Appropriate HTTP status codes

## Running Tests

### Option 1: Automated Test Runner (Recommended)

```bash
# Run all tests with automatic service management
./run_tests.sh
```

This script will:
- Check if the service is running, start it if needed
- Install required Python packages
- Execute all integration tests
- Clean up test data
- Stop the service if it was started by the script

### Option 2: Manual Test Execution

1. **Start the QuixLake API service:**
   ```bash
   python main.py
   ```

2. **Install test dependencies:**
   ```bash
   pip install -r test_requirements.txt
   ```

3. **Run the integration tests:**
   ```bash
   python test_integration.py
   ```

4. **Optionally specify a different service URL:**
   ```bash
   python test_integration.py http://localhost:8080
   ```

## Test Structure

### Test Files
- `test_integration.py` - Main integration test suite
- `run_tests.sh` - Automated test runner script
- `test_requirements.txt` - Python dependencies for tests
- `TESTING.md` - This documentation

### Test Categories

1. **Basic Insert and Query** - Verifies fundamental data operations
2. **Partitioned Insert** - Tests Hive partitioning with multiple columns
3. **Partition Tree** - Validates S3 directory structure reporting
4. **Compaction** - Tests file compaction without data duplication
5. **Delete Operations** - Validates conditional and table deletion
6. **Complex Queries** - Tests aggregations and filtering
7. **Error Handling** - Verifies proper error responses
8. **Tables Listing** - Tests metadata endpoints

## Test Results

Example successful test run:
```
🧪 QuixLake API Integration Tests
==================================================
✅ All 18 tests passed with 100% success rate!

Test Summary:
- Total tests: 18
- Passed: 18  
- Failed: 0
- Success rate: 100.0%
```

## Test Data Management

### Automatic Cleanup
- Tests automatically clean up before and after execution
- All test tables are prefixed with `test_` for easy identification
- Cleanup removes both data files and catalog entries

### Test Isolation
- Each test creates its own uniquely named tables
- Tests don't depend on each other's data
- Failed tests don't affect subsequent test execution

## Environment Requirements

### Prerequisites
- **Python 3.8+** with pip
- **QuixLake API Service** running or ready to start
- **S3 Configuration** in `.env` file
- **Network Access** to localhost:80 (or configured port)

### S3 Configuration
Tests use the same S3 configuration as the main service:
```bash
# Required S3 settings in .env
S3_BUCKET=your-test-bucket
S3_PREFIX=test-data
AWS_REGION=your-region
AWS_ACCESS_KEY_ID=your-key-id
AWS_SECRET_ACCESS_KEY=your-secret-key
```

## Troubleshooting

### Common Issues

**Service Connection Error:**
```
Service not available, exiting
```
- Ensure the QuixLake API is running on the expected port
- Check that S3 credentials are properly configured
- Verify network connectivity to localhost:80

**Import Errors:**
```
ModuleNotFoundError: No module named 'requests'
```
- Install test dependencies: `pip install -r test_requirements.txt`

**S3 Permission Errors:**
```
S3 connection failed: Unable to locate credentials
```
- Verify AWS credentials in `.env` file
- Ensure S3 bucket exists and is accessible
- Check AWS region configuration

**Test Failures:**
- Review the detailed test output for specific failure reasons
- Check service logs for underlying errors
- Ensure S3 bucket has proper read/write permissions

## Extending Tests

### Adding New Tests

1. **Add test method** to `TestQuixLakeAPI` class:
   ```python
   def test_new_functionality(self):
       """Test new feature"""
       self.log("\n=== Test: New Feature ===")
       # Test implementation
       self.assert_equal(actual, expected, "Test description")
   ```

2. **Call test method** in `run_all_tests()`:
   ```python
   def run_all_tests(self):
       # ... existing tests ...
       self.test_new_functionality()
   ```

### Test Utilities

Available assertion methods:
- `assert_equal(actual, expected, test_name)` - Compare values
- `assert_true(condition, test_name, message)` - Verify conditions
- `log(message)` - Output test progress

## Continuous Integration

The test suite is designed for CI/CD integration:

- **Exit Codes**: Returns 0 for success, 1 for failure
- **Automated Setup**: No manual intervention required
- **Clean Environment**: Cleans up test data automatically
- **Detailed Output**: Comprehensive logging for debugging

Example CI usage:
```bash
# In your CI pipeline
./run_tests.sh
if [ $? -eq 0 ]; then
    echo "✅ All tests passed - deployment ready"
else
    echo "❌ Tests failed - blocking deployment"
    exit 1
fi
```