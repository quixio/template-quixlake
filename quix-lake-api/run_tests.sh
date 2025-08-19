#!/bin/bash

# QuixLake API Integration Test Runner
# This script starts the service (if needed) and runs the integration tests

set -e

echo "🧪 QuixLake API Integration Test Runner"
echo "======================================="

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 is required but not found"
    exit 1
fi

# Check if service is already running
SERVICE_URL="http://localhost:80"
if curl -s "$SERVICE_URL/tables" > /dev/null 2>&1; then
    echo "✅ Service is already running at $SERVICE_URL"
    SERVICE_RUNNING=true
else
    echo "🚀 Starting QuixLake API service..."
    
    # Start the service in background
    python3 main.py &
    SERVICE_PID=$!
    SERVICE_RUNNING=false
    
    echo "⏳ Waiting for service to start..."
    sleep 5
    
    # Check if service started successfully
    for i in {1..30}; do
        if curl -s "$SERVICE_URL/tables" > /dev/null 2>&1; then
            echo "✅ Service started successfully"
            SERVICE_RUNNING=true
            break
        fi
        sleep 1
    done
    
    if [ "$SERVICE_RUNNING" = false ]; then
        echo "❌ Service failed to start"
        if [ ! -z "$SERVICE_PID" ]; then
            kill $SERVICE_PID 2>/dev/null || true
        fi
        exit 1
    fi
fi

# Install required Python packages if needed
echo "📦 Checking Python dependencies..."
python3 -c "import requests, pandas" 2>/dev/null || {
    echo "Installing required packages..."
    pip3 install requests pandas
}

# Run the integration tests
echo ""
echo "🧪 Running integration tests..."
echo "==============================="

python3 test_integration.py "$SERVICE_URL"
TEST_EXIT_CODE=$?

# Clean up if we started the service
if [ "$SERVICE_RUNNING" = false ] && [ ! -z "$SERVICE_PID" ]; then
    echo ""
    echo "🛑 Stopping service..."
    kill $SERVICE_PID 2>/dev/null || true
    sleep 2
    # Force kill if still running
    kill -9 $SERVICE_PID 2>/dev/null || true
fi

echo ""
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "🎉 All tests passed!"
else
    echo "💥 Some tests failed!"
fi

echo "======================================="
exit $TEST_EXIT_CODE