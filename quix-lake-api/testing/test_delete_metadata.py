#!/usr/bin/env python3
"""
Test script to verify that table deletion removes tables from metadata
even when S3 deletion fails due to permissions.
"""

import requests
import time

BASE_URL = "http://localhost:80"

def test_delete_metadata():
    print("Testing table deletion from metadata...")
    
    # Create a test table
    table_name = "test_delete_metadata_demo"
    csv_data = """id,name,value
1,item1,100
2,item2,200
3,item3,300"""
    
    print(f"\n1. Creating table '{table_name}'...")
    response = requests.post(
        f"{BASE_URL}/insert?table={table_name}",
        headers={"Content-Type": "text/plain"},
        data=csv_data
    )
    if response.status_code != 200:
        print(f"   Failed to create table: {response.text}")
        return False
    print(f"   ✓ Table created successfully")
    
    # Verify table exists in listing
    print(f"\n2. Checking if table appears in /tables...")
    response = requests.get(f"{BASE_URL}/tables")
    tables = response.json().get("tables", [])
    if table_name not in tables:
        print(f"   ✗ Table not found in listing: {tables}")
        return False
    print(f"   ✓ Table found in listing")
    
    # Delete the table
    print(f"\n3. Deleting table with delete_table=true...")
    response = requests.delete(f"{BASE_URL}/delete?table={table_name}&delete_table=true")
    if response.status_code != 200:
        print(f"   ✗ Delete request failed: {response.text}")
        return False
    
    result = response.json()
    print(f"   Delete response: {result}")
    
    # Check if S3 deletion failed
    if result.get("deletion_result", {}).get("s3_deletion_failed"):
        print(f"   ⚠ S3 deletion failed (expected with permission issues)")
        if "s3_deletion_error" in result.get("deletion_result", {}):
            print(f"   S3 Error: {result['deletion_result']['s3_deletion_error']}")
    else:
        print(f"   ✓ S3 deletion succeeded (or not attempted)")
    
    # Wait a moment
    time.sleep(1)
    
    # Check if table is removed from listing
    print(f"\n4. Checking if table is removed from /tables...")
    response = requests.get(f"{BASE_URL}/tables")
    tables = response.json().get("tables", [])
    if table_name in tables:
        print(f"   ✗ FAILED: Table still appears in listing: {tables}")
        print(f"   This means the table was not properly removed from metadata!")
        return False
    print(f"   ✓ SUCCESS: Table removed from listing")
    
    # Try to query the deleted table (should fail)
    print(f"\n5. Verifying queries to deleted table fail...")
    response = requests.post(
        f"{BASE_URL}/query",
        headers={"Content-Type": "text/plain"},
        data=f"SELECT COUNT(*) FROM {table_name}"
    )
    if response.status_code < 400:
        print(f"   ✗ FAILED: Query succeeded when it should have failed")
        print(f"   Response: {response.text}")
        return False
    print(f"   ✓ Query failed as expected (status {response.status_code})")
    
    print(f"\n✅ All tests passed! Table properly removed from metadata.")
    return True

if __name__ == "__main__":
    success = test_delete_metadata()
    exit(0 if success else 1)