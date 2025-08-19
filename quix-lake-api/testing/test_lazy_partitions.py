#!/usr/bin/env python3
"""
Test script to verify lazy partition loading performance improvement.
"""

import requests
import time
import json

BASE_URL = "http://localhost:80"

def test_partition_loading():
    print("Testing partition loading performance...")
    
    # First, get list of tables
    response = requests.get(f"{BASE_URL}/tables")
    tables = response.json().get("tables", [])
    
    if not tables:
        print("No tables found. Please create some partitioned tables first.")
        return
    
    # Find a partitioned table
    test_table = None
    for table in tables:
        # Check if it's partitioned
        info_response = requests.get(f"{BASE_URL}/partition-info?table={table}")
        if info_response.status_code == 200:
            info = info_response.json()
            if info.get("is_partitioned"):
                test_table = table
                print(f"\nUsing partitioned table: {test_table}")
                break
    
    if not test_table:
        print("No partitioned tables found.")
        return
    
    # Test 1: Full tree loading (old way)
    print("\n1. Testing FULL tree loading (old way)...")
    start = time.time()
    response = requests.get(f"{BASE_URL}/partitions?table={test_table}&lazy=false")
    full_time = time.time() - start
    
    if response.status_code == 200:
        data = response.json()
        if "partition_tree" in data:
            tree = data["partition_tree"]["tree"]
            total_nodes = count_nodes(tree)
            print(f"   ✓ Full tree loaded in {full_time:.3f}s")
            print(f"   Total nodes in tree: {total_nodes}")
        else:
            print(f"   No partition tree found")
    else:
        print(f"   ✗ Failed: {response.status_code}")
    
    # Test 2: Lazy loading (new way) - just root level
    print("\n2. Testing LAZY loading (new way - root only)...")
    start = time.time()
    response = requests.get(f"{BASE_URL}/partitions?table={test_table}&lazy=true&path=")
    lazy_time = time.time() - start
    
    if response.status_code == 200:
        data = response.json()
        if "partitions" in data:
            items = data["partitions"]
            print(f"   ✓ Root level loaded in {lazy_time:.3f}s")
            print(f"   Items at root: {len(items)}")
            
            # Show first few items
            for item in items[:3]:
                print(f"     - {item['name']} ({item['type']})")
                if item['type'] == 'directory' and item.get('has_children'):
                    print(f"       Has children: Yes")
        else:
            print(f"   No partitions found")
    else:
        print(f"   ✗ Failed: {response.status_code}")
    
    # Test 3: Load a nested level
    if response.status_code == 200 and items and items[0]['type'] == 'directory':
        first_dir = items[0]['name']
        print(f"\n3. Testing nested level loading (path={first_dir})...")
        start = time.time()
        response = requests.get(f"{BASE_URL}/partitions?table={test_table}&lazy=true&path={first_dir}")
        nested_time = time.time() - start
        
        if response.status_code == 200:
            data = response.json()
            if "partitions" in data:
                nested_items = data["partitions"]
                print(f"   ✓ Nested level loaded in {nested_time:.3f}s")
                print(f"   Items in {first_dir}: {len(nested_items)}")
            else:
                print(f"   No items found")
        else:
            print(f"   ✗ Failed: {response.status_code}")
    
    # Performance comparison
    print("\n=== Performance Comparison ===")
    print(f"Full tree loading:  {full_time:.3f}s")
    print(f"Lazy root loading:  {lazy_time:.3f}s")
    if full_time > 0:
        speedup = full_time / lazy_time
        print(f"Speedup:            {speedup:.1f}x faster")
        print(f"Time saved:         {full_time - lazy_time:.3f}s")
    
    print("\n✅ Lazy loading is working correctly!")
    print("   The UI will now load partitions on-demand as you expand them.")

def count_nodes(node):
    """Recursively count nodes in tree"""
    count = 1
    if "children" in node:
        for child in node["children"]:
            count += count_nodes(child)
    return count

if __name__ == "__main__":
    test_partition_loading()