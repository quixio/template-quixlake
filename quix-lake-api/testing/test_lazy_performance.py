#!/usr/bin/env python3
"""
Test performance comparison between fast lazy loading vs full tree loading.
"""

import requests
import time

BASE_URL = "http://localhost:80"

def test_performance():
    print("Testing partition loading performance...")
    
    # Get list of tables
    response = requests.get(f"{BASE_URL}/tables")
    tables = response.json().get("tables", [])
    
    if not tables:
        print("No tables found.")
        return
    
    # Find a table to test
    test_table = tables[0] if tables else None
    print(f"Testing with table: {test_table}")
    
    # Test 1: Fast lazy loading (no size calculation)
    print("\n1. Fast lazy loading (no recursive size calculation)...")
    start = time.time()
    response = requests.get(f"{BASE_URL}/partitions?table={test_table}&lazy=true&path=")
    fast_time = time.time() - start
    
    if response.status_code == 200:
        data = response.json()
        if "partitions" in data:
            print(f"   ✓ Fast lazy loading: {fast_time:.3f}s")
            print(f"   Root items: {len(data['partitions'])}")
        else:
            print(f"   No partitions found")
    else:
        print(f"   ✗ Failed: {response.status_code}")
        
    # Test 2: Lazy loading with size calculation (slower)
    print("\n2. Lazy loading WITH size calculation...")
    start = time.time()
    response = requests.get(f"{BASE_URL}/partitions?table={test_table}&lazy=true&path=&include_sizes=true")
    size_time = time.time() - start
    
    if response.status_code == 200:
        data = response.json()
        if "partitions" in data:
            print(f"   ✓ Lazy loading with sizes: {size_time:.3f}s")
            # Show first item to verify size info
            if data['partitions']:
                first_item = data['partitions'][0]
                size_mb = first_item.get('size_mb', 0)
                file_count = first_item.get('file_count', 0)
                print(f"   Sample: {first_item['name']} ({file_count} files, {size_mb:.3f}MB)")
        else:
            print(f"   No partitions found")
    else:
        print(f"   ✗ Failed: {response.status_code}")
    
    # Test 3: Full tree loading (slowest)
    print("\n3. Full tree loading (for comparison)...")
    start = time.time()
    response = requests.get(f"{BASE_URL}/partitions?table={test_table}&lazy=false")
    full_time = time.time() - start
    
    if response.status_code == 200:
        data = response.json()
        if "partition_tree" in data:
            print(f"   ✓ Full tree loading: {full_time:.3f}s")
        else:
            print(f"   No partition tree found")
    else:
        print(f"   ✗ Failed: {response.status_code}")
    
    # Performance comparison
    print("\n=== Performance Comparison ===")
    print(f"Fast lazy loading:       {fast_time:.3f}s")
    print(f"Lazy with sizes:         {size_time:.3f}s")
    print(f"Full tree loading:       {full_time:.3f}s")
    
    if full_time > 0:
        fast_speedup = full_time / fast_time if fast_time > 0 else float('inf')
        print(f"\nSpeedup (fast vs full):  {fast_speedup:.1f}x faster")
        
    print("\n✅ Performance test completed!")
    print("   Use lazy=true (default) for fastest UI loading")
    print("   Use include_sizes=true only when size info is needed")

if __name__ == "__main__":
    test_performance()