#!/usr/bin/env python3
"""Test explain functionality"""

import requests
import json

BASE_URL = "http://localhost:80"

def test_explain():
    print("Testing EXPLAIN functionality...")
    
    # Test 1: Simple query with explain
    print("\n1. Simple COUNT query with explain:")
    response = requests.post(
        f"{BASE_URL}/query?explain=true",
        headers={"Content-Type": "text/plain"},
        data="SELECT COUNT(*) FROM perf_test"
    )
    
    if response.status_code == 200:
        try:
            result = response.json()
            print(f"✓ Got JSON response")
            print(f"  Format: {result.get('format')}")
            print(f"  Data: {result.get('data', '').strip()}")
            print(f"  Explain length: {len(result.get('explain', ''))}")
            
            # Print full explain output
            explain = result.get('explain', '')
            if explain:
                print("\n--- Full EXPLAIN output ---")
                print(explain)
                print("--- End EXPLAIN ---")
            else:
                print("  ⚠️  Explain output is empty!")
                
        except json.JSONDecodeError:
            # Not JSON - probably CSV (no explain parameter)
            print("✗ Got non-JSON response (expected JSON with explain)")
    else:
        print(f"✗ Error: {response.status_code}")
    
    # Test 2: Regular query without explain
    print("\n2. Same query WITHOUT explain:")
    response = requests.post(
        f"{BASE_URL}/query",
        headers={"Content-Type": "text/plain"},
        data="SELECT COUNT(*) FROM perf_test"
    )
    
    if response.status_code == 200:
        content_type = response.headers.get('Content-Type', '')
        if 'json' in content_type:
            print("✗ Got JSON (expected CSV)")
        else:
            print("✓ Got CSV response")
            print(f"  Data: {response.text.strip()}")
    
    # Test 3: Complex query with explain
    print("\n3. Complex query with explain:")
    response = requests.post(
        f"{BASE_URL}/query?explain=true",
        headers={"Content-Type": "text/plain"},
        data="SELECT sensor_id, COUNT(*) as cnt FROM perf_test GROUP BY sensor_id ORDER BY cnt DESC LIMIT 3"
    )
    
    if response.status_code == 200:
        try:
            result = response.json()
            print(f"✓ Got JSON response")
            explain = result.get('explain', '')
            if explain and len(explain) > 50:
                print(f"✓ Explain plan included ({len(explain)} chars)")
                # Show first few lines
                lines = explain.split('\n')[:10]
                for line in lines:
                    print(f"  {line}")
                if len(explain.split('\n')) > 10:
                    print(f"  ... ({len(explain.split('\n'))} total lines)")
            else:
                print(f"⚠️  Short explain output: {explain}")
        except:
            print("✗ Failed to parse response")

if __name__ == "__main__":
    test_explain()