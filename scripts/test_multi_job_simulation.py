#!/usr/bin/env python3
"""
Test script for the multi-job simulation endpoint.

This script:
1. Creates a set of job configurations with different files, methods, and parameters
2. Sends them to the multi-job simulation endpoint
3. Displays the results

Usage:
    python test_multi_job_simulation.py
"""

import os
import sys
import json
import argparse
import asyncio
import aiohttp
from typing import List, Dict, Any, Optional

# Default API settings
DEFAULT_API_URL = "http://localhost:8001"  # Default URL for the dshpc-api container
api_url = DEFAULT_API_URL  # Global variable that can be modified

async def fetch_files() -> List[str]:
    """
    Fetch a list of file hashes from the API.
    Returns at most 5 files.
    """
    print("Fetching available files...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{api_url}/files") as response:
                if response.status != 200:
                    print(f"Error fetching files: {response.status}")
                    return []
                
                files = await response.json()
                if not files:
                    print("No files found in the database")
                    return []
                
                print(f"Found {len(files)} files")
                return [file.get("file_hash") for file in files[:5]]
    except Exception as e:
        print(f"Error fetching files: {e}")
        return []

async def fetch_methods() -> List[str]:
    """
    Fetch a list of available method names.
    """
    print("Fetching available methods...")
    try:
        # We don't have a direct API to list methods by name, so we'll use a few example names
        # In a real scenario, you would fetch these from the methods API
        return ["Example Bash Method", "Complex R Method", "Simple Python Method"]
    except Exception as e:
        print(f"Error fetching methods: {e}")
        return []

async def check_api_health() -> bool:
    """
    Check if the API is healthy.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{api_url}/health") as response:
                return response.status == 200
    except Exception:
        return False

async def send_multi_job_request(job_configs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Send a request to the multi-job simulation endpoint.
    """
    print(f"Sending request with {len(job_configs)} job configurations...")
    
    payload = {
        "jobs": job_configs
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{api_url}/simulate-jobs",
                json=payload
            ) as response:
                if response.status != 200:
                    error = await response.text()
                    print(f"Error response ({response.status}): {error}")
                    return {}
                
                return await response.json()
    except Exception as e:
        print(f"Error sending request: {e}")
        return {}

def generate_test_parameters() -> List[Dict[str, Any]]:
    """
    Generate a set of test parameters for different methods.
    """
    return [
        {},  # Empty parameters
        {"count_lines": True},  # For bash method
        {"summary_level": "basic"},  # For R method
        {"n_iterations": 100},  # For Python method
        {"verbose": True, "threshold": 0.5}  # Complex parameters
    ]

def safe_get(dictionary: Optional[Dict[str, Any]], key: str, default: Any = None) -> Any:
    """
    Safely get a value from a dictionary, handling None.
    """
    if dictionary is None:
        return default
    return dictionary.get(key, default)

def get_first_chars(value: Any, num_chars: int = 10) -> str:
    """
    Get the first n characters of a string, with error handling.
    """
    if value is None:
        return ""
    
    str_value = str(value)
    if len(str_value) <= num_chars:
        return str_value
    
    return str_value[:num_chars] + "..."

async def main():
    global api_url
    
    parser = argparse.ArgumentParser(description='Test multi-job simulation endpoint')
    parser.add_argument('--url', default=DEFAULT_API_URL, help='API URL')
    parser.add_argument('--num-jobs', type=int, default=5, help='Number of job configurations to generate')
    args = parser.parse_args()
    
    # Update the API URL
    api_url = args.url
    
    # Check if API is healthy
    print(f"Checking API health at {api_url}...")
    if not await check_api_health():
        print("API is not healthy. Please check that the API is running.")
        return
    
    # Fetch available files
    file_hashes = await fetch_files()
    if not file_hashes:
        print("No files available. Please upload some files first.")
        return
    
    # Fetch available methods
    method_names = await fetch_methods()
    if not method_names:
        print("No methods available. Please register some methods first.")
        return
    
    # Generate parameters
    parameters_list = generate_test_parameters()
    
    # Generate job configurations
    job_configs = []
    
    for i in range(args.num_jobs):
        # Use modulo to cycle through available resources
        file_hash = file_hashes[i % len(file_hashes)]
        method_name = method_names[i % len(method_names)]
        parameters = parameters_list[i % len(parameters_list)]
        
        job_configs.append({
            "file_hash": file_hash,
            "method_name": method_name,
            "parameters": parameters
        })
    
    # Print job configurations
    print("\nJob Configurations:")
    for i, config in enumerate(job_configs):
        print(f"Job {i+1}:")
        print(f"  File: {get_first_chars(config['file_hash'])}")
        print(f"  Method: {config['method_name']}")
        print(f"  Parameters: {config['parameters']}")
    
    # Send request
    print("\nSending request to multi-job simulation endpoint...")
    result = await send_multi_job_request(job_configs)
    
    if not result:
        print("No results returned from API.")
        return
    
    # Print results
    print("\nResults:")
    print(f"Total jobs: {safe_get(result, 'total_jobs', 0)}")
    print(f"Successful submissions: {safe_get(result, 'successful_submissions', 0)}")
    print(f"Failed submissions: {safe_get(result, 'failed_submissions', 0)}")
    print(f"Completed jobs: {safe_get(result, 'completed_jobs', 0)}")
    print(f"In-progress jobs: {safe_get(result, 'in_progress_jobs', 0)}")
    print(f"Resubmitted jobs: {safe_get(result, 'resubmitted_jobs', 0)}")
    
    print("\nDetailed Results:")
    for i, job_result in enumerate(safe_get(result, 'results', [])):
        print(f"\nJob {i+1} - {safe_get(job_result, 'method_name', 'Unknown')}:")
        print(f"  File: {get_first_chars(safe_get(job_result, 'file_hash', ''))}")
        print(f"  Function hash: {get_first_chars(safe_get(job_result, 'function_hash', ''))}")
        print(f"  Job ID: {safe_get(job_result, 'job_id', 'None')}")
        print(f"  New Status: {safe_get(job_result, 'new_status', 'Unknown')}")
        
        if safe_get(job_result, 'old_status'):
            print(f"  Old Status: {safe_get(job_result, 'old_status')}")
        
        print(f"  Message: {safe_get(job_result, 'message', '')}")
        
        output = safe_get(job_result, 'output')
        if output:
            if len(output) > 100:
                output = output[:100] + "..."
            print(f"  Output: {output}")

if __name__ == "__main__":
    asyncio.run(main()) 