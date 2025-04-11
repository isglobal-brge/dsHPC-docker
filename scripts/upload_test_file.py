#!/usr/bin/env python3
"""
Upload a test file to the API for testing purposes.

This script:
1. Creates a simple text file with example content
2. Calculates a hash for the file
3. Uploads it to the API
4. Prints the result

Usage:
    python upload_test_file.py
"""

import os
import sys
import json
import base64
import hashlib
import argparse
import requests
from typing import Dict, Any, Optional
import random
import string

# Default API settings
DEFAULT_API_URL = "http://localhost:8001"  # Default URL for the dshpc-api container

def create_test_file_content(size_kb: int = 5, file_num: int = 0) -> bytes:
    """
    Create test file content.
    
    Args:
        size_kb: Size of the file in KB
        file_num: File number (to make content unique)
        
    Returns:
        File content as bytes
    """
    # Create a unique identifier
    random_suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    
    # Create a sample text file with the given size and unique content
    content = f"This is test file #{file_num} ({random_suffix}) for the dsHPC API.\n".encode('utf-8')
    
    # Add some random data
    random_data = ''.join(random.choices(string.ascii_letters + string.digits, k=100)).encode('utf-8')
    content += random_data
    
    # Repeat the content to reach the desired size
    content = content * (size_kb * 1024 // len(content) + 1)
    
    # Truncate to exact size
    return content[:size_kb * 1024]

def calculate_file_hash(content: bytes) -> str:
    """
    Calculate a SHA-256 hash for the file content.
    
    Args:
        content: File content as bytes
        
    Returns:
        File hash as hex string
    """
    hasher = hashlib.sha256()
    hasher.update(content)
    return hasher.hexdigest()

def upload_file(api_url: str, file_content: bytes, filename: str) -> Dict[str, Any]:
    """
    Upload a file to the API.
    
    Args:
        api_url: The API URL
        file_content: File content as bytes
        filename: Name of the file
        
    Returns:
        API response
    """
    file_hash = calculate_file_hash(file_content)
    
    payload = {
        "file_hash": file_hash,
        "content": base64.b64encode(file_content).decode('utf-8'),
        "filename": filename,
        "content_type": "text/plain",
        "metadata": {
            "description": "Test file for multi-job simulation",
            "created_by": "upload_test_file.py"
        }
    }
    
    try:
        response = requests.post(
            f"{api_url}/files/upload",
            json=payload
        )
        
        if response.status_code != 201:
            print(f"Error uploading file: {response.status_code}")
            print(f"Response: {response.text}")
            return {}
        
        return response.json()
    except Exception as e:
        print(f"Error uploading file: {e}")
        return {}

def main():
    parser = argparse.ArgumentParser(description='Upload a test file to the API')
    parser.add_argument('--url', default=DEFAULT_API_URL, help='API URL')
    parser.add_argument('--size', type=int, default=5, help='File size in KB')
    parser.add_argument('--count', type=int, default=3, help='Number of files to upload')
    args = parser.parse_args()
    
    # Check API health
    try:
        health_response = requests.get(f"{args.url}/health")
        if health_response.status_code != 200:
            print(f"API is not healthy (status code: {health_response.status_code})")
            return
    except Exception as e:
        print(f"Error connecting to API: {e}")
        return
    
    # Upload files
    uploaded_files = []
    
    for i in range(args.count):
        print(f"\nUploading test file {i+1}/{args.count}...")
        
        # Create file content
        file_content = create_test_file_content(args.size, i)
        file_hash = calculate_file_hash(file_content)
        filename = f"test_file_{i+1}.txt"
        
        print(f"File: {filename}")
        print(f"Hash: {file_hash}")
        print(f"Size: {len(file_content)} bytes")
        
        # Upload file
        result = upload_file(args.url, file_content, filename)
        
        if result:
            print("Upload successful!")
            uploaded_files.append(result)
        else:
            print("Upload failed.")
    
    # Print summary
    print(f"\nUploaded {len(uploaded_files)}/{args.count} files")
    
    for i, file_info in enumerate(uploaded_files):
        print(f"\nFile {i+1}:")
        print(f"  Hash: {file_info.get('file_hash')}")
        print(f"  Filename: {file_info.get('filename')}")
        print(f"  Content Type: {file_info.get('content_type')}")
        print(f"  Created At: {file_info.get('created_at')}")

if __name__ == "__main__":
    main() 