#!/usr/bin/env python3
"""
Register a method with the Slurm API.

This script compresses a method directory, calculates its hash, and registers it in the database.

Usage:
    python register_method.py <method_dir> <method_json>

Where:
    <method_dir> is the path to the method directory
    <method_json> is the path to the method JSON file with metadata
"""

import os
import sys
import json
import argparse
from pymongo import MongoClient
import datetime

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from slurm_api.services.method_service import register_method

def parse_args():
    parser = argparse.ArgumentParser(description='Register a method with the Slurm API')
    parser.add_argument('method_dir', help='Path to the method directory')
    parser.add_argument('method_json', help='Path to the method JSON file with metadata')
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Check if the method directory exists
    if not os.path.isdir(args.method_dir):
        print(f"Error: Method directory {args.method_dir} does not exist")
        sys.exit(1)
    
    # Check if the method JSON file exists
    if not os.path.isfile(args.method_json):
        print(f"Error: Method JSON file {args.method_json} does not exist")
        sys.exit(1)
    
    # Load the method JSON file
    try:
        with open(args.method_json, 'r') as f:
            method_data = json.load(f)
    except Exception as e:
        print(f"Error loading method JSON file: {e}")
        sys.exit(1)
    
    # Validate required fields
    required_fields = ['name', 'command', 'script_path']
    for field in required_fields:
        if field not in method_data:
            print(f"Error: Required field '{field}' is missing from method JSON")
            sys.exit(1)
    
    # Make sure method_data includes a timestamp
    method_data["created_at"] = datetime.datetime.utcnow().isoformat()
    
    # Register the method
    success, message, function_hash = register_method(method_data, args.method_dir)
    
    if success:
        print(f"Method registered successfully!")
        print(f"Function hash: {function_hash}")
    else:
        print(f"Error registering method: {message}")
        sys.exit(1)

if __name__ == '__main__':
    main() 