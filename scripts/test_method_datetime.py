#!/usr/bin/env python3
"""
Test script for verifying the datetime handling of methods.

This script:
1. Checks if a method with a specific name exists
2. Gets the latest version of that method
3. Lists all versions of that method sorted by creation time

Usage:
    python test_method_datetime.py <method_name>

Where:
    <method_name> is the name of the method to check
"""

import os
import sys
import json
import argparse
from pprint import pprint
from datetime import datetime
from pymongo import MongoClient

# MongoDB connection settings
MONGO_URI = "mongodb://localhost:27019/"  # Using the mapped port for dshpc-methods in Docker
MONGO_DB = "dshpc-methods"

def connect_to_db():
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        return db
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def find_method_by_name(db, method_name, latest=True):
    """
    Find a method in the database by its name.
    """
    try:
        query = {"name": method_name}
        
        if latest:
            # Find the most recent version of the method
            method = db.methods.find_one(
                query,
                sort=[("created_at", -1)]  # Sort by created_at in descending order
            )
        else:
            # Find any version of the method
            method = db.methods.find_one(query)
        
        return method
    except Exception as e:
        print(f"Error finding method by name: {e}")
        return None

def list_method_versions(db, method_name):
    """
    List all versions of a method sorted by creation time (newest first).
    """
    try:
        methods = list(db.methods.find(
            {"name": method_name},
            {"bundle": 0},  # Exclude the bundle field
            sort=[("created_at", -1)]
        ))
        
        for method in methods:
            method["_id"] = str(method["_id"])
            
        return methods
    except Exception as e:
        print(f"Error listing method versions: {e}")
        return []

def parse_args():
    parser = argparse.ArgumentParser(description='Test method datetime handling')
    parser.add_argument('method_name', help='Name of the method to check')
    return parser.parse_args()

def main():
    args = parse_args()
    method_name = args.method_name
    
    print(f"Testing datetime handling for method: {method_name}")
    print("="*50)
    
    # Connect to MongoDB
    db = connect_to_db()
    if db is None:
        print("Failed to connect to MongoDB. Exiting.")
        return
    
    # Get the latest version of the method
    print("Getting latest version...")
    latest_method = find_method_by_name(db, method_name, latest=True)
    
    if latest_method:
        # Convert ObjectId to string for display
        latest_method["_id"] = str(latest_method["_id"])
        
        print(f"Found latest version with hash: {latest_method.get('function_hash')}")
        print(f"Created at: {latest_method.get('created_at')}")
        print(f"Updated at: {latest_method.get('updated_at')}")
        
        if 'runtime_info' in latest_method:
            print(f"Runtime info included: Yes")
            # Print the first few keys from runtime_info
            runtime_keys = list(latest_method.get('runtime_info', {}).keys())
            if runtime_keys:
                print(f"Runtime info keys: {runtime_keys[:3]}...")
        else:
            print(f"Runtime info included: No")
    else:
        print(f"No method found with name: {method_name}")
        return
    
    print("\nListing all versions...")
    all_versions = list_method_versions(db, method_name)
    
    if all_versions:
        print(f"Found {len(all_versions)} version(s)")
        for i, method in enumerate(all_versions):
            print(f"\nVersion {i+1}:")
            print(f"  Hash: {method.get('function_hash')}")
            print(f"  Created at: {method.get('created_at')}")
            print(f"  Updated at: {method.get('updated_at')}")
    else:
        print(f"No versions found for method: {method_name}")

if __name__ == '__main__':
    main() 