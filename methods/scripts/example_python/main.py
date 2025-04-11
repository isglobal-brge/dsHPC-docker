#!/usr/bin/env python3
"""
Example Python method for processing text files.

This script demonstrates how to create a method that can be used with the Slurm API.
It takes a file and parameters, applies processing, and outputs the results in JSON format.

Usage:
    python main.py <input_file> <params_file>
"""

import sys
import json
import os

def process_file(input_file, params):
    """Process a file according to the parameters and return result as dict."""
    # Read the input file
    with open(input_file, 'r') as f:
        content = f.read()
    
    # Apply processing based on parameters
    if params.get('uppercase', False):
        content = content.upper()
    
    prefix = params.get('prefix', '')
    if prefix:
        content = '\n'.join([f"{prefix}{line}" for line in content.split('\n')])
    
    # Return the results as a dictionary
    return {
        "status": "success",
        "processed_content": content,
        "original_file": input_file,
        "parameters_applied": params,
        "lines_count": len(content.split('\n'))
    }

def main():
    """Main entry point for the script."""
    if len(sys.argv) < 3:
        result = {
            "status": "error",
            "message": "Usage: python main.py <input_file> <params_file>"
        }
        print(json.dumps(result))
        return False
    
    input_file = sys.argv[1]
    params_file = sys.argv[2]
    
    # Check if the input file exists
    if not os.path.isfile(input_file):
        result = {
            "status": "error",
            "message": f"Error: Input file {input_file} does not exist"
        }
        print(json.dumps(result))
        return False
    
    # Check if the params file exists
    if not os.path.isfile(params_file):
        result = {
            "status": "error",
            "message": f"Error: Parameters file {params_file} does not exist"
        }
        print(json.dumps(result))
        return False
    
    # Load the parameters
    try:
        with open(params_file, 'r') as f:
            params = json.load(f)
    except Exception as e:
        result = {
            "status": "error",
            "message": f"Error loading parameters: {e}"
        }
        print(json.dumps(result))
        return False
    
    # Process the file and get results
    result = process_file(input_file, params)
    
    # Output the result as JSON
    print(json.dumps(result))
    return True

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1) 