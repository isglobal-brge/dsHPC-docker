#!/usr/bin/env python3
"""
Complex Python method main script.

This script imports functions from a subdirectory (utils) and processes text.
"""

import sys
import json
import os
from utils import analysis, transformation

def process_file(input_file, params):
    """Process a file according to parameters using utility modules."""
    # Read the input file
    with open(input_file, 'r') as f:
        content = f.read()
    
    results = {
        "status": "success",
        "original_file": input_file,
        "parameters_applied": params,
        "output": {},
        "error": None
    }
    
    try:
        operation = params.get('operation', 'count').lower()
        case_param = params.get('case', 'none').lower()
        include_stats_param = params.get('include_stats', False)

        # Apply transformations
        processed_content = transformation.apply_case(content, case_param)
        results["output"]["transformed_content"] = processed_content
        
        # Perform operation
        if operation == 'count':
            results["output"]["line_count"] = analysis.count_lines(processed_content)
        elif operation == 'analyze':
            results["output"]["word_count"] = analysis.count_words(processed_content)
            if include_stats_param:
                results["output"]["char_stats"] = analysis.char_frequency(processed_content)
        elif operation == 'transform':
            # The transformation was already applied
            pass
        else:
            results["status"] = "error"
            results["error"] = f"Unknown operation: {operation}"
            
    except Exception as e:
        results["status"] = "error"
        results["error"] = f"Error processing file: {str(e)}"
        
    return results

def main():
    """Main entry point."""
    if len(sys.argv) < 3:
        result = {
            "status": "error",
            "message": "Usage: python main.py <input_file> <params_file>",
            "error": "Missing arguments"
        }
        print(json.dumps(result))
        return False
    
    input_file = sys.argv[1]
    params_file = sys.argv[2]
    
    if not os.path.isfile(input_file):
        result = {
            "status": "error",
            "message": f"Input file not found: {input_file}",
            "error": "File not found"
        }
        print(json.dumps(result))
        return False
        
    if not os.path.isfile(params_file):
        result = {
            "status": "error",
            "message": f"Parameters file not found: {params_file}",
            "error": "File not found"
        }
        print(json.dumps(result))
        return False
    
    try:
        with open(params_file, 'r') as f:
            params = json.load(f)
    except Exception as e:
        result = {
            "status": "error",
            "message": f"Error loading parameters: {str(e)}",
            "error": "Parameter loading error"
        }
        print(json.dumps(result))
        return False
    
    result = process_file(input_file, params)
    print(json.dumps(result))
    return result["status"] == "success"

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1) 