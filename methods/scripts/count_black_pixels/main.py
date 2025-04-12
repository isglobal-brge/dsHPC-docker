#!/usr/bin/env python3
from PIL import Image
import numpy as np
import sys
import json
import os

def count_black_pixels(image_path, threshold=30):
    """
    Count black pixels in an image.
    
    Args:
        image_path: Path to the image file
        threshold: Pixel value below which pixels are considered black (0-255)
        
    Returns:
        Dict with count and percentage of black pixels
    """
    try:
        # Open the image
        img = Image.open(image_path)
        
        # Convert to grayscale
        if img.mode != 'L':
            img = img.convert('L')
        
        # Convert to numpy array for processing
        img_array = np.array(img)
        
        # Count pixels below threshold (black)
        black_pixels = np.sum(img_array < threshold)
        
        # Calculate percentage
        total_pixels = img_array.size
        black_percentage = (black_pixels / total_pixels) * 100
        
        return {
            "status": "success",
            "black_pixel_count": int(black_pixels),
            "total_pixels": int(total_pixels),
            "black_percentage": round(black_percentage, 2),
            "threshold_used": threshold,
            "original_file": image_path
        }
    except Exception as e:
        return {
            "status": "error",
            "error": f"Failed to process image: {str(e)}"
        }

def main():
    """Main entry point for the script."""
    # Check if file path was provided
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
    
    # Get threshold parameter
    threshold = params.get('threshold', 30)
    
    # Process the image
    result = count_black_pixels(input_file, threshold)
    
    # Add parameters applied to the result
    result["parameters_applied"] = params
    
    # Output the result as JSON
    print(json.dumps(result))
    return result["status"] == "success"

if __name__ == "__main__":
    success = main()
    # Siempre salir con código 0, incluso si hay errores
    sys.exit(0)