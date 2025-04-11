import base64
import requests
import json
import os

def upload_file(file_path, file_hash, metadata=None):
    """Upload any file as base64 encoded content"""
    try:
        # Get file name and determine content type based on extension
        filename = os.path.basename(file_path)
        _, ext = os.path.splitext(filename)
        
        # Simple mapping of extensions to content types
        content_types = {
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.csv': 'text/csv',
            '.txt': 'text/plain',
            '.zip': 'application/zip',
            '.pdf': 'application/pdf'
        }
        content_type = content_types.get(ext.lower(), 'application/octet-stream')
        
        # Read and encode file content
        with open(file_path, "rb") as file:
            encoded_content = base64.b64encode(file.read()).decode("utf-8")
        
        # Prepare upload data
        data = {
            "file_hash": file_hash,
            "content": encoded_content,
            "filename": filename,
            "content_type": content_type,
            "metadata": metadata or {}
        }
        
        # Upload to API
        response = requests.post(
            "http://localhost:8001/files/upload",
            headers={"Content-Type": "application/json"},
            data=json.dumps(data)
        )
        
        print(f"Uploaded {filename} (Content-Type: {content_type}):")
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.json()}")
        return response.json()
    
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None
    except Exception as e:
        print(f"Error uploading {file_path}: {str(e)}")
        return None

# Demo with test files
print("=== Upload Example Script ===")

# Try to upload real image file if exists
image_path = "/Users/david/Documents/GitHub/dsHPC/example.jpg"
if os.path.exists(image_path):
    print("\n[Uploading real image file]")
    upload_file(
        image_path, 
        "real_image_hash", 
        {"description": "Real image upload example"}
    )

# Create and upload a simple text file
print("\n[Uploading text file]")
text_path = "example_text.txt"
with open(text_path, "w") as f:
    f.write("This is a sample text file for testing the upload API.")

upload_file(
    text_path, 
    "text_file_hash", 
    {"description": "Text file upload example"}
)

# Demo with tiny image (always works)
print("\n[Uploading demo tiny image]")
tiny_image_path = "tiny_image.png"
# This is a tiny 1x1 pixel image in PNG format
tiny_image_data = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==")
with open(tiny_image_path, "wb") as f:
    f.write(tiny_image_data)

upload_file(
    tiny_image_path, 
    "tiny_image_hash", 
    {"description": "Tiny image upload example"}
)

# Clean up temp files
if os.path.exists(text_path):
    os.remove(text_path)
if os.path.exists(tiny_image_path):
    os.remove(tiny_image_path)

print("\nDone!") 