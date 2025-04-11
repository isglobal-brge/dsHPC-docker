# File Upload System Documentation

This document describes the file upload system for the dsHPC API, which allows for storing and retrieving files from the database using hash-based identification.

## Overview

The file upload system provides the following capabilities:

- Upload files of any type (images, documents, CSV, etc.) with unique hash identifiers
- Check if files with specific hashes already exist
- Prevent duplicate uploads based on file hash
- Retrieve metadata about uploaded files

## API Endpoints

### Upload a File

```
POST /files/upload
```

Uploads a new file to the system. All file content must be base64 encoded.

**Request Body:**
```json
{
  "file_hash": "unique_hash_identifier",
  "content": "base64_encoded_file_content",
  "filename": "example.jpg",  // optional
  "content_type": "image/jpeg",  // optional, defaults to "application/octet-stream"
  "metadata": {  // optional
    "description": "Example file",
    "owner": "user123",
    ...
  }
}
```

**Response (201 Created):**
```json
{
  "file_hash": "unique_hash_identifier",
  "filename": "example.jpg",
  "content_type": "image/jpeg",
  "upload_date": "2025-04-11T11:27:24.590000",
  "last_checked": "2025-04-11T11:27:24.590000",
  "metadata": {
    "description": "Example file",
    "owner": "user123"
  }
}
```

**Response (409 Conflict):** If a file with the same hash already exists.
```json
{
  "detail": "File with this hash already exists"
}
```

### Check If Files Exist

```
POST /files/check-hashes
```

Checks which file hashes from a provided list exist in the database and which are missing.

**Request Body:**
```json
{
  "hashes": [
    "existing_hash_1",
    "existing_hash_2",
    "nonexistent_hash_1",
    "nonexistent_hash_2"
  ]
}
```

**Response (200 OK):**
```json
{
  "existing_hashes": [
    "existing_hash_1",
    "existing_hash_2"
  ],
  "missing_hashes": [
    "nonexistent_hash_1",
    "nonexistent_hash_2"
  ]
}
```

### List All Files

```
GET /files
```

Returns a list of all files in the database with their metadata (without content).

**Response (200 OK):**
```json
[
  {
    "file_hash": "hash1",
    "filename": "example1.jpg",
    "content_type": "image/jpeg",
    "upload_date": "2025-04-11T11:20:52.256000",
    "last_checked": "2025-04-11T11:20:52.256000",
    "metadata": { "description": "Example 1" }
  },
  {
    "file_hash": "hash2",
    "filename": "example2.txt",
    "content_type": "text/plain",
    "upload_date": "2025-04-11T11:21:04.186000",
    "last_checked": "2025-04-11T11:21:04.186000",
    "metadata": { "description": "Example 2" }
  }
]
```

## File Hashing

Files are identified by their hash, which should be a unique identifier for each file. The API does not generate the hash - it's the client's responsibility to provide a consistent hash. This allows for:

1. Deduplication of files
2. Verification of file integrity
3. Distributed file management

We recommend using SHA-256 or similar algorithms for generating file hashes, but any string that uniquely identifies your file can be used.

## Base64 Encoding

All file content must be base64 encoded before sending to the API. This ensures binary data can be properly transmitted in JSON format.

### Example (Python):

```python
import base64

# Encoding a file to base64
with open("example.jpg", "rb") as file:
    encoded_content = base64.b64encode(file.read()).decode("utf-8")
```

## Content Types

The API supports all standard MIME types, including but not limited to:

- `image/jpeg`, `image/png`, `image/gif` - For images
- `application/pdf` - For PDF documents
- `text/plain` - For text files
- `text/csv` - For CSV files
- `application/zip` - For ZIP archives
- `application/octet-stream` - Default for generic binary data

## Example: Uploading a File

```python
import base64
import requests
import json

# 1. Read and encode the file
file_path = "example.jpg"
with open(file_path, "rb") as file:
    encoded_content = base64.b64encode(file.read()).decode("utf-8")

# 2. Calculate a hash (in this example we're just using a string)
file_hash = "my_unique_hash_for_example_jpg" 

# 3. Prepare the request
data = {
    "file_hash": file_hash,
    "content": encoded_content,
    "filename": "example.jpg",
    "content_type": "image/jpeg",
    "metadata": {"description": "Example image"}
}

# 4. Send the request
response = requests.post(
    "http://localhost:8001/files/upload",
    headers={"Content-Type": "application/json"},
    data=json.dumps(data)
)

# 5. Check the response
print(f"Status code: {response.status_code}")
print(f"Response: {response.json()}")
```

## Example: Checking File Existence

```python
import requests
import json

# Prepare the request
data = {
    "hashes": [
        "existing_file_hash_1",
        "existing_file_hash_2",
        "nonexistent_file_hash_1"
    ]
}

# Send the request
response = requests.post(
    "http://localhost:8001/files/check-hashes",
    headers={"Content-Type": "application/json"},
    data=json.dumps(data)
)

# Process the results
result = response.json()
print("Existing files:", result["existing_hashes"])
print("Missing files:", result["missing_hashes"])

# You might want to upload only the missing files
for missing_hash in result["missing_hashes"]:
    # Upload file with this hash...
    pass
```

## Best Practices

1. **Generate consistent hashes**: Ensure your hash generation method is consistent and produces the same hash for identical files.

2. **Handle large files appropriately**: For very large files, consider chunking the upload or using a different storage solution.

3. **Use descriptive metadata**: Include relevant information in the metadata field to help with file organization and search.

4. **Set proper content types**: Always specify the correct MIME type for your files to ensure they can be properly handled by clients.

5. **Check before uploading**: Use the `/files/check-hashes` endpoint to determine which files need to be uploaded, especially in batch operations.

## Limitations

- Maximum file size: The system is designed for small to medium-sized files. Very large files may encounter issues with memory or timeout constraints.
- Storage persistence: Files are stored in MongoDB. Consider the storage implications for your specific deployment.

## Security Considerations

- Authentication: This API should be deployed with proper authentication mechanisms.
- Input validation: Always validate file content and hashes before submission.
- Access control: Implement appropriate access controls to restrict who can upload and access files. 