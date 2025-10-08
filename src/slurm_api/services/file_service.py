import os
import base64
import uuid
from datetime import datetime
from typing import Tuple, Optional, Dict, Any
from gridfs import GridFS
from bson import ObjectId

from slurm_api.config.db_config import files_collection, get_files_db_client
from slurm_api.config.logging_config import logger

def find_file_by_hash(file_hash: str) -> Optional[Dict[str, Any]]:
    """
    Find a file in the database by its hash and update the last_checked timestamp.
    
    Args:
        file_hash: The hash of the file to find
        
    Returns:
        The file document if found, None otherwise
    """
    # Find the file in the database
    file_doc = files_collection.find_one({"file_hash": file_hash})
    
    if file_doc:
        # Update the last_checked timestamp
        current_time = datetime.utcnow()
        files_collection.update_one(
            {"file_hash": file_hash},
            {"$set": {"last_checked": current_time}}
        )
        
        # Update the in-memory document with the new timestamp
        file_doc["last_checked"] = current_time
        
    return file_doc

def download_file(file_hash: str, target_directory: str) -> Tuple[bool, str, Optional[str]]:
    """
    Download a file from the database to a specified directory.
    Handles both inline storage and GridFS storage.
    
    Args:
        file_hash: The hash of the file to download
        target_directory: The directory to save the file to
        
    Returns:
        Tuple containing:
        - success (bool): Whether the download was successful
        - message (str): Success or error message
        - file_path (str): Path to the downloaded file, or None if download failed
    """
    try:
        # Find the file in the database
        file_doc = find_file_by_hash(file_hash)
        
        if not file_doc:
            return False, f"File with hash {file_hash} not found", None
            
        # Create the target directory if it doesn't exist
        os.makedirs(target_directory, exist_ok=True)
        
        # Determine filename (use original if available, otherwise use hash)
        filename = file_doc.get("filename", f"{file_hash}")
        file_path = os.path.join(target_directory, filename)
        
        # Check storage type
        storage_type = file_doc.get("storage_type", "inline")
        
        if storage_type == "gridfs":
            # Download from GridFS
            try:
                db = get_files_db_client()
                fs = GridFS(db)
                
                # Get GridFS file ID
                gridfs_id = file_doc.get("gridfs_id")
                if not gridfs_id:
                    return False, f"GridFS ID not found for file {file_hash}", None
                
                # Convert to ObjectId if it's a string
                if isinstance(gridfs_id, str):
                    gridfs_id = ObjectId(gridfs_id)
                
                # Download file from GridFS
                grid_out = fs.get(gridfs_id)
                
                # Write to file
                with open(file_path, "wb") as f:
                    f.write(grid_out.read())
                
                grid_out.close()
                
                return True, f"File downloaded successfully from GridFS to {file_path}", file_path
                
            except Exception as e:
                logger.error(f"Error downloading from GridFS for file {file_hash}: {e}")
                return False, f"Error downloading from GridFS: {str(e)}", None
        else:
            # Download from inline storage (existing logic)
            # Get content from document
            content_str = file_doc.get("content", "")
            
            # Add padding if necessary to avoid incorrect padding errors
            padding = len(content_str) % 4
            if padding:
                content_str += "=" * (4 - padding)
                
            try:
                # Decode base64 content and write to file
                file_content = base64.b64decode(content_str)
                
                with open(file_path, "wb") as f:
                    f.write(file_content)
                    
                return True, f"File downloaded successfully to {file_path}", file_path
            except Exception as e:
                logger.error(f"Error decoding content for file {file_hash}: {e}")
                # Try to write the content as-is if decoding fails
                with open(file_path, "wb") as f:
                    f.write(content_str.encode('utf-8'))
                
                return True, f"File saved as text to {file_path}", file_path
        
    except Exception as e:
        logger.error(f"Error downloading file {file_hash}: {e}")
        return False, f"Error downloading file: {str(e)}", None

def create_job_workspace() -> str:
    """
    Create a unique workspace directory for a job.
    
    Returns:
        The path to the created workspace directory
    """
    # Generate a unique workspace ID
    workspace_id = str(uuid.uuid4())
    
    # Create the workspace directory path
    workspace_dir = os.path.join("/workspace", workspace_id)
    
    # Create the directory
    os.makedirs(workspace_dir, exist_ok=True)
    
    return workspace_dir 