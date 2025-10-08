from pymongo import MongoClient
from typing import List, Dict, Any, Tuple, Optional
import asyncio
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket
import base64
import hashlib

from dshpc_api.config.settings import get_settings

# Cached database connections
_jobs_db = None
_files_db = None
_gridfs_bucket = None
_jobs_gridfs_bucket = None

async def get_jobs_db():
    """
    Get a connection to the jobs database.
    """
    global _jobs_db
    if _jobs_db is None:
        settings = get_settings()
        client = AsyncIOMotorClient(settings.MONGO_JOBS_URI)
        _jobs_db = client[settings.MONGO_JOBS_DB]
    return _jobs_db

async def get_files_db():
    """
    Get a connection to the files database.
    """
    global _files_db
    if _files_db is None:
        settings = get_settings()
        client = AsyncIOMotorClient(settings.MONGO_FILES_URI)
        _files_db = client[settings.MONGO_FILES_DB]
    return _files_db

async def get_gridfs_bucket():
    """
    Get a GridFS bucket for storing large files.
    """
    global _gridfs_bucket
    if _gridfs_bucket is None:
        db = await get_files_db()
        _gridfs_bucket = AsyncIOMotorGridFSBucket(db)
    return _gridfs_bucket

async def get_jobs_gridfs_bucket():
    """
    Get a GridFS bucket for storing large job outputs.
    """
    global _jobs_gridfs_bucket
    if _jobs_gridfs_bucket is None:
        db = await get_jobs_db()
        _jobs_gridfs_bucket = AsyncIOMotorGridFSBucket(db, bucket_name="job_outputs")
    return _jobs_gridfs_bucket

async def get_job_by_id(job_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a job from the jobs database by ID, retrieving large outputs from GridFS if needed.
    """
    db = await get_jobs_db()
    job = await db.jobs.find_one({"job_id": job_id})
    if job:
        job["_id"] = str(job["_id"])  # Convert ObjectId to string
        
        # Check if output is stored in GridFS
        if job.get("output_storage") == "gridfs" and job.get("output_gridfs_id"):
            try:
                bucket = await get_jobs_gridfs_bucket()
                grid_id = job["output_gridfs_id"]
                
                # Download output from GridFS
                grid_out = await bucket.open_download_stream(grid_id)
                output_bytes = await grid_out.read()
                job["output"] = output_bytes.decode('utf-8')
                
                # Convert GridFS ID to string for JSON serialization
                job["output_gridfs_id"] = str(job["output_gridfs_id"])
            except Exception as e:
                # Log error but return metadata
                job["output"] = f"[Error retrieving output from GridFS: {str(e)}]"
        
        # Check if error is stored in GridFS
        if job.get("error_storage") == "gridfs" and job.get("error_gridfs_id"):
            try:
                bucket = await get_jobs_gridfs_bucket()
                grid_id = job["error_gridfs_id"]
                
                # Download error from GridFS
                grid_out = await bucket.open_download_stream(grid_id)
                error_bytes = await grid_out.read()
                job["error"] = error_bytes.decode('utf-8')
                
                # Convert GridFS ID to string for JSON serialization
                job["error_gridfs_id"] = str(job["error_gridfs_id"])
            except Exception as e:
                # Log error but return metadata
                job["error"] = f"[Error retrieving error from GridFS: {str(e)}]"
    
    return job

async def get_files(limit: int = 100) -> List[Dict[str, Any]]:
    """
    Get a list of files from the files database.
    """
    db = await get_files_db()
    cursor = db.files.find().limit(limit)
    files = []
    async for file in cursor:
        file["_id"] = str(file["_id"])  # Convert ObjectId to string
        files.append(file)
    return files

async def file_exists(file_hash: str) -> bool:
    """
    Check if a file with the given hash exists in the database.
    """
    db = await get_files_db()
    count = await db.files.count_documents({"file_hash": file_hash})
    return count > 0

async def upload_file(file_data: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Upload a file to the files database.
    For large files (>15MB), uses GridFS for storage.
    
    Returns:
        Tuple containing:
        - success (bool): Whether the upload was successful
        - message (str): Success or error message
        - data (Dict): Data of the uploaded file
    """
    # Check if file with same hash already exists
    if await file_exists(file_data["file_hash"]):
        return False, "File with this hash already exists", None
    
    # Add timestamps
    now = datetime.utcnow()
    file_data["upload_date"] = now
    file_data["last_checked"] = now
    
    # Decode base64 content to get actual file size
    content_base64 = file_data.get("content", "")
    try:
        content_bytes = base64.b64decode(content_base64)
        file_size = len(content_bytes)
    except Exception as e:
        return False, f"Error decoding file content: {str(e)}", None
    
    # Use GridFS for files larger than 15MB (leaving 1MB margin for BSON overhead)
    # 15MB = 15 * 1024 * 1024 bytes
    GRIDFS_THRESHOLD = 15 * 1024 * 1024
    
    if file_size > GRIDFS_THRESHOLD:
        # Use GridFS for large files
        try:
            bucket = await get_gridfs_bucket()
            
            # Store file in GridFS
            grid_id = await bucket.upload_from_stream(
                file_data["file_hash"],  # Use hash as filename in GridFS
                content_bytes,
                metadata={
                    "filename": file_data.get("filename"),
                    "content_type": file_data.get("content_type"),
                    "original_metadata": file_data.get("metadata"),
                    "upload_date": now,
                    "file_hash": file_data["file_hash"]
                }
            )
            
            # Store metadata in regular collection (without content)
            metadata_doc = {
                "file_hash": file_data["file_hash"],
                "filename": file_data.get("filename"),
                "content_type": file_data.get("content_type"),
                "metadata": file_data.get("metadata"),
                "upload_date": now,
                "last_checked": now,
                "storage_type": "gridfs",
                "gridfs_id": grid_id,
                "file_size": file_size
            }
            
            db = await get_files_db()
            result = await db.files.insert_one(metadata_doc)
            
            # Get the uploaded file metadata
            uploaded_file = await db.files.find_one({"_id": result.inserted_id})
            if uploaded_file:
                uploaded_file["_id"] = str(uploaded_file["_id"])
                uploaded_file["gridfs_id"] = str(uploaded_file["gridfs_id"])
                # Don't return the content for GridFS files
                uploaded_file["content"] = "[Stored in GridFS]"
            
            return True, "Large file uploaded successfully using GridFS", uploaded_file
            
        except Exception as e:
            return False, f"Error uploading file to GridFS: {str(e)}", None
    else:
        # Use regular storage for small files
        db = await get_files_db()
        file_data["storage_type"] = "inline"
        file_data["file_size"] = file_size
        result = await db.files.insert_one(file_data)
        
        # Get the uploaded file
        uploaded_file = await db.files.find_one({"_id": result.inserted_id})
        if uploaded_file:
            uploaded_file["_id"] = str(uploaded_file["_id"])
        
        return True, "File uploaded successfully", uploaded_file

async def check_hashes(hashes: List[str]) -> Tuple[List[str], List[str]]:
    """
    Check which hashes already exist in the database.
    
    Args:
        hashes: List of file hashes to check
        
    Returns:
        Tuple containing:
        - existing_hashes: List of hashes that already exist
        - missing_hashes: List of hashes that don't exist
    """
    db = await get_files_db()
    
    # Find all documents with hashes in the provided list
    cursor = db.files.find({"file_hash": {"$in": hashes}}, {"file_hash": 1})
    
    # Extract the hashes that exist
    existing_hashes = []
    async for doc in cursor:
        existing_hashes.append(doc["file_hash"])
    
    # Update last_checked timestamp for existing files
    if existing_hashes:
        now = datetime.utcnow()
        await db.files.update_many(
            {"file_hash": {"$in": existing_hashes}},
            {"$set": {"last_checked": now}}
        )
    
    # Find missing hashes
    missing_hashes = [h for h in hashes if h not in existing_hashes]
    
    return existing_hashes, missing_hashes

async def get_file_by_hash(file_hash: str) -> Optional[Dict[str, Any]]:
    """
    Get a file by its hash, including content from GridFS if applicable.
    """
    db = await get_files_db()
    file = await db.files.find_one({"file_hash": file_hash})
    if file:
        file["_id"] = str(file["_id"])
        
        # If file is stored in GridFS, retrieve the content
        if file.get("storage_type") == "gridfs":
            try:
                bucket = await get_gridfs_bucket()
                grid_id = file.get("gridfs_id")
                
                if grid_id:
                    # Download content from GridFS
                    grid_out = await bucket.open_download_stream(grid_id)
                    content_bytes = await grid_out.read()
                    # Encode back to base64 for consistency
                    file["content"] = base64.b64encode(content_bytes).decode('utf-8')
                    file["gridfs_id"] = str(file["gridfs_id"])
            except Exception as e:
                # Log error but return metadata
                file["content"] = ""  # Empty content on error
                file["error"] = f"Failed to retrieve content from GridFS: {str(e)}"
    
    return file 