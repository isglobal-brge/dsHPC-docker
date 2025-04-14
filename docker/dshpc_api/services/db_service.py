from pymongo import MongoClient
from typing import List, Dict, Any, Tuple
import asyncio
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient

from dshpc_api.config.settings import get_settings

# Cached database connections
_jobs_db = None
_files_db = None

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

async def get_job_by_id(job_id: str) -> Dict[str, Any]:
    """
    Get a job from the jobs database by ID.
    """
    db = await get_jobs_db()
    job = await db.jobs.find_one({"job_id": job_id})
    if job:
        job["_id"] = str(job["_id"])  # Convert ObjectId to string
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
    
    # Upload to database
    db = await get_files_db()
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

async def get_file_by_hash(file_hash: str) -> Dict[str, Any]:
    """
    Get a file by its hash.
    """
    db = await get_files_db()
    file = await db.files.find_one({"file_hash": file_hash})
    if file:
        file["_id"] = str(file["_id"])
    return file 