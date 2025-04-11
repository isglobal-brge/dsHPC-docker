from pymongo import MongoClient
from typing import List, Dict, Any
import asyncio
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

async def add_file(file_data: Dict[str, Any]) -> str:
    """
    Add a file to the files database.
    """
    db = await get_files_db()
    result = await db.files.insert_one(file_data)
    return str(result.inserted_id) 