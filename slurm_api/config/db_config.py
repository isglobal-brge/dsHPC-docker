import os
from pymongo import MongoClient

# Jobs MongoDB setup
MONGO_JOBS_URI = os.getenv("MONGO_JOBS_URI", "mongodb://dshpc-jobs:27017/")
MONGO_JOBS_DB = os.getenv("MONGO_JOBS_DB", "dshpc-jobs")

# Files MongoDB setup
MONGO_FILES_URI = os.getenv("MONGO_FILES_URI", "mongodb://dshpc-files:27017/")
MONGO_FILES_DB = os.getenv("MONGO_FILES_DB", "dshpc-files")

# Methods MongoDB setup
MONGO_METHODS_URI = os.getenv("MONGO_METHODS_URI", "mongodb://dshpc-methods:27017/")
MONGO_METHODS_DB = os.getenv("MONGO_METHODS_DB", "dshpc-methods")

def get_jobs_db_client():
    """Get client for the jobs database."""
    client = MongoClient(MONGO_JOBS_URI)
    db = client[MONGO_JOBS_DB]
    return db

def get_files_db_client():
    """Get client for the files database."""
    client = MongoClient(MONGO_FILES_URI)
    db = client[MONGO_FILES_DB]
    return db

def get_methods_db_client():
    """Get client for the methods database."""
    client = MongoClient(MONGO_METHODS_URI)
    db = client[MONGO_METHODS_DB]
    return db

# Initialize database connections
jobs_db = get_jobs_db_client()
files_db = get_files_db_client()
methods_db = get_methods_db_client()

# Collections
jobs_collection = jobs_db["jobs"]
files_collection = files_db["files"]
methods_collection = methods_db["methods"] 