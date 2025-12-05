import os
from pymongo import MongoClient

# Jobs MongoDB setup
MONGO_JOBS_URI = os.getenv("MONGO_JOBS_URI")
MONGO_JOBS_DB = os.getenv("MONGO_JOBS_DB")

# Files MongoDB setup
MONGO_FILES_URI = os.getenv("MONGO_FILES_URI")
MONGO_FILES_DB = os.getenv("MONGO_FILES_DB")

# Methods MongoDB setup
MONGO_METHODS_URI = os.getenv("MONGO_METHODS_URI")
MONGO_METHODS_DB = os.getenv("MONGO_METHODS_DB")

# Validate required environment variables
required_vars = [
    ("MONGO_JOBS_URI", MONGO_JOBS_URI),
    ("MONGO_JOBS_DB", MONGO_JOBS_DB),
    ("MONGO_FILES_URI", MONGO_FILES_URI),
    ("MONGO_FILES_DB", MONGO_FILES_DB),
    ("MONGO_METHODS_URI", MONGO_METHODS_URI),
    ("MONGO_METHODS_DB", MONGO_METHODS_DB),
]

for var_name, var_value in required_vars:
    if not var_value:
        raise ValueError(f"Required environment variable {var_name} is not set")

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
meta_jobs_collection = jobs_db["meta_jobs"]
pipelines_collection = jobs_db["pipelines"]
files_collection = files_db["files"]
methods_collection = methods_db["methods"]

# Create unique indexes to prevent duplicates
# These are idempotent - calling create_index on an existing index is a no-op
jobs_collection.create_index("job_hash", unique=True, background=True)
meta_jobs_collection.create_index("meta_job_hash", unique=True, background=True)
pipelines_collection.create_index("pipeline_hash", unique=True, background=True) 