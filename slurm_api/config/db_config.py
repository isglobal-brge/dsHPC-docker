import os
from pymongo import MongoClient

# MongoDB setup
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "outputs")

def get_db_client():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return db

db = get_db_client()
jobs_collection = db["jobs"] 