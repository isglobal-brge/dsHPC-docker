import os
from functools import lru_cache
from pydantic import BaseSettings

class Settings(BaseSettings):
    # Slurm API settings
    SLURM_API_URL: str = os.getenv("SLURM_API_URL", "http://dshpc-slurm:8000")
    
    # Jobs database settings
    MONGO_JOBS_URI: str = os.getenv("MONGO_JOBS_URI", "mongodb://dshpc-jobs:27017/")
    MONGO_JOBS_DB: str = os.getenv("MONGO_JOBS_DB", "dshpc-jobs")
    
    # Files database settings
    MONGO_FILES_URI: str = os.getenv("MONGO_FILES_URI", "mongodb://dshpc-files:27017/")
    MONGO_FILES_DB: str = os.getenv("MONGO_FILES_DB", "dshpc-files")
    
    # Methods database settings
    MONGO_METHODS_URI: str = os.getenv("MONGO_METHODS_URI", "mongodb://dshpc-methods:27017/")
    MONGO_METHODS_DB: str = os.getenv("MONGO_METHODS_DB", "dshpc-methods")
    
    # API settings
    API_PORT: int = int(os.getenv("API_PORT", 8001))
    
    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    """
    Get cached settings to avoid reloading from environment each time.
    """
    return Settings() 