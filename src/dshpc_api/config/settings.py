import os
from functools import lru_cache
from pydantic import BaseSettings

class Settings(BaseSettings):
    # Slurm API settings
    SLURM_API_URL: str
    
    # Jobs database settings
    MONGO_JOBS_URI: str
    MONGO_JOBS_DB: str
    
    # Files database settings
    MONGO_FILES_URI: str
    MONGO_FILES_DB: str
    
    # Methods database settings
    MONGO_METHODS_URI: str
    MONGO_METHODS_DB: str
    
    # API settings
    API_PORT: int = int(os.getenv("API_PORT", 8001))
    API_KEY: str
    
    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    """
    Get cached settings to avoid reloading from environment each time.
    """
    return Settings() 