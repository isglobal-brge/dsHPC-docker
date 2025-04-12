import aiohttp
import asyncio
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime

from dshpc_api.config.settings import get_settings
from motor.motor_asyncio import AsyncIOMotorClient

# Cached database connection
_methods_db = None

async def get_methods_db():
    """
    Get a connection to the methods database.
    """
    global _methods_db
    if _methods_db is None:
        settings = get_settings()
        client = AsyncIOMotorClient(settings.MONGO_METHODS_URI)
        _methods_db = client[settings.MONGO_METHODS_DB]
    return _methods_db

async def get_available_methods() -> Tuple[List[Dict[str, Any]], int]:
    """
    Get a list of all available (active) methods.
    
    Returns:
        Tuple containing:
        - List of method documents
        - Total count of methods
    """
    settings = get_settings()
    methods = []
    
    try:
        # Try to get methods through the Slurm API first
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{settings.SLURM_API_URL}/methods?active_only=true") as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, list):
                        # If data is a list, use it directly
                        methods = data
                    elif isinstance(data, dict) and "methods" in data:
                        # If data is a dict with a "methods" key, extract the methods
                        methods = data["methods"]
                    
                    # Ensure only active methods are included and convert ObjectId to string if present
                    active_methods = []
                    total_methods = len(methods)
                    for method in methods:
                        if method.get("active", False):  # Check for active field
                            if "_id" in method:
                                method["_id"] = str(method["_id"])
                            active_methods.append(method)
                    
                    print(f"API methods: Found {total_methods} total methods, {len(active_methods)} active methods")
                    return active_methods, len(active_methods)
    except Exception as e:
        print(f"Error getting methods from Slurm API: {e}")
    
    # Fallback to direct database access if the API endpoint fails or doesn't exist
    try:
        db = await get_methods_db()
        
        # First count all methods to compare
        total_count = await db.methods.count_documents({})
        
        # Then get only active methods
        cursor = db.methods.find({"active": True})
        
        methods = []
        async for method in cursor:
            # Convert ObjectId to string
            method["_id"] = str(method["_id"])
            methods.append(method)
        
        print(f"DB methods: Found {total_count} total methods, {len(methods)} active methods")
        return methods, len(methods)
    except Exception as e:
        print(f"Error getting methods from database: {e}")
        return [], 0

async def check_method_functionality(method_name: str) -> Tuple[bool, str]:
    """
    Check if a method is functional by making a test request to the Slurm API.
    
    Args:
        method_name: The name of the method to check
        
    Returns:
        Tuple containing:
        - is_functional: Boolean indicating if the method is functional
        - message: Success or error message
    """
    settings = get_settings()
    try:
        # Try to get the method through the Slurm API
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{settings.SLURM_API_URL}/methods/by-name/{method_name}?latest=true"
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and "function_hash" in data:
                        # Method exists and has a hash, consider it functional
                        return True, f"Method '{method_name}' is available and functional"
                else:
                    # Method doesn't exist or is not accessible via API
                    return False, f"Method '{method_name}' is not available in the Slurm API"
    except Exception as e:
        return False, f"Error checking method functionality: {str(e)}"
    
    # If we reached here without returning, try the database
    try:
        # Fallback to direct database access
        db = await get_methods_db()
        
        # Query for methods with the given name, sorted by created_at (descending)
        method = await db.methods.find_one(
            {"name": method_name, "active": True},
            sort=[("created_at", -1)]
        )
        
        if method:
            return True, f"Method '{method_name}' is available in the database"
        else:
            return False, f"Method '{method_name}' not found"
    except Exception as e:
        return False, f"Error checking method in database: {str(e)}" 