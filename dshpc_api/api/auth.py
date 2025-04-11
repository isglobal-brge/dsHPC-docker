from fastapi import Depends, HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
from typing import Optional

from dshpc_api.config.settings import get_settings

# Define the API key header
API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

async def get_api_key(api_key_header: Optional[str] = Security(API_KEY_HEADER)):
    """
    Validate the API key from the X-API-Key header.
    
    Args:
        api_key_header: The API key from the request header
        
    Returns:
        The validated API key
        
    Raises:
        HTTPException: If the API key is invalid or missing
    """
    settings = get_settings()
    
    if not api_key_header:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key missing",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    if api_key_header != settings.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    return api_key_header 