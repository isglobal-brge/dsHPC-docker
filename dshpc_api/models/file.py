from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class FileUpload(BaseModel):
    """Model for file upload request."""
    file_hash: str
    content: str  # Base64 encoded content
    filename: Optional[str] = None
    content_type: Optional[str] = "application/octet-stream"  # MIME type of the content
    metadata: Optional[dict] = None


class FileResponse(BaseModel):
    """Model for file response."""
    file_hash: str
    filename: Optional[str] = None
    content_type: Optional[str] = "application/octet-stream"
    upload_date: datetime
    last_checked: datetime
    metadata: Optional[dict] = None


class HashCheckRequest(BaseModel):
    """Model for checking multiple hashes."""
    hashes: List[str]


class HashCheckResponse(BaseModel):
    """Model for hash check response."""
    existing_hashes: List[str]
    missing_hashes: List[str] 