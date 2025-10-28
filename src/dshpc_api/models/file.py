from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime


class FileUpload(BaseModel):
    """Model for file upload request."""
    file_hash: str
    content: str  # Base64 encoded content
    filename: Optional[str] = None
    content_type: Optional[str] = "application/octet-stream"  # MIME type of the content
    metadata: Optional[Dict[str, Any]] = None


class FileResponse(BaseModel):
    """Model for file response."""
    file_hash: str
    filename: Optional[str] = None
    content_type: Optional[str] = "application/octet-stream"
    upload_date: Optional[datetime] = None
    last_checked: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None


class HashCheckRequest(BaseModel):
    """Model for checking multiple hashes."""
    hashes: List[str]


class HashCheckResponse(BaseModel):
    """Model for hash check response."""
    existing_hashes: List[str]
    missing_hashes: List[str]


# Chunked upload models

class ChunkedUploadInitRequest(BaseModel):
    """Model for initiating a chunked upload session."""
    file_hash: str = Field(..., description="SHA-256 hash of the complete file")
    filename: Optional[str] = Field(None, description="Original filename")
    content_type: Optional[str] = Field("application/octet-stream", description="MIME type of the content")
    total_size: int = Field(..., description="Total size of the file in bytes", gt=0)
    chunk_size: int = Field(..., description="Size of each chunk in bytes", gt=0)
    metadata: Optional[Dict[str, Any]] = Field(None, description="Optional metadata")


class ChunkedUploadInitResponse(BaseModel):
    """Model for chunked upload session initialization response."""
    session_id: str = Field(..., description="Unique session identifier")
    file_hash: str = Field(..., description="File hash associated with this session")
    message: str = Field(..., description="Status message")


class ChunkedUploadChunk(BaseModel):
    """Model for uploading a single chunk."""
    chunk_number: int = Field(..., description="Sequential chunk number (0-based)", ge=0)
    chunk_data: str = Field(..., description="Base64 encoded chunk data")


class ChunkedUploadChunkResponse(BaseModel):
    """Model for chunk upload response."""
    session_id: str
    chunk_number: int
    chunks_received: int
    total_chunks: Optional[int] = None
    message: str


class ChunkedUploadFinalizeRequest(BaseModel):
    """Model for finalizing a chunked upload."""
    total_chunks: int = Field(..., description="Total number of chunks uploaded", gt=0)


class ChunkedUploadSession(BaseModel):
    """Internal model for tracking chunked upload sessions."""
    session_id: str
    file_hash: str
    filename: Optional[str] = None
    content_type: str = "application/octet-stream"
    total_size: int
    chunk_size: int
    chunks_received: List[int] = Field(default_factory=list)
    created_at: datetime
    last_updated: datetime
    status: str = "active"  # active, finalizing, completed, failed, cancelled
    metadata: Optional[Dict[str, Any]] = None 