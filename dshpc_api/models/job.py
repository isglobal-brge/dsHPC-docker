from pydantic import BaseModel
from typing import Dict, Any, Optional

class JobRequest(BaseModel):
    """Model for job request."""
    file_hash: str
    method_name: str
    parameters: Optional[Dict[str, Any]] = None

class JobResponse(BaseModel):
    """Model for job response."""
    job_id: str
    new_status: str
    old_status: Optional[str] = None
    output: Optional[str] = None
    message: Optional[str] = None 