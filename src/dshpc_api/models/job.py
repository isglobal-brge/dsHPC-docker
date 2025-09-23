from pydantic import BaseModel
from typing import Dict, Any, Optional, List, Union, Literal

class JobRequest(BaseModel):
    """Model for job request."""
    file_hash: str
    method_name: str
    parameters: Optional[Dict[str, Any]] = None

class JobResponse(BaseModel):
    """Model for job response."""
    job_id: str
    status: str
    old_status: Optional[str] = None
    output: Optional[str] = None
    message: Optional[str] = None
    status_detail: Optional[str] = None
    is_resubmitted: Optional[bool] = False
    error_details: Optional[str] = None 