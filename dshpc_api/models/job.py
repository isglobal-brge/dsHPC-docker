from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List

class JobSimulationRequest(BaseModel):
    """Model for job simulation request."""
    file_hash: str
    method_name: str
    parameters: Optional[Dict[str, Any]] = None

class JobSimulationResponse(BaseModel):
    """Model for job simulation response."""
    job_id: str
    new_status: str
    old_status: Optional[str] = None
    output: Optional[str] = None
    message: Optional[str] = None 