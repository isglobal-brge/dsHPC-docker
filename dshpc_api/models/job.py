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

class JobConfig(BaseModel):
    """Model for a single job configuration within a multi-job request."""
    file_hash: str
    method_name: str
    parameters: Optional[Dict[str, Any]] = None

class MultiJobSimulationRequest(BaseModel):
    """Model for multiple job simulation request."""
    jobs: List[JobConfig]

class MultiJobResult(BaseModel):
    """Model for a single job result within a multi-job response."""
    job_id: Optional[str] = None
    new_status: Optional[str] = None
    old_status: Optional[str] = None
    output: Optional[str] = None
    message: Optional[str] = None
    # Include the input configuration for easy identification
    file_hash: str
    method_name: str
    function_hash: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)

class MultiJobSimulationResponse(BaseModel):
    """Model for multiple job simulation response."""
    results: List[MultiJobResult]
    total_jobs: int
    successful_submissions: int
    failed_submissions: int
    completed_jobs: int
    in_progress_jobs: int
    resubmitted_jobs: int 