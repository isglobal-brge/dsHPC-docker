from pydantic import BaseModel, Field, validator
from typing import Dict, Any, Optional, List, Union, Literal

class JobRequest(BaseModel):
    """Model for job request.
    
    Supports both single file (file_hash) and multiple files (file_inputs).
    Exactly one of file_hash or file_inputs must be provided, not both.
    """
    file_hash: Optional[str] = Field(None, description="Hash of the input file (single file - legacy)")
    file_inputs: Optional[Dict[str, Any]] = Field(None, description="Named dict of file hashes (multi-file - new). Supports str (single) or List[str] (array)")
    method_name: str
    parameters: Optional[Dict[str, Any]] = None
    
    @validator('file_inputs')
    def check_file_inputs(cls, v, values):
        # Exactly one of file_hash or file_inputs must be provided (or neither for params-only jobs)
        has_hash = values.get('file_hash') is not None
        has_inputs = v is not None
        
        if has_hash and has_inputs:
            raise ValueError("Provide either file_hash OR file_inputs, not both")
        
        return v

class JobResponse(BaseModel):
    """Model for job response."""
    job_hash: str
    status: str
    old_status: Optional[str] = None
    output: Optional[str] = None
    output_file_hash: Optional[str] = None
    message: Optional[str] = None
    status_detail: Optional[str] = None
    is_resubmitted: Optional[bool] = False
    error_details: Optional[str] = None 