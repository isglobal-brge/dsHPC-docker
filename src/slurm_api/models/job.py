from pydantic import BaseModel, Field, validator
from typing import Dict, Any, Optional
from enum import Enum

class JobStatus(str, Enum):
    PENDING = "PD"         # Job is queued and waiting for resources
    RUNNING = "R"          # Job is running
    COMPLETED = "CD"       # Job completed successfully
    FAILED = "F"           # Job failed
    CANCELLED = "CA"       # Job was cancelled by user or system
    TIMEOUT = "TO"         # Job reached time limit
    NODE_FAIL = "NF"       # Job failed due to node failure
    OUT_OF_MEMORY = "OOM"  # Job experienced out of memory error
    SUSPENDED = "S"        # Job has been suspended
    STOPPED = "ST"         # Job has been stopped
    BOOT_FAIL = "BF"       # Job failed during node boot
    DEADLINE = "DL"        # Job terminated on deadline
    COMPLETING = "CG"      # Job is in the process of completing
    CONFIGURING = "CF"     # Job is in the process of configuring
    PREEMPTED = "PR"       # Job was preempted by another job

class JobSubmission(BaseModel):
    script: Optional[str] = None
    name: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    file_hash: Optional[str] = None  # Single file (legacy)
    file_inputs: Optional[Dict[str, str]] = None  # Multi-file (new)
    function_hash: str
    
    @validator('file_inputs')
    def validate_file_input(cls, v, values):
        """Validate that exactly one of file_hash or file_inputs is provided."""
        has_hash = values.get('file_hash') is not None
        has_inputs = v is not None
        
        if not has_hash and not has_inputs:
            raise ValueError("Must provide either file_hash or file_inputs")
        if has_hash and has_inputs:
            raise ValueError("Provide either file_hash OR file_inputs, not both")
        return v
    
    @validator('script')
    def validate_script_or_function_hash(cls, v, values):
        """Validate that either script or function_hash is provided."""
        if v is None and ('function_hash' not in values or values['function_hash'] is None):
            raise ValueError("Either script or function_hash must be provided")
        return v
