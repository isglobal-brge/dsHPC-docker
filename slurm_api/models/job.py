from pydantic import BaseModel
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
    script: str
    name: str | None = None
    parameters: Dict[str, Any] | None = None 