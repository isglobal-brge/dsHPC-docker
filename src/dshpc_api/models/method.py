from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from datetime import datetime

class MethodParameter(BaseModel):
    """Model for a method parameter"""
    name: str
    description: Optional[str] = None
    type: str
    required: bool = False
    default: Any = None

class MethodResources(BaseModel):
    """Model for method resource requirements"""
    cpus: int = 1  # Number of CPUs per task (default: 1)
    memory_mb: Optional[int] = None  # Memory in MB (None = use Slurm default)
    time_limit: Optional[str] = None  # Time limit (e.g., "01:00:00" for 1 hour)
    gpus: Optional[int] = None  # Number of GPUs (optional)

class Method(BaseModel):
    """Model for a method"""
    function_hash: str
    name: str
    description: Optional[str] = None
    command: str
    script_path: Optional[str] = None
    parameters: Optional[List[MethodParameter]] = None
    resources: Optional[MethodResources] = None  # Resource requirements
    version: Optional[str] = None
    created_at: Optional[datetime] = None
    active: bool = True

class MethodsResponse(BaseModel):
    """Model for a list of methods response"""
    methods: List[Method]
    total_count: int 