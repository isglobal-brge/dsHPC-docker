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

class Method(BaseModel):
    """Model for a method"""
    function_hash: str
    name: str
    description: Optional[str] = None
    command: str
    script_path: Optional[str] = None
    parameters: Optional[List[MethodParameter]] = None
    version: Optional[str] = None
    created_at: Optional[datetime] = None
    active: bool = True

class MethodsResponse(BaseModel):
    """Model for a list of methods response"""
    methods: List[Method]
    total_count: int 