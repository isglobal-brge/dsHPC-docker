from typing import Dict, List, Any, Optional, Union
from pydantic import BaseModel, Field

class MethodParameter(BaseModel):
    """Model for a method parameter."""
    name: str
    description: str
    type: str = "string"  # string, number, boolean, etc.
    required: bool = True
    default: Optional[Any] = None

class Method(BaseModel):
    """Model for a method definition."""
    name: str
    description: str
    command: str  # The command to run (e.g., "python", "Rscript", etc.)
    script_path: str  # Path to the main script relative to method root
    parameters: List[MethodParameter] = []
    function_hash: str  # Hash of the compressed script bundle
    version: str = "1.0.0"
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    
class MethodExecution(BaseModel):
    """Model for method execution."""
    function_hash: str
    file_hash: Optional[str] = None  # Single file (legacy)
    file_inputs: Optional[Dict[str, Union[str, List[str]]]] = None  # Multi-file - supports str (single) or List[str] (array)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    name: Optional[str] = None 