"""
Meta-job models for chaining multiple processing steps.
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


class MetaJobStatus(str, Enum):
    """Status values for meta-jobs."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class MethodChainStep(BaseModel):
    """A single step in a method processing chain."""
    method_name: str
    parameters: Optional[Dict[str, Any]] = Field(default_factory=dict)


class MetaJobRequest(BaseModel):
    """Request model for submitting a meta-job."""
    initial_file_hash: str = Field(..., description="Hash of the initial input file")
    method_chain: List[MethodChainStep] = Field(..., min_items=1, description="Chain of methods to execute sequentially")
    
    class Config:
        schema_extra = {
            "example": {
                "initial_file_hash": "abc123...",
                "method_chain": [
                    {
                        "method_name": "lung_mask",
                        "parameters": {}
                    },
                    {
                        "method_name": "extract_radiomics",
                        "parameters": {"feature_set": "all"}
                    }
                ]
            }
        }


class MetaJobStepInfo(BaseModel):
    """Information about a single step in a meta-job chain."""
    step: int
    method_name: str
    function_hash: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)
    input_file_hash: Optional[str] = None  # Can be None initially, filled during processing
    output_hash: Optional[str] = None  # Hash of the output (for chaining)
    job_id: Optional[str] = None
    status: Optional[str] = None
    cached: bool = False  # Whether this step used cached results


class CurrentStepInfo(BaseModel):
    """Information about the currently processing step in a meta-job."""
    step_number: int  # 1-based for user display
    method_name: str
    parameters: Dict[str, Any]
    job_id: str
    job_status: str
    status_description: str
    is_resubmitted: bool = False


class MetaJobResponse(BaseModel):
    """Response model for meta-job submission."""
    meta_job_id: str
    status: MetaJobStatus
    estimated_steps: int
    message: Optional[str] = None


class MetaJobInfo(BaseModel):
    """Full status information for a meta-job."""
    meta_job_id: str
    initial_file_hash: str
    chain: List[MetaJobStepInfo]
    status: MetaJobStatus
    current_step: Optional[int] = None
    current_step_info: Optional[CurrentStepInfo] = None  # Info about current step
    final_job_id: Optional[str] = None  # ID of the final job in the chain
    final_output: Optional[Any] = None  # Output from final job (when completed)
    error: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
