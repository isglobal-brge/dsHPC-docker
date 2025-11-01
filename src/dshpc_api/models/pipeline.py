"""
Data models for Pipeline (DAG orchestration of meta-jobs)
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum


class PipelineNodeStatus(str, Enum):
    """Status of a pipeline node"""
    WAITING = "waiting"          # Waiting for dependencies
    READY = "ready"              # Dependencies met, ready to submit
    RUNNING = "running"          # Meta-job submitted and running
    COMPLETED = "completed"      # Meta-job completed successfully
    FAILED = "failed"            # Meta-job failed
    CANCELLED = "cancelled"      # Cancelled by user or dependency failure


class PipelineStatus(str, Enum):
    """Overall pipeline status"""
    PENDING = "pending"          # Just created
    RUNNING = "running"          # At least one node is running
    COMPLETED = "completed"      # All nodes completed
    FAILED = "failed"            # At least one critical node failed
    CANCELLED = "cancelled"      # User cancelled


class PipelineNodeSubmit(BaseModel):
    """Node definition for pipeline submission"""
    chain: List[Dict[str, Any]]  # Same as meta-job chain
    dependencies: List[str] = Field(default_factory=list)  # List of node_ids
    input_file_hash: Optional[str] = None  # For root nodes with file input


class PipelineSubmission(BaseModel):
    """Pipeline submission request"""
    name: Optional[str] = None
    nodes: Dict[str, PipelineNodeSubmit]  # node_id -> node definition


class PipelineNodeInfo(BaseModel):
    """Information about a pipeline node"""
    node_id: str
    status: PipelineNodeStatus
    meta_job_id: Optional[str] = None
    dependencies: List[str]
    depth_level: int
    submitted_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    # Current progress if running
    current_step: Optional[int] = None
    total_steps: Optional[int] = None


class PipelineInfo(BaseModel):
    """Complete pipeline information"""
    pipeline_id: str
    name: Optional[str]
    status: PipelineStatus
    nodes: List[PipelineNodeInfo]
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    # Progress stats
    total_nodes: int
    completed_nodes: int
    failed_nodes: int
    running_nodes: int
    waiting_nodes: int
    # Execution details
    final_node_id: Optional[str] = None  # The final node in DAG
    final_output: Optional[str] = None   # Output from final node
    error: Optional[str] = None


class PipelineResponse(BaseModel):
    """Response after submitting pipeline"""
    pipeline_id: str
    status: PipelineStatus
    total_nodes: int
    message: str

