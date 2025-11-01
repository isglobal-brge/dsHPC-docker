"""
API endpoints for pipeline (DAG) orchestration
"""
from fastapi import APIRouter, HTTPException, Security, status
from typing import Dict, Any

from dshpc_api.api.auth import get_api_key
from dshpc_api.models.pipeline import PipelineSubmission, PipelineResponse, PipelineInfo
from dshpc_api.services.pipeline_service import create_pipeline, get_pipeline_status

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@router.post("/submit", response_model=PipelineResponse, status_code=status.HTTP_201_CREATED)
async def submit_pipeline(pipeline: PipelineSubmission, api_key: str = Security(get_api_key)):
    """
    Submit a new pipeline (DAG of meta-jobs).
    
    A pipeline allows orchestrating multiple meta-job chains with dependencies.
    Nodes execute in parallel when their dependencies are met.
    
    Example pipeline:
    ```json
    {
      "name": "My Pipeline",
      "nodes": {
        "node_1": {
          "chain": [{"method_name": "concat", "parameters": {"a": "Hello", "b": "World"}}],
          "dependencies": [],
          "input_file_hash": "abc123..."
        },
        "node_2": {
          "chain": [{"method_name": "concat", "parameters": {"a": "$ref:node_1", "b": "Python"}}],
          "dependencies": ["node_1"]
        }
      }
    }
    ```
    
    References format:
    - `"$ref:node_1"` - Use complete output from node_1
    - `"$ref:node_1.data.text"` - Extract specific field from node_1 output
    """
    try:
        pipeline_dict = pipeline.dict()
        pipeline_id, message = await create_pipeline(pipeline_dict)
        
        return PipelineResponse(
            pipeline_id=pipeline_id,
            status="pending",
            total_nodes=len(pipeline_dict["nodes"]),
            message=message
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating pipeline: {str(e)}"
        )


@router.get("/{pipeline_id}", response_model=Dict[str, Any])
async def get_pipeline(pipeline_id: str, api_key: str = Security(get_api_key)):
    """
    Get pipeline status and detailed information about all nodes.
    
    Returns a tree structure showing:
    - Overall pipeline status
    - Progress percentage
    - Status of each node with depth level
    - Dependencies between nodes
    - Current step information for running nodes
    """
    pipeline_info = await get_pipeline_status(pipeline_id)
    
    if not pipeline_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline {pipeline_id} not found"
        )
    
    return pipeline_info


@router.delete("/{pipeline_id}")
async def cancel_pipeline(pipeline_id: str, api_key: str = Security(get_api_key)):
    """
    Cancel a running pipeline.
    
    This will mark the pipeline as cancelled and stop submitting new nodes.
    Already running nodes will continue until completion.
    """
    from dshpc_api.services.pipeline_service import get_jobs_db
    from dshpc_api.models.pipeline import PipelineStatus
    from datetime import datetime
    
    db = await get_jobs_db()
    
    result = await db.pipelines.update_one(
        {"pipeline_id": pipeline_id},
        {"$set": {
            "status": PipelineStatus.CANCELLED.value,
            "completed_at": datetime.utcnow()
        }}
    )
    
    if result.matched_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline {pipeline_id} not found"
        )
    
    return {"message": f"Pipeline {pipeline_id} cancelled"}

