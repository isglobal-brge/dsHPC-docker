"""
Background worker for pipeline orchestration
"""
import asyncio
from datetime import datetime
from typing import Dict, Any, List
import copy

from dshpc_api.services.db_service import get_jobs_db
from dshpc_api.config.logging_config import logger
from dshpc_api.models.pipeline import PipelineStatus, PipelineNodeStatus
from dshpc_api.services.pipeline_service import resolve_references
from dshpc_api.services.meta_job_service import get_meta_job_info
from dshpc_api.models.meta_job import MetaJobRequest


async def check_and_submit_ready_nodes(pipeline_id: str, pipeline_doc: Dict[str, Any]) -> bool:
    """
    Check for nodes whose dependencies are met and submit them.
    
    Returns:
        True if any changes were made
    """
    db = await get_jobs_db()
    nodes = pipeline_doc["nodes"]
    changes_made = False
    
    # Build map of completed nodes -> output_hash
    completed_outputs = {}
    for node_id, node_data in nodes.items():
        if node_data["status"] == PipelineNodeStatus.COMPLETED.value and node_data.get("output_hash"):
            completed_outputs[node_id] = node_data["output_hash"]
    
    # Check each waiting node
    for node_id, node_data in nodes.items():
        if node_data["status"] != PipelineNodeStatus.WAITING.value:
            continue
        
        # Check if all dependencies are completed
        dependencies = node_data.get("dependencies", [])
        all_deps_met = all(
            nodes.get(dep_id, {}).get("status") == PipelineNodeStatus.COMPLETED.value
            for dep_id in dependencies
        )
        
        if all_deps_met:
            logger.info(f"Pipeline {pipeline_id}: Node {node_id} dependencies met, submitting...")
            
            try:
                # Resolve chain with references
                chain = copy.deepcopy(node_data["chain"])
                
                # For each step in chain, resolve parameter references
                for step in chain:
                    if "parameters" in step and isinstance(step["parameters"], dict):
                        step["parameters"] = resolve_references(step["parameters"], completed_outputs)
                
                # Determine input file hash
                # If node has dependencies, use output from first dependency
                # If no dependencies, use input_file_hash if provided
                # If no input_file_hash and no dependencies, create empty placeholder
                if dependencies:
                    # Use output from first completed dependency
                    input_hash = completed_outputs[dependencies[0]]
                else:
                    input_hash = node_data.get("input_file_hash")
                    
                    # If no input hash and no dependencies, check if all params are static
                    if not input_hash:
                        # Check if chain uses any references
                        has_refs = any(
                            any(str(v).startswith("$ref:") for v in step.get("parameters", {}).values())
                            for step in chain
                        )
                        
                        if has_refs:
                            raise ValueError(f"Node {node_id} has parameter references but no input source")
                        
                        # Create a dummy input hash for parameter-only methods
                        # Use a special marker hash that indicates "no file input"
                        input_hash = "PARAMS_ONLY_" + node_id
                
                # Submit meta-job
                from dshpc_api.services.meta_job_service import submit_meta_job
                from dshpc_api.models.meta_job import MethodChainStep
                
                # Convert chain to MethodChainStep objects
                method_steps = [MethodChainStep(**step) for step in chain]
                
                request = MetaJobRequest(
                    initial_file_hash=input_hash,
                    method_chain=method_steps
                )
                success, message, response = await submit_meta_job(request)
                
                if not success:
                    raise Exception(f"Failed to submit meta-job: {message}")
                
                meta_job_id = response.meta_job_id
                
                # Update node status
                await db.pipelines.update_one(
                    {"pipeline_id": pipeline_id, f"nodes.{node_id}.status": PipelineNodeStatus.WAITING.value},
                    {"$set": {
                        f"nodes.{node_id}.status": PipelineNodeStatus.RUNNING.value,
                        f"nodes.{node_id}.meta_job_id": meta_job_id,
                        f"nodes.{node_id}.submitted_at": datetime.utcnow()
                    }}
                )
                
                changes_made = True
                logger.info(f"Pipeline {pipeline_id}: Submitted node {node_id} as meta-job {meta_job_id}")
                
            except Exception as e:
                logger.error(f"Pipeline {pipeline_id}: Failed to submit node {node_id}: {e}")
                # Mark node as failed
                await db.pipelines.update_one(
                    {"pipeline_id": pipeline_id},
                    {"$set": {
                        f"nodes.{node_id}.status": PipelineNodeStatus.FAILED.value,
                        f"nodes.{node_id}.error": str(e),
                        f"nodes.{node_id}.completed_at": datetime.utcnow()
                    }}
                )
                changes_made = True
    
    return changes_made


async def update_running_nodes(pipeline_id: str, pipeline_doc: Dict[str, Any]) -> bool:
    """
    Update status of running nodes by checking their meta-jobs.
    
    Returns:
        True if any changes were made
    """
    db = await get_jobs_db()
    nodes = pipeline_doc["nodes"]
    changes_made = False
    
    for node_id, node_data in nodes.items():
        if node_data["status"] != PipelineNodeStatus.RUNNING.value:
            continue
        
        meta_job_id = node_data.get("meta_job_id")
        if not meta_job_id:
            continue
        
        # Get meta-job status
        meta_job_info = await get_meta_job_info(meta_job_id)
        
        if not meta_job_info:
            logger.warning(f"Pipeline {pipeline_id}: Meta-job {meta_job_id} not found for node {node_id}")
            continue
        
        # Check if meta-job completed
        if meta_job_info.status == "completed":
            # Extract final output hash (the hash of the final job's output)
            # This is the job_id of the final job, which contains the output
            final_job_id = meta_job_info.final_job_id
            
            # Query the final job to get its output_file_hash
            jobs_db = await get_jobs_db()
            final_job = await jobs_db.jobs.find_one({"job_id": final_job_id})
            
            if final_job and final_job.get("output_file_hash"):
                output_hash = final_job["output_file_hash"]
            else:
                # Fallback: use final_job_id as hash
                output_hash = final_job_id
            
            await db.pipelines.update_one(
                {"pipeline_id": pipeline_id},
                {"$set": {
                    f"nodes.{node_id}.status": PipelineNodeStatus.COMPLETED.value,
                    f"nodes.{node_id}.output_hash": output_hash,
                    f"nodes.{node_id}.completed_at": datetime.utcnow()
                }}
            )
            changes_made = True
            logger.info(f"Pipeline {pipeline_id}: Node {node_id} completed with output_hash {output_hash[:16]}...")
            
        elif meta_job_info.status == "failed":
            error_msg = meta_job_info.error or "Unknown error"
            
            await db.pipelines.update_one(
                {"pipeline_id": pipeline_id},
                {"$set": {
                    f"nodes.{node_id}.status": PipelineNodeStatus.FAILED.value,
                    f"nodes.{node_id}.error": error_msg,
                    f"nodes.{node_id}.completed_at": datetime.utcnow()
                }}
            )
            changes_made = True
            logger.warning(f"Pipeline {pipeline_id}: Node {node_id} failed: {error_msg}")
    
    return changes_made


async def update_pipeline_overall_status(pipeline_id: str) -> None:
    """
    Update overall pipeline status based on node statuses.
    """
    db = await get_jobs_db()
    pipeline = await db.pipelines.find_one({"pipeline_id": pipeline_id})
    
    if not pipeline:
        return
    
    nodes = pipeline["nodes"]
    statuses = [n["status"] for n in nodes.values()]
    
    # Determine overall status
    if all(s == PipelineNodeStatus.COMPLETED.value for s in statuses):
        new_status = PipelineStatus.COMPLETED.value
        completed_at = datetime.utcnow()
        
        await db.pipelines.update_one(
            {"pipeline_id": pipeline_id},
            {"$set": {
                "status": new_status,
                "completed_at": completed_at
            }}
        )
        logger.info(f"Pipeline {pipeline_id} completed successfully")
        
    elif any(s == PipelineNodeStatus.FAILED.value for s in statuses):
        # At least one node failed
        if pipeline["status"] != PipelineStatus.FAILED.value:
            await db.pipelines.update_one(
                {"pipeline_id": pipeline_id},
                {"$set": {
                    "status": PipelineStatus.FAILED.value,
                    "completed_at": datetime.utcnow()
                }}
            )
            logger.warning(f"Pipeline {pipeline_id} marked as failed")
    
    elif any(s == PipelineNodeStatus.RUNNING.value for s in statuses):
        # At least one running
        if pipeline["status"] == PipelineStatus.PENDING.value:
            await db.pipelines.update_one(
                {"pipeline_id": pipeline_id},
                {"$set": {
                    "status": PipelineStatus.RUNNING.value,
                    "started_at": datetime.utcnow()
                }}
            )


async def process_pipeline_once(pipeline_id: str) -> None:
    """
    Process a single pipeline iteration.
    """
    db = await get_jobs_db()
    pipeline = await db.pipelines.find_one({"pipeline_id": pipeline_id})
    
    if not pipeline:
        return
    
    # Skip if already completed/failed
    if pipeline["status"] in [PipelineStatus.COMPLETED.value, PipelineStatus.FAILED.value]:
        return
    
    # Update running nodes
    await update_running_nodes(pipeline_id, pipeline)
    
    # Reload pipeline after updates
    pipeline = await db.pipelines.find_one({"pipeline_id": pipeline_id})
    
    # Submit ready nodes
    await check_and_submit_ready_nodes(pipeline_id, pipeline)
    
    # Update overall status
    await update_pipeline_overall_status(pipeline_id)


async def pipeline_orchestrator():
    """
    Background task to orchestrate pipelines.
    Polls active pipelines and submits nodes when dependencies are ready.
    """
    logger.info("Pipeline orchestrator started")
    
    while True:
        try:
            db = await get_jobs_db()
            
            # Find all active pipelines
            active_pipelines = db.pipelines.find({
                "status": {"$in": [PipelineStatus.PENDING.value, PipelineStatus.RUNNING.value]}
            })
            
            async for pipeline in active_pipelines:
                try:
                    await process_pipeline_once(pipeline["pipeline_id"])
                except Exception as e:
                    logger.error(f"Error processing pipeline {pipeline['pipeline_id']}: {e}")
            
        except Exception as e:
            logger.error(f"Error in pipeline orchestrator loop: {e}")
        
        # Wait before next check
        await asyncio.sleep(5)  # Check every 5 seconds

