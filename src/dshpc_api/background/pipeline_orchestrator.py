"""
Background worker for pipeline orchestration
"""
import asyncio
from datetime import datetime
from typing import Dict, Any, List
import copy
import json
import hashlib

from dshpc_api.services.db_service import get_jobs_db, get_files_db
from dshpc_api.config.logging_config import logger
from dshpc_api.models.pipeline import PipelineStatus, PipelineNodeStatus
from dshpc_api.services.pipeline_service import resolve_references
from dshpc_api.services.meta_job_service import get_meta_job_info
from dshpc_api.models.meta_job import MetaJobRequest


async def extract_and_store_path(source_hash: str, path: str, pipeline_node: str, param_name: str) -> str:
    """
    Extract a value from a JSON file using a path and store it as a new file.
    
    Args:
        source_hash: Hash of the source file containing JSON
        path: Slash-separated path to extract (e.g., "data/text")
        pipeline_node: Node ID (for logging)
        param_name: Parameter name (for logging)
        
    Returns:
        Hash of the newly created file with extracted value
    """
    files_db = await get_files_db()
    
    # Get source file
    source_doc = await files_db.files.find_one({"file_hash": source_hash})
    if not source_doc:
        raise ValueError(f"Source file {source_hash} not found for path extraction")
    
    # Decode content
    import base64
    content_bytes = base64.b64decode(source_doc["content"])
    content_str = content_bytes.decode('utf-8')
    
    # Parse JSON
    try:
        data = json.loads(content_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON from {source_hash}: {e}")
    
    # Navigate path (slash-separated)
    path_parts = path.split("/")
    result = data
    for part in path_parts:
        if isinstance(result, dict) and part in result:
            result = result[part]
        else:
            raise ValueError(f"Path '{path}' not found in source file {source_hash}")
    
    # Create new JSON with extracted value
    # Wrap in a simple structure that scripts can easily read
    extracted_data = {
        "value": result,
        "extracted_from": source_hash,
        "path": path
    }
    
    extracted_json = json.dumps(extracted_data, indent=2)
    extracted_bytes = extracted_json.encode('utf-8')
    extracted_b64 = base64.b64encode(extracted_bytes).decode('utf-8')
    
    # Calculate hash
    extracted_hash = hashlib.sha256(extracted_bytes).hexdigest()
    
    # Store in database
    await files_db.files.update_one(
        {"file_hash": extracted_hash},
        {"$set": {
            "file_hash": extracted_hash,
            "content": extracted_b64,
            "filename": f"extracted_{pipeline_node}_{param_name}.json",
            "content_type": "application/json",
            "storage_type": "inline",
            "file_size": len(extracted_bytes),
            "upload_date": datetime.utcnow(),
            "last_checked": datetime.utcnow(),
            "metadata": {
                "source": "path_extraction",
                "source_hash": source_hash,
                "path": path,
                "pipeline_node": pipeline_node,
                "parameter": param_name
            }
        }},
        upsert=True
    )
    
    logger.info(f"Extracted path '{path}' from {source_hash[:16]}... → {extracted_hash[:16]}...")
    
    return extracted_hash


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
                
                # Resolve references in parameters - extract values and pass directly
                # All resolution happens server-side, no markers or file_inputs needed
                for step_idx, step in enumerate(chain):
                    if "parameters" in step and isinstance(step["parameters"], dict):
                        resolved_params = {}
                        
                        for key, value in step["parameters"].items():
                            if isinstance(value, str) and value.startswith("$ref:"):
                                ref_full = value[5:]  # Remove "$ref:" prefix
                                
                                # $ref:prev is an internal meta-job reference (previous step in chain)
                                # Don't resolve it here - let the meta-job system handle it
                                if ref_full == "prev" or ref_full.startswith("prev/"):
                                    resolved_params[key] = value  # Keep as-is
                                    continue
                                
                                # Check if it's a path reference: node_1/data/text
                                if "/" in ref_full:
                                    # Extract node_id (before first slash) and path (after)
                                    ref_node = ref_full.split("/")[0]
                                    ref_path = ref_full[len(ref_node)+1:]  # Everything after node_id/
                                else:
                                    ref_node = ref_full
                                    ref_path = None
                                
                                if ref_node in completed_outputs:
                                    output_hash = completed_outputs[ref_node]
                                    
                                    # Extract value from output and pass directly as parameter
                                    # NO markers - server resolves everything
                                    files_db = await get_files_db()
                                    output_doc = await files_db.files.find_one({"file_hash": output_hash})
                                    if not output_doc:
                                        raise ValueError(f"Output from {ref_node} not found: {output_hash}")
                                    
                                    # Decode content
                                    import base64
                                    content_bytes = base64.b64decode(output_doc["content"])
                                    content_str = content_bytes.decode('utf-8')
                                    
                                    # Parse JSON
                                    try:
                                        output_data = json.loads(content_str)
                                    except json.JSONDecodeError as e:
                                        raise ValueError(f"Failed to parse output from {ref_node}: {e}")
                                    
                                    # If path extraction needed, navigate to the value
                                    if ref_path:
                                        current = output_data
                                        parts = ref_path.split("/")
                                        for part in parts:
                                            if isinstance(current, dict) and part in current:
                                                current = current[part]
                                            else:
                                                raise ValueError(f"Path {ref_path} not found in {ref_node} output")
                                        # Pass extracted value directly as parameter
                                        resolved_params[key] = current
                                    else:
                                        # Pass full output directly as parameter
                                        resolved_params[key] = output_data
                                else:
                                    raise ValueError(f"Reference to incomplete node: {ref_node}")
                            else:
                                resolved_params[key] = value
                        
                        step["parameters"] = resolved_params
                
                # Determine input file hash (only for nodes with explicit file inputs)
                input_hash = None
                file_inputs = None  # No longer used - all refs are resolved to parameter values
                
                if dependencies:
                    # Use output from first completed dependency
                    input_hash = completed_outputs[dependencies[0]]
                else:
                    input_hash = node_data.get("input_file_hash")
                    
                    # If no input hash and no dependencies, use PARAMS_ONLY marker
                    # All $ref: have been resolved to actual values in parameters
                    if not input_hash:
                        input_hash = "PARAMS_ONLY_" + node_id
                
                # Submit meta-job
                from dshpc_api.services.meta_job_service import submit_meta_job
                from dshpc_api.models.meta_job import MethodChainStep
                
                # Convert chain to MethodChainStep objects
                method_steps = [MethodChainStep(**step) for step in chain]
                
                # All references are now resolved in parameters, just use simple file_hash
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
            # Trigger immediate pipeline check for dependent nodes
            trigger_pipeline_check()
            
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
            # Trigger immediate pipeline check to propagate failure
            trigger_pipeline_check()
    
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


# Global event for immediate pipeline processing trigger
_pipeline_check_event = asyncio.Event()


def trigger_pipeline_check():
    """
    Trigger an immediate check of all pipelines.
    Call this when a meta-job completes to speed up pipeline execution.
    """
    _pipeline_check_event.set()


async def pipeline_orchestrator():
    """
    Background task to orchestrate pipelines.
    Polls active pipelines and submits nodes when dependencies are ready.
    Also responds immediately to completion events from meta-jobs.
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
        
        # Wait for either: event trigger (immediate) or timeout (2 seconds)
        try:
            await asyncio.wait_for(_pipeline_check_event.wait(), timeout=2.0)
            _pipeline_check_event.clear()  # Reset for next trigger
            logger.debug("Pipeline check triggered by meta-job completion")
        except asyncio.TimeoutError:
            # Timeout is normal - just means no trigger in 2 seconds
            pass

