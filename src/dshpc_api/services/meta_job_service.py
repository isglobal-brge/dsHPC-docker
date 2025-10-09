"""
Meta-job service for handling chained processing steps.
"""
import uuid
import hashlib
import json
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional
from dshpc_api.models.meta_job import (
    MetaJobStatus, MetaJobRequest, MetaJobResponse, 
    MetaJobInfo, MetaJobStepInfo
)
from dshpc_api.services.db_service import (
    get_meta_jobs_db, get_files_db, get_job_by_id, get_file_by_hash
)
from dshpc_api.services.job_service import (
    submit_job, find_existing_job, get_job_status,
    get_latest_method_hash
)
from dshpc_api.services.method_service import check_method_functionality
from dshpc_api.utils.parameter_utils import sort_parameters
import asyncio
import logging

logger = logging.getLogger(__name__)


async def submit_meta_job(request: MetaJobRequest) -> Tuple[bool, str, Optional[MetaJobResponse]]:
    """
    Submit a meta-job that chains multiple processing steps.
    
    Args:
        request: Meta-job request with initial file and method chain
        
    Returns:
        Tuple of (success, message, response)
    """
    try:
        # Validate initial file exists
        files_db = await get_files_db()
        file_exists = await files_db.files.count_documents({"file_hash": request.initial_file_hash}) > 0
        if not file_exists:
            return False, f"Initial file with hash {request.initial_file_hash} not found", None
        
        # Validate all methods exist and are functional
        for step in request.method_chain:
            is_functional, msg = await check_method_functionality(step.method_name)
            if not is_functional:
                return False, f"Method '{step.method_name}' validation failed: {msg}", None
        
        # Create meta-job record
        meta_job_id = str(uuid.uuid4())
        meta_jobs_db = await get_meta_jobs_db()
        
        # Initialize chain info
        chain_info = []
        for i, step in enumerate(request.method_chain):
            function_hash = await get_latest_method_hash(step.method_name)
            if not function_hash:
                return False, f"Method '{step.method_name}' not found or not active", None
                
            chain_info.append({
                "step": i,
                "method_name": step.method_name,
                "function_hash": function_hash,
                "parameters": sort_parameters(step.parameters),
                "input_file_hash": request.initial_file_hash if i == 0 else None,
                "output_file_hash": None,
                "job_id": None,
                "status": "pending",
                "cached": False
            })
        
        # Create meta-job document
        meta_job_doc = {
            "meta_job_id": meta_job_id,
            "initial_file_hash": request.initial_file_hash,
            "chain": chain_info,
            "status": MetaJobStatus.PENDING,
            "current_step": None,
            "final_output": None,
            "final_output_hash": None,
            "error": None,
            "created_at": datetime.utcnow(),
            "updated_at": None,
            "completed_at": None
        }
        
        await meta_jobs_db.meta_jobs.insert_one(meta_job_doc)
        
        # Start processing asynchronously
        asyncio.create_task(process_meta_job_chain(meta_job_id))
        
        return True, "Meta-job submitted successfully", MetaJobResponse(
            meta_job_id=meta_job_id,
            status=MetaJobStatus.PENDING,
            estimated_steps=len(request.method_chain),
            message="Processing chain asynchronously"
        )
        
    except Exception as e:
        logger.error(f"Error submitting meta-job: {e}")
        return False, f"Error submitting meta-job: {str(e)}", None


async def process_meta_job_chain(meta_job_id: str):
    """
    Process a meta-job chain asynchronously.
    
    Args:
        meta_job_id: ID of the meta-job to process
    """
    meta_jobs_db = await get_meta_jobs_db()
    
    try:
        # Update status to running
        await meta_jobs_db.meta_jobs.update_one(
            {"meta_job_id": meta_job_id},
            {"$set": {
                "status": MetaJobStatus.RUNNING,
                "updated_at": datetime.utcnow()
            }}
        )
        
        # Get meta-job document
        meta_job = await meta_jobs_db.meta_jobs.find_one({"meta_job_id": meta_job_id})
        if not meta_job:
            logger.error(f"Meta-job {meta_job_id} not found")
            return
        
        current_input_hash = meta_job["initial_file_hash"]
        
        # Process each step in the chain
        for i, step in enumerate(meta_job["chain"]):
            # Update current step
            await meta_jobs_db.meta_jobs.update_one(
                {"meta_job_id": meta_job_id},
                {"$set": {
                    "current_step": i,
                    "updated_at": datetime.utcnow()
                }}
            )
            
            # Set input hash for this step
            step["input_file_hash"] = current_input_hash
            
            # Check if this step already exists (deduplication)
            existing_job = await find_existing_job(
                file_hash=current_input_hash,
                function_hash=step["function_hash"],
                parameters=step["parameters"]
            )
            
            if existing_job and existing_job.get("status") == "CD":
                # Job already completed, use its output
                logger.info(f"Meta-job {meta_job_id} step {i}: Using cached result from job {existing_job['job_id']}")
                
                # Get output file hash from existing job
                output_file_hash = existing_job.get("output_file_hash")
                
                if not output_file_hash:
                    # Job doesn't have output_file_hash, need to wait or handle differently
                    # This might happen for older jobs - we should still have the output
                    job_output = existing_job.get("output")
                    if job_output:
                        # Hash the output and create a file
                        output_file_hash = await create_file_from_output(job_output, existing_job["job_id"])
                    else:
                        raise Exception(f"Cached job {existing_job['job_id']} has no output")
                
                step["job_id"] = existing_job["job_id"]
                step["output_file_hash"] = output_file_hash
                step["status"] = "completed"
                step["cached"] = True
                
                # Update chain in database
                await meta_jobs_db.meta_jobs.update_one(
                    {"meta_job_id": meta_job_id},
                    {"$set": {
                        f"chain.{i}": step,
                        "updated_at": datetime.utcnow()
                    }}
                )
                
                # Use output as input for next step
                current_input_hash = output_file_hash
                
            else:
                # Need to submit new job
                logger.info(f"Meta-job {meta_job_id} step {i}: Submitting new job")
                
                success, message, job_data = await submit_job(
                    file_hash=current_input_hash,
                    function_hash=step["function_hash"],
                    parameters=step["parameters"]
                )
                
                if not success:
                    raise Exception(f"Failed to submit job for step {i}: {message}")
                
                job_id = job_data["job_id"]
                step["job_id"] = job_id
                step["status"] = "running"
                
                # Update chain in database
                await meta_jobs_db.meta_jobs.update_one(
                    {"meta_job_id": meta_job_id},
                    {"$set": {
                        f"chain.{i}": step,
                        "updated_at": datetime.utcnow()
                    }}
                )
                
                # Wait for job to complete
                job_result = await wait_for_job_completion(job_id)
                
                if job_result["status"] != "CD":
                    raise Exception(f"Job {job_id} failed with status {job_result['status']}: {job_result.get('error', 'Unknown error')}")
                
                # Get output file hash
                output_file_hash = job_result.get("output_file_hash")
                
                if not output_file_hash:
                    # Job completed but doesn't have output_file_hash yet
                    # This might happen if the output upload is still processing
                    # Wait a bit and check again
                    await asyncio.sleep(2)
                    job_result = await get_job_by_id(job_id)
                    output_file_hash = job_result.get("output_file_hash")
                    
                    if not output_file_hash and job_result.get("output"):
                        # Create file from output
                        output_file_hash = await create_file_from_output(job_result["output"], job_id)
                    elif not output_file_hash:
                        raise Exception(f"Job {job_id} has no output")
                
                step["output_file_hash"] = output_file_hash
                step["status"] = "completed"
                
                # Update chain in database
                await meta_jobs_db.meta_jobs.update_one(
                    {"meta_job_id": meta_job_id},
                    {"$set": {
                        f"chain.{i}": step,
                        "updated_at": datetime.utcnow()
                    }}
                )
                
                # Use output as input for next step
                current_input_hash = output_file_hash
        
        # All steps completed successfully
        # Get final output from the last step's output file
        final_output_hash = meta_job["chain"][-1]["output_file_hash"] or current_input_hash
        
        # Retrieve final output content
        final_file = await get_file_by_hash(final_output_hash)
        final_output = None
        
        if final_file:
            # Decode content if stored as base64
            if final_file.get("content"):
                import base64
                try:
                    final_output = base64.b64decode(final_file["content"]).decode('utf-8')
                except:
                    final_output = final_file["content"]
        
        # Update meta-job as completed
        await meta_jobs_db.meta_jobs.update_one(
            {"meta_job_id": meta_job_id},
            {"$set": {
                "status": MetaJobStatus.COMPLETED,
                "final_output": final_output,
                "final_output_hash": final_output_hash,
                "current_step": None,
                "updated_at": datetime.utcnow(),
                "completed_at": datetime.utcnow()
            }}
        )
        
        logger.info(f"Meta-job {meta_job_id} completed successfully")
        
    except Exception as e:
        logger.error(f"Error processing meta-job {meta_job_id}: {e}")
        
        # Update meta-job as failed
        await meta_jobs_db.meta_jobs.update_one(
            {"meta_job_id": meta_job_id},
            {"$set": {
                "status": MetaJobStatus.FAILED,
                "error": str(e),
                "updated_at": datetime.utcnow()
            }}
        )


async def wait_for_job_completion(job_id: str, timeout: int = 3600, interval: int = 5) -> Dict[str, Any]:
    """
    Wait for a job to complete.
    
    Args:
        job_id: Job ID to wait for
        timeout: Maximum time to wait in seconds
        interval: Polling interval in seconds
        
    Returns:
        Final job data
    """
    start_time = datetime.utcnow()
    
    while True:
        # Check timeout
        if (datetime.utcnow() - start_time).total_seconds() > timeout:
            raise TimeoutError(f"Job {job_id} timed out after {timeout} seconds")
        
        # Get job status
        job_data = await get_job_status(job_id)
        
        if not job_data:
            # Try getting from database directly
            job_data = await get_job_by_id(job_id)
        
        if job_data:
            status = job_data.get("status")
            
            # Check if job is completed or failed
            if status in ["CD", "F", "CA", "TO", "NF", "OOM", "BF", "DL", "ER"]:
                return job_data
        
        # Wait before next check
        await asyncio.sleep(interval)


async def create_file_from_output(output: str, job_id: str) -> str:
    """
    Create a file from job output and return its hash.
    
    Args:
        output: Job output content
        job_id: Source job ID
        
    Returns:
        File hash
    """
    import base64
    
    # Calculate hash
    output_bytes = output.encode('utf-8')
    file_hash = hashlib.sha256(output_bytes).hexdigest()
    
    # Check if file already exists
    files_db = await get_files_db()
    existing = await files_db.files.find_one({"file_hash": file_hash})
    if existing:
        return file_hash
    
    # Create file document
    file_doc = {
        "file_hash": file_hash,
        "content": base64.b64encode(output_bytes).decode('utf-8'),
        "filename": f"job_output_{job_id}.json",
        "content_type": "application/json",
        "storage_type": "inline",
        "file_size": len(output_bytes),
        "upload_date": datetime.utcnow(),
        "last_checked": datetime.utcnow(),
        "metadata": {
            "source": "meta_job_output",
            "job_id": job_id
        }
    }
    
    await files_db.files.insert_one(file_doc)
    return file_hash


async def get_meta_job_info(meta_job_id: str) -> Optional[MetaJobInfo]:
    """
    Get full information about a meta-job.
    
    Args:
        meta_job_id: Meta-job ID
        
    Returns:
        MetaJobInfo if found, None otherwise
    """
    meta_jobs_db = await get_meta_jobs_db()
    meta_job = await meta_jobs_db.meta_jobs.find_one({"meta_job_id": meta_job_id})
    
    if not meta_job:
        return None
    
    # Convert to MetaJobInfo model
    chain = [MetaJobStepInfo(**step) for step in meta_job["chain"]]
    
    return MetaJobInfo(
        meta_job_id=meta_job["meta_job_id"],
        initial_file_hash=meta_job["initial_file_hash"],
        chain=chain,
        status=meta_job["status"],
        current_step=meta_job.get("current_step"),
        final_output=meta_job.get("final_output"),
        final_output_hash=meta_job.get("final_output_hash"),
        error=meta_job.get("error"),
        created_at=meta_job["created_at"],
        updated_at=meta_job.get("updated_at"),
        completed_at=meta_job.get("completed_at")
    )
