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
    MetaJobInfo, MetaJobStepInfo, CurrentStepInfo
)
from dshpc_api.services.db_service import (
    get_meta_jobs_db, get_files_db, get_job_by_id, get_file_by_hash
)
from dshpc_api.services.job_service import (
    submit_job, find_existing_job, get_job_status,
    get_latest_method_hash,
    RETRIABLE_FAILED_STATUSES,
    NON_RETRIABLE_FAILED_STATUSES,
    STATUS_DESCRIPTIONS,
    COMPLETED_STATUSES,
    IN_PROGRESS_STATUSES
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
        logger.info(f"📋 NEW META-JOB SUBMISSION")
        logger.info(f"  Initial file: {request.initial_file_hash[:8]}...")
        logger.info(f"  Chain length: {len(request.method_chain)} steps")
        for i, step in enumerate(request.method_chain):
            logger.info(f"  Step {i}: {step.method_name} with {len(step.parameters)} params")
        
        # Validate initial file exists
        files_db = await get_files_db()
        file_exists = await files_db.files.count_documents({"file_hash": request.initial_file_hash}) > 0
        if not file_exists:
            logger.error(f"Initial file {request.initial_file_hash} not found")
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
            "final_job_id": None,
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
            
            if existing_job and existing_job.get("status") in COMPLETED_STATUSES:
                # Job already completed, use its output
                logger.info(f"✅ CACHE HIT - Meta-job {meta_job_id} step {i}:")
                logger.info(f"  Method: {step['method_name']}")
                logger.info(f"  Input hash: {current_input_hash[:8]}...")
                logger.info(f"  Cached job ID: {existing_job['job_id']}")
                logger.info(f"  Skipping execution - using cached output")
                
                # Update current step info even for cached jobs
                await update_meta_job_current_step(meta_job_id, i, existing_job['job_id'], 
                                                   existing_job['status'], False)
                
                # Get output hash from existing job
                # The output_hash represents the hash of the job's output content
                output_hash = existing_job.get("output_hash")
                
                if not output_hash:
                    # Calculate hash from output content
                    job_output = existing_job.get("output")
                    output_gridfs_id = existing_job.get("output_gridfs_id")
                    
                    if job_output:
                        # Small output stored inline
                        import hashlib
                        output_hash = hashlib.sha256(job_output.encode('utf-8')).hexdigest()
                    elif output_gridfs_id:
                        # Large output in GridFS - use gridfs_id as proxy for hash
                        # In future, we should calculate actual hash when storing
                        output_hash = str(output_gridfs_id)
                    else:
                        raise Exception(f"Cached job {existing_job['job_id']} has no output")
                
                step["job_id"] = existing_job["job_id"]
                step["output_hash"] = output_hash
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
                
                # Use output hash as input for next step
                current_input_hash = output_hash
                
            else:
                # Need to submit new job
                logger.info(f"🔄 CACHE MISS - Meta-job {meta_job_id} step {i}:")
                logger.info(f"  Method: {step['method_name']}")
                logger.info(f"  Input hash: {current_input_hash[:8]}...")
                if existing_job:
                    logger.info(f"  Found job but status is: {existing_job.get('status', 'unknown')}")
                else:
                    logger.info(f"  No existing job found - submitting new job")
                
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
                
                # Wait for job to complete with automatic retry on retriable failures
                job_result = await wait_for_job_completion_with_retry(job_id, meta_job_id, i)
                
                if job_result["status"] not in COMPLETED_STATUSES:
                    # Should not happen as wait_for_job_completion_with_retry raises exception for failures
                    raise Exception(f"Job {job_id} failed with status {job_result['status']}: {job_result.get('error', 'Unknown error')}")
                
                # Get output hash from job result
                output_hash = job_result.get("output_hash")
                
                if not output_hash:
                    # Calculate hash from output content
                    job_output = job_result.get("output")
                    output_gridfs_id = job_result.get("output_gridfs_id")
                    
                    if job_output:
                        # Small output stored inline
                        import hashlib
                        output_hash = hashlib.sha256(job_output.encode('utf-8')).hexdigest()
                    elif output_gridfs_id:
                        # Large output in GridFS
                        output_hash = str(output_gridfs_id)
                    else:
                        raise Exception(f"Job {job_id} has no output")
                
                step["output_hash"] = output_hash
                step["status"] = "completed"
                
                # Update chain in database
                await meta_jobs_db.meta_jobs.update_one(
                    {"meta_job_id": meta_job_id},
                    {"$set": {
                        f"chain.{i}": step,
                        "updated_at": datetime.utcnow()
                    }}
                )
                
                # Use output hash as input for next step
                current_input_hash = output_hash
        
        # All steps completed successfully
        # Get final job ID from the last step
        final_job_id = meta_job["chain"][-1]["job_id"]
        
        logger.info(f"✅ META-JOB COMPLETED - {meta_job_id}")
        logger.info(f"  Total steps: {len(meta_job['chain'])}")
        cached_count = sum(1 for s in meta_job['chain'] if s.get('cached', False))
        logger.info(f"  Cached steps: {cached_count}/{len(meta_job['chain'])}")
        logger.info(f"  Final job ID: {final_job_id}")
        
        # NOTE: The final output is stored in the last job's document (jobs collection).
        # Clients should retrieve it using the final_job_id.
        # This avoids duplicating data and respects the job-based architecture.
        
        # Update meta-job as completed
        await meta_jobs_db.meta_jobs.update_one(
            {"meta_job_id": meta_job_id},
            {"$set": {
                "status": MetaJobStatus.COMPLETED,
                "final_job_id": final_job_id,
                "current_step": None,
                "current_step_info": None,  # Clear current step when completed
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


async def update_meta_job_current_step(meta_job_id: str, step_index: int, job_id: str, 
                                       job_status: str, is_resubmitted: bool):
    """
    Update meta-job with current step information for tracking.
    
    Args:
        meta_job_id: Meta-job ID
        step_index: Index of the current step (0-based)
        job_id: Current job ID
        job_status: Current job status
        is_resubmitted: Whether this job is a resubmission
    """
    meta_jobs_db = await get_meta_jobs_db()
    
    # Get step details
    meta_job = await meta_jobs_db.meta_jobs.find_one({"meta_job_id": meta_job_id})
    if not meta_job:
        logger.error(f"Meta-job {meta_job_id} not found when updating current step")
        return
    
    step = meta_job["chain"][step_index]
    
    current_step_info = {
        "step_number": step_index + 1,  # 1-based for user display
        "method_name": step["method_name"],
        "parameters": step["parameters"],
        "job_id": job_id,
        "job_status": job_status,
        "status_description": STATUS_DESCRIPTIONS.get(job_status, "Unknown status"),
        "is_resubmitted": is_resubmitted
    }
    
    await meta_jobs_db.meta_jobs.update_one(
        {"meta_job_id": meta_job_id},
        {"$set": {
            "current_step": step_index,
            "current_step_info": current_step_info,
            "updated_at": datetime.utcnow()
        }}
    )


async def wait_for_job_completion_with_retry(job_id: str, meta_job_id: str, 
                                             step_index: int, max_retries: int = 3) -> Dict[str, Any]:
    """
    Wait for job completion with automatic retry on retriable failures.
    Updates meta-job current_step info during processing.
    
    Args:
        job_id: Initial job ID to wait for
        meta_job_id: Meta-job ID for tracking updates
        step_index: Index of this step in the chain (0-based)
        max_retries: Maximum number of retry attempts
        
    Returns:
        Final job data when completed successfully
        
    Raises:
        Exception: If job fails with non-retriable status or exceeds max retries
    """
    retry_count = 0
    current_job_id = job_id
    
    while retry_count <= max_retries:
        # Wait for current job to reach terminal state
        job_result = await wait_for_job_completion(current_job_id)
        job_status = job_result.get("status")
        
        # Update meta-job with current step info
        await update_meta_job_current_step(meta_job_id, step_index, current_job_id, 
                                           job_status, retry_count > 0)
        
        if job_status in COMPLETED_STATUSES:
            logger.info(f"Meta-job {meta_job_id} step {step_index}: Job {current_job_id} completed successfully")
            return job_result
        
        elif job_status in RETRIABLE_FAILED_STATUSES:
            # Resubmit job automatically
            logger.warning(f"Meta-job {meta_job_id} step {step_index}: Job {current_job_id} failed with retriable status {job_status}")
            logger.info(f"  Status description: {STATUS_DESCRIPTIONS.get(job_status, 'Unknown')}")
            logger.info(f"  Attempting automatic resubmission (retry {retry_count + 1}/{max_retries})")
            
            # Get job details for resubmission
            old_job = await get_job_by_id(current_job_id)
            if not old_job:
                raise Exception(f"Cannot resubmit: Job {current_job_id} not found in database")
            
            success, message, new_job_data = await submit_job(
                file_hash=old_job["file_hash"],
                function_hash=old_job["function_hash"],
                parameters=old_job["parameters"]
            )
            
            if not success:
                raise Exception(f"Failed to resubmit job after retriable failure: {message}")
            
            current_job_id = new_job_data["job_id"]
            retry_count += 1
            logger.info(f"  Successfully resubmitted as job {current_job_id}")
            
        elif job_status in NON_RETRIABLE_FAILED_STATUSES:
            # Non-retriable failure - fail the meta-job
            error_msg = job_result.get("error", "No error details available")
            logger.error(f"Meta-job {meta_job_id} step {step_index}: Job {current_job_id} failed with non-retriable status {job_status}")
            logger.error(f"  Error: {error_msg}")
            raise Exception(f"Job failed with non-retriable status {job_status}: {error_msg}")
        
        else:
            # Unknown status - treat as non-retriable
            logger.error(f"Meta-job {meta_job_id} step {step_index}: Job {current_job_id} has unknown status {job_status}")
            raise Exception(f"Job has unknown status: {job_status}")
    
    # Exceeded max retries
    raise Exception(f"Job failed after {max_retries} retry attempts")


async def get_meta_job_info(meta_job_id: str) -> Optional[MetaJobInfo]:
    """
    Get full information about a meta-job.
    
    When the meta-job is completed, includes the final output from the last job.
    This allows clients to get results without knowing internal file hashes.
    
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
    
    # Get current step info if available
    current_step_info = None
    if meta_job.get("current_step_info"):
        try:
            current_step_info = CurrentStepInfo(**meta_job["current_step_info"])
        except Exception as e:
            logger.error(f"Error parsing current_step_info for meta-job {meta_job_id}: {e}")
    
    # If meta-job is completed, get the output from the final job
    final_output = None
    if meta_job["status"] == MetaJobStatus.COMPLETED and meta_job.get("final_job_id"):
        try:
            final_job = await get_job_by_id(meta_job["final_job_id"])
            if final_job:
                # Get output from job (could be inline or in GridFS)
                final_output = final_job.get("output")
                if not final_output and final_job.get("output_gridfs_id"):
                    # Large output in GridFS
                    from dshpc_api.services.db_service import get_jobs_db
                    import gridfs
                    
                    jobs_db = await get_jobs_db()
                    fs = gridfs.GridFS(jobs_db._database)
                    grid_out = fs.get(final_job["output_gridfs_id"])
                    final_output = grid_out.read().decode('utf-8')
        except Exception as e:
            logger.error(f"Error retrieving final output for meta-job {meta_job_id}: {e}")
            # Don't fail, just return without output
    
    return MetaJobInfo(
        meta_job_id=meta_job["meta_job_id"],
        initial_file_hash=meta_job["initial_file_hash"],
        chain=chain,
        status=meta_job["status"],
        current_step=meta_job.get("current_step"),
        current_step_info=current_step_info,  # Include current step tracking info
        final_job_id=meta_job.get("final_job_id"),
        final_output=final_output,  # Include the actual output when completed
        error=meta_job.get("error"),
        created_at=meta_job["created_at"],
        updated_at=meta_job.get("updated_at"),
        completed_at=meta_job.get("completed_at")
    )
