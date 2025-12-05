from fastapi import APIRouter, HTTPException
from typing import Dict, Any, List
import asyncio

from slurm_api.config.logging_config import logger
from slurm_api.models.job import JobSubmission, JobStatus
from slurm_api.models.method import Method, MethodExecution
from slurm_api.services.job_service import (
    prepare_job_script, submit_slurm_job, get_job_info, 
    create_job, update_job_status
)
from slurm_api.services.slurm_service import get_queue_status
from slurm_api.utils.db_utils import update_job_status
from slurm_api.config.db_config import jobs_collection
from slurm_api.background.tasks import check_jobs_once
from slurm_api.services.file_service import find_file_by_hash
from slurm_api.services.method_service import (
    find_method_by_hash, list_available_methods, register_method, prepare_method_execution, find_method_by_name,
    list_method_versions
)
from slurm_api.utils.parameter_utils import sort_parameters

router = APIRouter()

@router.post("/submit")
async def submit_job(job: JobSubmission):
    """Submit a new job to SLURM."""
    logger.info(f"=== JOB SUBMISSION RECEIVED ===")
    logger.info(f"file_hash: {job.file_hash}")
    logger.info(f"file_inputs: {job.file_inputs}")
    logger.info(f"function_hash: {job.function_hash}")
    
    try:
        # Validate file(s) exist in database
        logger.info("Starting file validation...")
        if job.file_inputs:
            # Multi-file: validate all files (handles both single files and arrays)
            for name, file_ref in job.file_inputs.items():
                if isinstance(file_ref, list):
                    # Array of files
                    for idx, file_hash in enumerate(file_ref):
                        file_doc = find_file_by_hash(file_hash)
                        if not file_doc:
                            raise HTTPException(
                                status_code=400,
                                detail=f"File '{name}[{idx}]' with hash {file_hash} not found in database"
                            )
                else:
                    # Single file
                    file_doc = find_file_by_hash(file_ref)
                    if not file_doc:
                        raise HTTPException(
                            status_code=400,
                            detail=f"File '{name}' with hash {file_ref} not found in database"
                        )
        else:
            # Single file (optional for params-only jobs)
            if job.file_hash:
                file_doc = find_file_by_hash(job.file_hash)
                if not file_doc:
                    raise HTTPException(
                        status_code=400,
                        detail=f"File with hash {job.file_hash} not found in database"
                    )
            # If no file_hash, it's a params-only job (valid)
        
        # Validate function_hash exists if provided
        if job.function_hash:
            method_doc = find_method_by_hash(job.function_hash)
            if not method_doc:
                raise HTTPException(
                    status_code=400,
                    detail=f"Method with hash {job.function_hash} not found in database"
                )
        elif not job.script:
            raise HTTPException(
                status_code=400,
                detail="Either script or function_hash must be provided"
            )
        
        # Sort parameters to ensure consistent ordering
        sorted_params = sort_parameters(job.parameters)
        
        # Check for duplicate jobs
        # Find any existing job with the same core identifiers
        if job.file_inputs:
            from slurm_api.utils.sorting_utils import sort_file_inputs
            sorted_inputs = sort_file_inputs(job.file_inputs)
            query = {
                "function_hash": job.function_hash,
                "file_inputs": sorted_inputs,
                "parameters": sorted_params
            }
        else:
            query = {
                "function_hash": job.function_hash,
                "file_hash": job.file_hash,
                "parameters": sorted_params
            }
        
        existing_job = jobs_collection.find_one(query)
        
        if existing_job:
            # If the existing job is completed or still active, return it as duplicate
            if existing_job['status'] in [
                JobStatus.COMPLETED, 
                JobStatus.PENDING, 
                JobStatus.RUNNING, 
                JobStatus.COMPLETING, 
                JobStatus.CONFIGURING
            ]:
                logger.info(f"Identical job found (status: {existing_job['status']}) with hash {job.function_hash}, returning existing job_hash: {existing_job['job_hash']}")
                return {
                    "message": f"Identical job already exists with status {existing_job['status']}, returning existing job hash",
                    "job_hash": existing_job['job_hash'],
                    "duplicate": True
                }
            # If the existing job failed or was cancelled, reuse it (reset status to PENDING)
            else:
                logger.info(f"Found previous identical job (status: {existing_job['status']}), resetting to PENDING for resubmission.")
                # Reset the existing job instead of creating a duplicate
                jobs_collection.update_one(
                    {"job_hash": existing_job['job_hash']},
                    {"$set": {
                        "status": JobStatus.PENDING,
                        "slurm_id": None,
                        "error": None,
                        "output": None,
                        "last_submission_attempt": None,
                        "submission_attempts": 0
                    }}
                )
                job_hash = existing_job['job_hash']
                job_doc = existing_job
                job_doc["status"] = JobStatus.PENDING
                # Skip to submission (don't create new job)
                logger.info(f"Job reset: {job_hash}")

                try:
                    # Prepare job script file
                    logger.info(f"Preparing job script...")
                    script_path = prepare_job_script(job_hash, job)
                    logger.info(f"Script prepared: {script_path}")

                    # Submit job to Slurm
                    success, message, slurm_id = submit_slurm_job(script_path)

                    if not success:
                        update_job_status(job_hash, JobStatus.CANCELLED, error=f"Submission failed (service unavailable): {message}")
                        raise HTTPException(
                            status_code=400,
                            detail=f"Job submission failed: {message}"
                        )

                    # Update job document with Slurm ID
                    jobs_collection.update_one(
                        {"job_hash": job_hash},
                        {"$set": {"slurm_id": slurm_id}}
                    )

                    await check_jobs_once()
                    return {"message": message, "job_hash": job_hash, "resubmitted": True}

                except HTTPException:
                    raise
                except Exception as e:
                    update_job_status(job_hash, JobStatus.CANCELLED, error=f"Submission error (service unavailable): {str(e)}")
                    raise HTTPException(
                        status_code=500,
                        detail=f"Job submission failed: {str(e)}"
                    )

        # No existing job found - create new job in database
        logger.info(f"Creating job in DB...")
        job_hash, job_doc = create_job(job)
        logger.info(f"Job created: {job_hash}")
        
        try:
            # Prepare job script file
            logger.info(f"Preparing job script...")
            script_path = prepare_job_script(job_hash, job)
            logger.info(f"Script prepared: {script_path}")
            
            # Submit job to Slurm
            success, message, slurm_id = submit_slurm_job(script_path)
            
            if not success:
                # Mark as CANCELLED instead of FAILED - submission failures are typically
                # service issues (restart, network, etc.) not job errors. CANCELLED allows resubmission.
                update_job_status(job_hash, JobStatus.CANCELLED, error=f"Submission failed (service unavailable): {message}")
                raise HTTPException(
                    status_code=400,
                    detail=f"Job submission failed: {message}"
                )
            
            # Update job document with Slurm ID
            jobs_collection.update_one(
                {"job_hash": job_hash},
                {"$set": {"slurm_id": slurm_id}}
            )
            
            # Trigger job status check immediately and wait for it to complete
            await check_jobs_once()
            
            return {"message": message, "job_hash": job_hash}
            
        except Exception as e:
            # Mark as CANCELLED instead of FAILED - exceptions during submission are typically
            # service issues (restart, network, etc.) not job errors. CANCELLED allows resubmission.
            update_job_status(job_hash, JobStatus.CANCELLED, error=f"Submission error (service unavailable): {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Job submission failed: {str(e)}"
            )
            
    except Exception as e:
        import traceback
        logger.error(f"Error submitting job: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@router.get("/methods", response_model=List[Dict[str, Any]])
async def get_methods(active_only: bool = False):
    """
    Get all available methods.
    
    Args:
        active_only: If True, only return methods that are active in the current session
    """
    try:
        methods = list_available_methods(active_only=active_only)
        return methods
    except Exception as e:
        logger.error(f"Error retrieving methods: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@router.get("/methods/{function_hash}")
async def get_method(function_hash: str):
    """Get method information by hash."""
    method = find_method_by_hash(function_hash)
    if not method:
        raise HTTPException(
            status_code=404,
            detail=f"Method with hash {function_hash} not found"
        )
    
    # Remove bundle to avoid sending large data
    if "bundle" in method:
        del method["bundle"]
    
    # Convert ObjectId to string
    method["_id"] = str(method["_id"])
    
    return method

@router.get("/check-jobs")
async def trigger_job_check():
    """Manually trigger a job status check."""
    try:
        # Run job status check
        await check_jobs_once()
        return {"message": "Job status check completed successfully"}
    except Exception as e:
        logger.error(f"Error running job status check: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@router.get("/queue")
async def get_queue():
    """Get current Slurm queue status."""
    try:
        success, message, jobs = get_queue_status()
        
        if not success:
            # Log the actual error but return empty queue instead of failing
            logger.warning(f"Queue query failed: {message}")
            # Return empty queue instead of error - controller might be temporarily unavailable
            return {"jobs": [], "warning": "Slurm controller temporarily unavailable"}
        
        return {"jobs": jobs}
        
    except Exception as e:
        logger.error(f"Error getting queue: {e}")
        # Return empty queue on error instead of raising exception
        return {"jobs": [], "error": str(e)}

@router.get("/job/{job_hash}")
async def get_job(job_hash: str):
    """Get job information from MongoDB."""
    job = get_job_info(job_hash)
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_hash} not found"
        )
    
    return job

@router.get("/methods/by-name/{method_name}")
async def get_method_by_name(method_name: str, latest: bool = True):
    """Get method information by name, optionally returning the latest version."""
    method = find_method_by_name(method_name, latest)
    if not method:
        raise HTTPException(
            status_code=404,
            detail=f"Method with name {method_name} not found"
        )
    
    # Remove bundle to avoid sending large data
    if "bundle" in method:
        del method["bundle"]
    
    # Convert ObjectId to string
    method["_id"] = str(method["_id"])
    
    return method

@router.get("/methods/versions/{method_name}")
async def get_method_versions(method_name: str):
    """Get all versions of a method by name, sorted by creation time (newest first)."""
    methods = list_method_versions(method_name)
    if not methods:
        raise HTTPException(
            status_code=404,
            detail=f"No methods found with name {method_name}"
        )
    
    return methods 