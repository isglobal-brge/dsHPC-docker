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
    try:
        # Validate file_hash exists in database
        file_doc = find_file_by_hash(job.file_hash)
        if not file_doc:
            raise HTTPException(
                status_code=400,
                detail=f"File with hash {job.file_hash} not found in database"
            )
        
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
        existing_job = jobs_collection.find_one({
            "function_hash": job.function_hash,
            "file_hash": job.file_hash,
            "parameters": sorted_params
        })
        
        if existing_job:
            # If the existing job is completed or still active, return it as duplicate
            if existing_job['status'] in [
                JobStatus.COMPLETED, 
                JobStatus.PENDING, 
                JobStatus.RUNNING, 
                JobStatus.COMPLETING, 
                JobStatus.CONFIGURING
            ]:
                logger.info(f"Identical job found (status: {existing_job['status']}) with hash {job.function_hash}, returning existing job_id: {existing_job['job_id']}")
                return {
                    "message": f"Identical job already exists with status {existing_job['status']}, returning existing job ID",
                    "job_id": existing_job['job_id'],
                    "duplicate": True
                }
            # If the existing job failed or was cancelled, allow resubmission (don't return duplicate)
            else:
                 logger.info(f"Found previous identical job (status: {existing_job['status']}) but allowing resubmission.")
                 # Proceed to create new job below
        
        # If no completed or active duplicate found, create job in database
        job_id, job_doc = create_job(job)
        
        try:
            # Prepare job script file
            script_path = prepare_job_script(job_id, job)
            
            # Submit job to Slurm
            success, message, slurm_id = submit_slurm_job(script_path)
            
            if not success:
                update_job_status(job_id, JobStatus.FAILED, error=message)
                raise HTTPException(
                    status_code=400,
                    detail=f"Job submission failed: {message}"
                )
            
            # Update job document with Slurm ID
            jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {"slurm_id": slurm_id}}
            )
            
            # Trigger job status check immediately and wait for it to complete
            await check_jobs_once()
            
            return {"message": message, "job_id": job_id}
            
        except Exception as e:
            update_job_status(job_id, JobStatus.FAILED, error=str(e))
            raise HTTPException(
                status_code=500,
                detail=f"Job submission failed: {str(e)}"
            )
            
    except Exception as e:
        logger.error(f"Error submitting job: {e}")
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
            raise HTTPException(
                status_code=400,
                detail=f"Queue query failed: {message}"
            )
        
        return {"jobs": jobs}
        
    except Exception as e:
        logger.error(f"Error getting queue: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@router.get("/job/{job_id}")
async def get_job(job_id: str):
    """Get job information from MongoDB."""
    job = get_job_info(job_id)
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_id} not found"
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