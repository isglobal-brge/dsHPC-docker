from fastapi import APIRouter, HTTPException
from typing import Dict, Any

from slurm_api.config.logging_config import logger
from slurm_api.models.job import JobSubmission, JobStatus
from slurm_api.services.job_service import create_job, prepare_job_script, get_job_by_id
from slurm_api.services.slurm_service import submit_slurm_job, get_queue_status
from slurm_api.utils.db_utils import update_job_status
from slurm_api.config.db_config import jobs_collection

router = APIRouter()

@router.post("/submit")
async def submit_job(job: JobSubmission):
    try:
        # Create job in database
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
    job = get_job_by_id(job_id)
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_id} not found"
        )
    
    return job 