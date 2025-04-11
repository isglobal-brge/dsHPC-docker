from fastapi import APIRouter, HTTPException, Depends
import requests
from typing import Dict, Any, List

from dshpc_api.config.settings import get_settings
from dshpc_api.services.slurm_service import get_job, submit_job, check_jobs
from dshpc_api.services.db_service import get_files

router = APIRouter()

@router.get("/jobs/check")
async def trigger_job_check():
    """
    Trigger a job status check in the Slurm API.
    """
    try:
        result = await check_jobs()
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error checking jobs: {str(e)}"
        )

@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """
    Get status of a job from the Slurm API.
    """
    try:
        job = await get_job(job_id)
        return job
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error getting job status: {str(e)}"
        )

@router.post("/jobs")
async def submit_new_job(job_data: Dict[str, Any]):
    """
    Submit a new job to the Slurm API.
    """
    try:
        result = await submit_job(job_data)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error submitting job: {str(e)}"
        )

@router.get("/files")
async def list_files():
    """
    List files from the files database.
    """
    try:
        files = await get_files()
        return {"files": files}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error listing files: {str(e)}"
        )

@router.get("/services/status")
async def get_services_status():
    """
    Check status of all connected services.
    """
    settings = get_settings()
    status = {}
    
    # Check Slurm API
    try:
        slurm_response = requests.get(f"{settings.SLURM_API_URL}/health", timeout=5)
        status["slurm_api"] = {"status": "up" if slurm_response.status_code == 200 else "down"}
    except Exception as e:
        status["slurm_api"] = {"status": "down", "error": str(e)}
    
    # Add database status checks
    try:
        status["jobs_db"] = {"status": "up"}
        status["files_db"] = {"status": "up"}
    except Exception as e:
        status["databases"] = {"status": "error", "error": str(e)}
    
    return status 