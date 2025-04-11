from fastapi import APIRouter, HTTPException, Depends, status
import requests
from typing import Dict, Any, List

from dshpc_api.config.settings import get_settings
from dshpc_api.services.db_service import upload_file, check_hashes, get_files
from dshpc_api.services.job_service import simulate_job
from dshpc_api.models.file import FileUpload, FileResponse, HashCheckRequest, HashCheckResponse
from dshpc_api.models.job import JobSimulationRequest, JobSimulationResponse

router = APIRouter()

@router.post("/files/upload", response_model=FileResponse, status_code=status.HTTP_201_CREATED)
async def upload_new_file(file_data: FileUpload):
    """
    Upload a new file to the database.
    If a file with the same hash already exists, the upload will be rejected.
    
    All file content should be base64 encoded, regardless of file type.
    Set the appropriate content_type (e.g., "image/jpeg", "application/zip", "text/csv") 
    to indicate the file format.
    """
    try:
        # Prepare data for database
        db_file_data = {
            "file_hash": file_data.file_hash,
            "content": file_data.content,
            "filename": file_data.filename,
            "content_type": file_data.content_type,
            "metadata": file_data.metadata
        }
        
        # Upload file
        success, message, uploaded_file = await upload_file(db_file_data)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=message
            )
        
        return uploaded_file
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error uploading file: {str(e)}"
        )

@router.post("/files/check-hashes", response_model=HashCheckResponse)
async def check_file_hashes(hash_data: HashCheckRequest):
    """
    Check which hashes from the provided list already exist in the database.
    """
    try:
        existing_hashes, missing_hashes = await check_hashes(hash_data.hashes)
        return {
            "existing_hashes": existing_hashes,
            "missing_hashes": missing_hashes
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error checking hashes: {str(e)}"
        )

@router.get("/files", response_model=List[FileResponse])
async def list_all_files():
    """
    List all files from the files database.
    """
    try:
        files = await get_files()
        return files
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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

@router.get("/health")
async def health_check():
    """Simple health check endpoint."""
    return {"status": "ok"}

@router.post("/simulate-job", response_model=JobSimulationResponse)
async def simulate_job_endpoint(job_data: JobSimulationRequest):
    """
    Simulate a job execution based on file_hash, method_name, and parameters.
    
    This endpoint will:
    1. Check for the most recent hash of the specified method
    2. Check if a job with the same parameters already exists
    3. Based on the job status, either return results or submit a new job
    """
    try:
        result = await simulate_job(
            job_data.file_hash,
            job_data.method_name,
            job_data.parameters
        )
        
        if not result.get("job_id") and not result.get("message").startswith("Error"):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result.get("message", "Method or file not found")
            )
            
        return result
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error simulating job: {str(e)}"
        ) 