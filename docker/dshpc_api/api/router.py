from fastapi import APIRouter, HTTPException, Depends, status, Security
import requests
from typing import Dict, Any, List
import base64
import hashlib

from dshpc_api.config.settings import get_settings
from dshpc_api.api.auth import get_api_key
from dshpc_api.services.db_service import upload_file, check_hashes, get_files
from dshpc_api.services.job_service import simulate_job
from dshpc_api.services.method_service import get_available_methods
from dshpc_api.models.file import FileUpload, FileResponse, HashCheckRequest, HashCheckResponse
from dshpc_api.models.job import JobRequest, JobResponse
from dshpc_api.models.method import Method, MethodsResponse

router = APIRouter()

def verify_content_hash(content_base64: str, provided_hash: str) -> bool:
    """
    Verify that the SHA-256 hash of the decoded content matches the provided hash.
    
    Args:
        content_base64: Base64 encoded content
        provided_hash: Hash provided by the client
        
    Returns:
        bool: True if hashes match, False otherwise
    """
    try:
        # Decode base64 content
        content_bytes = base64.b64decode(content_base64)
        
        # Calculate SHA-256 hash
        calculated_hash = hashlib.sha256(content_bytes).hexdigest()
        
        # Compare with provided hash
        return calculated_hash == provided_hash
    except Exception:
        return False

@router.post("/files/upload", response_model=FileResponse, status_code=status.HTTP_201_CREATED)
async def upload_new_file(file_data: FileUpload, api_key: str = Security(get_api_key)):
    """
    Upload a new file to the database.
    If a file with the same hash already exists, the upload will be rejected.
    
    All file content should be base64 encoded, regardless of file type.
    Set the appropriate content_type (e.g., "image/jpeg", "application/zip", "text/csv") 
    to indicate the file format.
    """
    try:
        # Verify the hash matches the content
        if not verify_content_hash(file_data.content, file_data.file_hash):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Content hash verification failed. The provided hash does not match the content."
            )
            
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
async def check_file_hashes(hash_data: HashCheckRequest, api_key: str = Security(get_api_key)):
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
async def list_all_files(api_key: str = Security(get_api_key)):
    """
    List all files from the files database.
    """
    try:
        db_files = await get_files()
        
        # Transform the files to match the FileResponse model
        files = []
        for file in db_files:
            if "file_hash" in file:  # Ensure the file has a hash
                file_response = {
                    "file_hash": file.get("file_hash"),
                    "filename": file.get("filename"),
                    "content_type": file.get("content_type", "application/octet-stream"),
                    "upload_date": file.get("upload_date"),
                    "last_checked": file.get("last_checked"),
                    "metadata": file.get("metadata")
                }
                files.append(file_response)
        
        return files
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing files: {str(e)}"
        )

@router.get("/services/status")
async def get_services_status(api_key: str = Security(get_api_key)):
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

@router.post("/query-job", response_model=JobResponse)
async def simulate_job_endpoint(job_data: JobRequest, api_key: str = Security(get_api_key)):
    """
    Simulate a job execution based on file_hash, method_name, and parameters.
    
    This endpoint will:
    1. Check for the most recent hash of the specified method
    2. Check if a job with the same parameters already exists
    3. Based on the job status, either return results, submit a new job, or return error status
    
    The response includes detailed information about the job status, including:
    - Human-readable status descriptions
    - Whether the job was resubmitted
    - Detailed error messages for failed jobs
    - Original status for resubmitted jobs
    """
    try:
        result = await simulate_job(
            job_data.file_hash,
            job_data.method_name,
            job_data.parameters
        )
        
        # Handle cases where no job_id was returned and it's not an internal error
        if not result.get("job_id") and not result.get("message", "").startswith("Error"):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "message": result.get("message", "Method or file not found"),
                    "status_detail": result.get("status_detail"),
                    "error_details": result.get("error_details")
                }
            )
        
        # Check for non-retriable job failures
        message = result.get("message", "")
        if "(non-retriable)" in message:
            job_status = result.get("status", "")
            error_details = result.get("error_details", "No additional error details")
            status_detail = result.get("status_detail", "Job failed with non-retriable status")
            
            # Return 422 Unprocessable Entity for non-retriable job failures
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "message": f"Job execution failed with non-retriable status: {job_status}",
                    "status_detail": status_detail,
                    "error_details": error_details,
                    "job_id": result.get("job_id")
                }
            )
            
        return result
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error simulating job: {str(e)}"
        )

@router.get("/methods", response_model=MethodsResponse)
async def list_available_methods(api_key: str = Security(get_api_key)):
    """
    List all available methods that can be used for processing.
    
    Returns a list of methods with their details including name, description, parameters, etc.
    Only active methods are returned.
    """
    try:
        methods, count = await get_available_methods()
        return {
            "methods": methods,
            "total_count": count
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing methods: {str(e)}"
        ) 