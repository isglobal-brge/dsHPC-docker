from fastapi import APIRouter, HTTPException, Depends, status, Security, Response
import requests
from typing import Dict, Any, List
import base64
import hashlib
import gzip
import json

from dshpc_api.config.settings import get_settings
from dshpc_api.api.auth import get_api_key
from dshpc_api.services.db_service import upload_file, check_hashes, get_files
from dshpc_api.services.job_service import simulate_job
from dshpc_api.services.method_service import get_available_methods
from dshpc_api.services.meta_job_service import submit_meta_job as submit_meta_job_service, get_meta_job_info
from dshpc_api.services.chunked_upload_service import get_chunked_upload_service
from dshpc_api.models.file import (
    FileUpload, FileResponse, HashCheckRequest, HashCheckResponse,
    ChunkedUploadInitRequest, ChunkedUploadInitResponse, ChunkedUploadChunk,
    ChunkedUploadChunkResponse, ChunkedUploadFinalizeRequest
)
from dshpc_api.models.job import JobRequest, JobResponse
from dshpc_api.models.method import Method, MethodsResponse
from dshpc_api.models.meta_job import MetaJobRequest, MetaJobResponse, MetaJobInfo

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

@router.post("/query-job")
async def simulate_job_endpoint(job_data: JobRequest, api_key: str = Security(get_api_key)):
    """
    Simulate a job execution based on file_hash/file_inputs, method_name, and parameters.
    
    Supports both single file (file_hash) and multiple files (file_inputs).
    Exactly one of file_hash or file_inputs must be provided, not both.
    
    This endpoint will:
    1. Check for the most recent hash of the specified method
    2. Check if a job with the same parameters already exists
    3. Based on the job status, either return results, submit a new job, or return error status
    
    The response includes detailed information about the job status, including:
    - Human-readable status descriptions
    - Whether the job was resubmitted
    - Detailed error messages for failed jobs
    - Original status for resubmitted jobs
    
    For large responses (>10KB), the response will be gzip compressed automatically.
    """
    try:
        result = await simulate_job(
            file_hash=job_data.file_hash,
            file_inputs=job_data.file_inputs,
            method_name=job_data.method_name,
            parameters=job_data.parameters
        )
        
        # Handle cases where no job_hash was returned and it's not an internal error
        if not result.get("job_hash") and not result.get("message", "").startswith("Error"):
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
                    "job_hash": result.get("job_hash")
                }
            )
        
        # Serialize the result to JSON
        json_data = json.dumps(result)
        json_bytes = json_data.encode('utf-8')
        
        # If the response is large (> 10KB), compress it
        if len(json_bytes) > 10 * 1024:  # 10KB threshold
            compressed_data = gzip.compress(json_bytes, compresslevel=6)
            
            # Return compressed response with appropriate headers
            return Response(
                content=compressed_data,
                media_type="application/json",
                headers={
                    "Content-Encoding": "gzip",
                    "Vary": "Accept-Encoding"
                }
            )
        else:
            # For small responses, return uncompressed
            return Response(
                content=json_bytes,
                media_type="application/json"
            )
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

@router.post("/submit-meta-job", response_model=MetaJobResponse)
async def submit_meta_job_endpoint(request: MetaJobRequest, api_key: str = Security(get_api_key)):
    """
    Submit a meta-job that chains multiple processing steps.
    
    Each step's output becomes the input for the next step.
    The system automatically caches intermediate results to avoid redundant processing.
    """
    try:
        success, message, response = await submit_meta_job_service(request)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=message
            )
        
        return response
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error submitting meta-job: {str(e)}"
        )

@router.get("/meta-job/{meta_job_hash}", response_model=MetaJobInfo)
async def get_meta_job_status_endpoint(meta_job_hash: str, api_key: str = Security(get_api_key)):
    """
    Get the current status and results of a meta-job.
    
    Returns detailed information about each step in the processing chain,
    including which steps used cached results.
    """
    try:
        meta_job_info = await get_meta_job_info(meta_job_hash)
        
        if not meta_job_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Meta-job {meta_job_hash} not found"
            )
        
        return meta_job_info
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving meta-job status: {str(e)}"
        )

# Chunked upload endpoints

@router.post("/files/upload-chunked/init", response_model=ChunkedUploadInitResponse, status_code=status.HTTP_201_CREATED)
async def init_chunked_upload(
    request: ChunkedUploadInitRequest,
    api_key: str = Security(get_api_key)
):
    """
    Initialize a chunked upload session.
    
    This endpoint creates a new session for uploading large files in chunks.
    The client must provide the complete file hash, total size, and chunk size.
    
    Returns a session_id that must be used for subsequent chunk uploads.
    """
    try:
        service = get_chunked_upload_service()
        success, message, session_id = await service.init_upload_session(
            file_hash=request.file_hash,
            filename=request.filename,
            content_type=request.content_type,
            total_size=request.total_size,
            chunk_size=request.chunk_size,
            metadata=request.metadata
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=message
            )
        
        return ChunkedUploadInitResponse(
            session_id=session_id,
            file_hash=request.file_hash,
            message=message
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error initializing chunked upload: {str(e)}"
        )


@router.post("/files/upload-chunked/{session_id}/chunk", response_model=ChunkedUploadChunkResponse)
async def upload_chunk(
    session_id: str,
    chunk: ChunkedUploadChunk,
    api_key: str = Security(get_api_key)
):
    """
    Upload a single chunk for an active session.
    
    Chunks can be uploaded in any order, but must be numbered sequentially starting from 0.
    Each chunk must be base64 encoded.
    """
    try:
        service = get_chunked_upload_service()
        success, message, response_data = await service.store_chunk(
            session_id=session_id,
            chunk_number=chunk.chunk_number,
            chunk_data=chunk.chunk_data
        )
        
        if not success:
            # Check if session not found
            if "not found" in message.lower():
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=message
                )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=message
            )
        
        return ChunkedUploadChunkResponse(
            session_id=response_data["session_id"],
            chunk_number=response_data["chunk_number"],
            chunks_received=response_data["chunks_received"],
            message=message
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error uploading chunk: {str(e)}"
        )


@router.post("/files/upload-chunked/{session_id}/finalize", response_model=FileResponse)
async def finalize_chunked_upload(
    session_id: str,
    request: ChunkedUploadFinalizeRequest,
    api_key: str = Security(get_api_key)
):
    """
    Finalize a chunked upload session.
    
    This endpoint combines all uploaded chunks into the final file, verifies the hash,
    and stores the file in the database. After successful finalization, temporary chunks
    are automatically cleaned up.
    
    The total_chunks parameter must match the number of chunks actually uploaded.
    """
    try:
        service = get_chunked_upload_service()
        success, message, file_response = await service.finalize_upload(
            session_id=session_id,
            total_chunks=request.total_chunks
        )
        
        if not success:
            # Check if session not found
            if "not found" in message.lower():
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=message
                )
            # Hash verification or other errors
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=message
            )
        
        return file_response
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error finalizing upload: {str(e)}"
        )


@router.get("/files/upload-chunked/{session_id}/status")
async def get_chunked_upload_status(
    session_id: str,
    api_key: str = Security(get_api_key)
):
    """
    Get the status of a chunked upload session.
    
    Returns information about the session including chunks received,
    total expected chunks, and current status.
    Useful for debugging and implementing resume logic.
    """
    try:
        service = get_chunked_upload_service()
        session = await service.get_session(session_id)
        
        if not session:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Session {session_id} not found"
            )
        
        return {
            "session_id": session.session_id,
            "file_hash": session.file_hash,
            "filename": session.filename,
            "total_size": session.total_size,
            "chunk_size": session.chunk_size,
            "chunks_received": len(session.chunks_received),
            "chunks_list": sorted(session.chunks_received),
            "status": session.status,
            "created_at": session.created_at,
            "last_updated": session.last_updated
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting session status: {str(e)}"
        )


@router.delete("/files/upload-chunked/{session_id}")
async def cancel_chunked_upload(
    session_id: str,
    api_key: str = Security(get_api_key)
):
    """
    Cancel a chunked upload session.
    
    This endpoint cancels an active session and cleans up all temporary chunks.
    Useful for handling client-side errors or user cancellations.
    """
    try:
        service = get_chunked_upload_service()
        success, message = await service.cancel_upload(session_id)
        
        if not success:
            if "not found" in message.lower():
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=message
                )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=message
            )
        
        return {"message": message}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error cancelling upload: {str(e)}"
        ) 