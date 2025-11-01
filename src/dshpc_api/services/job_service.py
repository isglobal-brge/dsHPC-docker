import aiohttp
import requests
import logging
from typing import Dict, Any, Optional, Tuple
from datetime import datetime

from dshpc_api.config.settings import get_settings
from dshpc_api.services.db_service import get_jobs_db, get_files_db, get_job_by_id
from dshpc_api.services.method_service import check_method_functionality
from dshpc_api.utils.parameter_utils import sort_parameters
from dshpc_api.utils.sorting_utils import sort_file_inputs

logger = logging.getLogger(__name__)

# Helper constants for job status categories
COMPLETED_STATUSES = ["CD"]  # Completed successfully
IN_PROGRESS_STATUSES = ["PD", "R", "CG", "CF"]  # Pending, Running, Completing, Configuring

# Split failed states into retriable and non-retriable
RETRIABLE_FAILED_STATUSES = ["CA", "NF", "BF", "PR", "OOM", "TO"]  # States that warrant retry
NON_RETRIABLE_FAILED_STATUSES = ["F", "DL"]  # States that should not be retried

# Combined status list for all failed states
FAILED_STATUSES = RETRIABLE_FAILED_STATUSES + NON_RETRIABLE_FAILED_STATUSES

# Dictionary mapping status codes to human-readable descriptions
STATUS_DESCRIPTIONS = {
    "SD": "Job has been submitted for the first time",
    "PD": "Job is pending in queue awaiting resources",
    "R": "Job is currently running",
    "CD": "Job has completed successfully",
    "F": "Job has failed with non-retriable error",
    "CA": "Job was cancelled by user or system",
    "TO": "Job exceeded its time limit",
    "NF": "Job failed due to node failure",
    "OOM": "Job failed due to out of memory error",
    "S": "Job has been suspended",
    "ST": "Job has been stopped",
    "BF": "Job failed during node boot",
    "DL": "Job terminated due to deadline",
    "CG": "Job is in the process of completing",
    "CF": "Job is in the process of configuring",
    "PR": "Job was preempted by another job"
}

async def get_latest_method_hash(method_name: str) -> Optional[str]:
    """
    Get the most recent hash for a method with the given name.
    
    Args:
        method_name: The name of the method
        
    Returns:
        The hash of the most recent version of the method, or None if not found or not active
    """
    settings = get_settings()
    try:
        # First try to get the method through the Slurm API
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{settings.SLURM_API_URL}/methods/by-name/{method_name}?latest=true"
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and "function_hash" in data:
                        # Only return the hash if the method is active
                        if data.get("active", False):
                            return data["function_hash"]
                        else:
                            print(f"Method {method_name} is not active")
                            return None
        
        # Fallback to direct database access if the API endpoint fails or doesn't exist
        client = await get_methods_db()
        
        # Query for methods with the given name, sorted by created_at (descending)
        # Explicitly adding active:true to ensure we only get active methods
        method = await client.methods.find_one(
            {"name": method_name, "active": True},
            sort=[("created_at", -1)]
        )
        
        if method:
            return method.get("function_hash")
        print(f"No active method found with name: {method_name}")
        return None
    except Exception as e:
        print(f"Error getting latest method hash: {e}")
        return None

async def get_methods_db():
    """
    Get a connection to the methods database.
    """
    settings = get_settings()
    from motor.motor_asyncio import AsyncIOMotorClient
    client = AsyncIOMotorClient(settings.MONGO_METHODS_URI)
    return client[settings.MONGO_METHODS_DB]

async def find_existing_job(
    file_hash: Optional[str] = None,
    file_inputs: Optional[Dict[str, str]] = None,
    function_hash: str = None,
    parameters: Dict[str, Any] = None
) -> Optional[Dict[str, Any]]:
    """
    Find an existing job with the given file_hash/file_inputs, function_hash, and parameters.
    
    Args:
        file_hash: The hash of the input file (single file)
        file_inputs: Dict of input name → hash (multi-file)
        function_hash: The hash of the method
        parameters: The job parameters
        
    Returns:
        The job document if found, None otherwise
    """
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # Connect to jobs database
        jobs_db = await get_jobs_db()
        
        # Sort parameters to ensure consistent ordering
        sorted_params = sort_parameters(parameters)
        
        # Build query based on input type
        if file_inputs:
            # Multi-file: sort file_inputs by key for deterministic comparison
            sorted_inputs = sort_file_inputs(file_inputs)
            query = {
                "file_inputs": sorted_inputs,
                "function_hash": function_hash,
                "parameters": sorted_params
            }
            logger.debug(f"🔍 Searching for multi-file job: {list(sorted_inputs.keys())}")
        else:
            # Single file (legacy)
            query = {
                "file_hash": file_hash,
                "function_hash": function_hash,
                "parameters": sorted_params
            }
            logger.debug(f"🔍 Searching for single-file job: {file_hash[:8] if file_hash else 'none'}...")
        
        # Query for jobs with the given parameters
        job = await jobs_db.jobs.find_one(query, sort=[("created_at", -1)])
        
        if job:
            job_id = job.get("job_id")
            status = job.get("status")
            logger.info(f"✅ FOUND existing job: {job_id} (status: {status})")
            # Use get_job_by_id to retrieve the full job data including GridFS output if needed
            if job_id:
                job = await get_job_by_id(job_id)
        else:
            logger.info(f"❌ NO existing job found for this combination")
        
        return job
    except Exception as e:
        print(f"Error finding existing job: {e}")
        return None

async def submit_job(
    file_hash: Optional[str] = None,
    file_inputs: Optional[Dict[str, str]] = None,
    function_hash: str = None,
    parameters: Dict[str, Any] = None
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Submit a job to the slurm_api.
    
    Args:
        file_hash: The hash of the input file (single file)
        file_inputs: Dict of input name → hash (multi-file)
        function_hash: The hash of the method
        parameters: The job parameters
        
    Returns:
        Tuple containing:
        - success (bool): Whether the submission was successful
        - message (str): Success or error message
        - data (Dict): Job data if successful, None otherwise
    """
    settings = get_settings()
    
    try:
        # Sort parameters to ensure consistent ordering
        sorted_params = sort_parameters(parameters)
        
        # Prepare job submission payload
        payload = {
            "function_hash": function_hash,
            "parameters": sorted_params
        }
        
        # Add file input (single or multi)
        if file_inputs:
            # Multi-file: sort by key for deterministic submission
            sorted_inputs = sort_file_inputs(file_inputs)
            payload["file_inputs"] = sorted_inputs
        else:
            # Single file (legacy)
            # Don't send PARAMS_ONLY_ hashes - they're placeholders
            if file_hash and not file_hash.startswith("PARAMS_ONLY_"):
                payload["file_hash"] = file_hash
            # If PARAMS_ONLY or None, omit file_hash (params-only job)
        
        # Submit job to slurm_api
        logger.info(f"Submitting to slurm_api with payload: {payload}")
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{settings.SLURM_API_URL}/submit",
                json=payload
            ) as response:
                response_data = await response.json()
                
                if response.status != 200:
                    return False, f"Error submitting job: {response_data.get('detail', 'Unknown error')}", None
                
                # Get job details
                job_id = response_data.get("job_id")
                if not job_id:
                    return False, "No job ID returned from slurm_api", None
                
                # Get job status
                job_data = await get_job_status(job_id)
                if not job_data:
                    return False, f"Could not get status for job {job_id}", None
                
                return True, "Job submitted successfully", job_data
    except Exception as e:
        return False, f"Error submitting job: {str(e)}", None

async def get_job_status(job_id: str) -> Optional[Dict[str, Any]]:
    """
    Get the status of a job from the slurm_api.
    
    Args:
        job_id: The ID of the job
        
    Returns:
        The job data if available, None otherwise
    """
    settings = get_settings()
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{settings.SLURM_API_URL}/job/{job_id}"
            ) as response:
                if response.status != 200:
                    return None
                
                return await response.json()
    except Exception as e:
        print(f"Error getting job status: {e}")
        return None

async def trigger_job_check() -> Tuple[bool, str]:
    """
    Trigger a job status check on the slurm_api.
    
    Returns:
        Tuple containing:
        - success (bool): Whether the call was successful
        - message (str): Success or error message
    """
    settings = get_settings()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{settings.SLURM_API_URL}/check-jobs") as response:
                if response.status == 200:
                    data = await response.json()
                    return True, data.get("message", "Job check triggered successfully")
                else:
                    return False, f"Error triggering job check: HTTP {response.status}"
    except Exception as e:
        return False, f"Error triggering job check: {str(e)}"

async def simulate_job(file_hash: str, method_name: str, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Simulate a job by checking for existing jobs and conditionally submitting a new one.
    
    Args:
        file_hash: The hash of the input file
        method_name: The name of the method
        parameters: The job parameters
        
    Returns:
        A dictionary containing the job results with enhanced status information
    """
    if parameters is None:
        parameters = {}
    
    # Sort parameters to ensure consistent ordering
    sorted_params = sort_parameters(parameters)
    
    try:
        # First trigger a job status check on slurm_api to ensure up-to-date status
        success, message = await trigger_job_check()
        print(f"Job check trigger: {success}, Message: {message}")
        
        # First check if the method is functional
        is_functional, message = await check_method_functionality(method_name)
        if not is_functional:
            return {
                "job_id": None,
                "status": None,
                "message": f"Method check failed: {message}",
                "status_detail": "Method validation failed",
                "error_details": message
            }
        
        # Get the latest hash for the method
        function_hash = await get_latest_method_hash(method_name)
        if not function_hash:
            return {
                "job_id": None,
                "status": None,
                "message": f"Method '{method_name}' not found or is not active",
                "status_detail": "Method not available",
                "error_details": f"Could not find an active method with name '{method_name}'"
            }
        
        # Check if the file exists (skip for params-only jobs)
        file_doc = None
        if file_hash:
            files_db = await get_files_db()
            file_doc = await files_db.files.find_one({"file_hash": file_hash})
        
        # Only validate file if file_hash was provided
        if file_hash and not file_doc:
            return {
                "job_id": None,
                "status": None,
                "message": f"File with hash '{file_hash}' not found",
                "status_detail": "Input file not found",
                "error_details": f"No file exists in the database with hash '{file_hash}'"
            }
        
        # Check if file upload is completed (only if file_hash was provided)
        if file_hash and file_doc and file_doc.get("status") != "completed":
            file_status = file_doc.get("status", "unknown")
            return {
                "job_id": None,
                "status": None,
                "message": f"File with hash '{file_hash}' is not ready (status: {file_status})",
                "status_detail": "Input file not ready",
                "error_details": f"File upload is in progress or failed. Current status: {file_status}. Please wait for upload to complete."
            }
        
        # Check if there's an existing job with these parameters (using sorted parameters)
        existing_job = await find_existing_job(
            file_hash=file_hash,
            file_inputs=None,
            function_hash=function_hash,
            parameters=sorted_params
        )
        
        if not existing_job:
            # No existing job, submit a new one (using sorted parameters)
            success, message, job_data = await submit_job(
                file_hash=file_hash,
                function_hash=function_hash,
                parameters=sorted_params
            )
            
            if not success:
                return {
                    "job_id": None,
                    "status": None,
                    "output_file_hash": None,
                    "message": message,
                    "status_detail": "Job submission failed",
                    "error_details": message
                }
            
            # Use "SD" as status code for newly submitted jobs
            status_code = job_data.get("status", "SD")
            return {
                "job_id": job_data.get("job_id"),
                "status": status_code,
                "output_file_hash": None,
                "message": "New job submitted",
                "status_detail": STATUS_DESCRIPTIONS.get(status_code, "Job has been submitted for the first time"),
                "is_resubmitted": False
            }
        
        # Check the status of the existing job
        job_status = existing_job.get("status")
        job_id = existing_job.get("job_id")
        
        if job_status in COMPLETED_STATUSES:
            # Job completed successfully, return status and output
            return {
                "job_id": job_id,
                "status": job_status,
                "output": existing_job.get("output"),
                "output_file_hash": existing_job.get("output_file_hash"),
                "message": "Completed job found",
                "status_detail": STATUS_DESCRIPTIONS.get(job_status, "Job completed successfully"),
                "is_resubmitted": False
            }
        
        elif job_status in IN_PROGRESS_STATUSES:
            # Job is in progress, return status without output
            return {
                "job_id": job_id,
                "status": job_status,
                "output_file_hash": existing_job.get("output_file_hash"),
                "message": "Job in progress",
                "status_detail": STATUS_DESCRIPTIONS.get(job_status, "Job is currently being processed"),
                "is_resubmitted": False
            }
        
        elif job_status in RETRIABLE_FAILED_STATUSES:
            # Job failed but in a retriable way, submit a new one
            success, message, job_data = await submit_job(file_hash, function_hash, sorted_params)
            
            if not success:
                return {
                    "job_id": job_id,
                    "status": None,
                    "output_file_hash": None,
                    "old_status": job_status,
                    "message": message,
                    "status_detail": "Job resubmission failed",
                    "error_details": message,
                    "is_resubmitted": False
                }
            
            new_status = job_data.get("status")
            return {
                "job_id": job_data.get("job_id"),
                "status": new_status,
                "output_file_hash": None,
                "old_status": job_status,
                "message": f"New job submitted after previous retriable failure (status: {job_status})",
                "status_detail": STATUS_DESCRIPTIONS.get(new_status, "Job has been resubmitted after a retriable failure"),
                "error_details": f"Previous job failed with status: {job_status} - {STATUS_DESCRIPTIONS.get(job_status, '')}",
                "is_resubmitted": True
            }
        
        elif job_status in NON_RETRIABLE_FAILED_STATUSES:
            # Job failed in a way that shouldn't be retried
            error_message = existing_job.get("error", "No error details available")
            return {
                "job_id": job_id,
                "status": job_status,
                "output_file_hash": existing_job.get("output_file_hash"),
                "message": f"Job previously failed with status: {job_status} (non-retriable)",
                "status_detail": STATUS_DESCRIPTIONS.get(job_status, "Job failed permanently"),
                "error_details": error_message,
                "is_resubmitted": False
            }
        
        else:
            # Unknown status, log it but don't resubmit
            print(f"Unknown job status: {job_status}")
            return {
                "job_id": job_id,
                "status": job_status,
                "output_file_hash": existing_job.get("output_file_hash"),
                "message": f"Unknown job status: {job_status}",
                "status_detail": "Job has an unrecognized status",
                "is_resubmitted": False
            }
            
    except Exception as e:
        return {
            "job_id": None,
            "status": None,
            "output_file_hash": None,
            "message": f"Error simulating job: {str(e)}",
            "status_detail": "Internal server error",
            "error_details": str(e)
        } 