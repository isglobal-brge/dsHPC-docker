import aiohttp
import requests
import asyncio
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime

from dshpc_api.config.settings import get_settings
from dshpc_api.services.db_service import get_jobs_db, get_files_db
from dshpc_api.services.method_service import check_method_functionality

# Helper constants for job status categories
COMPLETED_STATUSES = ["CD"]  # Completed successfully
IN_PROGRESS_STATUSES = ["PD", "R", "CG", "CF"]  # Pending, Running, Completing, Configuring
FAILED_STATUSES = ["F", "CA", "TO", "NF", "OOM", "BF", "DL", "PR"]  # Failed, Cancelled, Timeout, etc.

async def get_latest_method_hash(method_name: str) -> Optional[str]:
    """
    Get the most recent hash for a method with the given name.
    
    Args:
        method_name: The name of the method
        
    Returns:
        The hash of the most recent version of the method, or None if not found
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
                        return data["function_hash"]
        
        # Fallback to direct database access if the API endpoint fails or doesn't exist
        client = await get_methods_db()
        
        # Query for methods with the given name, sorted by created_at (descending)
        method = await client.methods.find_one(
            {"name": method_name},
            sort=[("created_at", -1)]
        )
        
        if method:
            return method.get("function_hash")
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

async def find_existing_job(file_hash: str, function_hash: str, parameters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Find an existing job with the given file_hash, function_hash, and parameters.
    
    Args:
        file_hash: The hash of the input file
        function_hash: The hash of the method
        parameters: The job parameters
        
    Returns:
        The job document if found, None otherwise
    """
    try:
        # Connect to jobs database
        jobs_db = await get_jobs_db()
        
        # Query for jobs with the given parameters
        job = await jobs_db.jobs.find_one({
            "file_hash": file_hash,
            "function_hash": function_hash,
            "parameters": parameters
        }, sort=[("created_at", -1)])
        
        return job
    except Exception as e:
        print(f"Error finding existing job: {e}")
        return None

async def submit_job(file_hash: str, function_hash: str, parameters: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Submit a job to the slurm_api.
    
    Args:
        file_hash: The hash of the input file
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
        # Prepare job submission payload
        payload = {
            "file_hash": file_hash,
            "function_hash": function_hash,
            "parameters": parameters
        }
        
        # Submit job to slurm_api
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
        A dictionary containing the job results
    """
    if parameters is None:
        parameters = {}
    
    try:
        # First trigger a job status check on slurm_api to ensure up-to-date status
        success, message = await trigger_job_check()
        print(f"Job check trigger: {success}, Message: {message}")
        
        # First check if the method is functional
        is_functional, message = await check_method_functionality(method_name)
        if not is_functional:
            return {
                "job_id": None,
                "new_status": None,
                "message": f"Method check failed: {message}"
            }
        
        # Get the latest hash for the method
        function_hash = await get_latest_method_hash(method_name)
        if not function_hash:
            return {
                "job_id": None,
                "new_status": None,
                "message": f"Method '{method_name}' not found"
            }
        
        # Check if the file exists
        files_db = await get_files_db()
        file_exists = await files_db.files.count_documents({"file_hash": file_hash}) > 0
        if not file_exists:
            return {
                "job_id": None,
                "new_status": None,
                "message": f"File with hash '{file_hash}' not found"
            }
        
        # Check if there's an existing job with these parameters
        existing_job = await find_existing_job(file_hash, function_hash, parameters)
        
        if not existing_job:
            # No existing job, submit a new one
            success, message, job_data = await submit_job(file_hash, function_hash, parameters)
            
            if not success:
                return {
                    "job_id": None,
                    "new_status": None,
                    "message": message
                }
            
            return {
                "job_id": job_data.get("job_id"),
                "new_status": job_data.get("status"),
                "message": "New job submitted"
            }
        
        # Check the status of the existing job
        job_status = existing_job.get("status")
        job_id = existing_job.get("job_id")
        
        if job_status in COMPLETED_STATUSES:
            # Job completed successfully, return status and output
            return {
                "job_id": job_id,
                "new_status": job_status,
                "output": existing_job.get("output"),
                "message": "Completed job found"
            }
        
        elif job_status in IN_PROGRESS_STATUSES:
            # Job is in progress, return status without output
            return {
                "job_id": job_id,
                "new_status": job_status,
                "message": "Job in progress"
            }
        
        elif job_status in FAILED_STATUSES:
            # Job failed/cancelled, submit a new one
            success, message, job_data = await submit_job(file_hash, function_hash, parameters)
            
            if not success:
                return {
                    "job_id": job_id,
                    "new_status": None,
                    "old_status": job_status,
                    "message": message
                }
            
            return {
                "job_id": job_data.get("job_id"),
                "new_status": job_data.get("status"),
                "old_status": job_status,
                "message": "New job submitted after previous failure"
            }
        
        else:
            # Unknown status, submit a new job to be safe
            success, message, job_data = await submit_job(file_hash, function_hash, parameters)
            
            if not success:
                return {
                    "job_id": job_id,
                    "new_status": None,
                    "old_status": job_status,
                    "message": message
                }
            
            return {
                "job_id": job_data.get("job_id"),
                "new_status": job_data.get("status"),
                "old_status": job_status,
                "message": "New job submitted (unknown previous status)"
            }
            
    except Exception as e:
        return {
            "job_id": None,
            "new_status": None,
            "message": f"Error simulating job: {str(e)}"
        }

async def simulate_multiple_jobs(job_configs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Simulate multiple jobs in parallel, checking for existing jobs and conditionally submitting new ones.
    
    Args:
        job_configs: List of job configurations, each containing file_hash, method_name, and optional parameters
        
    Returns:
        A dictionary containing the results for all jobs, with statistics
    """
    # Process all jobs in parallel
    result_tasks = []
    for job_config in job_configs:
        file_hash = job_config.get('file_hash')
        method_name = job_config.get('method_name')
        parameters = job_config.get('parameters', {})
        
        # Schedule the job task
        task = asyncio.create_task(
            process_single_job(file_hash, method_name, parameters)
        )
        result_tasks.append(task)
    
    # Wait for all tasks to complete
    job_results = await asyncio.gather(*result_tasks)
    
    # Count statistics
    total_jobs = len(job_results)
    successful_submissions = 0
    failed_submissions = 0
    completed_jobs = 0
    in_progress_jobs = 0
    resubmitted_jobs = 0
    
    # Analyze results
    for result in job_results:
        if result.get('new_status') in COMPLETED_STATUSES:
            completed_jobs += 1
        elif result.get('new_status') in IN_PROGRESS_STATUSES:
            in_progress_jobs += 1
            
        if result.get('old_status') is not None and result.get('new_status') is not None:
            resubmitted_jobs += 1
            
        if result.get('job_id') is not None and result.get('message', '').startswith('New job'):
            successful_submissions += 1
        elif result.get('message', '').startswith('Error'):
            failed_submissions += 1
    
    return {
        'results': job_results,
        'total_jobs': total_jobs,
        'successful_submissions': successful_submissions,
        'failed_submissions': failed_submissions,
        'completed_jobs': completed_jobs,
        'in_progress_jobs': in_progress_jobs,
        'resubmitted_jobs': resubmitted_jobs
    }

async def process_single_job(file_hash: str, method_name: str, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Process a single job by checking for existing jobs and conditionally submitting a new one.
    This is a wrapper around simulate_job that adds the input parameters to the result for identification.
    
    Args:
        file_hash: The hash of the input file
        method_name: The name of the method
        parameters: The job parameters
        
    Returns:
        A dictionary containing the job results with added input parameters
    """
    if parameters is None:
        parameters = {}
    
    # Get the function hash before simulating to include in the result
    function_hash = await get_latest_method_hash(method_name)
    
    # Run the job
    result = await simulate_job(file_hash, method_name, parameters)
    
    # Add the input parameters to the result for identification
    result['file_hash'] = file_hash
    result['method_name'] = method_name
    result['function_hash'] = function_hash
    result['parameters'] = parameters
    
    return result 