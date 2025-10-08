import os
import uuid
from datetime import datetime
from typing import Dict, Any, Tuple
from gridfs import GridFS
from bson import ObjectId

from slurm_api.config.db_config import jobs_collection, get_jobs_db_client
from slurm_api.config.logging_config import logger
from slurm_api.models.job import JobStatus, JobSubmission
from slurm_api.utils.db_utils import update_job_status
from slurm_api.services.slurm_service import submit_slurm_job
from slurm_api.services.file_service import find_file_by_hash, download_file, create_job_workspace
from slurm_api.services.method_service import find_method_by_hash, prepare_method_execution
from slurm_api.models.method import MethodExecution
from slurm_api.utils.parameter_utils import sort_parameters

def prepare_job_script(job_id: str, job: JobSubmission) -> str:
    """Prepare a job script file and return its path."""
    # Check if file hash exists in database
    file_doc = find_file_by_hash(job.file_hash)
    if not file_doc:
        raise ValueError(f"File with hash {job.file_hash} not found in database")
    
    # Create a unique workspace for the job
    workspace_dir = create_job_workspace()
    
    # Download the file to the workspace
    success, message, file_path = download_file(job.file_hash, workspace_dir)
    if not success:
        raise ValueError(f"Failed to download file: {message}")
    
    script_path = f"/tmp/job_{job_id}.sh"
    
    # Check if we're using a method (function_hash is provided)
    method_doc = None
    method_execution_data = None
    
    if job.function_hash:
        method_doc = find_method_by_hash(job.function_hash)
        if not method_doc:
            raise ValueError(f"Method with hash {job.function_hash} not found in database")
        
        # Sort parameters to ensure consistent ordering
        sorted_params = sort_parameters(job.parameters)
        
        # Prepare method execution
        method_execution = MethodExecution(
            function_hash=job.function_hash,
            file_hash=job.file_hash,
            parameters=sorted_params,
            name=job.name
        )
        
        success, message, method_execution_data = prepare_method_execution(
            workspace_dir, method_execution
        )
        
        if not success:
            raise ValueError(f"Failed to prepare method: {message}")
    
    # Write the job script to a file
    with open(script_path, "w") as f:
        f.write("#!/bin/bash\n")
        if job.name:
            f.write(f"#SBATCH --job-name={job.name}\n")
        # Capture output to a file
        output_path = f"/tmp/output_{job_id}.txt"
        error_path = f"/tmp/error_{job_id}.txt"
        # Redirect stdout to output file and stderr to error file (separate them)
        f.write(f"exec 1> {output_path} 2> {error_path}\n")
        
        # Add workspace directory and file path to environment variables
        f.write(f"export JOB_WORKSPACE=\"{workspace_dir}\"\n")
        f.write(f"export INPUT_FILE=\"{file_path}\"\n")
        
        if method_execution_data:
            # Add method-specific environment variables
            method_dir = method_execution_data['method_dir']
            f.write(f"export METHOD_DIR=\"{method_dir}\"\n")
            f.write(f"export PARAMS_FILE=\"{method_execution_data['params_file']}\"\n")
            f.write(f"export METHOD_NAME=\"{method_execution_data['method_name']}\"\n")
            
            # Add METHOD_DIR to PYTHONPATH environment variable to support imports in Python scripts
            f.write(f"export PYTHONPATH=\"{method_dir}:$PYTHONPATH\"\n")
            
            # Change to the method directory
            f.write(f"cd \"{method_dir}\"\n\n")
            
            # Execute the method command with the script name (basename)
            command = method_execution_data['command']
            script_filename = os.path.basename(method_execution_data['script_path'])
            # Use relative path for input file and params file as we are in workspace_dir
            relative_input_file = os.path.relpath(file_path, method_dir)
            relative_params_file = os.path.relpath(method_execution_data['params_file'], method_dir)
            f.write(f"{command} \"{script_filename}\" \"{relative_input_file}\" \"{relative_params_file}\"\n")
        else:
            # If no method is provided, we must have a script
            if not job.script:
                raise ValueError("Either script or function_hash must be provided")
                
            # Write the original script
            f.write(job.script)
        
        f.write(f"\necho $? > /tmp/exit_code_{job_id}")
    
    # Make the script executable
    os.chmod(script_path, 0o755)
    
    return script_path

def create_job(job: JobSubmission) -> Tuple[str, Dict[str, Any]]:
    """Create a new job in the database and return its ID and document."""
    # Generate a unique job ID
    job_id = str(uuid.uuid4())
    
    # Sort parameters to ensure consistent ordering
    sorted_params = sort_parameters(job.parameters)
    
    # Create job document
    job_doc = {
        "job_id": job_id,
        "slurm_id": None,
        "function_hash": job.function_hash,
        "file_hash": job.file_hash,
        "parameters": sorted_params,
        "status": JobStatus.PENDING,
        "created_at": datetime.utcnow(),
        "name": job.name,
        "output": None,
        "error": None
    }
    
    # Insert job document
    jobs_collection.insert_one(job_doc)
    
    return job_id, job_doc

def get_job_info(job_id: str) -> Dict[str, Any]:
    """Get job information from MongoDB, retrieving large outputs from GridFS if needed."""
    job = jobs_collection.find_one({"job_id": job_id})
    if job:
        # Convert MongoDB ObjectId to string for JSON serialization
        job["_id"] = str(job["_id"])
        
        # Check if output is stored in GridFS
        if job.get("output_storage") == "gridfs" and job.get("output_gridfs_id"):
            try:
                db = get_jobs_db_client()
                fs = GridFS(db, collection="job_outputs")
                
                grid_id = job["output_gridfs_id"]
                if isinstance(grid_id, str):
                    grid_id = ObjectId(grid_id)
                
                # Retrieve output from GridFS
                grid_out = fs.get(grid_id)
                job["output"] = grid_out.read().decode('utf-8')
                grid_out.close()
                
                # Convert GridFS ID to string for JSON serialization
                job["output_gridfs_id"] = str(job["output_gridfs_id"])
            except Exception as e:
                logger.error(f"Error retrieving output from GridFS for job {job_id}: {e}")
                job["output"] = f"[Error retrieving output from GridFS: {str(e)}]"
        
        # Check if error is stored in GridFS
        if job.get("error_storage") == "gridfs" and job.get("error_gridfs_id"):
            try:
                db = get_jobs_db_client()
                fs = GridFS(db, collection="job_outputs")
                
                grid_id = job["error_gridfs_id"]
                if isinstance(grid_id, str):
                    grid_id = ObjectId(grid_id)
                
                # Retrieve error from GridFS
                grid_out = fs.get(grid_id)
                job["error"] = grid_out.read().decode('utf-8')
                grid_out.close()
                
                # Convert GridFS ID to string for JSON serialization
                job["error_gridfs_id"] = str(job["error_gridfs_id"])
            except Exception as e:
                logger.error(f"Error retrieving error from GridFS for job {job_id}: {e}")
                job["error"] = f"[Error retrieving error from GridFS: {str(e)}]"
    
    return job

def process_job_output(job_id: str, slurm_id: str, final_state: str) -> bool:
    """Process job output files and update job status."""
    output_path = f"/tmp/output_{job_id}.txt"
    error_path = f"/tmp/error_{job_id}.txt"
    exit_code_path = f"/tmp/exit_code_{job_id}"
    exit_code = 1 # Default to error if exit code file not found or invalid
    output = None
    stderr_content = None
    error = None
    job_status = JobStatus.FAILED # Default to FAILED
    
    # Read exit code if file exists
    if os.path.exists(exit_code_path):
        try:
            with open(exit_code_path) as f:
                exit_code = int(f.read().strip())
        except ValueError:
            error = "Invalid exit code format."
            logger.warning(f"Invalid exit code format for job {job_id}")
        except Exception as e:
            error = f"Error reading exit code: {str(e)}"
            logger.error(f"Error reading exit code for job {job_id}: {e}")
    else:
        error = "Exit code file not found."
        logger.warning(f"Exit code file not found for job {job_id}")

    # Read stdout (output) file if exists
    if os.path.exists(output_path):
        try:
            with open(output_path) as f:
                output = f.read()
        except Exception as e:
            error = f"{(error + ' ') if error else ''}Error reading output file: {str(e)}"
            logger.error(f"Error reading output file for job {job_id}: {e}")
    else:
        error = f"{(error + ' ') if error else ''}Output file not found."
        logger.warning(f"Output file not found for job {job_id}")
    
    # Read stderr file if exists (only use it for errors)
    if os.path.exists(error_path):
        try:
            with open(error_path) as f:
                stderr_content = f.read().strip()
        except Exception as e:
            logger.error(f"Error reading stderr file for job {job_id}: {e}")

    # Determine final job status based on exit code and Slurm state
    if exit_code == 0:
        try:
            # If exit code is 0, use the Slurm final state if it's terminal
            if final_state in [s.value for s in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.TIMEOUT, JobStatus.NODE_FAIL, JobStatus.OUT_OF_MEMORY, JobStatus.BOOT_FAIL, JobStatus.DEADLINE, JobStatus.PREEMPTED]]:
                job_status = JobStatus(final_state)
                if job_status != JobStatus.COMPLETED:
                    # If Slurm state is failure/cancelled, use it and append exit code info
                     error = f"{(error + ' ') if error else ''}Slurm state: {final_state}. Exit code: {exit_code}."
                else:
                     # If Slurm says completed and exit code 0, it's truly completed
                     pass
            else:
                # If Slurm state is non-terminal (like RUNNING, PENDING) but exit code is 0, mark COMPLETED
                job_status = JobStatus.COMPLETED
        except ValueError:
            # If Slurm state is unknown but exit code 0, mark COMPLETED
            logger.warning(f"Unknown Slurm state: {final_state} for job {job_id}, using COMPLETED due to exit code 0.")
            job_status = JobStatus.COMPLETED
    else:
        # If exit code is non-zero, always mark as FAILED
        job_status = JobStatus.FAILED
        error = f"{(error + ' ') if error else ''}Script exited with non-zero code: {exit_code}."
        # Add stderr content to error message only if job failed
        if stderr_content:
            error = f"{error} Stderr: {stderr_content}"
        logger.info(f"Job {job_id} marked as FAILED due to non-zero exit code: {exit_code}")

    # Update the job status in the database
    try:
        update_job_status(job_id, job_status, output=output, error=error)
        # Clean up temporary files only after successful update
        if os.path.exists(output_path):
            os.remove(output_path)
        if os.path.exists(error_path):
            os.remove(error_path)
        if os.path.exists(exit_code_path):
            os.remove(exit_code_path)
        return True
    except Exception as e:
        logger.error(f"Error updating job status for {job_id} after processing output: {e}")
        return False 