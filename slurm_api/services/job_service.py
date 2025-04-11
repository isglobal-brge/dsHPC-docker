import os
import uuid
from datetime import datetime
from typing import Dict, Any, Tuple

from slurm_api.config.db_config import jobs_collection
from slurm_api.config.logging_config import logger
from slurm_api.models.job import JobStatus, JobSubmission
from slurm_api.utils.hashing import mock_function_hash, mock_file_hash
from slurm_api.utils.db_utils import update_job_status
from slurm_api.services.slurm_service import submit_slurm_job

def prepare_job_script(job_id: str, job: JobSubmission) -> str:
    """Prepare a job script file and return its path."""
    script_path = f"/tmp/job_{job_id}.sh"
    
    # Write the job script to a file
    with open(script_path, "w") as f:
        f.write("#!/bin/bash\n")
        if job.name:
            f.write(f"#SBATCH --job-name={job.name}\n")
        # Capture output to a file
        output_path = f"/tmp/output_{job_id}.txt"
        f.write(f"exec 1> {output_path} 2>&1\n")  # Redirect both stdout and stderr
        f.write(job.script)
        f.write(f"\necho $? > /tmp/exit_code_{job_id}")
    
    # Make the script executable
    os.chmod(script_path, 0o755)
    
    return script_path

def create_job(job: JobSubmission) -> Tuple[str, Dict[str, Any]]:
    """Create a new job in the database and return its ID and document."""
    # Generate a unique job ID
    job_id = str(uuid.uuid4())
    
    # Create job document
    job_doc = {
        "job_id": job_id,
        "slurm_id": None,
        "function_hash": mock_function_hash(job.script),
        "file_hash": mock_file_hash(),
        "parameters": job.parameters,
        "status": JobStatus.PENDING,
        "created_at": datetime.utcnow(),
        "name": job.name,
        "output": None,
        "error": None
    }
    
    # Insert job document
    jobs_collection.insert_one(job_doc)
    
    return job_id, job_doc

def get_job_by_id(job_id: str) -> Dict[str, Any]:
    """Get job information from MongoDB."""
    job = jobs_collection.find_one({"job_id": job_id})
    if job:
        # Convert MongoDB ObjectId to string for JSON serialization
        job["_id"] = str(job["_id"])
    
    return job

def process_job_output(job_id: str, slurm_id: str, final_state: str) -> bool:
    """Process job output files and update job status."""
    output_path = f"/tmp/output_{job_id}.txt"
    exit_code_path = f"/tmp/exit_code_{job_id}"
    
    try:
        if os.path.exists(output_path):
            with open(output_path) as f:
                output = f.read()
            
            # Update job status based on final state
            try:
                update_job_status(job_id, JobStatus(final_state), output=output)
            except ValueError:
                # If we can't map the state, default to COMPLETED
                logger.warning(f"Unknown Slurm state: {final_state}, defaulting to COMPLETED")
                update_job_status(job_id, JobStatus.COMPLETED, output=output)
            
            # Clean up temporary files
            os.remove(output_path)
            if os.path.exists(exit_code_path):
                os.remove(exit_code_path)
                
            return True
        else:
            # No output file found, mark as failed
            update_job_status(job_id, JobStatus.FAILED, error="No output file found")
            return False
    except Exception as e:
        logger.error(f"Error processing job output for {job_id}: {e}")
        update_job_status(job_id, JobStatus.FAILED, error=str(e))
        return False 