import asyncio

from slurm_api.config.db_config import jobs_collection
from slurm_api.config.logging_config import logger
from slurm_api.models.job import JobStatus
from slurm_api.services.slurm_service import get_active_jobs, get_job_final_state
from slurm_api.services.job_service import process_job_output
from slurm_api.utils.db_utils import update_job_status

async def check_jobs_once():
    """Run a single iteration of job status check."""
    try:
        # Find all jobs that are in a non-terminal state
        active_jobs = jobs_collection.find({
            "status": {"$nin": [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED, 
                              JobStatus.TIMEOUT, JobStatus.NODE_FAIL, JobStatus.OUT_OF_MEMORY,
                              JobStatus.BOOT_FAIL, JobStatus.DEADLINE, JobStatus.PREEMPTED]},
            "slurm_id": {"$ne": None}
        })
        
        # Get all job statuses from Slurm
        slurm_statuses = get_active_jobs()
        
        # Check each active job
        for job in active_jobs:
            slurm_id = job["slurm_id"]
            job_id = job["job_id"]
            
            # If job not in Slurm queue, check sacct for final status
            if slurm_id not in slurm_statuses:
                # Get final status from sacct
                final_state = get_job_final_state(slurm_id)
                process_job_output(job_id, slurm_id, final_state)
            else:
                # Update status if job is still in queue
                slurm_state = slurm_statuses[slurm_id]
                if slurm_state != job["status"]:
                    try:
                        update_job_status(job_id, JobStatus(slurm_state))
                    except ValueError:
                        logger.warning(f"Unknown Slurm state: {slurm_state}")
        
    except Exception as e:
        logger.error(f"Error in job status checking: {e}")
        
async def check_job_status():
    """Background task to check and update status of running jobs."""
    # First run the function once immediately
    await check_jobs_once()
    
    # Then continue with the periodic check loop
    while True:
        try:
            await check_jobs_once()
        except Exception as e:
            logger.error(f"Error in job status checking loop: {e}")
        
        # Wait before next check
        await asyncio.sleep(5)  # Check every 5 seconds 