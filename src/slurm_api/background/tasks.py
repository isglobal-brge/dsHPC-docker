import asyncio

from slurm_api.config.db_config import jobs_collection
from slurm_api.config.logging_config import logger
from slurm_api.models.job import JobStatus
from slurm_api.services.slurm_service import get_active_jobs, get_job_final_state
from slurm_api.services.job_service import process_job_output
from slurm_api.utils.db_utils import update_job_status

async def check_orphaned_jobs():
    """Find and retry jobs that are pending but were never submitted to Slurm."""
    from slurm_api.services.job_service import prepare_job_script, submit_slurm_job
    from slurm_api.models.job import JobSubmission
    from datetime import datetime, timedelta
    
    try:
        # Find jobs that are pending but have no slurm_id (orphaned jobs)
        # Only retry jobs older than 30 seconds to avoid retrying jobs still being processed
        thirty_seconds_ago = datetime.utcnow() - timedelta(seconds=30)
        
        orphaned_jobs = jobs_collection.find({
            "status": JobStatus.PENDING,
            "slurm_id": None,
            "created_at": {"$lt": thirty_seconds_ago}
        }).sort("created_at", 1).limit(10)  # Process max 10 at a time, oldest first (FIFO)
        
        for job in orphaned_jobs:
            job_id = job["job_id"]
            logger.warning(f"Found orphaned job {job_id}, attempting to submit to Slurm...")
            
            try:
                # Reconstruct JobSubmission from database document
                job_submission = JobSubmission(
                    file_hash=job.get("file_hash"),
                    file_inputs=job.get("file_inputs"),
                    function_hash=job["function_hash"],
                    parameters=job.get("parameters"),
                    name=job.get("name")
                )
                
                # Prepare and submit job script
                script_path = prepare_job_script(job_id, job_submission)
                success, message, slurm_id = submit_slurm_job(script_path)
                
                if success:
                    # Update job with slurm_id
                    jobs_collection.update_one(
                        {"job_id": job_id},
                        {"$set": {"slurm_id": slurm_id}}
                    )
                    logger.info(f"Successfully submitted orphaned job {job_id} to Slurm with ID {slurm_id}")
                else:
                    # Mark as failed if submission failed
                    update_job_status(job_id, JobStatus.FAILED, error=f"Failed to submit to Slurm: {message}")
                    logger.error(f"Failed to submit orphaned job {job_id}: {message}")
                    
            except Exception as e:
                logger.error(f"Error retrying orphaned job {job_id}: {e}")
                update_job_status(job_id, JobStatus.FAILED, error=str(e))
                
    except Exception as e:
        logger.error(f"Error checking orphaned jobs: {e}")


async def check_jobs_once():
    """Run a single iteration of job status check."""
    try:
        # First, check for orphaned jobs and retry them
        await check_orphaned_jobs()
        
        # Find all jobs that are in a non-terminal state AND have slurm_id
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