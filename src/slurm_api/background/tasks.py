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
        # Use last_submission_attempt to determine if job needs retry
        # Two cases:
        # 1. last_submission_attempt is None (never tried) - retry immediately
        # 2. last_submission_attempt exists - retry if >2 minutes ago (exponential backoff could be added)
        two_minutes_ago = datetime.utcnow() - timedelta(minutes=2)
        
        orphaned_jobs = jobs_collection.find({
            "$and": [
                {"status": JobStatus.PENDING},
                {"slurm_id": None},
                {"$or": [
                    {"last_submission_attempt": None},  # Never attempted
                    {"last_submission_attempt": {"$lt": two_minutes_ago}}  # Last attempt >2 min ago
                ]}
            ]
        }).sort("created_at", 1).limit(10)  # Process max 10 at a time, oldest first (FIFO)
        
        for job in orphaned_jobs:
            job_hash = job["job_hash"]
            attempts = job.get("submission_attempts", 0)
            logger.warning(f"Found orphaned job {job_hash} (attempt #{attempts + 1}), submitting to Slurm...")
            
            try:
                # Update last_submission_attempt timestamp BEFORE attempting
                jobs_collection.update_one(
                    {"job_hash": job_hash},
                    {"$set": {
                        "last_submission_attempt": datetime.utcnow(),
                        "submission_attempts": attempts + 1
                    }}
                )
                
                # Reconstruct JobSubmission from database document
                job_submission = JobSubmission(
                    file_hash=job.get("file_hash"),
                    file_inputs=job.get("file_inputs"),
                    function_hash=job["function_hash"],
                    parameters=job.get("parameters"),
                    name=job.get("name"),
                    job_hash=job_hash
                )
                
                # Prepare and submit job script
                script_path = prepare_job_script(job_hash, job_submission)
                success, message, slurm_id = submit_slurm_job(script_path)
                
                if success:
                    # Update job with slurm_id and clear retry tracking
                    jobs_collection.update_one(
                        {"job_hash": job_hash},
                        {"$set": {
                            "slurm_id": slurm_id,
                            "last_submission_attempt": None  # Clear since now submitted
                        }}
                    )
                    logger.info(f"✅ Successfully submitted orphaned job {job_hash} to Slurm with ID {slurm_id}")
                else:
                    # Don't mark as failed yet - will retry based on last_submission_attempt
                    logger.warning(f"⚠️ Could not submit job {job_hash} (attempt #{attempts + 1}): {message}")
                    # Mark as failed after 5 attempts
                    if attempts >= 4:  # 5th attempt failed
                        update_job_status(job_hash, JobStatus.FAILED, error=f"Failed after {attempts + 1} attempts: {message}")
                        logger.error(f"❌ Job {job_hash} marked as FAILED after {attempts + 1} submission attempts")
                    
            except Exception as e:
                # Log error but allow retries
                logger.error(f"❌ Error retrying job {job_hash}: {e}")
                # Mark as failed after 5 attempts
                if attempts >= 4:
                    update_job_status(job_id, JobStatus.FAILED, error=f"Failed after {attempts + 1} attempts: {str(e)}")
                    logger.error(f"Job {job_id} marked as FAILED after {attempts + 1} submission attempts")
                
    except Exception as e:
        # Never let this crash the background task
        logger.error(f"Error checking orphaned jobs (will retry next iteration): {e}")


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
            job_hash = job["job_hash"]
            
            # If job not in Slurm queue, check sacct for final status
            if slurm_id not in slurm_statuses:
                # Get final status from sacct
                final_state = get_job_final_state(slurm_id)
                process_job_output(job_hash, slurm_id, final_state)
            else:
                # Update status if job is still in queue
                slurm_state = slurm_statuses[slurm_id]
                if slurm_state != job["status"]:
                    try:
                        update_job_status(job_hash, JobStatus(slurm_state))
                    except ValueError:
                        logger.warning(f"Unknown Slurm state: {slurm_state}")
        
    except Exception as e:
        logger.error(f"Error in job status checking: {e}")
        
async def check_job_status():
    """Background task to check and update status of running jobs."""
    # First run the function once immediately (with error protection)
    try:
        await check_jobs_once()
    except Exception as e:
        logger.error(f"Error in initial job check: {e}")
    
    # Then continue with the periodic check loop
    while True:
        try:
            await check_jobs_once()
        except Exception as e:
            logger.error(f"Error in job status checking loop: {e}")
            # Log the error but CONTINUE the loop - never exit
        
        # Wait before next check
        await asyncio.sleep(5)  # Check every 5 seconds 