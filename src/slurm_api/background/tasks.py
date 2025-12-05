import asyncio
import os

from slurm_api.config.db_config import jobs_collection, meta_jobs_collection, pipelines_collection
from slurm_api.config.logging_config import logger
from slurm_api.models.job import JobStatus
from slurm_api.services.slurm_service import get_active_jobs, get_job_final_state
from slurm_api.services.job_service import process_job_output
from slurm_api.utils.db_utils import update_job_status


def cascade_delete_pipelines(meta_job_hash: str) -> int:
    """
    Delete all pipelines that reference the given meta_job_hash in their nodes.
    Returns the number of pipelines deleted.
    """
    deleted_count = 0

    # Find pipelines that have this meta_job_hash in any of their nodes
    pipelines = pipelines_collection.find({
        "nodes": {"$exists": True}
    })

    for pipeline in pipelines:
        pipeline_hash = pipeline.get("pipeline_hash")
        nodes = pipeline.get("nodes", {})

        # Check if any node references this meta_job_hash
        for node_id, node in nodes.items():
            if node.get("meta_job_hash") == meta_job_hash:
                result = pipelines_collection.delete_one({"pipeline_hash": pipeline_hash})
                if result.deleted_count > 0:
                    logger.info(f"🗑️ Cascade deleted pipeline {pipeline_hash} (referenced deleted meta-job)")
                    deleted_count += 1
                break  # Pipeline already deleted, no need to check more nodes

    return deleted_count


def cascade_delete_meta_jobs(job_hash: str) -> tuple[int, int]:
    """
    Delete all meta-jobs that reference the given job_hash in their chain.
    Also cascade-deletes pipelines that reference those meta-jobs.
    Returns tuple of (meta_jobs_deleted, pipelines_deleted).
    """
    meta_deleted_count = 0
    pipeline_deleted_count = 0

    # Find meta-jobs that have this job_hash anywhere in their chain
    meta_jobs = meta_jobs_collection.find({
        "chain.job_hash": job_hash
    })

    for meta_job in meta_jobs:
        meta_job_hash = meta_job["meta_job_hash"]

        # First cascade-delete pipelines that reference this meta-job
        pipeline_deleted_count += cascade_delete_pipelines(meta_job_hash)

        # Then delete the meta-job itself
        result = meta_jobs_collection.delete_one({"meta_job_hash": meta_job_hash})
        if result.deleted_count > 0:
            logger.info(f"🗑️ Cascade deleted meta-job {meta_job_hash} (referenced deleted job)")
            meta_deleted_count += 1

    return meta_deleted_count, pipeline_deleted_count


async def check_cancelled_jobs():
    """
    Detect and clean up jobs that were cancelled externally (e.g., via scancel).

    A job is considered cancelled if:
    1. It has a slurm_id (was submitted to Slurm)
    2. It's not in a terminal state in our DB
    3. It's no longer in the Slurm queue
    4. It has no output files (meaning it was killed before completing)

    Also cleans up jobs that failed with "Exit code file not found" error,
    which indicates they were killed before producing output.

    When a job is deleted, also cascade-deletes any meta-jobs that reference it.
    """
    try:
        cancelled_count = 0
        meta_job_count = 0
        pipeline_count = 0

        # Part 1: Check active jobs that disappeared from Slurm queue
        slurm_statuses = get_active_jobs()

        active_db_jobs = jobs_collection.find({
            "status": {"$in": [JobStatus.PENDING, JobStatus.RUNNING, "PD", "R", "CG"]},
            "slurm_id": {"$ne": None}
        })

        for job in active_db_jobs:
            slurm_id = job["slurm_id"]
            job_hash = job["job_hash"]

            if slurm_id not in slurm_statuses:
                output_path = f"/tmp/output_{job_hash}.txt"
                exit_code_path = f"/tmp/exit_code_{job_hash}"
                error_path = f"/tmp/error_{job_hash}.txt"

                if not os.path.exists(output_path) and not os.path.exists(exit_code_path) and not os.path.exists(error_path):
                    logger.warning(f"Detected cancelled job {job_hash} (slurm_id={slurm_id}) - no output files found")
                    result = jobs_collection.delete_one({"job_hash": job_hash})
                    if result.deleted_count > 0:
                        logger.info(f"🗑️ Deleted cancelled job {job_hash} from database")
                        cancelled_count += 1
                        # Cascade delete meta-jobs and pipelines
                        meta_deleted, pipeline_deleted = cascade_delete_meta_jobs(job_hash)
                        meta_job_count += meta_deleted
                        pipeline_count += pipeline_deleted

        # Part 2: Clean up failed jobs where the exit code file was not found
        # This means the job was killed externally (scancel, OOM, timeout, container restart)
        # before it could write its exit code - NOT a logical error in the job itself
        # These jobs should be deleted so they can be retried
        failed_external_jobs = jobs_collection.find({
            "status": {"$in": [JobStatus.FAILED, "FA", "F"]},
            "error": {"$regex": "Exit code file not found"}
        })

        for job in failed_external_jobs:
            job_hash = job["job_hash"]
            logger.warning(f"Found externally-killed job {job_hash} (no exit code file)")
            result = jobs_collection.delete_one({"job_hash": job_hash})
            if result.deleted_count > 0:
                logger.info(f"🗑️ Deleted killed job {job_hash} from database (will retry)")
                cancelled_count += 1
                # Cascade delete meta-jobs and pipelines
                meta_deleted, pipeline_deleted = cascade_delete_meta_jobs(job_hash)
                meta_job_count += meta_deleted
                pipeline_count += pipeline_deleted

        if cancelled_count > 0:
            logger.info(f"🧹 Cleaned up {cancelled_count} cancelled/orphaned jobs")
        if meta_job_count > 0:
            logger.info(f"🧹 Cascade deleted {meta_job_count} meta-jobs")
        if pipeline_count > 0:
            logger.info(f"🧹 Cascade deleted {pipeline_count} pipelines")

    except Exception as e:
        logger.error(f"Error checking cancelled jobs: {e}")

async def check_orphaned_meta_jobs():
    """
    Find and delete meta-jobs whose current job no longer exists in the database.
    This handles the case where a job was deleted but its meta-job wasn't cascade-deleted.
    Also cascade-deletes pipelines that reference those meta-jobs.
    """
    try:
        deleted_count = 0
        pipeline_count = 0

        # Find meta-jobs that are running (not completed/failed)
        running_meta_jobs = meta_jobs_collection.find({
            "status": {"$in": ["running", "pending"]}
        })

        for meta_job in running_meta_jobs:
            meta_job_hash = meta_job["meta_job_hash"]
            current_step = meta_job.get("current_step", 0)
            chain = meta_job.get("chain", [])

            # Get the job_hash for the current step
            if current_step < len(chain):
                current_job_hash = chain[current_step].get("job_hash")

                if current_job_hash:
                    # Check if this job exists
                    job_exists = jobs_collection.find_one({"job_hash": current_job_hash})
                    if not job_exists:
                        logger.warning(f"Found orphaned meta-job {meta_job_hash} - current job {current_job_hash} no longer exists")

                        # First cascade-delete pipelines that reference this meta-job
                        pipeline_count += cascade_delete_pipelines(meta_job_hash)

                        # Then delete the meta-job
                        result = meta_jobs_collection.delete_one({"meta_job_hash": meta_job_hash})
                        if result.deleted_count > 0:
                            logger.info(f"🗑️ Deleted orphaned meta-job {meta_job_hash}")
                            deleted_count += 1

        if deleted_count > 0:
            logger.info(f"🧹 Cleaned up {deleted_count} orphaned meta-jobs")
        if pipeline_count > 0:
            logger.info(f"🧹 Cascade deleted {pipeline_count} pipelines")

    except Exception as e:
        logger.error(f"Error checking orphaned meta-jobs: {e}")


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
        # First, check for cancelled jobs and clean them up
        await check_cancelled_jobs()

        # Check for orphaned meta-jobs (where referenced job no longer exists)
        await check_orphaned_meta_jobs()

        # Then, check for orphaned jobs and retry them
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
                # Get final status from sacct (pass job_hash to help detect cancelled jobs)
                final_state = get_job_final_state(slurm_id, job_hash)
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