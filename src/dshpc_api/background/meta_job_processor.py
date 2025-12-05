"""
Background processor for meta-jobs.
This module monitors, recovers, and cleans up meta-jobs.

Key functionality:
- recover_stalled_meta_jobs: Detect and continue meta-jobs that were interrupted
  (e.g., if dshpc_api restarted while processing). Checks if current job is complete
  and advances the chain.
- check_stalled_meta_jobs: Mark truly stalled meta-jobs as failed (no activity for 60+ min)
- cleanup_old_meta_jobs: Remove old completed/failed meta-jobs
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from dshpc_api.services.db_service import get_meta_jobs_db, get_jobs_db
from dshpc_api.models.meta_job import MetaJobStatus

logger = logging.getLogger(__name__)


async def recover_stalled_meta_jobs():
    """
    Recover meta-jobs that are marked as 'running' but their processing task died.

    This happens when:
    - dshpc_api restarts while a meta-job is being processed
    - The async task processing the meta-job crashes

    Detection: meta-job is 'running' but current step's job is already completed.
    Recovery: Re-launch the processing chain from current position.
    """
    try:
        meta_jobs_db = await get_meta_jobs_db()
        jobs_db = await get_jobs_db()

        # Find all running meta-jobs
        running_meta_jobs = await meta_jobs_db.meta_jobs.find({
            "status": MetaJobStatus.RUNNING
        }).to_list(None)

        recovered_count = 0

        for meta_job in running_meta_jobs:
            meta_job_hash = meta_job["meta_job_hash"]
            current_step = meta_job.get("current_step")
            chain = meta_job.get("chain", [])

            if current_step is None or current_step >= len(chain):
                # No current step or invalid - skip
                continue

            step = chain[current_step]
            job_hash = step.get("job_hash")
            step_status = step.get("status")

            # If step status is already 'completed' or 'cached', the meta-job processor died
            # before advancing. We need to re-launch.
            if step_status in ["completed", "cached"]:
                logger.warning(f"🔄 RECOVERY: Meta-job {meta_job_hash[:16]}... step {current_step} is '{step_status}' but meta-job still 'running'")
                logger.info(f"   Re-launching processing from step {current_step + 1}")

                # Re-launch the processing task
                from dshpc_api.services.meta_job_service import process_meta_job_chain
                asyncio.create_task(process_meta_job_chain(meta_job_hash))
                recovered_count += 1
                continue

            # If step status is 'running' but job_hash exists, check job status
            if job_hash and step_status == "running":
                job_doc = await jobs_db.jobs.find_one({"job_hash": job_hash})

                if job_doc:
                    job_status = job_doc.get("status")

                    # Job completed successfully - need to update step and continue
                    if job_status in ["CD", "completed"]:
                        logger.warning(f"🔄 RECOVERY: Meta-job {meta_job_hash[:16]}... step {current_step} job {job_hash[:12]}... is COMPLETED but step still 'running'")

                        # Update step status and output_hash
                        output_hash = job_doc.get("output_file_hash")

                        await meta_jobs_db.meta_jobs.update_one(
                            {"meta_job_hash": meta_job_hash},
                            {"$set": {
                                f"chain.{current_step}.status": "completed",
                                f"chain.{current_step}.output_hash": output_hash,
                                "updated_at": datetime.utcnow()
                            }}
                        )

                        logger.info(f"   Updated step {current_step} to 'completed', re-launching processing")

                        # Re-launch processing from this point
                        from dshpc_api.services.meta_job_service import process_meta_job_chain
                        asyncio.create_task(process_meta_job_chain(meta_job_hash))
                        recovered_count += 1

                    # Job failed - need to fail the meta-job
                    elif job_status in ["F", "failed", "CA", "cancelled", "OOM", "TO", "NF", "BF", "DL", "ER"]:
                        error_msg = job_doc.get("error", f"Job failed with status {job_status}")
                        logger.warning(f"🔄 RECOVERY: Meta-job {meta_job_hash[:16]}... step {current_step} job FAILED ({job_status})")
                        logger.info(f"   Marking meta-job as failed")

                        await meta_jobs_db.meta_jobs.update_one(
                            {"meta_job_hash": meta_job_hash},
                            {"$set": {
                                "status": MetaJobStatus.FAILED,
                                f"chain.{current_step}.status": "failed",
                                "error": f"Step {current_step} failed: {error_msg}",
                                "updated_at": datetime.utcnow()
                            }}
                        )
                        recovered_count += 1

        if recovered_count > 0:
            logger.info(f"🔄 RECOVERY: Recovered {recovered_count} stalled meta-jobs")

    except Exception as e:
        logger.error(f"Error recovering stalled meta-jobs: {e}")


async def cleanup_old_meta_jobs(days_to_keep: int = 30):
    """
    Clean up meta-jobs older than specified days.
    
    Args:
        days_to_keep: Number of days to keep meta-jobs
    """
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        
        meta_jobs_db = await get_meta_jobs_db()
        
        # Find and delete old completed/failed meta-jobs
        result = await meta_jobs_db.meta_jobs.delete_many({
            "status": {"$in": [MetaJobStatus.COMPLETED, MetaJobStatus.FAILED, MetaJobStatus.CANCELLED]},
            "created_at": {"$lt": cutoff_date}
        })
        
        if result.deleted_count > 0:
            logger.info(f"Cleaned up {result.deleted_count} old meta-jobs")
            
    except Exception as e:
        logger.error(f"Error cleaning up old meta-jobs: {e}")


async def check_stalled_meta_jobs(stall_timeout_minutes: int = 60):
    """
    Check for meta-jobs that have stalled and mark them as failed.
    
    Args:
        stall_timeout_minutes: Minutes after which a running job is considered stalled
    """
    try:
        cutoff_time = datetime.utcnow() - timedelta(minutes=stall_timeout_minutes)
        
        meta_jobs_db = await get_meta_jobs_db()
        
        # Find stalled meta-jobs
        stalled_jobs = await meta_jobs_db.meta_jobs.find({
            "status": MetaJobStatus.RUNNING,
            "updated_at": {"$lt": cutoff_time}
        }).to_list(None)
        
        for job in stalled_jobs:
            logger.warning(f"Marking stalled meta-job {job['meta_job_hash']} as failed")
            
            await meta_jobs_db.meta_jobs.update_one(
                {"meta_job_hash": job["meta_job_hash"]},
                {"$set": {
                    "status": MetaJobStatus.FAILED,
                    "error": f"Job stalled - no updates for {stall_timeout_minutes} minutes",
                    "updated_at": datetime.utcnow()
                }}
            )
            
    except Exception as e:
        logger.error(f"Error checking stalled meta-jobs: {e}")


async def monitor_meta_jobs_loop(
    cleanup_interval_hours: int = 24,
    stall_check_interval_minutes: int = 10,
    recovery_check_interval_seconds: int = 30
):
    """
    Main monitoring loop for meta-jobs.

    Args:
        cleanup_interval_hours: Hours between cleanup runs
        stall_check_interval_minutes: Minutes between stall checks
        recovery_check_interval_seconds: Seconds between recovery checks (default: 30)
    """
    last_cleanup = datetime.utcnow()
    last_stall_check = datetime.utcnow()

    # Run recovery immediately on startup
    logger.info("🔄 Running initial meta-job recovery check...")
    await recover_stalled_meta_jobs()

    while True:
        try:
            # Always check for recoverable meta-jobs (every loop iteration)
            await recover_stalled_meta_jobs()

            # Check for truly stalled jobs (jobs stuck for 60+ min with no progress)
            if (datetime.utcnow() - last_stall_check).total_seconds() > stall_check_interval_minutes * 60:
                await check_stalled_meta_jobs()
                last_stall_check = datetime.utcnow()

            # Run cleanup if enough time has passed
            if (datetime.utcnow() - last_cleanup).total_seconds() > cleanup_interval_hours * 3600:
                await cleanup_old_meta_jobs()
                last_cleanup = datetime.utcnow()

            # Wait before next check (recovery runs frequently)
            await asyncio.sleep(recovery_check_interval_seconds)

        except Exception as e:
            logger.error(f"Error in meta-job monitoring loop: {e}")
            await asyncio.sleep(60)  # Wait a minute before retrying


def start_background_processor():
    """
    Start the background processor for meta-jobs.
    This should be called when the application starts.
    """
    asyncio.create_task(monitor_meta_jobs_loop())
    logger.info("Meta-job background processor started")
