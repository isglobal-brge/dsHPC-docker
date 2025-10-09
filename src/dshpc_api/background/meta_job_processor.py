"""
Background processor for meta-jobs.
This module can be used to monitor and clean up meta-jobs if needed.
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from dshpc_api.services.db_service import get_meta_jobs_db
from dshpc_api.models.meta_job import MetaJobStatus

logger = logging.getLogger(__name__)


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
            logger.warning(f"Marking stalled meta-job {job['meta_job_id']} as failed")
            
            await meta_jobs_db.meta_jobs.update_one(
                {"meta_job_id": job["meta_job_id"]},
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
    stall_check_interval_minutes: int = 10
):
    """
    Main monitoring loop for meta-jobs.
    
    Args:
        cleanup_interval_hours: Hours between cleanup runs
        stall_check_interval_minutes: Minutes between stall checks
    """
    last_cleanup = datetime.utcnow()
    
    while True:
        try:
            # Check for stalled jobs
            await check_stalled_meta_jobs()
            
            # Run cleanup if enough time has passed
            if (datetime.utcnow() - last_cleanup).total_seconds() > cleanup_interval_hours * 3600:
                await cleanup_old_meta_jobs()
                last_cleanup = datetime.utcnow()
            
            # Wait before next check
            await asyncio.sleep(stall_check_interval_minutes * 60)
            
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
