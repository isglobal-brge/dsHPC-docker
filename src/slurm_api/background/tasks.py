import asyncio
import os
import concurrent.futures

from slurm_api.config.db_config import jobs_collection, meta_jobs_collection, pipelines_collection
from slurm_api.config.logging_config import logger
from slurm_api.models.job import JobStatus
from slurm_api.services.slurm_service import get_active_jobs, get_job_final_state
from slurm_api.services.job_service import process_job_output, process_pending_file_upload
from slurm_api.utils.db_utils import update_job_status

# Shared executor for background tasks to avoid creating/destroying executors repeatedly
# Using max_workers=2 to allow some parallelism but prevent resource exhaustion
_background_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=2,
    thread_name_prefix="bg_task_"
)


def cascade_reactivate_pipelines(meta_job_hash: str) -> int:
    """
    Reactivate all pipelines that reference the given meta_job_hash.
    Called when a meta-job is reactivated due to service failure recovery.
    Returns the number of pipelines reactivated.
    """
    reactivated_count = 0

    # Find pipelines that have this meta_job_hash in any of their nodes and are cancelled
    pipelines = pipelines_collection.find({
        "nodes": {"$exists": True},
        "status": "cancelled"
    })

    for pipeline in pipelines:
        pipeline_hash = pipeline.get("pipeline_hash")
        nodes = pipeline.get("nodes", {})

        # Check if any node references this meta_job_hash
        for node_id, node in nodes.items():
            if node.get("meta_job_hash") == meta_job_hash:
                result = pipelines_collection.update_one(
                    {"pipeline_hash": pipeline_hash},
                    {"$set": {"status": "running", "error": None}}
                )
                if result.modified_count > 0:
                    logger.info(f"🔄 Cascade reactivated pipeline {pipeline_hash} (meta-job {meta_job_hash} was reactivated)")
                    reactivated_count += 1
                break  # Pipeline already updated, no need to check more nodes

    return reactivated_count


def cascade_reactivate_meta_jobs(job_hash: str) -> tuple[int, int]:
    """
    Reactivate all meta-jobs that reference the given job_hash.
    Also cascade-reactivates pipelines that reference those meta-jobs.
    Called when a job is reactivated due to service failure recovery.
    Returns tuple of (meta_jobs_reactivated, pipelines_reactivated).
    """
    meta_reactivated_count = 0
    pipeline_reactivated_count = 0

    # Find meta-jobs that have this job_hash anywhere in their chain and are cancelled
    meta_jobs = meta_jobs_collection.find({
        "chain.job_hash": job_hash,
        "status": "cancelled"
    })

    for meta_job in meta_jobs:
        meta_job_hash = meta_job["meta_job_hash"]

        # Mark the meta-job as running and clear any old error
        result = meta_jobs_collection.update_one(
            {"meta_job_hash": meta_job_hash},
            {"$set": {"status": "running", "error": None}}
        )
        if result.modified_count > 0:
            logger.info(f"🔄 Cascade reactivated meta-job {meta_job_hash} (job {job_hash} was reactivated)")
            meta_reactivated_count += 1

            # Cascade reactivate pipelines that reference this meta-job
            pipeline_reactivated_count += cascade_reactivate_pipelines(meta_job_hash)

    return meta_reactivated_count, pipeline_reactivated_count


def cascade_cancel_pipelines(meta_job_hash: str) -> int:
    """
    Mark all pipelines that reference the given meta_job_hash as cancelled.
    Returns the number of pipelines cancelled.
    """
    cancelled_count = 0

    # Find pipelines that have this meta_job_hash in any of their nodes
    pipelines = pipelines_collection.find({
        "nodes": {"$exists": True},
        "status": {"$nin": ["cancelled", "completed", "failed"]}
    })

    for pipeline in pipelines:
        pipeline_hash = pipeline.get("pipeline_hash")
        nodes = pipeline.get("nodes", {})

        # Check if any node references this meta_job_hash
        for node_id, node in nodes.items():
            if node.get("meta_job_hash") == meta_job_hash:
                result = pipelines_collection.update_one(
                    {"pipeline_hash": pipeline_hash},
                    {"$set": {"status": "cancelled"}}
                )
                if result.modified_count > 0:
                    logger.info(f"🚫 Cascade cancelled pipeline {pipeline_hash} (referenced cancelled meta-job)")
                    cancelled_count += 1
                break  # Pipeline already updated, no need to check more nodes

    return cancelled_count


def cascade_cancel_meta_jobs(job_hash: str) -> tuple[int, int]:
    """
    Mark all meta-jobs that reference the given job_hash as cancelled.
    Also cascade-cancels pipelines that reference those meta-jobs.
    Returns tuple of (meta_jobs_cancelled, pipelines_cancelled).
    """
    meta_cancelled_count = 0
    pipeline_cancelled_count = 0

    # Find meta-jobs that have this job_hash anywhere in their chain and are not already cancelled
    meta_jobs = meta_jobs_collection.find({
        "chain.job_hash": job_hash,
        "status": {"$nin": ["cancelled", "completed", "failed"]}
    })

    for meta_job in meta_jobs:
        meta_job_hash = meta_job["meta_job_hash"]

        # First cascade-cancel pipelines that reference this meta-job
        pipeline_cancelled_count += cascade_cancel_pipelines(meta_job_hash)

        # Then mark the meta-job as cancelled
        result = meta_jobs_collection.update_one(
            {"meta_job_hash": meta_job_hash},
            {"$set": {"status": "cancelled"}}
        )
        if result.modified_count > 0:
            logger.info(f"🚫 Cascade cancelled meta-job {meta_job_hash} (referenced cancelled job)")
            meta_cancelled_count += 1

    return meta_cancelled_count, pipeline_cancelled_count


def _check_cancelled_jobs_sync():
    """
    Synchronous implementation of cancelled job detection.
    Detect and mark jobs that were cancelled or killed externally.

    Handles three cases:
    1. Jobs that disappeared from Slurm queue with no output files (scancel)
    2. Jobs that failed with "Exit code file not found" (killed before completion)
    3. Jobs killed by signals: exit code 137 (SIGKILL/OOM), 139 (SIGSEGV), 143 (SIGTERM)

    These are NOT logical errors in the jobs themselves, so they are marked as CANCELLED
    to allow manual resubmission. When a job is cancelled, also cascade-cancels
    any meta-jobs and pipelines that reference it.
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
                    # No output files = job disappeared without trace (service restart, scancel, etc.)
                    # Mark with specific error so auto-retry can identify service failures
                    logger.warning(f"Detected cancelled job {job_hash} (slurm_id={slurm_id}) - no output files found")
                    result = jobs_collection.update_one(
                        {"job_hash": job_hash},
                        {"$set": {
                            "status": JobStatus.CANCELLED,
                            "error": "Job disappeared from Slurm without output (likely service restart or scancel)"
                        }}
                    )
                    if result.modified_count > 0:
                        logger.info(f"🚫 Marked job {job_hash} as CANCELLED")
                        cancelled_count += 1
                        # Cascade cancel meta-jobs and pipelines
                        meta_cancelled, pipeline_cancelled = cascade_cancel_meta_jobs(job_hash)
                        meta_job_count += meta_cancelled
                        pipeline_count += pipeline_cancelled

        # Part 2: Mark failed jobs where the exit code file was not found
        # This means the job was killed externally (scancel, OOM, timeout, container restart)
        # before it could write its exit code - NOT a logical error in the job itself
        # These jobs are marked as CANCELLED so they can be manually resubmitted
        failed_external_jobs = jobs_collection.find({
            "status": {"$in": [JobStatus.FAILED, "FA", "F"]},
            "error": {"$regex": "Exit code file not found"}
        })

        for job in failed_external_jobs:
            job_hash = job["job_hash"]
            logger.warning(f"Found externally-killed job {job_hash} (no exit code file)")
            # Job was killed before writing exit code - could be service restart
            # Mark with error that allows auto-retry
            result = jobs_collection.update_one(
                {"job_hash": job_hash},
                {"$set": {
                    "status": JobStatus.CANCELLED,
                    "error": "Job killed before completion - no exit code (likely service restart)"
                }}
            )
            if result.modified_count > 0:
                logger.info(f"🚫 Marked killed job {job_hash} as CANCELLED")
                cancelled_count += 1
                # Cascade cancel meta-jobs and pipelines
                meta_cancelled, pipeline_cancelled = cascade_cancel_meta_jobs(job_hash)
                meta_job_count += meta_cancelled
                pipeline_count += pipeline_cancelled

        # Part 3: Mark jobs killed by signals (exit codes 137, 139, 143, etc.)
        # These are jobs killed by SIGKILL (137=OOM), SIGSEGV (139), SIGTERM (143)
        # Exit codes 128+ indicate the process was killed by a signal (128 + signal_number)
        #
        # OOM RETRY MECHANISM:
        # - OOM kills (137) can be auto-retried with MORE MEMORY (up to 2 times)
        # - Other signals are NOT auto-retried - they need manual intervention
        signal_killed_jobs = jobs_collection.find({
            "status": {"$in": [JobStatus.FAILED, "FA", "F"]},
            "$or": [
                {"error": {"$regex": "non-zero code: 137"}},  # SIGKILL (OOM killer)
                {"error": {"$regex": "non-zero code: 139"}},  # SIGSEGV
                {"error": {"$regex": "non-zero code: 143"}},  # SIGTERM
            ]
        })

        for job in signal_killed_jobs:
            job_hash = job["job_hash"]
            error_msg = job.get("error", "")
            logger.warning(f"Found signal-killed job {job_hash}: {error_msg}")

            # OOM kills get special treatment: auto-retry with more memory
            if "137" in error_msg:
                from slurm_api.services.job_service import get_slurm_node_memory

                oom_retry_count = job.get("oom_retry_count", 0)
                current_memory = job.get("memory_override_mb") or job.get("last_memory_mb") or 4096  # Default 4GB

                # Get max memory dynamically from node specs (95% of node memory)
                node_memory = get_slurm_node_memory()
                max_memory = int(node_memory * 0.95) if node_memory else 16384  # Fallback 16GB if unknown

                # Check if we've already hit max memory - no point retrying
                if current_memory >= max_memory:
                    cancel_reason = f"Job killed by OOM at max memory ({current_memory}MB of {node_memory}MB available) - job requires more memory than node has"
                # Max 2 OOM retries
                elif oom_retry_count < 2:
                    # Increase memory: max(current * 1.5, current + 2048)
                    new_memory = max(int(current_memory * 1.5), current_memory + 2048)

                    # Cap to node capacity (dynamic based on actual hardware)
                    new_memory = min(new_memory, max_memory)

                    logger.info(f"🔄 OOM retry #{oom_retry_count + 1} for job {job_hash}: {current_memory}MB -> {new_memory}MB")

                    # Mark for retry with increased memory
                    result = jobs_collection.update_one(
                        {"job_hash": job_hash},
                        {"$set": {
                            "status": JobStatus.CANCELLED,
                            "error": f"OOM retry #{oom_retry_count + 1}: increasing memory from {current_memory}MB to {new_memory}MB",
                            "oom_retry_count": oom_retry_count + 1,
                            "memory_override_mb": new_memory,
                            "last_memory_mb": current_memory
                        }}
                    )
                    if result.modified_count > 0:
                        logger.info(f"🔄 Queued OOM retry for job {job_hash}")
                        cancelled_count += 1
                        # Don't cascade cancel - this job will be retried
                    continue
                else:
                    # Max retries exceeded
                    cancel_reason = f"Job killed by OOM after {oom_retry_count} retries (exit code 137) - needs manual intervention"
            elif "139" in error_msg:
                cancel_reason = "Job crashed with SIGSEGV (exit code 139) - needs debugging"
            elif "143" in error_msg:
                cancel_reason = "Job terminated by SIGTERM (exit code 143) - manual termination"
            else:
                cancel_reason = f"Job killed by signal: {error_msg}"

            result = jobs_collection.update_one(
                {"job_hash": job_hash},
                {"$set": {
                    "status": JobStatus.CANCELLED,
                    "error": cancel_reason
                }}
            )
            if result.modified_count > 0:
                logger.info(f"🚫 Marked signal-killed job {job_hash} as CANCELLED")
                cancelled_count += 1
                # Cascade cancel meta-jobs and pipelines
                meta_cancelled, pipeline_cancelled = cascade_cancel_meta_jobs(job_hash)
                meta_job_count += meta_cancelled
                pipeline_count += pipeline_cancelled

        # Part 4: Mark jobs that failed with empty error
        # These are jobs that failed without a clear reason:
        # - May have been interrupted by service restart
        # - May have been killed externally without leaving error traces
        # They should be CANCELLED to allow manual resubmission
        empty_error_failed_jobs = jobs_collection.find({
            "status": {"$in": [JobStatus.FAILED, "FA", "F"]},
            "$or": [
                {"error": ""},
                {"error": None},
                {"error": {"$exists": False}},
            ]
        })

        for job in empty_error_failed_jobs:
            job_hash = job["job_hash"]
            logger.warning(f"Found failed job with empty error {job_hash} - marking as CANCELLED")
            result = jobs_collection.update_one(
                {"job_hash": job_hash},
                {"$set": {
                    "status": JobStatus.CANCELLED,
                    "error": "Job failed without error details (likely interrupted by service restart)"
                }}
            )
            if result.modified_count > 0:
                logger.info(f"🚫 Marked empty-error job {job_hash} as CANCELLED")
                cancelled_count += 1
                # Cascade cancel meta-jobs and pipelines
                meta_cancelled, pipeline_cancelled = cascade_cancel_meta_jobs(job_hash)
                meta_job_count += meta_cancelled
                pipeline_count += pipeline_cancelled

        if cancelled_count > 0:
            logger.info(f"🚫 Marked {cancelled_count} jobs as CANCELLED")
        if meta_job_count > 0:
            logger.info(f"🚫 Cascade cancelled {meta_job_count} meta-jobs")
        if pipeline_count > 0:
            logger.info(f"🚫 Cascade cancelled {pipeline_count} pipelines")

    except Exception as e:
        logger.error(f"Error checking cancelled jobs: {e}")

def _check_orphaned_meta_jobs_sync():
    """
    Synchronous implementation of orphaned meta-job check.
    Find and cancel meta-jobs whose current job no longer exists in the database
    OR whose current job has been cancelled.
    This handles the case where a job was cancelled but its meta-job wasn't cascade-cancelled.
    Also cascade-cancels pipelines that reference those meta-jobs.
    """
    try:
        cancelled_count = 0
        pipeline_count = 0

        # Find meta-jobs that are running (not completed/failed/cancelled)
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
                    # Check if this job exists and its status
                    job = jobs_collection.find_one({"job_hash": current_job_hash})
                    should_cancel = False

                    if not job:
                        logger.warning(f"Found orphaned meta-job {meta_job_hash} - current job {current_job_hash} no longer exists")
                        should_cancel = True
                    elif job.get("status") in [JobStatus.CANCELLED, "CA"]:
                        logger.warning(f"Found meta-job {meta_job_hash} with cancelled job {current_job_hash}")
                        should_cancel = True

                    if should_cancel:
                        # First cascade-cancel pipelines that reference this meta-job
                        pipeline_count += cascade_cancel_pipelines(meta_job_hash)

                        # Then mark the meta-job as cancelled
                        result = meta_jobs_collection.update_one(
                            {"meta_job_hash": meta_job_hash},
                            {"$set": {"status": "cancelled"}}
                        )
                        if result.modified_count > 0:
                            logger.info(f"🚫 Marked orphaned meta-job {meta_job_hash} as cancelled")
                            cancelled_count += 1

        if cancelled_count > 0:
            logger.info(f"🚫 Marked {cancelled_count} orphaned meta-jobs as cancelled")
        if pipeline_count > 0:
            logger.info(f"🚫 Cascade cancelled {pipeline_count} pipelines")

    except Exception as e:
        logger.error(f"Error checking orphaned meta-jobs: {e}")


def _check_orphaned_jobs_sync():
    """
    Synchronous implementation of orphaned job processing.

    This is run in a thread pool to avoid blocking the API event loop.
    All operations here (MongoDB, file downloads, Slurm submission) can block.
    """
    from slurm_api.services.job_service import prepare_job_script, is_files_db_available
    from slurm_api.services.slurm_service import submit_slurm_job
    from slurm_api.models.job import JobSubmission
    from datetime import datetime, timedelta

    try:
        # CRITICAL: Check if files DB is available BEFORE processing orphaned jobs
        # This prevents blocking for 30s per job when files DB is down
        if not is_files_db_available():
            logger.warning("⏸️ Files DB unavailable - skipping orphaned job retry to prevent API blocking")
            return

        two_minutes_ago = datetime.utcnow() - timedelta(minutes=2)

        # Patterns that indicate service/infrastructure failure
        # ONLY these jobs should be auto-retried because the failure wasn't caused by the job itself
        #
        # NOT auto-retried (but can be manually resubmitted via API):
        # - "scancel" / "cancelled by user" - user intentionally cancelled
        # - "OOM" / "SIGKILL" / "exit code 137" - job exceeded memory (needs manual intervention)
        # - "SIGSEGV" / "exit code 139" - job crashed (needs debugging)
        # - "SIGTERM" / "exit code 143" - job was terminated
        #
        # Auto-retried:
        # - Service unavailable / connection errors - infrastructure issue
        # - Memory specification errors - will work now with dynamic memory
        # - Submission errors - temporary service issue
        service_failure_patterns = [
            # Submission/service errors
            "service unavailable",
            "Submission failed",
            "Submission error",
            "Connection refused",
            "Connection reset",
            "Internal server error",
            # Infrastructure errors that will work now with dynamic memory
            "Memory specification can not be satisfied",
            # Jobs that disappeared during service restart
            "failed without error details",
            "likely service restart",
            "no exit code",
            "disappeared from Slurm",
            # OOM retry mechanism - jobs marked for retry with increased memory
            "OOM retry #",
            # Memory Guardian cancellations (system maintenance)
            "system maintenance",
            "memory optimization",
            # MongoDB/network transient failures (can retry)
            "Name or service not known",
            "Timeout:",
            "AutoReconnect",
            # Script-requested retry (exit code 75 = EX_TEMPFAIL)
            "exit code 75",
            "transient error",
            # Signal kills that are retriable (OOM, SIGTERM)
            "SIGKILL/OOM",
            "exit code 137",
            "exit code 143",
            "will retry",
        ]

        # Build regex for service failures ONLY
        service_failure_regex = "|".join(service_failure_patterns)

        # Find jobs that need submission:
        # 1. PENDING with no slurm_id (never submitted)
        # 2. CANCELLED due to service failure (can retry)
        orphaned_jobs = jobs_collection.find({
            "$or": [
                # Case 1: Pending jobs never submitted
                {
                    "status": JobStatus.PENDING,
                    "slurm_id": None,
                    "$or": [
                        {"last_submission_attempt": None},
                        {"last_submission_attempt": {"$lt": two_minutes_ago}}
                    ]
                },
                # Case 2: Cancelled due to service failure (retry them)
                {
                    "status": JobStatus.CANCELLED,
                    "error": {"$regex": service_failure_regex, "$options": "i"},
                    "$or": [
                        {"last_submission_attempt": None},
                        {"last_submission_attempt": {"$lt": two_minutes_ago}}
                    ]
                }
            ]
        }).sort("created_at", 1).limit(10)  # Process max 10 at a time, oldest first (FIFO)
        
        for job in orphaned_jobs:
            job_hash = job["job_hash"]
            previous_status = job.get("status")
            attempts = job.get("submission_attempts", 0)

            # If job was cancelled, reset attempts counter for fresh start
            if previous_status == JobStatus.CANCELLED:
                attempts = 0
                logger.info(f"🔄 Reactivating cancelled job {job_hash} for retry...")

            logger.warning(f"Found orphaned job {job_hash} (attempt #{attempts + 1}), submitting to Slurm...")

            try:
                # Update status to PENDING and track submission attempt
                jobs_collection.update_one(
                    {"job_hash": job_hash},
                    {"$set": {
                        "status": JobStatus.PENDING,  # Reset to PENDING
                        "slurm_id": None,  # Clear any old slurm_id
                        "error": None,  # Clear previous error
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
                    # Update job with slurm_id and set to RUNNING
                    jobs_collection.update_one(
                        {"job_hash": job_hash},
                        {"$set": {
                            "status": JobStatus.RUNNING,
                            "slurm_id": slurm_id,
                            "last_submission_attempt": None  # Clear since now submitted
                        }}
                    )
                    logger.info(f"✅ Successfully submitted job {job_hash} to Slurm with ID {slurm_id}")

                    # Cascade reactivate: if this job was reactivated, also reactivate meta-jobs and pipelines
                    if previous_status == JobStatus.CANCELLED:
                        meta_count, pipeline_count = cascade_reactivate_meta_jobs(job_hash)
                        if meta_count > 0:
                            logger.info(f"  ↳ Cascade reactivated {meta_count} meta-job(s), {pipeline_count} pipeline(s)")
                else:
                    # Keep as PENDING for retry, but log the failure
                    logger.warning(f"⚠️ Could not submit job {job_hash} (attempt #{attempts + 1}): {message}")
                    # Mark as CANCELLED after 5 attempts
                    if attempts >= 4:  # 5th attempt failed
                        update_job_status(job_hash, JobStatus.CANCELLED, error=f"Submission failed after {attempts + 1} attempts: {message}")
                        logger.error(f"❌ Job {job_hash} marked as CANCELLED after {attempts + 1} submission attempts")
                    
            except Exception as e:
                # Log error but allow retries
                logger.error(f"❌ Error retrying job {job_hash}: {e}")
                # Mark as CANCELLED after 5 attempts
                if attempts >= 4:
                    update_job_status(job_hash, JobStatus.CANCELLED, error=f"Submission error after {attempts + 1} attempts: {str(e)}")
                    logger.error(f"❌ Job {job_hash} marked as CANCELLED after {attempts + 1} submission attempts")
                
    except Exception as e:
        # Never let this crash the background task
        logger.error(f"Error checking orphaned jobs (will retry next iteration): {e}")


async def check_orphaned_jobs():
    """
    Find and retry jobs that need (re)submission to Slurm.

    This handles:
    1. PENDING jobs that were never submitted (no slurm_id)
    2. CANCELLED jobs that failed due to service issues (not job logic errors)

    Jobs cancelled due to service unavailability can be retried automatically
    because the failure wasn't caused by the job itself.

    IMPORTANT: This runs the blocking operations in a thread pool to avoid
    blocking the FastAPI event loop. Without this, the API becomes unresponsive
    while jobs are being prepared/submitted.
    """
    # Run the blocking operations in the shared thread pool
    # This prevents MongoDB/file operations from blocking the API
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_background_executor, _check_orphaned_jobs_sync)


def _check_stale_cancelled_meta_jobs_sync():
    """
    Synchronous implementation of stale cancelled/failed meta-job check.
    Find and reactivate meta-jobs that were cancelled/failed due to transient errors.

    This handles the case where:
    1. dshpc_api died while processing a meta-job (marked as cancelled)
    2. A transient network error occurred (marked as failed with network error)

    Only reactivates if ALL existing jobs in the chain are OK (no real failures).
    """
    try:
        reactivated_count = 0
        pipeline_count = 0

        # Transient network errors that should trigger auto-retry
        network_error_patterns = [
            "Name or service not known",
            "Timeout:",
            "AutoReconnect",
            "Connection refused",
            "Connection reset",
            "ServerSelectionTimeoutError",
        ]

        # Find cancelled meta-jobs OR failed meta-jobs with network errors
        stale_meta_jobs = meta_jobs_collection.find({
            "$or": [
                {"status": "cancelled"},
                {
                    "status": "failed",
                    "error": {"$regex": "|".join(network_error_patterns), "$options": "i"}
                }
            ]
        })

        for meta_job in stale_meta_jobs:
            meta_job_hash = meta_job["meta_job_hash"]
            chain = meta_job.get("chain", [])

            if not chain:
                continue

            # Check ALL jobs in the chain
            has_real_failure = False
            has_active_or_completed_job = False

            for step in chain:
                job_hash = step.get("job_hash")
                if not job_hash:
                    # Job not yet created for this step - that's ok, will be created when resumed
                    continue

                job = jobs_collection.find_one({"job_hash": job_hash})
                if not job:
                    continue

                job_status = job.get("status")
                job_error = job.get("error", "") or ""

                # Check for real failures (not service failures)
                if job_status in [JobStatus.CANCELLED, JobStatus.FAILED, "CA", "F", "FA"]:
                    # Real job failures - OOM, crashes, etc. - should NOT be reactivated
                    real_failure_patterns = [
                        "OOM", "SIGKILL", "SIGSEGV", "SIGTERM",
                        "137", "139", "143",
                        "exit code", "crashed", "killed"
                    ]
                    if any(pattern.lower() in job_error.lower() for pattern in real_failure_patterns):
                        has_real_failure = True
                        break

                # Track if we have any active or completed jobs
                if job_status in [JobStatus.COMPLETED, JobStatus.RUNNING, JobStatus.PENDING, "CD", "R", "PD"]:
                    has_active_or_completed_job = True

            # Only reactivate if:
            # 1. No real job failures
            # 2. At least one job is active or completed
            if not has_real_failure and has_active_or_completed_job:
                prev_status = meta_job.get("status")
                prev_error = meta_job.get("error", "")[:50] if meta_job.get("error") else ""
                result = meta_jobs_collection.update_one(
                    {"meta_job_hash": meta_job_hash},
                    {"$set": {"status": "running", "error": None}}
                )
                if result.modified_count > 0:
                    logger.info(f"🔄 Reactivated stale meta-job {meta_job_hash} (was {prev_status}: {prev_error}...)")
                    reactivated_count += 1

                    # Cascade reactivate pipelines
                    pipeline_count += cascade_reactivate_pipelines(meta_job_hash)

        if reactivated_count > 0:
            logger.info(f"🔄 Reactivated {reactivated_count} stale meta-job(s), {pipeline_count} pipeline(s)")

    except Exception as e:
        logger.error(f"Error checking stale cancelled/failed meta-jobs: {e}")


def _check_pending_file_uploads_sync():
    """
    Synchronous implementation of pending file upload processing.

    This is run in a thread pool to avoid blocking the API event loop.
    File uploads to MongoDB can be slow.
    """
    from slurm_api.services.job_service import is_files_db_available

    try:
        # CRITICAL: Check if files DB is available BEFORE processing uploads
        # This prevents blocking when files DB is down
        if not is_files_db_available():
            logger.debug("⏸️ Files DB unavailable - skipping pending file uploads")
            return

        # Find jobs with pending file uploads (limit to 10 at a time)
        pending_jobs = jobs_collection.find({
            "status": JobStatus.COMPLETED,
            "output_file_upload_pending": True
        }).limit(10)

        processed_count = 0
        success_count = 0

        for job in pending_jobs:
            job_hash = job["job_hash"]
            processed_count += 1

            if process_pending_file_upload(job_hash):
                success_count += 1

        if processed_count > 0:
            logger.info(f"📤 Processed {processed_count} pending file uploads, {success_count} successful")

    except Exception as e:
        logger.error(f"Error checking pending file uploads: {e}")


async def check_pending_file_uploads():
    """
    Process pending file uploads that failed when jobs completed.

    This handles the case where files DB was unavailable when a job completed:
    - The job output was saved to persistent storage
    - The job was marked with output_file_upload_pending: true
    - We now retry uploading to files DB

    This ensures job outputs are eventually uploaded as files for deduplication/caching.

    IMPORTANT: This runs in a thread pool to avoid blocking the API.
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_background_executor, _check_pending_file_uploads_sync)


def _check_completed_jobs_missing_output_sync():
    """
    Synchronous implementation of missing output check.

    This is run in a thread pool to avoid blocking the API event loop.
    MongoDB operations can block when DB is slow/overloaded.
    """
    from slurm_api.services.job_service import is_files_db_available, PENDING_UPLOADS_DIR

    try:
        # CRITICAL: Only run this check if files DB is available
        # If files DB is down, we can't know if the output is missing or just unreachable
        if not is_files_db_available():
            logger.debug("⏸️ Files DB unavailable - skipping missing output check")
            return

        # Find COMPLETED jobs that should have output but don't
        # - output_file_hash is null/missing (not in files DB)
        # - output_file_upload_pending is not true (not waiting to be uploaded)
        completed_jobs_without_output = jobs_collection.find({
            "status": JobStatus.COMPLETED,
            "$or": [
                {"output_file_hash": None},
                {"output_file_hash": {"$exists": False}}
            ],
            "$and": [
                {"$or": [
                    {"output_file_upload_pending": {"$ne": True}},
                    {"output_file_upload_pending": {"$exists": False}}
                ]}
            ]
        }).limit(5)  # Process max 5 at a time to avoid overload

        recompute_count = 0

        for job in completed_jobs_without_output:
            job_hash = job["job_hash"]

            # Double-check: is there a pending upload file?
            pending_path = os.path.join(PENDING_UPLOADS_DIR, f"{job_hash}.json")
            if os.path.exists(pending_path):
                # There IS a pending file - mark it for upload retry instead
                logger.info(f"🔄 Found pending upload file for {job_hash}, marking for retry")
                jobs_collection.update_one(
                    {"job_hash": job_hash},
                    {"$set": {"output_file_upload_pending": True}}
                )
                continue

            # No output anywhere - need to recompute
            # Reset job to PENDING so it will be resubmitted
            logger.warning(f"🔄 COMPLETED job {job_hash} has no output anywhere - marking for recomputation")

            result = jobs_collection.update_one(
                {"job_hash": job_hash},
                {"$set": {
                    "status": JobStatus.PENDING,
                    "slurm_id": None,
                    "error": None,
                    "output_file_hash": None,
                    "output_file_upload_pending": False,
                    "recomputation_reason": "Output missing from files DB and no pending upload found"
                },
                "$inc": {"recomputation_count": 1}}
            )

            if result.modified_count > 0:
                recompute_count += 1
                logger.info(f"🔄 Job {job_hash} reset to PENDING for recomputation")

                # Also need to cascade-reactivate any meta-jobs/pipelines that depend on this job
                meta_reactivated, pipeline_reactivated = cascade_reactivate_meta_jobs(job_hash)
                if meta_reactivated > 0 or pipeline_reactivated > 0:
                    logger.info(f"  ↳ Cascade reactivated {meta_reactivated} meta-job(s), {pipeline_reactivated} pipeline(s)")

        if recompute_count > 0:
            logger.info(f"🔄 Marked {recompute_count} job(s) for recomputation due to missing output")

    except Exception as e:
        logger.error(f"Error checking completed jobs with missing output: {e}")


async def check_completed_jobs_missing_output():
    """
    Find and recompute COMPLETED jobs that have no output anywhere.

    This is a LAST RESORT recovery mechanism for jobs that:
    1. Are marked as COMPLETED (succeeded)
    2. Have NO output_file_hash (not in files DB)
    3. Have NO pending upload file in /persistent/pending_uploads/
    4. Files DB IS available (so it's not a connectivity issue)

    These jobs completed but their output was lost somehow. The only way to recover
    is to recompute them by resetting to PENDING status.

    This check runs AFTER check_pending_file_uploads to ensure we don't recompute
    jobs that just haven't been uploaded yet.

    IMPORTANT: This runs in a thread pool to avoid blocking the API.
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_background_executor, _check_completed_jobs_missing_output_sync)


def _check_stale_cancelled_pipelines_sync():
    """
    Synchronous implementation of stale cancelled pipeline check.
    Find and reactivate pipelines that were cancelled but their meta-jobs are running/completed.

    This handles the case where dshpc_api died while processing a pipeline:
    - The meta-jobs are running or completed
    - But the pipeline got marked as cancelled because dshpc_api wasn't there to track it

    These pipelines should be reactivated so dshpc_api can resume processing them.
    """
    try:
        reactivated_count = 0

        # Find cancelled pipelines
        cancelled_pipelines = pipelines_collection.find({
            "status": "cancelled"
        })

        for pipeline in cancelled_pipelines:
            pipeline_hash = pipeline["pipeline_hash"]
            nodes = pipeline.get("nodes", {})

            if not nodes:
                continue

            # Check if any meta-job in the pipeline is running or completed
            should_reactivate = False
            has_failed_meta_job = False

            for node_id, node in nodes.items():
                meta_job_hash = node.get("meta_job_hash")
                if not meta_job_hash:
                    continue

                meta_job = meta_jobs_collection.find_one({"meta_job_hash": meta_job_hash})
                if not meta_job:
                    continue

                meta_status = meta_job.get("status")

                # If any meta-job is running or completed, pipeline should be active
                if meta_status in ["running", "pending", "completed"]:
                    should_reactivate = True

                # If meta-job failed with real error (not service failure), don't reactivate
                if meta_status == "failed":
                    has_failed_meta_job = True

            # Reactivate if we have active meta-jobs and no real failures
            if should_reactivate and not has_failed_meta_job:
                result = pipelines_collection.update_one(
                    {"pipeline_hash": pipeline_hash},
                    {"$set": {"status": "running", "error": None}}
                )
                if result.modified_count > 0:
                    logger.info(f"🔄 Reactivated stale pipeline {pipeline_hash} (meta-jobs active but pipeline was cancelled)")
                    reactivated_count += 1

        if reactivated_count > 0:
            logger.info(f"🔄 Reactivated {reactivated_count} stale pipeline(s)")

    except Exception as e:
        logger.error(f"Error checking stale cancelled pipelines: {e}")


def _check_jobs_once_sync():
    """
    Synchronous implementation of job status check.

    This runs ALL blocking operations (MongoDB, subprocess, file I/O) in a single
    synchronous function that is called from the async wrapper via run_in_executor.

    This ensures the FastAPI event loop is NEVER blocked by background tasks.
    """
    from slurm_api.services.job_service import is_files_db_available, PENDING_UPLOADS_DIR

    try:
        # ========== Part 1: Check for cancelled jobs ==========
        _check_cancelled_jobs_sync()

        # ========== Part 2: Check for orphaned meta-jobs ==========
        _check_orphaned_meta_jobs_sync()

        # ========== Part 3: Check for orphaned jobs and retry them ==========
        _check_orphaned_jobs_sync()

        # ========== Part 4: Check for stale cancelled meta-jobs ==========
        _check_stale_cancelled_meta_jobs_sync()

        # ========== Part 5: Check for stale cancelled pipelines ==========
        _check_stale_cancelled_pipelines_sync()

        # ========== Part 6: Check for pending file uploads ==========
        _check_pending_file_uploads_sync()

        # ========== Part 7: Check for completed jobs missing output ==========
        _check_completed_jobs_missing_output_sync()

        # ========== Part 8: Update active job statuses from Slurm ==========
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


async def check_jobs_once():
    """
    Run a single iteration of job status check.

    IMPORTANT: All blocking operations (MongoDB queries, subprocess calls, file I/O)
    are executed in a thread pool to avoid blocking the FastAPI event loop.
    This ensures the API remains responsive while background tasks are running.
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_background_executor, _check_jobs_once_sync)
        
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