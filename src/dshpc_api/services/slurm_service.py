import httpx
from typing import Dict, Any
import asyncio
import logging

from dshpc_api.config.settings import get_settings

logger = logging.getLogger(__name__)

# Retry configuration for connection errors
MAX_RETRIES = 5
INITIAL_BACKOFF = 2  # seconds
MAX_BACKOFF = 30  # seconds


def is_connection_error(exc: Exception) -> bool:
    """Check if exception is a retriable connection error."""
    if isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout)):
        return True
    # Also check for common network errors in the message
    error_msg = str(exc).lower()
    return any(phrase in error_msg for phrase in [
        "connect call failed",
        "connection refused",
        "no route to host",
        "network is unreachable",
        "name or service not known"
    ])


async def retry_on_connection_error(func, *args, operation_name: str = "operation", **kwargs):
    """
    Retry an async function with exponential backoff on connection errors.

    This handles temporary network failures during Docker restarts.
    """
    last_exception = None
    backoff = INITIAL_BACKOFF

    for attempt in range(MAX_RETRIES):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if not is_connection_error(e):
                # Not a connection error, raise immediately
                raise

            last_exception = e
            if attempt < MAX_RETRIES - 1:
                logger.warning(
                    f"Connection error in {operation_name} (attempt {attempt + 1}/{MAX_RETRIES}): {e}. "
                    f"Retrying in {backoff}s..."
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
            else:
                logger.error(
                    f"Connection error in {operation_name} after {MAX_RETRIES} attempts: {e}"
                )

    # All retries exhausted
    raise last_exception


async def _get_job_impl(job_id: str) -> Dict[str, Any]:
    """Internal implementation of get_job."""
    settings = get_settings()
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{settings.SLURM_API_URL}/job/{job_id}")
        response.raise_for_status()
        return response.json()


async def get_job(job_id: str) -> Dict[str, Any]:
    """
    Get job status from Slurm API with retry on connection errors.
    """
    return await retry_on_connection_error(
        _get_job_impl, job_id,
        operation_name=f"get_job({job_id[:12]}...)"
    )


async def _submit_job_impl(job_data: Dict[str, Any]) -> Dict[str, Any]:
    """Internal implementation of submit_job."""
    settings = get_settings()
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{settings.SLURM_API_URL}/submit",
            json=job_data
        )
        response.raise_for_status()
        return response.json()


async def submit_job(job_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Submit a job to Slurm API with retry on connection errors.
    """
    job_hash = job_data.get("job_hash", "unknown")[:12]
    return await retry_on_connection_error(
        _submit_job_impl, job_data,
        operation_name=f"submit_job({job_hash}...)"
    )


async def _check_jobs_impl() -> Dict[str, Any]:
    """Internal implementation of check_jobs."""
    settings = get_settings()
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{settings.SLURM_API_URL}/check-jobs")
        response.raise_for_status()
        return response.json()


async def check_jobs() -> Dict[str, Any]:
    """
    Trigger a job status check in Slurm API with retry on connection errors.
    """
    return await retry_on_connection_error(
        _check_jobs_impl,
        operation_name="check_jobs"
    )


async def _get_queue_impl() -> Dict[str, Any]:
    """Internal implementation of get_queue."""
    settings = get_settings()
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{settings.SLURM_API_URL}/queue")
        response.raise_for_status()
        return response.json()


async def get_queue() -> Dict[str, Any]:
    """
    Get current job queue from Slurm API with retry on connection errors.
    """
    return await retry_on_connection_error(
        _get_queue_impl,
        operation_name="get_queue"
    ) 