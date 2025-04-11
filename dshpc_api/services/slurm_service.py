import httpx
from typing import Dict, Any
import asyncio

from dshpc_api.config.settings import get_settings

async def get_job(job_id: str) -> Dict[str, Any]:
    """
    Get job status from Slurm API.
    """
    settings = get_settings()
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{settings.SLURM_API_URL}/job/{job_id}")
        response.raise_for_status()
        return response.json()

async def submit_job(job_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Submit a job to Slurm API.
    """
    settings = get_settings()
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{settings.SLURM_API_URL}/submit",
            json=job_data
        )
        response.raise_for_status()
        return response.json()

async def check_jobs() -> Dict[str, Any]:
    """
    Trigger a job status check in Slurm API.
    """
    settings = get_settings()
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{settings.SLURM_API_URL}/check-jobs")
        response.raise_for_status()
        return response.json()

async def get_queue() -> Dict[str, Any]:
    """
    Get current job queue from Slurm API.
    """
    settings = get_settings()
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{settings.SLURM_API_URL}/queue")
        response.raise_for_status()
        return response.json() 