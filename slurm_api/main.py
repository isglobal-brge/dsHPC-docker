import asyncio
from fastapi import FastAPI
import logging

from slurm_api.api.endpoints import router
from slurm_api.background.tasks import check_job_status

# Create FastAPI application
app = FastAPI(title="Slurm Job Submission API")

# Include API routes
app.include_router(router)

@app.on_event("startup")
async def startup_event():
    """Start the background task on application startup."""
    asyncio.create_task(check_job_status())

@app.get("/health")
async def health_check():
    """Simple health check endpoint."""
    return {"status": "ok"} 