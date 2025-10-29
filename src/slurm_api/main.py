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
    # Create background task with error protection
    # This ensures the API starts even if the task has initial issues
    async def safe_background_task():
        try:
            await check_job_status()
        except Exception as e:
            logging.error(f"Critical error in background task: {e}")
            # Task will end but API continues
    
    asyncio.create_task(safe_background_task())
    logging.info("Background job checker started")

@app.get("/health")
async def health_check():
    """Simple health check endpoint."""
    return {"status": "ok"} 