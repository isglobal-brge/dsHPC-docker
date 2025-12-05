import asyncio
from fastapi import FastAPI
import logging
import sys

# Import colored logging config
from dshpc_api.config.logging_config import logger

# Create FastAPI application with colored title
app = FastAPI(
    title="dsHPC API",
    description="High-Performance Computing for DataSHIELD",
)

# Startup event to suppress default uvicorn logs
@app.on_event("startup")
async def startup_event():
    # Silence the uvicorn logger
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_logger.setLevel(logging.WARNING)
    
    # Start background cleanup task for chunked uploads
    from dshpc_api.background.cleanup_task import start_cleanup_task
    start_cleanup_task()
    
    # Start pipeline orchestrator
    from dshpc_api.background.pipeline_orchestrator import pipeline_orchestrator
    asyncio.create_task(pipeline_orchestrator())
    logger.info("\033[1;36mPipeline orchestrator started\033[0m")

    # Start meta-job background processor (recovery + monitoring)
    from dshpc_api.background.meta_job_processor import start_background_processor
    start_background_processor()
    logger.info("\033[1;36mMeta-job background processor started\033[0m")
    
    # Log only important messages at API startup
    logger.info("\033[1;32mdsHPC API service started successfully!\033[0m")

# Include API routes
from dshpc_api.api.router import router
from dshpc_api.api.pipeline import router as pipeline_router

app.include_router(router) 
app.include_router(pipeline_router) 