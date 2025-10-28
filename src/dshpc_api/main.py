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
    
    # Log only important messages at API startup
    logger.info("\033[1;32mdsHPC API service started successfully!\033[0m")

# Include API routes
from dshpc_api.api.router import router
app.include_router(router) 