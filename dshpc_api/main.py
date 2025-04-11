import asyncio
from fastapi import FastAPI
import logging

from dshpc_api.api.router import router

# Create FastAPI application
app = FastAPI(title="dsHPC Main API")

# Include API routes
app.include_router(router)

@app.get("/health")
async def health_check():
    """Simple health check endpoint."""
    return {"status": "ok"} 