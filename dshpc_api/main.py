import asyncio
from fastapi import FastAPI
import logging

from dshpc_api.api.router import router

# Create FastAPI application
app = FastAPI(title="dsHPC API")

# Include API routes
app.include_router(router) 