"""
Background task for cleaning up abandoned chunked upload sessions.

This task runs periodically to remove sessions that have been inactive
for a specified period of time.
"""

import asyncio
import logging
from datetime import datetime

from dshpc_api.services.chunked_upload_service import get_chunked_upload_service
from dshpc_api.config.settings import get_settings


logger = logging.getLogger(__name__)


async def cleanup_abandoned_sessions_task():
    """
    Background task that periodically cleans up abandoned upload sessions.
    
    This task runs every hour and removes sessions that have been inactive
    for more than 24 hours (configurable).
    """
    settings = get_settings()
    timeout_hours = getattr(settings, 'CHUNKED_UPLOAD_SESSION_TIMEOUT_HOURS', 24)
    check_interval_seconds = 3600  # Run every hour
    
    logger.info(f"Starting chunked upload cleanup task (checking every {check_interval_seconds}s, timeout: {timeout_hours}h)")
    
    while True:
        try:
            await asyncio.sleep(check_interval_seconds)
            
            logger.info("Running cleanup of abandoned chunked upload sessions...")
            
            service = get_chunked_upload_service()
            cleaned, failed = await service.cleanup_abandoned_sessions(hours=timeout_hours)
            
            if cleaned > 0 or failed > 0:
                logger.info(f"Cleanup completed: {cleaned} sessions cleaned, {failed} failed")
            else:
                logger.debug("No abandoned sessions found")
        
        except Exception as e:
            logger.error(f"Error in cleanup task: {str(e)}")
            # Continue running even if there's an error
            await asyncio.sleep(60)  # Wait a minute before retrying


def start_cleanup_task():
    """
    Start the cleanup background task.
    
    This should be called during application startup.
    """
    asyncio.create_task(cleanup_abandoned_sessions_task())
    logger.info("Chunked upload cleanup task started")

