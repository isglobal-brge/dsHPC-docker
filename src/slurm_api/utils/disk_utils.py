"""
Disk space monitoring utilities.

Provides functions to check available disk space and prevent operations
when disk is critically low. This prevents data loss and system instability.
"""
import os
import shutil
from typing import Tuple, Optional
from slurm_api.config.logging_config import logger


# Minimum required free space in bytes (default: 1GB)
MIN_FREE_SPACE_BYTES = 1 * 1024 * 1024 * 1024  # 1 GB

# Warning threshold (default: 5GB)
WARNING_FREE_SPACE_BYTES = 5 * 1024 * 1024 * 1024  # 5 GB

# Critical threshold for immediate action (default: 500MB)
CRITICAL_FREE_SPACE_BYTES = 500 * 1024 * 1024  # 500 MB

# Path to check (usually /tmp for job outputs, but we check root filesystem)
CHECK_PATHS = ["/tmp", "/"]


def get_disk_space(path: str = "/") -> Tuple[int, int, int]:
    """
    Get disk space information for the given path.

    Args:
        path: Path to check (defaults to root filesystem)

    Returns:
        Tuple of (total_bytes, used_bytes, free_bytes)
    """
    try:
        stat = shutil.disk_usage(path)
        return stat.total, stat.used, stat.free
    except Exception as e:
        logger.error(f"Error getting disk space for {path}: {e}")
        # Return safe defaults that will trigger warnings
        return 0, 0, 0


def check_disk_space(path: str = "/tmp") -> Tuple[bool, str, int]:
    """
    Check if there's enough disk space for operations.

    Args:
        path: Path to check

    Returns:
        Tuple of (is_ok, message, free_bytes)
        - is_ok: True if space is sufficient, False if critical
        - message: Human readable status message
        - free_bytes: Available free space in bytes
    """
    total, used, free = get_disk_space(path)

    if free == 0 and total == 0:
        return False, "Unable to determine disk space", 0

    free_gb = free / (1024 * 1024 * 1024)
    total_gb = total / (1024 * 1024 * 1024)
    used_percent = (used / total * 100) if total > 0 else 100

    if free < CRITICAL_FREE_SPACE_BYTES:
        return False, f"CRITICAL: Only {free_gb:.2f}GB free ({used_percent:.1f}% used)", free

    if free < MIN_FREE_SPACE_BYTES:
        return False, f"LOW SPACE: Only {free_gb:.2f}GB free ({used_percent:.1f}% used)", free

    if free < WARNING_FREE_SPACE_BYTES:
        return True, f"WARNING: {free_gb:.2f}GB free ({used_percent:.1f}% used)", free

    return True, f"OK: {free_gb:.2f}GB free ({used_percent:.1f}% used)", free


def is_disk_space_sufficient(min_bytes: int = None, path: str = "/tmp") -> bool:
    """
    Quick check if disk space is sufficient.

    Args:
        min_bytes: Minimum required bytes (defaults to MIN_FREE_SPACE_BYTES)
        path: Path to check

    Returns:
        True if sufficient space available, False otherwise
    """
    if min_bytes is None:
        min_bytes = MIN_FREE_SPACE_BYTES

    _, _, free = get_disk_space(path)
    return free >= min_bytes


def get_disk_space_error() -> Optional[str]:
    """
    Get disk space error message if space is critically low.

    Returns:
        Error message if disk is critically low, None otherwise
    """
    for path in CHECK_PATHS:
        is_ok, message, free = check_disk_space(path)
        if not is_ok:
            return f"Disk space error on {path}: {message}"
    return None


def log_disk_status():
    """Log current disk space status for monitoring."""
    for path in CHECK_PATHS:
        is_ok, message, free = check_disk_space(path)
        if not is_ok:
            logger.error(f"Disk space {path}: {message}")
        elif "WARNING" in message:
            logger.warning(f"Disk space {path}: {message}")
        else:
            logger.debug(f"Disk space {path}: {message}")


class DiskSpaceError(Exception):
    """Exception raised when disk space is critically low."""

    def __init__(self, message: str, free_bytes: int = 0):
        self.free_bytes = free_bytes
        super().__init__(message)


def require_disk_space(min_bytes: int = None, path: str = "/tmp"):
    """
    Decorator/context manager to require sufficient disk space.

    Raises DiskSpaceError if disk space is critically low.

    Args:
        min_bytes: Minimum required bytes
        path: Path to check
    """
    if min_bytes is None:
        min_bytes = MIN_FREE_SPACE_BYTES

    is_ok, message, free = check_disk_space(path)
    if not is_ok:
        raise DiskSpaceError(
            f"Insufficient disk space: {message}. Required: {min_bytes / (1024*1024*1024):.2f}GB",
            free
        )


def estimate_job_output_size(method_name: str = None) -> int:
    """
    Estimate the size needed for a job's output.

    This is a conservative estimate to ensure we don't run jobs
    that would fail due to disk space.

    Args:
        method_name: Optional method name for method-specific estimates

    Returns:
        Estimated bytes needed (default: 100MB)
    """
    # Default estimate: 100MB per job output
    # This is conservative for most imaging jobs
    default_estimate = 100 * 1024 * 1024  # 100 MB

    # Method-specific estimates could be added here
    # For now, use default
    return default_estimate


def clean_old_job_files(max_age_hours: int = 24, dry_run: bool = False) -> Tuple[int, int]:
    """
    Clean up old job output files from /tmp.

    Args:
        max_age_hours: Maximum age of files to keep
        dry_run: If True, only report what would be deleted

    Returns:
        Tuple of (files_deleted, bytes_freed)
    """
    import time
    import glob

    files_deleted = 0
    bytes_freed = 0
    max_age_seconds = max_age_hours * 3600
    now = time.time()

    patterns = [
        "/tmp/output_*.txt",
        "/tmp/error_*.txt",
        "/tmp/exit_code_*.txt",
        "/tmp/job_*",  # Job workspaces
    ]

    for pattern in patterns:
        for filepath in glob.glob(pattern):
            try:
                stat = os.stat(filepath)
                age = now - stat.st_mtime

                if age > max_age_seconds:
                    size = stat.st_size if os.path.isfile(filepath) else 0

                    if dry_run:
                        logger.info(f"Would delete: {filepath} (age: {age/3600:.1f}h, size: {size/1024:.1f}KB)")
                    else:
                        if os.path.isfile(filepath):
                            os.remove(filepath)
                        elif os.path.isdir(filepath):
                            shutil.rmtree(filepath)
                        logger.info(f"Deleted: {filepath} (age: {age/3600:.1f}h, size: {size/1024:.1f}KB)")

                    files_deleted += 1
                    bytes_freed += size

            except Exception as e:
                logger.warning(f"Error processing {filepath}: {e}")

    return files_deleted, bytes_freed
