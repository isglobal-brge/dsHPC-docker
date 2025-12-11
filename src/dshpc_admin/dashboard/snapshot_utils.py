"""
Utilities to read data from system snapshots instead of direct queries.

ARCHITECTURE:
Snapshots are stored in the DEDICATED admin_db (not jobs_db).
This isolates visualization data from critical system data.
If admin_db dies, the job processing system continues working.
"""
from datetime import datetime
from .db_connections import MongoDBConnections


def get_latest_snapshot():
    """
    Get the most recent system snapshot.

    ISOLATION: Reads from admin_db, NOT from jobs_db.
    This ensures snapshot queries don't affect job processing performance.
    """
    admin_db = MongoDBConnections.get_admin_db()

    try:
        snapshot = admin_db.system_snapshots.find_one(
            {},
            sort=[('timestamp', -1)]
        )
        return snapshot
    except Exception as e:
        return None


def get_container_status():
    """Get container status from latest snapshot."""
    snapshot = get_latest_snapshot()
    
    if not snapshot or 'containers' not in snapshot:
        return {
            'containers': [],
            'timestamp': None,
            'age_seconds': None
        }
    
    age_seconds = (datetime.utcnow() - snapshot['timestamp']).total_seconds()
    
    return {
        'containers': snapshot['containers'],
        'timestamp': snapshot['timestamp'],
        'age_seconds': int(age_seconds)
    }


def get_system_resources():
    """Get system resources from latest snapshot."""
    snapshot = get_latest_snapshot()
    
    if not snapshot or 'system_resources' not in snapshot:
        return {
            'resources': {},
            'timestamp': None,
            'age_seconds': None
        }
    
    age_seconds = (datetime.utcnow() - snapshot['timestamp']).total_seconds()
    
    return {
        'resources': snapshot['system_resources'],
        'timestamp': snapshot['timestamp'],
        'age_seconds': int(age_seconds)
    }


def get_job_logs(slurm_id=None, job_hash=None):
    """Get job logs from latest snapshot."""
    snapshot = get_latest_snapshot()
    
    if not snapshot or 'job_logs' not in snapshot:
        return None
    
    # Find log entry for this job
    for log_entry in snapshot['job_logs']:
        if slurm_id and log_entry.get('slurm_id') == slurm_id:
            return log_entry
        if job_hash and log_entry.get('job_hash') == job_hash:
            return log_entry
    
    return None


def get_all_job_logs():
    """Get all job logs from latest snapshot."""
    snapshot = get_latest_snapshot()
    
    if not snapshot or 'job_logs' not in snapshot:
        return []
    
    return snapshot['job_logs']


def get_slurm_queue():
    """Get Slurm queue from latest snapshot."""
    snapshot = get_latest_snapshot()
    
    if not snapshot or 'slurm_queue' not in snapshot:
        return {
            'queue': [],
            'timestamp': None,
            'age_seconds': None
        }
    
    age_seconds = (datetime.utcnow() - snapshot['timestamp']).total_seconds()
    
    return {
        'queue': snapshot['slurm_queue'],
        'timestamp': snapshot['timestamp'],
        'age_seconds': int(age_seconds)
    }


def get_environment_info():
    """Get environment information from latest snapshot."""
    snapshot = get_latest_snapshot()
    
    if not snapshot or 'environment' not in snapshot:
        return {
            'python': {},
            'r': {},
            'system': {},
            'slurm': {},
            'timestamp': None,
            'age_seconds': None
        }
    
    age_seconds = (datetime.utcnow() - snapshot['timestamp']).total_seconds()
    
    # Merge system_resources into system for backward compatibility
    env_data = snapshot['environment'].copy()
    
    # If we have system_resources at top level, merge them into system
    if 'system_resources' in snapshot and env_data.get('system'):
        env_data['system'].update(snapshot['system_resources'])
    
    return {
        **env_data,
        'timestamp': snapshot['timestamp'],
        'age_seconds': int(age_seconds),
        'cached': True,
        'cache_age': int(age_seconds)
    }


def get_method_source(function_hash):
    """Get source code for a specific method."""
    snapshot = get_latest_snapshot()

    if not snapshot or 'method_sources' not in snapshot:
        return None

    for method in snapshot['method_sources']:
        if method.get('function_hash') == function_hash:
            return method

    return None


def get_jobs_list():
    """Get pre-enriched jobs list from latest snapshot."""
    snapshot = get_latest_snapshot()
    if not snapshot or 'jobs_list' not in snapshot:
        return None
    return snapshot['jobs_list']


def get_meta_jobs_list():
    """Get pre-enriched meta-jobs list from latest snapshot."""
    snapshot = get_latest_snapshot()
    if not snapshot or 'meta_jobs_list' not in snapshot:
        return None
    return snapshot['meta_jobs_list']


def get_files_list():
    """Get pre-enriched files list from latest snapshot."""
    snapshot = get_latest_snapshot()
    if not snapshot or 'files_list' not in snapshot:
        return None
    return snapshot['files_list']


def get_stats_from_snapshot():
    """
    Get system statistics from latest snapshot.

    This avoids hitting the databases directly on page load.
    Stats are collected by the worker and stored in snapshot.
    """
    snapshot = get_latest_snapshot()
    if not snapshot or 'stats' not in snapshot:
        # Return empty stats if not available yet
        return {
            'total_jobs': 0,
            'active_jobs': 0,
            'completed_jobs': 0,
            'failed_jobs': 0,
            'cancelled_jobs': 0,
            'total_meta_jobs': 0,
            'running_meta_jobs': 0,
            'total_files': 0,
            'total_methods': 0,
            'active_methods': 0,
        }
    return snapshot['stats']

