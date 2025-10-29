"""
MongoDB connection utilities for accessing dsHPC databases.
"""
from pymongo import MongoClient
from django.conf import settings


class MongoDBConnections:
    """Singleton for MongoDB connections."""
    
    _jobs_client = None
    _files_client = None
    _methods_client = None
    
    @classmethod
    def get_jobs_db(cls):
        """Get jobs database connection."""
        if cls._jobs_client is None:
            cls._jobs_client = MongoClient(settings.MONGO_JOBS_URI)
        return cls._jobs_client[settings.MONGO_JOBS_DB]
    
    @classmethod
    def get_files_db(cls):
        """Get files database connection."""
        if cls._files_client is None:
            cls._files_client = MongoClient(settings.MONGO_FILES_URI)
        return cls._files_client[settings.MONGO_FILES_DB]
    
    @classmethod
    def get_methods_db(cls):
        """Get methods database connection."""
        if cls._methods_client is None:
            cls._methods_client = MongoClient(settings.MONGO_METHODS_URI)
        return cls._methods_client[settings.MONGO_METHODS_DB]


def get_stats():
    """Get system statistics."""
    jobs_db = MongoDBConnections.get_jobs_db()
    files_db = MongoDBConnections.get_files_db()
    methods_db = MongoDBConnections.get_methods_db()
    
    stats = {
        'total_jobs': jobs_db.jobs.count_documents({}),
        'active_jobs': jobs_db.jobs.count_documents({'status': {'$in': ['PD', 'R', 'CG', 'CF']}}),
        'completed_jobs': jobs_db.jobs.count_documents({'status': 'CD'}),
        'failed_jobs': jobs_db.jobs.count_documents({'status': 'F'}),
        'total_meta_jobs': jobs_db.meta_jobs.count_documents({}),
        'running_meta_jobs': jobs_db.meta_jobs.count_documents({'status': 'running'}),
        'total_files': files_db.files.count_documents({}),
        'completed_files': files_db.files.count_documents({'status': 'completed'}),
        'total_methods': methods_db.methods.count_documents({}),
        'active_methods': methods_db.methods.count_documents({'active': True}),
    }
    
    return stats

