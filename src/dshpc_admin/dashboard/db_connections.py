"""
MongoDB connection utilities for accessing dsHPC databases.

ARCHITECTURE:
- jobs_db, files_db, methods_db: DATA SOURCE databases (critical system data)
- admin_db: ISOLATED database for admin panel snapshots/cache (non-critical)

The admin_db is completely separated from critical data. If the admin-db container
dies or is OOM-killed, the job processing system continues to work normally.

PERFORMANCE: Automatically creates indexes on first connection for fast filtered queries.
"""
from pymongo import MongoClient, ASCENDING, DESCENDING
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


def _ensure_indexes(db, collection_name, indexes):
    """Create indexes if they don't exist (idempotent)."""
    try:
        collection = db[collection_name]
        existing = set(collection.index_information().keys())
        for index_spec, options in indexes:
            index_name = options.get('name', '_'.join(f'{k}_{v}' for k, v in index_spec))
            if index_name not in existing:
                collection.create_index(index_spec, **options)
                logger.info(f"Created index {index_name} on {collection_name}")
    except Exception as e:
        logger.warning(f"Could not create indexes on {collection_name}: {e}")


class MongoDBConnections:
    """Singleton for MongoDB connections with automatic index creation."""

    _jobs_client = None
    _files_client = None
    _methods_client = None
    _admin_client = None  # Dedicated admin database client
    _indexes_created = False

    @classmethod
    def _create_indexes(cls, jobs_db, files_db, methods_db):
        """Create performance indexes on first connection."""
        if cls._indexes_created:
            return

        # Jobs collection indexes
        _ensure_indexes(jobs_db, 'jobs', [
            ([('status', ASCENDING), ('created_at', DESCENDING)], {'name': 'status_created_idx', 'background': True}),
            ([('function_hash', ASCENDING)], {'name': 'function_hash_idx', 'background': True}),
            ([('job_hash', ASCENDING)], {'name': 'job_hash_idx', 'background': True}),
            ([('created_at', DESCENDING)], {'name': 'created_at_idx', 'background': True}),
        ])

        # Meta-jobs collection indexes
        _ensure_indexes(jobs_db, 'meta_jobs', [
            ([('status', ASCENDING)], {'name': 'status_idx', 'background': True}),
            ([('created_at', DESCENDING)], {'name': 'created_at_idx', 'background': True}),
        ])

        # Files collection indexes
        _ensure_indexes(files_db, 'files', [
            ([('upload_date', DESCENDING)], {'name': 'upload_date_idx', 'background': True}),
            ([('file_hash', ASCENDING)], {'name': 'file_hash_idx', 'background': True}),
            ([('filename', ASCENDING)], {'name': 'filename_idx', 'background': True}),
            ([('storage_type', ASCENDING), ('gridfs_id', ASCENDING), ('content_preview', ASCENDING)],
             {'name': 'preview_cache_idx', 'background': True, 'sparse': True}),
        ])

        # Methods collection indexes
        _ensure_indexes(methods_db, 'methods', [
            ([('function_hash', ASCENDING)], {'name': 'function_hash_idx', 'background': True}),
            ([('name', ASCENDING)], {'name': 'name_idx', 'background': True}),
            ([('active', ASCENDING)], {'name': 'active_idx', 'background': True}),
        ])

        cls._indexes_created = True
        logger.info("Database indexes verified/created")

    @classmethod
    def get_jobs_db(cls):
        """Get jobs database connection."""
        if cls._jobs_client is None:
            cls._jobs_client = MongoClient(settings.MONGO_JOBS_URI)
        db = cls._jobs_client[settings.MONGO_JOBS_DB]
        if not cls._indexes_created:
            cls._create_indexes(db, cls.get_files_db(), cls.get_methods_db())
        return db

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

    @classmethod
    def get_admin_db(cls):
        """
        Get admin database connection (ISOLATED from critical data).

        This database stores ONLY:
        - system_snapshots: Visualization data for admin panel
        - admin_cache: Cached counts and metadata
        - worker_status: Worker heartbeat and status

        If this DB dies, the job processing system is NOT affected.
        """
        if cls._admin_client is None:
            cls._admin_client = MongoClient(settings.MONGO_ADMIN_URI)
        return cls._admin_client[settings.MONGO_ADMIN_DB]


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

