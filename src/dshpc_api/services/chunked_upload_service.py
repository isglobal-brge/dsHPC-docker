"""
Service for handling chunked file uploads.

This service manages the lifecycle of chunked uploads:
- Initializing upload sessions
- Storing chunks temporarily
- Combining chunks into final files
- Cleaning up abandoned sessions
"""

import os
import uuid
import base64
import hashlib
import shutil
from datetime import datetime, timedelta
from typing import Optional, Tuple, List
from pathlib import Path

from dshpc_api.config.settings import get_settings
from dshpc_api.services.db_service import upload_file, get_files_db
from dshpc_api.models.file import ChunkedUploadSession


class ChunkedUploadService:
    """Service for managing chunked file uploads."""
    
    def __init__(self):
        self.settings = get_settings()
        # Use temp dir from settings or default
        self.temp_base_dir = getattr(self.settings, 'CHUNKED_UPLOAD_TEMP_DIR', '/tmp/chunked_uploads')
        self.session_timeout_hours = getattr(self.settings, 'CHUNKED_UPLOAD_SESSION_TIMEOUT_HOURS', 24)
        self.max_chunk_size = getattr(self.settings, 'CHUNKED_UPLOAD_MAX_CHUNK_SIZE', 100 * 1024 * 1024)  # 100 MB
        
        # Ensure temp directory exists
        Path(self.temp_base_dir).mkdir(parents=True, exist_ok=True)
    
    async def init_upload_session(
        self,
        file_hash: str,
        filename: Optional[str],
        content_type: str,
        total_size: int,
        chunk_size: int,
        metadata: Optional[dict] = None
    ) -> Tuple[bool, str, Optional[str]]:
        """
        Initialize a new chunked upload session.
        
        Args:
            file_hash: SHA-256 hash of the complete file
            filename: Original filename
            content_type: MIME type
            total_size: Total file size in bytes
            chunk_size: Size of each chunk in bytes
            metadata: Optional metadata
        
        Returns:
            Tuple of (success, message, session_id)
        """
        try:
            # Validate chunk size
            if chunk_size > self.max_chunk_size:
                return False, f"Chunk size exceeds maximum of {self.max_chunk_size} bytes", None
            
            # Generate unique session ID
            session_id = str(uuid.uuid4())
            
            # Create session data
            session_data = ChunkedUploadSession(
                session_id=session_id,
                file_hash=file_hash,
                filename=filename,
                content_type=content_type,
                total_size=total_size,
                chunk_size=chunk_size,
                chunks_received=[],
                created_at=datetime.utcnow(),
                last_updated=datetime.utcnow(),
                status="active",
                metadata=metadata
            )
            
            # Store session in database
            db = await get_files_db()
            sessions_collection = db['chunked_upload_sessions']
            await sessions_collection.insert_one(session_data.dict())
            
            # Create temporary directory for chunks
            session_dir = os.path.join(self.temp_base_dir, session_id)
            os.makedirs(session_dir, exist_ok=True)
            
            return True, f"Session {session_id} initialized", session_id
        
        except Exception as e:
            return False, f"Error initializing session: {str(e)}", None
    
    async def get_session(self, session_id: str) -> Optional[ChunkedUploadSession]:
        """
        Retrieve a session by ID.
        
        Args:
            session_id: Session identifier
        
        Returns:
            ChunkedUploadSession or None if not found
        """
        try:
            db = await get_files_db()
            sessions_collection = db['chunked_upload_sessions']
            session_data = await sessions_collection.find_one({"session_id": session_id})
            
            if session_data:
                # Remove MongoDB _id field
                session_data.pop('_id', None)
                return ChunkedUploadSession(**session_data)
            return None
        
        except Exception:
            return None
    
    async def store_chunk(
        self,
        session_id: str,
        chunk_number: int,
        chunk_data: str
    ) -> Tuple[bool, str, Optional[dict]]:
        """
        Store a chunk for the given session.
        
        Args:
            session_id: Session identifier
            chunk_number: Sequential chunk number (0-based)
            chunk_data: Base64 encoded chunk data
        
        Returns:
            Tuple of (success, message, response_data)
        """
        try:
            # Retrieve session
            session = await self.get_session(session_id)
            if not session:
                return False, f"Session {session_id} not found", None
            
            if session.status != "active":
                return False, f"Session is not active (status: {session.status})", None
            
            # Decode chunk data
            try:
                chunk_bytes = base64.b64decode(chunk_data)
            except Exception as e:
                return False, f"Invalid base64 encoding: {str(e)}", None
            
            # Validate chunk size
            if len(chunk_bytes) > session.chunk_size * 1.1:  # Allow 10% over
                return False, f"Chunk size exceeds session chunk size", None
            
            # Store chunk to disk
            session_dir = os.path.join(self.temp_base_dir, session_id)
            chunk_path = os.path.join(session_dir, f"chunk_{chunk_number:06d}")
            
            with open(chunk_path, 'wb') as f:
                f.write(chunk_bytes)
            
            # Update session in database
            db = await get_files_db()
            sessions_collection = db['chunked_upload_sessions']
            
            # Add chunk number to chunks_received if not already present
            if chunk_number not in session.chunks_received:
                await sessions_collection.update_one(
                    {"session_id": session_id},
                    {
                        "$push": {"chunks_received": chunk_number},
                        "$set": {"last_updated": datetime.utcnow()}
                    }
                )
                session.chunks_received.append(chunk_number)
            
            response_data = {
                "session_id": session_id,
                "chunk_number": chunk_number,
                "chunks_received": len(session.chunks_received)
            }
            
            return True, f"Chunk {chunk_number} stored successfully", response_data
        
        except Exception as e:
            return False, f"Error storing chunk: {str(e)}", None
    
    async def finalize_upload(
        self,
        session_id: str,
        total_chunks: int
    ) -> Tuple[bool, str, Optional[dict]]:
        """
        Finalize upload by combining chunks and verifying hash.
        
        Args:
            session_id: Session identifier
            total_chunks: Expected total number of chunks
        
        Returns:
            Tuple of (success, message, file_response)
        """
        try:
            # Retrieve session
            session = await self.get_session(session_id)
            if not session:
                return False, f"Session {session_id} not found", None
            
            if session.status != "active":
                return False, f"Session is not active (status: {session.status})", None
            
            # Import db functions at the start
            from dshpc_api.services.db_service import get_files_db, get_gridfs_bucket, upload_file
            
            # Update session status to finalizing
            db = await get_files_db()
            sessions_collection = db['chunked_upload_sessions']
            await sessions_collection.update_one(
                {"session_id": session_id},
                {"$set": {"status": "finalizing", "last_updated": datetime.utcnow()}}
            )
            
            # Verify all chunks are present
            if len(session.chunks_received) != total_chunks:
                await sessions_collection.update_one(
                    {"session_id": session_id},
                    {"$set": {"status": "failed"}}
                )
                return False, f"Missing chunks: expected {total_chunks}, got {len(session.chunks_received)}", None
            
            # Sort chunks to ensure correct order
            sorted_chunks = sorted(session.chunks_received)
            for i, chunk_num in enumerate(sorted_chunks):
                if chunk_num != i:
                    await sessions_collection.update_one(
                        {"session_id": session_id},
                        {"$set": {"status": "failed"}}
                    )
                    return False, f"Chunks are not sequential", None
            
            # Combine chunks
            session_dir = os.path.join(self.temp_base_dir, session_id)
            combined_path = os.path.join(session_dir, "combined_file")
            
            with open(combined_path, 'wb') as outfile:
                for chunk_num in sorted_chunks:
                    chunk_path = os.path.join(session_dir, f"chunk_{chunk_num:06d}")
                    with open(chunk_path, 'rb') as infile:
                        outfile.write(infile.read())
            
            # Verify hash
            with open(combined_path, 'rb') as f:
                calculated_hash = hashlib.sha256(f.read()).hexdigest()
            
            if calculated_hash != session.file_hash:
                await sessions_collection.update_one(
                    {"session_id": session_id},
                    {"$set": {"status": "failed"}}
                )
                return False, f"Hash verification failed: expected {session.file_hash}, got {calculated_hash}", None
            
            # Get file size
            file_size = os.path.getsize(combined_path)
            
            # Upload to database - use GridFS for files > 15 MB
            GRIDFS_THRESHOLD = 15 * 1024 * 1024
            now = datetime.utcnow()
            
            if file_size > GRIDFS_THRESHOLD:
                # Use GridFS for large files - stream directly from disk
                try:
                    bucket = await get_gridfs_bucket()
                    
                    # Stream file directly to GridFS without loading into memory
                    with open(combined_path, 'rb') as f:
                        grid_id = await bucket.upload_from_stream(
                            session.file_hash,  # Use hash as filename
                            f,
                            metadata={
                                "filename": session.filename,
                                "content_type": session.content_type,
                                "original_metadata": session.metadata,
                                "upload_date": now,
                                "file_hash": session.file_hash
                            }
                        )
                    
                    # Store metadata in regular collection
                    metadata_doc = {
                        "file_hash": session.file_hash,
                        "filename": session.filename,
                        "content_type": session.content_type,
                        "metadata": session.metadata,
                        "upload_date": now,
                        "last_checked": now,
                        "storage_type": "gridfs",
                        "gridfs_id": grid_id,
                        "file_size": file_size
                    }
                    
                    db = await get_files_db()
                    result = await db.files.insert_one(metadata_doc)
                    uploaded_file = await db.files.find_one({"_id": result.inserted_id})
                    
                    if uploaded_file:
                        uploaded_file["_id"] = str(uploaded_file["_id"])
                        uploaded_file["gridfs_id"] = str(uploaded_file["gridfs_id"])
                        uploaded_file["content"] = "[Stored in GridFS]"
                    
                    success = True
                    message = "Large file uploaded successfully using GridFS"
                
                except Exception as e:
                    await sessions_collection.update_one(
                        {"session_id": session_id},
                        {"$set": {"status": "failed"}}
                    )
                    return False, f"Error uploading to GridFS: {str(e)}", None
            
            else:
                # For files < 15 MB, use regular storage with base64
                with open(combined_path, 'rb') as f:
                    file_content = f.read()
                
                content_base64 = base64.b64encode(file_content).decode('utf-8')
                
                file_data = {
                    "file_hash": session.file_hash,
                    "content": content_base64,
                    "filename": session.filename,
                    "content_type": session.content_type,
                    "metadata": session.metadata
                }
                
                success, message, uploaded_file = await upload_file(file_data)
            
            if success:
                # Mark session as completed
                await sessions_collection.update_one(
                    {"session_id": session_id},
                    {"$set": {"status": "completed", "last_updated": datetime.utcnow()}}
                )
                
                # Clean up chunks
                await self.cleanup_session(session_id)
                
                return True, "Upload finalized successfully", uploaded_file
            else:
                await sessions_collection.update_one(
                    {"session_id": session_id},
                    {"$set": {"status": "failed"}}
                )
                return False, f"Failed to store file: {message}", None
        
        except Exception as e:
            # Try to mark session as failed
            try:
                db = await get_files_db()
                sessions_collection = db['chunked_upload_sessions']
                await sessions_collection.update_one(
                    {"session_id": session_id},
                    {"$set": {"status": "failed"}}
                )
            except:
                pass
            
            return False, f"Error finalizing upload: {str(e)}", None
    
    async def cancel_upload(self, session_id: str) -> Tuple[bool, str]:
        """
        Cancel an upload session and clean up.
        
        Args:
            session_id: Session identifier
        
        Returns:
            Tuple of (success, message)
        """
        try:
            # Retrieve session
            session = await self.get_session(session_id)
            if not session:
                return False, f"Session {session_id} not found"
            
            # Update session status
            db = await get_files_db()
            sessions_collection = db['chunked_upload_sessions']
            await sessions_collection.update_one(
                {"session_id": session_id},
                {"$set": {"status": "cancelled", "last_updated": datetime.utcnow()}}
            )
            
            # Clean up chunks
            await self.cleanup_session(session_id)
            
            return True, f"Session {session_id} cancelled successfully"
        
        except Exception as e:
            return False, f"Error cancelling session: {str(e)}"
    
    async def cleanup_session(self, session_id: str) -> None:
        """
        Remove temporary files for a session.
        
        Args:
            session_id: Session identifier
        """
        try:
            session_dir = os.path.join(self.temp_base_dir, session_id)
            if os.path.exists(session_dir):
                shutil.rmtree(session_dir)
        except Exception:
            # Log error but don't raise
            pass
    
    async def cleanup_abandoned_sessions(self, hours: int = 24) -> Tuple[int, int]:
        """
        Clean up sessions that haven't been updated in the specified hours.
        
        Args:
            hours: Number of hours of inactivity before cleanup
        
        Returns:
            Tuple of (sessions_cleaned, sessions_failed)
        """
        try:
            db = await get_files_db()
            sessions_collection = db['chunked_upload_sessions']
            
            # Find abandoned sessions
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            abandoned_sessions = await sessions_collection.find({
                "last_updated": {"$lt": cutoff_time},
                "status": {"$in": ["active", "finalizing"]}
            }).to_list(length=None)
            
            cleaned = 0
            failed = 0
            
            for session_data in abandoned_sessions:
                session_id = session_data['session_id']
                try:
                    # Mark as cancelled
                    await sessions_collection.update_one(
                        {"session_id": session_id},
                        {"$set": {"status": "abandoned", "last_updated": datetime.utcnow()}}
                    )
                    
                    # Clean up files
                    await self.cleanup_session(session_id)
                    cleaned += 1
                except Exception:
                    failed += 1
            
            return cleaned, failed
        
        except Exception:
            return 0, 0


# Singleton instance
_chunked_upload_service = None

def get_chunked_upload_service() -> ChunkedUploadService:
    """Get the singleton ChunkedUploadService instance."""
    global _chunked_upload_service
    if _chunked_upload_service is None:
        _chunked_upload_service = ChunkedUploadService()
    return _chunked_upload_service

