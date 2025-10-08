from slurm_api.config.db_config import jobs_collection, get_jobs_db_client
from slurm_api.models.job import JobStatus
from gridfs import GridFS
from bson import ObjectId
import sys

def update_job_status(job_id: str, status: JobStatus, output: str | None = None, error: str | None = None):
    """Update job status in MongoDB, using GridFS for large outputs."""
    update_data = {"status": status}
    
    # Threshold for using GridFS (15MB to leave margin for BSON overhead)
    GRIDFS_THRESHOLD = 15 * 1024 * 1024
    
    # Handle output storage
    if output is not None:
        output_size = sys.getsizeof(output)
        
        if output_size > GRIDFS_THRESHOLD:
            # Use GridFS for large output
            db = get_jobs_db_client()
            fs = GridFS(db, collection="job_outputs")
            
            # Store output in GridFS
            output_id = fs.put(output.encode('utf-8'), 
                             filename=f"output_{job_id}",
                             job_id=job_id,
                             field_type="output")
            
            update_data["output"] = None  # Don't store inline
            update_data["output_gridfs_id"] = output_id
            update_data["output_storage"] = "gridfs"
            update_data["output_size"] = output_size
        else:
            # Store inline for small outputs
            update_data["output"] = output
            update_data["output_storage"] = "inline"
            update_data["output_size"] = output_size
            # Clear any existing GridFS references
            update_data["output_gridfs_id"] = None
    
    # Handle error storage
    if error is not None:
        error_size = sys.getsizeof(error)
        
        if error_size > GRIDFS_THRESHOLD:
            # Use GridFS for large error
            db = get_jobs_db_client()
            fs = GridFS(db, collection="job_outputs")
            
            # Store error in GridFS
            error_id = fs.put(error.encode('utf-8'),
                            filename=f"error_{job_id}",
                            job_id=job_id,
                            field_type="error")
            
            update_data["error"] = None  # Don't store inline
            update_data["error_gridfs_id"] = error_id
            update_data["error_storage"] = "gridfs"
            update_data["error_size"] = error_size
        else:
            # Store inline for small errors
            update_data["error"] = error
            update_data["error_storage"] = "inline"
            update_data["error_size"] = error_size
            # Clear any existing GridFS references
            update_data["error_gridfs_id"] = None
    
    jobs_collection.update_one(
        {"job_id": job_id},
        {"$set": update_data}
    ) 