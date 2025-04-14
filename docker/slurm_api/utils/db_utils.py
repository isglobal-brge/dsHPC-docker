from slurm_api.config.db_config import jobs_collection
from slurm_api.models.job import JobStatus

def update_job_status(job_id: str, status: JobStatus, output: str | None = None, error: str | None = None):
    """Update job status in MongoDB."""
    update_data = {"status": status}
    if output is not None:
        update_data["output"] = output
    if error is not None:
        update_data["error"] = error
    
    jobs_collection.update_one(
        {"job_id": job_id},
        {"$set": update_data}
    ) 