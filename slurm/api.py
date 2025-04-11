from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import subprocess
import uuid
import os
import hashlib
from datetime import datetime
from pymongo import MongoClient
import json
from enum import Enum

# MongoDB setup
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "outputs")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
jobs_collection = db["jobs"]

app = FastAPI(title="Slurm Job Submission API")

class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class JobSubmission(BaseModel):
    script: str
    name: str | None = None
    parameters: Dict[str, Any] | None = None

def mock_function_hash(script: str) -> str:
    """Mock function to generate a function hash."""
    return hashlib.md5(script.encode()).hexdigest()

def mock_file_hash() -> str | None:
    """Mock function to generate a file hash."""
    return None

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

@app.post("/submit")
async def submit_job(job: JobSubmission):
    try:
        # Generate a unique job ID
        job_id = str(uuid.uuid4())
        script_path = f"/tmp/job_{job_id}.sh"
        output_path = f"/tmp/output_{job_id}.txt"
        exit_code_path = f"/tmp/exit_code_{job_id}"
        
        # Create job document
        job_doc = {
            "job_id": job_id,
            "slurm_id": None,
            "function_hash": mock_function_hash(job.script),
            "file_hash": mock_file_hash(),
            "parameters": job.parameters,
            "status": JobStatus.PENDING,
            "created_at": datetime.utcnow(),
            "name": job.name,
            "output": None,
            "error": None
        }
        
        # Insert job document
        jobs_collection.insert_one(job_doc)
        
        try:
            # Write the job script to a file with error handling
            with open(script_path, "w") as f:
                f.write("#!/bin/bash\n")
                if job.name:
                    f.write(f"#SBATCH --job-name={job.name}\n")
                
                # Redirect output and set up error handling
                f.write(f"""
# Redirect all output to our output file
exec 1> {output_path} 2>&1

# Error handling
set -e
trap 'echo $? > {exit_code_path}; exit 1' ERR

# Execute the actual job script
{job.script}

# If we get here, the job succeeded
echo 0 > {exit_code_path}
""")
            
            # Make the script executable
            os.chmod(script_path, 0o755)
            
            # Submit the job using sbatch
            result = subprocess.run(
                ["sbatch", script_path],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                update_job_status(job_id, JobStatus.FAILED, error=result.stderr)
                raise HTTPException(
                    status_code=400,
                    detail=f"Job submission failed: {result.stderr}"
                )
            
            # Extract Slurm job ID
            slurm_id = result.stdout.strip().split()[-1]
            
            # Update job document with Slurm ID
            jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {"slurm_id": slurm_id, "status": JobStatus.RUNNING}}
            )
            
            return {"message": result.stdout.strip(), "job_id": job_id}
            
        except Exception as e:
            update_job_status(job_id, JobStatus.FAILED, error=str(e))
            raise HTTPException(
                status_code=500,
                detail=f"Job submission failed: {str(e)}"
            )
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/queue")
async def get_queue():
    try:
        result = subprocess.run(
            ["squeue", "--format=%i|%j|%u|%t|%M|%l|%D|%P"],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            raise HTTPException(
                status_code=400,
                detail=f"Queue query failed: {result.stderr}"
            )
        
        # Parse the output into a structured format
        lines = result.stdout.strip().split("\n")
        headers = ["job_id", "name", "user", "state", "time", "time_limit", "nodes", "partition"]
        jobs = []
        
        # Get all running jobs from MongoDB
        running_jobs = list(jobs_collection.find({"status": JobStatus.RUNNING}))
        running_jobs_dict = {job["slurm_id"]: job for job in running_jobs}
        
        # Track seen jobs
        seen_slurm_ids = set()
        
        for line in lines[1:]:  # Skip header line
            values = line.split("|")
            jobs.append(dict(zip(headers, values)))
            seen_slurm_ids.add(values[0])
        
        # Check for completed jobs (those that were running but are no longer in queue)
        for job in running_jobs:
            if job["slurm_id"] not in seen_slurm_ids:
                try:
                    # Read the output file
                    output_path = f"/tmp/output_{job['job_id']}.txt"
                    exit_code_path = f"/tmp/exit_code_{job['job_id']}"
                    
                    output = None
                    if os.path.exists(output_path):
                        with open(output_path) as f:
                            output = f.read()
                    
                    # Check exit code
                    status = JobStatus.COMPLETED  # Default to completed
                    if os.path.exists(exit_code_path):
                        with open(exit_code_path) as f:
                            exit_code = int(f.read().strip())
                            if exit_code != 0:
                                status = JobStatus.FAILED
                    
                    # Update job status
                    update_data = {
                        "status": status,
                        "output": output
                    }
                    if status == JobStatus.FAILED:
                        update_data["error"] = f"Job failed with exit code {exit_code}"
                    
                    jobs_collection.update_one(
                        {"job_id": job["job_id"]},
                        {"$set": update_data}
                    )
                    
                    # Clean up temporary files
                    if os.path.exists(output_path):
                        os.remove(output_path)
                    if os.path.exists(exit_code_path):
                        os.remove(exit_code_path)
                        
                except Exception as e:
                    print(f"Error processing job output: {e}")
        
        return {"jobs": jobs}
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/job/{job_id}")
async def get_job(job_id: str):
    """Get job information from MongoDB."""
    job = jobs_collection.find_one({"job_id": job_id})
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_id} not found"
        )
    
    # Convert MongoDB ObjectId to string for JSON serialization
    job["_id"] = str(job["_id"])
    return job 