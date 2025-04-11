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
import asyncio
from fastapi.background import BackgroundTasks
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB setup
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "outputs")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
jobs_collection = db["jobs"]

app = FastAPI(title="Slurm Job Submission API")

class JobStatus(str, Enum):
    PENDING = "PD"          # Job is queued and waiting for resources
    RUNNING = "R"           # Job is running
    COMPLETED = "CD"        # Job completed successfully
    FAILED = "F"           # Job failed
    CANCELLED = "CA"        # Job was cancelled by user or system
    TIMEOUT = "TO"         # Job reached time limit
    NODE_FAIL = "NF"       # Job failed due to node failure
    OUT_OF_MEMORY = "OOM"  # Job experienced out of memory error
    SUSPENDED = "S"        # Job has been suspended
    STOPPED = "ST"         # Job has been stopped
    BOOT_FAIL = "BF"       # Job failed during node boot
    DEADLINE = "DL"        # Job terminated on deadline
    COMPLETING = "CG"      # Job is in the process of completing
    CONFIGURING = "CF"     # Job is in the process of configuring
    PREEMPTED = "PR"       # Job was preempted by another job

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

async def check_job_status():
    """Background task to check and update status of running jobs."""
    while True:
        try:
            # Find all jobs that are in a non-terminal state
            active_jobs = jobs_collection.find({
                "status": {"$nin": [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED, 
                                  JobStatus.TIMEOUT, JobStatus.NODE_FAIL, JobStatus.OUT_OF_MEMORY,
                                  JobStatus.BOOT_FAIL, JobStatus.DEADLINE, JobStatus.PREEMPTED]},
                "slurm_id": {"$ne": None}
            })
            
            # Get all job statuses from Slurm
            result = subprocess.run(
                ["squeue", "--format=%i|%t|%j", "--noheader"],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                # Create a dict of Slurm job statuses
                slurm_statuses = {}
                for line in result.stdout.strip().split("\n"):
                    if line:
                        job_id, state, name = line.split("|")
                        slurm_statuses[job_id] = state
                
                # Check each active job
                for job in active_jobs:
                    slurm_id = job["slurm_id"]
                    job_id = job["job_id"]
                    
                    # If job not in Slurm queue, check sacct for final status
                    if slurm_id not in slurm_statuses:
                        try:
                            # Read the output file
                            output_path = f"/tmp/output_{job_id}.txt"
                            exit_code_path = f"/tmp/exit_code_{job_id}"
                            
                            # Get final status from sacct
                            sacct_result = subprocess.run(
                                ["sacct", "-j", slurm_id, "--format=State", "--noheader", "--parsable2"],
                                capture_output=True,
                                text=True
                            )
                            
                            final_state = "CD"  # Default to completed
                            if sacct_result.returncode == 0 and sacct_result.stdout.strip():
                                state = sacct_result.stdout.strip().split("\n")[0]
                                if state in [s.value for s in JobStatus]:
                                    final_state = state
                            
                            if os.path.exists(output_path):
                                with open(output_path) as f:
                                    output = f.read()
                                
                                # Update job status based on final state
                                update_job_status(job_id, JobStatus(final_state), output=output)
                                
                                # Clean up temporary files
                                os.remove(output_path)
                                if os.path.exists(exit_code_path):
                                    os.remove(exit_code_path)
                            else:
                                # No output file found, mark as failed
                                update_job_status(job_id, JobStatus.FAILED, error="No output file found")
                        except Exception as e:
                            logger.error(f"Error processing job output for {job_id}: {e}")
                            update_job_status(job_id, JobStatus.FAILED, error=str(e))
                    else:
                        # Update status if job is still in queue
                        slurm_state = slurm_statuses[slurm_id]
                        if slurm_state != job["status"]:
                            try:
                                update_job_status(job_id, JobStatus(slurm_state))
                            except ValueError:
                                logger.warning(f"Unknown Slurm state: {slurm_state}")
            
        except Exception as e:
            logger.error(f"Error in job status checking: {e}")
        
        # Wait before next check
        await asyncio.sleep(5)  # Check every 5 seconds

@app.on_event("startup")
async def startup_event():
    """Start the background task on application startup."""
    asyncio.create_task(check_job_status())

@app.post("/submit")
async def submit_job(job: JobSubmission):
    try:
        # Generate a unique job ID
        job_id = str(uuid.uuid4())
        script_path = f"/tmp/job_{job_id}.sh"
        
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
            # Write the job script to a file
            with open(script_path, "w") as f:
                f.write("#!/bin/bash\n")
                if job.name:
                    f.write(f"#SBATCH --job-name={job.name}\n")
                # Capture output to a file
                output_path = f"/tmp/output_{job_id}.txt"
                f.write(f"exec 1> {output_path} 2>&1\n")  # Redirect both stdout and stderr
                f.write(job.script)
                f.write(f"\necho $? > /tmp/exit_code_{job_id}")
            
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
                {"$set": {"slurm_id": slurm_id}}
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
    """Get current Slurm queue status."""
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
        
        for line in lines[1:]:  # Skip header line
            if line:
                values = line.split("|")
                jobs.append(dict(zip(headers, values)))
        
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