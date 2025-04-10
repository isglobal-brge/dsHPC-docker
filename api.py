from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import subprocess
import uuid
import os

app = FastAPI(title="Slurm Job Submission API")

class JobSubmission(BaseModel):
    script: str
    name: str | None = None

@app.post("/submit")
async def submit_job(job: JobSubmission):
    try:
        # Create a unique job script file
        job_id = str(uuid.uuid4())
        script_path = f"/tmp/job_{job_id}.sh"
        
        # Write the job script to a file
        with open(script_path, "w") as f:
            f.write("#!/bin/bash\n")
            if job.name:
                f.write(f"#SBATCH --job-name={job.name}\n")
            f.write(job.script)
        
        # Make the script executable
        os.chmod(script_path, 0o755)
        
        # Submit the job using sbatch
        result = subprocess.run(
            ["sbatch", script_path],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            raise HTTPException(
                status_code=400,
                detail=f"Job submission failed: {result.stderr}"
            )
        
        # Clean up the temporary script file
        os.remove(script_path)
        
        return {"message": result.stdout.strip()}
    
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
        
        for line in lines[1:]:  # Skip header line
            values = line.split("|")
            jobs.append(dict(zip(headers, values)))
        
        return {"jobs": jobs}
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        ) 