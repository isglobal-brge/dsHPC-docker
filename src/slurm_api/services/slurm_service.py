import subprocess
from typing import Dict, List, Tuple

def get_queue_status() -> Tuple[bool, str, List[Dict]]:
    """Get current Slurm queue status."""
    result = subprocess.run(
        ["squeue", "--format=%i|%j|%u|%t|%M|%l|%D|%P"],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        return False, result.stderr, []
    
    # Parse the output into a structured format
    lines = result.stdout.strip().split("\n")
    headers = ["job_id", "name", "user", "state", "time", "time_limit", "nodes", "partition"]
    jobs = []
    
    for line in lines[1:]:  # Skip header line
        if line:
            values = line.split("|")
            jobs.append(dict(zip(headers, values)))
    
    return True, result.stdout.strip(), jobs

def get_active_jobs() -> Dict[str, str]:
    """Get all jobs currently in the Slurm queue with their statuses."""
    result = subprocess.run(
        ["squeue", "--format=%i|%t|%j", "--noheader"],
        capture_output=True,
        text=True
    )
    
    slurm_statuses = {}
    if result.returncode == 0:
        for line in result.stdout.strip().split("\n"):
            if line:
                parts = line.split("|")
                if len(parts) >= 2:  # Ensure we have at least job_id and state
                    job_id, state = parts[0], parts[1]
                    slurm_statuses[job_id] = state
    
    return slurm_statuses

def get_job_final_state(slurm_id: str, job_hash: str = None) -> str:
    """Get final state of a completed job from sacct or scontrol."""
    import os

    # First try sacct
    sacct_result = subprocess.run(
        ["sacct", "-j", slurm_id, "--format=State", "--noheader", "--parsable2"],
        capture_output=True,
        text=True
    )

    if sacct_result.returncode == 0 and sacct_result.stdout.strip():
        state = sacct_result.stdout.strip().split("\n")[0]
        # sacct might return states like "CANCELLED by 0" - extract just the state
        if " " in state:
            state = state.split()[0]
        return state

    # If sacct doesn't work (accounting disabled), try scontrol
    scontrol_result = subprocess.run(
        ["scontrol", "show", "job", slurm_id],
        capture_output=True,
        text=True
    )

    if scontrol_result.returncode == 0 and "JobState=" in scontrol_result.stdout:
        # Parse JobState from scontrol output
        for part in scontrol_result.stdout.split():
            if part.startswith("JobState="):
                return part.split("=")[1]

    # If we can't get state from Slurm, check if output files exist
    # This helps distinguish between completed and cancelled jobs
    if job_hash:
        output_path = f"/tmp/output_{job_hash}.txt"
        exit_code_path = f"/tmp/exit_code_{job_hash}"

        # If output files exist, job completed (success or failure will be determined by process_job_output)
        if os.path.exists(output_path) or os.path.exists(exit_code_path):
            return "CD"  # Completed (final status determined by exit code)
        else:
            # No output files = job was likely cancelled or never ran
            return "CA"  # Cancelled

    return "CD"  # Default to completed if we can't determine

def submit_slurm_job(script_path: str) -> Tuple[bool, str, str]:
    """Submit a job to Slurm and return success, message, and job ID."""
    result = subprocess.run(
        ["sbatch", script_path],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        return False, result.stderr, ""
    
    # Extract Slurm job ID
    slurm_id = result.stdout.strip().split()[-1]
    return True, result.stdout.strip(), slurm_id 