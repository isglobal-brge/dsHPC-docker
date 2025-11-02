import os
import uuid
import json
import hashlib
import base64
from datetime import datetime
from typing import Dict, Any, Tuple, Optional
from gridfs import GridFS
from bson import ObjectId

from slurm_api.config.db_config import jobs_collection, get_jobs_db_client, files_collection
from slurm_api.config.logging_config import logger
from slurm_api.models.job import JobStatus, JobSubmission
from slurm_api.utils.db_utils import update_job_status
from slurm_api.services.slurm_service import submit_slurm_job
from slurm_api.services.file_service import find_file_by_hash, download_file, create_job_workspace
from slurm_api.services.method_service import find_method_by_hash, prepare_method_execution
from slurm_api.models.method import MethodExecution
from slurm_api.utils.parameter_utils import sort_parameters
from slurm_api.utils.sorting_utils import sort_file_inputs

def prepare_job_script(job_hash: str, job: JobSubmission) -> str:
    """Prepare a job script file and return its path."""
    logger.info(f"prepare_job_script called for job {job_hash}")
    logger.info(f"  file_hash: {job.file_hash}")
    logger.info(f"  file_inputs: {job.file_inputs}")
    
    # Create a unique workspace for the job
    workspace_dir = create_job_workspace()
    logger.info(f"  workspace: {workspace_dir}")
    
    # Download file(s) to workspace using folder structure
    file_paths = {}
    
    if job.file_inputs:
        # Multi-file: download each to its named folder
        logger.info(f"Multi-file mode: {len(job.file_inputs)} inputs")
        for input_name, file_ref in job.file_inputs.items():
            if isinstance(file_ref, list):
                # Array of files
                logger.info(f"  Input '{input_name}': array with {len(file_ref)} files")
                file_paths[input_name] = []
                
                for idx, file_hash in enumerate(file_ref):
                    logger.info(f"    [{idx}] Downloading: {file_hash[:12]}...")
                    
                    # Check file exists
                    file_doc = find_file_by_hash(file_hash)
                    if not file_doc:
                        logger.error(f"File '{input_name}[{idx}]' not found")
                        raise ValueError(f"File '{input_name}[{idx}]' with hash {file_hash} not found in database")
                    
                    # Create subfolder: workspace/input_name/idx/
                    file_folder = os.path.join(workspace_dir, input_name, str(idx))
                    os.makedirs(file_folder, exist_ok=True)
                    
                    # Download file
                    success, message, file_path = download_file(file_hash, file_folder)
                    if not success:
                        logger.error(f"Download failed for '{input_name}[{idx}]': {message}")
                        raise ValueError(f"Failed to download file '{input_name}[{idx}]': {message}")
                    
                    logger.info(f"      Downloaded to: {file_path}")
                    file_paths[input_name].append(file_path)
            else:
                # Single file
                file_hash = file_ref
                logger.info(f"  Downloading '{input_name}': {file_hash[:12]}...")
                
                # Check file exists
                file_doc = find_file_by_hash(file_hash)
                if not file_doc:
                    logger.error(f"File '{input_name}' not found")
                    raise ValueError(f"File '{input_name}' with hash {file_hash} not found in database")
                
                # Create folder for this input
                file_folder = os.path.join(workspace_dir, input_name)
                os.makedirs(file_folder, exist_ok=True)
                logger.info(f"    Folder created: {file_folder}")
                
                # Download file
                success, message, file_path = download_file(file_hash, file_folder)
                if not success:
                    logger.error(f"Download failed for '{input_name}': {message}")
                    raise ValueError(f"Failed to download file '{input_name}': {message}")
                
                logger.info(f"    Downloaded to: {file_path}")
                file_paths[input_name] = file_path
    else:
        # Single file: use "input" folder (unified approach)
        # Handle case where there's no input file (params-only job)
        if job.file_hash:
            file_doc = find_file_by_hash(job.file_hash)
            if not file_doc:
                raise ValueError(f"File with hash {job.file_hash} not found in database")
            
            file_folder = os.path.join(workspace_dir, "input")
            os.makedirs(file_folder, exist_ok=True)
            
            success, message, file_path = download_file(job.file_hash, file_folder)
            if not success:
                raise ValueError(f"Failed to download file: {message}")
            
            file_paths["input"] = file_path
        # If no file_hash, leave file_paths empty (params-only job)
    
    # Create metadata.json with file paths and workspace info
    metadata = {
        "workspace_dir": workspace_dir,
        "files": file_paths  # Dict of all files (single or multi)
    }
    
    metadata_file_path = os.path.join(workspace_dir, "metadata.json")
    with open(metadata_file_path, "w") as meta_f:
        json.dump(metadata, meta_f, indent=2)
    
    script_path = f"/tmp/job_{job_hash}.sh"
    
    # Check if we're using a method (function_hash is provided)
    method_doc = None
    method_execution_data = None
    
    if job.function_hash:
        method_doc = find_method_by_hash(job.function_hash)
        if not method_doc:
            raise ValueError(f"Method with hash {job.function_hash} not found in database")
        
        # Sort parameters to ensure consistent ordering
        sorted_params = sort_parameters(job.parameters)
        
        # Prepare method execution
        # For backward compatibility, use file_hash or file_inputs
        if job.file_inputs:
            # Multi-file: pass file_inputs instead
            method_execution = MethodExecution(
                function_hash=job.function_hash,
                file_hash=None,  # Not used for multi-file
                file_inputs=job.file_inputs,
                parameters=sorted_params,
                name=job.name
            )
        else:
            # Single file (legacy)
            method_execution = MethodExecution(
                function_hash=job.function_hash,
                file_hash=job.file_hash,
                file_inputs=None,
                parameters=sorted_params,
                name=job.name
            )
        
        success, message, method_execution_data = prepare_method_execution(
            workspace_dir, method_execution
        )
        
        if not success:
            raise ValueError(f"Failed to prepare method: {message}")
    
    # Write the job script to a file
    with open(script_path, "w") as f:
        f.write("#!/bin/bash\n")
        if job.name:
            f.write(f"#SBATCH --job-name={job.name}\n")
        # Allocate maximum available memory (0 means all available)
        f.write("#SBATCH --mem=0\n")
        # Use all available CPUs
        f.write("#SBATCH --cpus-per-task=8\n")
        # Capture output to a file
        output_path = f"/tmp/output_{job_hash}.txt"
        error_path = f"/tmp/error_{job_hash}.txt"
        # Redirect stdout to output file and stderr to error file (separate them)
        f.write(f"exec 1> {output_path} 2> {error_path}\n")
        
        # Add workspace directory to environment variables
        f.write(f"export JOB_WORKSPACE=\"{workspace_dir}\"\n")
        
        # Add file paths as environment variables
        for input_name, input_path in file_paths.items():
            # Export as INPUT_FILE_XXX (e.g., INPUT_FILE_PRIMARY)
            env_var_name = f"INPUT_FILE_{input_name.upper()}"
            f.write(f"export {env_var_name}=\"{input_path}\"\n")
        
        # For backward compatibility: also export INPUT_FILE for first file (if any)
        if file_paths:
            first_file_key = sorted(file_paths.keys())[0]
            f.write(f"export INPUT_FILE=\"{file_paths[first_file_key]}\"\n")
        else:
            # Params-only job: create empty input file for scripts that expect it
            empty_input_file = os.path.join(workspace_dir, "empty_input.json")
            with open(empty_input_file, 'w') as ef:
                ef.write("{}")
            f.write(f"export INPUT_FILE=\"{empty_input_file}\"\n")
        
        if method_execution_data:
            # Add method-specific environment variables
            method_dir = method_execution_data['method_dir']
            f.write(f"export METHOD_DIR=\"{method_dir}\"\n")
            f.write(f"export PARAMS_FILE=\"{method_execution_data['params_file']}\"\n")
            f.write(f"export METHOD_NAME=\"{method_execution_data['method_name']}\"\n")
            
            # Add METHOD_DIR to PYTHONPATH environment variable to support imports in Python scripts
            f.write(f"export PYTHONPATH=\"{method_dir}:$PYTHONPATH\"\n")
            
            # Change to the method directory
            f.write(f"cd \"{method_dir}\"\n\n")
            
            # Execute the method command with the script name (basename)
            command = method_execution_data['command']
            script_filename = os.path.basename(method_execution_data['script_path'])
            
            # For args[1] (backward compat): use first file or empty file if params-only
            # Single file: file_paths["input"]
            # Multi-file: file_paths[first_key] (sorted alphabetically)
            # Array: file_paths[first_key][0] (first element)
            # Params-only: use empty_input.json
            if file_paths:
                first_file_key = sorted(file_paths.keys())[0]
                first_file_path = file_paths[first_file_key]
                
                # If it's an array, take the first element
                if isinstance(first_file_path, list):
                    first_file_path = first_file_path[0] if len(first_file_path) > 0 else os.path.join(workspace_dir, "empty_input.json")
            else:
                # Params-only: use the empty input file we created earlier
                first_file_path = os.path.join(workspace_dir, "empty_input.json")
            
            # Use relative paths for input file, metadata, and params
            relative_input_file = os.path.relpath(first_file_path, method_dir)
            relative_metadata_file = os.path.relpath(metadata_file_path, method_dir)
            relative_params_file = os.path.relpath(method_execution_data['params_file'], method_dir)
            # Pass: script input_file metadata.json params.json
            f.write(f"{command} \"{script_filename}\" \"{relative_input_file}\" \"{relative_metadata_file}\" \"{relative_params_file}\"\n")
        else:
            # If no method is provided, we must have a script
            if not job.script:
                raise ValueError("Either script or function_hash must be provided")
                
            # Write the original script
            f.write(job.script)
        
        f.write(f"\necho $? > /tmp/exit_code_{job_hash}")
    
    # Make the script executable
    os.chmod(script_path, 0o755)
    
    return script_path

def create_job(job: JobSubmission) -> Tuple[str, Dict[str, Any]]:
    """Create a new job in the database and return its hash and document."""
    # Use the pre-computed job hash from dshpc_api
    if not job.job_hash:
        raise ValueError("job_hash must be provided by dshpc_api")
    
    job_hash = job.job_hash
    
    # Sort parameters to ensure consistent ordering
    sorted_params = sort_parameters(job.parameters)
    
    # Create job document
    job_doc = {
        "job_hash": job_hash,
        "slurm_id": None,
        "function_hash": job.function_hash,
        "parameters": sorted_params,
        "status": JobStatus.PENDING,
        "created_at": datetime.utcnow(),
        "last_submission_attempt": None,  # Track when we last tried to submit
        "submission_attempts": 0,  # Count submission attempts
        "name": job.name,
        "output": None,
        "error": None
    }
    
    # Add file information (single or multi)
    if job.file_inputs:
        # Multi-file: sort by key for deterministic storage
        sorted_inputs = sort_file_inputs(job.file_inputs)
        job_doc["file_inputs"] = sorted_inputs
        job_doc["file_hash"] = None  # Explicitly None for multi-file
    else:
        # Single file
        job_doc["file_hash"] = job.file_hash
        job_doc["file_inputs"] = None
    
    # Insert job document
    jobs_collection.insert_one(job_doc)
    
    return job_hash, job_doc

def get_job_info(job_hash: str) -> Dict[str, Any]:
    """Get job information from MongoDB, retrieving large outputs from GridFS if needed."""
    job = jobs_collection.find_one({"job_hash": job_hash})
    if job:
        # Convert MongoDB ObjectId to string for JSON serialization
        job["_id"] = str(job["_id"])
        
        # Check if output is stored in GridFS
        if job.get("output_storage") == "gridfs" and job.get("output_gridfs_id"):
            try:
                db = get_jobs_db_client()
                fs = GridFS(db, collection="job_outputs")
                
                grid_id = job["output_gridfs_id"]
                if isinstance(grid_id, str):
                    grid_id = ObjectId(grid_id)
                
                # Retrieve output from GridFS
                grid_out = fs.get(grid_id)
                job["output"] = grid_out.read().decode('utf-8')
                grid_out.close()
                
                # Convert GridFS ID to string for JSON serialization
                job["output_gridfs_id"] = str(job["output_gridfs_id"])
            except Exception as e:
                logger.error(f"Error retrieving output from GridFS for job {job_hash}: {e}")
                job["output"] = f"[Error retrieving output from GridFS: {str(e)}]"
        
        # Check if error is stored in GridFS
        if job.get("error_storage") == "gridfs" and job.get("error_gridfs_id"):
            try:
                db = get_jobs_db_client()
                fs = GridFS(db, collection="job_outputs")
                
                grid_id = job["error_gridfs_id"]
                if isinstance(grid_id, str):
                    grid_id = ObjectId(grid_id)
                
                # Retrieve error from GridFS
                grid_out = fs.get(grid_id)
                job["error"] = grid_out.read().decode('utf-8')
                grid_out.close()
                
                # Convert GridFS ID to string for JSON serialization
                job["error_gridfs_id"] = str(job["error_gridfs_id"])
            except Exception as e:
                logger.error(f"Error retrieving error from GridFS for job {job_hash}: {e}")
                job["error"] = f"[Error retrieving error from GridFS: {str(e)}]"
    
    return job

def process_job_output(job_hash: str, slurm_id: str, final_state: str) -> bool:
    """Process job output files and update job status."""
    output_path = f"/tmp/output_{job_hash}.txt"
    error_path = f"/tmp/error_{job_hash}.txt"
    exit_code_path = f"/tmp/exit_code_{job_hash}"
    exit_code = 1 # Default to error if exit code file not found or invalid
    output = None
    stderr_content = None
    error = None
    job_status = JobStatus.FAILED # Default to FAILED
    
    # Read exit code if file exists
    if os.path.exists(exit_code_path):
        try:
            with open(exit_code_path) as f:
                exit_code_str = f.read().strip()
                exit_code = int(exit_code_str)
                logger.info(f"Job {job_hash}: Read exit code {exit_code} from file")
        except ValueError:
            error = "Invalid exit code format."
            logger.warning(f"Invalid exit code format for job {job_hash}: {exit_code_str}")
        except Exception as e:
            error = f"Error reading exit code: {str(e)}"
            logger.error(f"Error reading exit code for job {job_hash}: {e}")
    else:
        error = "Exit code file not found."
        logger.warning(f"Exit code file not found for job {job_hash}")

    # Read stdout (output) file if exists
    if os.path.exists(output_path):
        try:
            # Check file size first
            file_size = os.path.getsize(output_path)
            
            # For files larger than 100MB, read in chunks to avoid memory issues
            if file_size > 100 * 1024 * 1024:
                logger.info(f"Large output file detected for job {job_hash}: {file_size / (1024*1024):.1f} MB")
                # Read file in chunks and build the string
                output = ""
                chunk_size = 10 * 1024 * 1024  # 10MB chunks
                with open(output_path, 'r') as f:
                    while True:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        output += chunk
                logger.info(f"Successfully read large output file for job {job_hash}")
            else:
                # For smaller files, read normally
                with open(output_path) as f:
                    output = f.read()
        except Exception as e:
            error = f"{(error + ' ') if error else ''}Error reading output file: {str(e)}"
            logger.error(f"Error reading output file for job {job_hash}: {e}")
    else:
        error = f"{(error + ' ') if error else ''}Output file not found."
        logger.warning(f"Output file not found for job {job_hash}")
    
    # Read stderr file if exists (only use it for errors)
    if os.path.exists(error_path):
        try:
            with open(error_path) as f:
                stderr_content = f.read().strip()
        except Exception as e:
            logger.error(f"Error reading stderr file for job {job_hash}: {e}")

    # Parse output JSON to check for internal errors (if output exists and is JSON)
    output_has_error = False
    if output:
        try:
            import json
            output_data = json.loads(output)
            if isinstance(output_data, dict) and output_data.get('status') == 'error':
                output_has_error = True
                # Extract error message from output
                output_error_msg = output_data.get('error') or output_data.get('message') or 'Unknown error from script'
                logger.warning(f"Job {job_hash}: Output indicates internal error: {output_error_msg[:100]}")
        except:
            # Output is not JSON or can't be parsed - that's OK
            pass
    
    # Determine final job status based on exit code, Slurm state, AND output content
    if output_has_error:
        # If output explicitly says "error", mark as FAILED regardless of exit code
        job_status = JobStatus.FAILED
        error = f"Script reported error status: {output_error_msg}"
        logger.info(f"Job {job_hash} marked as FAILED due to error status in output")
    elif exit_code == 0:
        try:
            # If exit code is 0, use the Slurm final state if it's terminal
            if final_state in [s.value for s in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.TIMEOUT, JobStatus.NODE_FAIL, JobStatus.OUT_OF_MEMORY, JobStatus.BOOT_FAIL, JobStatus.DEADLINE, JobStatus.PREEMPTED]]:
                job_status = JobStatus(final_state)
                if job_status != JobStatus.COMPLETED:
                    # If Slurm state is failure/cancelled, use it and append exit code info
                     error = f"{(error + ' ') if error else ''}Slurm state: {final_state}. Exit code: {exit_code}."
                else:
                     # If Slurm says completed and exit code 0, it's truly completed
                     pass
            else:
                # If Slurm state is non-terminal (like RUNNING, PENDING) but exit code is 0, mark COMPLETED
                job_status = JobStatus.COMPLETED
        except ValueError:
            # If Slurm state is unknown but exit code 0, mark COMPLETED
            logger.warning(f"Unknown Slurm state: {final_state} for job {job_hash}, using COMPLETED due to exit code 0.")
            job_status = JobStatus.COMPLETED
    else:
        # If exit code is non-zero, always mark as FAILED
        job_status = JobStatus.FAILED
        error = f"{(error + ' ') if error else ''}Script exited with non-zero code: {exit_code}."
        # Add stderr content to error message only if job failed
        if stderr_content:
            error = f"{error} Stderr: {stderr_content}"
        logger.info(f"Job {job_hash} marked as FAILED due to non-zero exit code: {exit_code}")

    # Update the job status in the database
    try:
        update_job_status(job_hash, job_status, output=output, error=error)
        
        # If job completed successfully and has output, upload it as a new file
        if job_status == JobStatus.COMPLETED and output:
            try:
                output_file_hash = upload_job_output_as_file(job_hash, output)
                if output_file_hash:
                    # Update job record with output file hash
                    jobs_collection.update_one(
                        {"job_hash": job_hash},
                        {"$set": {"output_file_hash": output_file_hash}}
                    )
                    logger.info(f"Job {job_hash} output uploaded as file with hash: {output_file_hash}")
            except Exception as e:
                logger.error(f"Error uploading job output as file for {job_hash}: {e}")
                # Don't fail the job processing if output upload fails
        
        # Clean up temporary files only after successful update
        if os.path.exists(output_path):
            os.remove(output_path)
        if os.path.exists(error_path):
            os.remove(error_path)
        if os.path.exists(exit_code_path):
            os.remove(exit_code_path)
        return True
    except Exception as e:
        logger.error(f"Error updating job status for {job_hash} after processing output: {e}")
        return False


def upload_job_output_as_file(job_hash: str, output: str) -> Optional[str]:
    """
    Upload job output as a new file in the files database.
    
    Args:
        job_hash: The job hash that produced this output
        output: The output content to store
        
    Returns:
        The file hash if successfully uploaded, None otherwise
    """
    try:
        # Convert output to bytes
        output_bytes = output.encode('utf-8')
        
        # Calculate hash
        file_hash = hashlib.sha256(output_bytes).hexdigest()
        
        # Check if file with this hash already exists
        existing_file = files_collection.find_one({"file_hash": file_hash})
        if existing_file:
            logger.info(f"Output for job {job_hash} already exists as file {file_hash}")
            return file_hash
        
        # Base64 encode for storage
        content_base64 = base64.b64encode(output_bytes).decode('utf-8')
        
        # Determine storage type based on size
        file_size = len(output_bytes)
        GRIDFS_THRESHOLD = 15 * 1024 * 1024
        
        if file_size > GRIDFS_THRESHOLD:
            # Use GridFS for large outputs
            from slurm_api.config.db_config import get_files_db_client
            files_db = get_files_db_client()
            fs = GridFS(files_db, collection="fs")
            
            # Store in GridFS
            grid_id = fs.put(output_bytes,
                           filename=f"job_output_{job_hash}",
                           metadata={
                               "source_job_hash": job_hash,
                               "file_hash": file_hash,
                               "content_type": "application/json",
                               "upload_date": datetime.utcnow()
                           })
            
            # Create metadata document
            file_doc = {
                "file_hash": file_hash,
                "filename": f"job_output_{job_hash}.json",
                "content_type": "application/json",
                "storage_type": "gridfs",
                "gridfs_id": grid_id,
                "file_size": file_size,
                "status": "completed",
                "upload_date": datetime.utcnow(),
                "last_checked": datetime.utcnow(),
                "metadata": {
                    "source": "job_output",
                    "job_hash": job_hash
                }
            }
        else:
            # Store inline for small outputs
            file_doc = {
                "file_hash": file_hash,
                "content": content_base64,
                "filename": f"job_output_{job_hash}.json",
                "content_type": "application/json",
                "storage_type": "inline",
                "file_size": file_size,
                "status": "completed",
                "upload_date": datetime.utcnow(),
                "last_checked": datetime.utcnow(),
                "metadata": {
                    "source": "job_output",
                    "job_hash": job_hash
                }
            }
        
        # Insert into files collection
        files_collection.insert_one(file_doc)
        logger.info(f"Successfully uploaded job {job_hash} output as file {file_hash}")
        
        return file_hash
        
    except Exception as e:
        logger.error(f"Error uploading job output as file: {e}")
        return None 