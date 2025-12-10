import os
import uuid
import json
import hashlib
import base64
import subprocess
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


# Cache for node memory capacity (refreshed periodically)
_node_memory_cache = {"memory_mb": None, "last_check": None}

# Cache for learned memory requirements from successful jobs
# Key: method_name, Value: {"avg_memory_mb": int, "max_memory_mb": int, "samples": int}
_method_memory_history = {}


def get_slurm_node_memory() -> Optional[int]:
    """
    Get the maximum memory available on Slurm nodes.

    Returns the memory in MB, or None if unable to determine.
    Uses caching to avoid calling sinfo too frequently.
    """
    import time

    # Cache for 60 seconds
    cache_ttl = 60
    now = time.time()

    if (_node_memory_cache["memory_mb"] is not None and
        _node_memory_cache["last_check"] is not None and
        now - _node_memory_cache["last_check"] < cache_ttl):
        return _node_memory_cache["memory_mb"]

    try:
        # Get memory from sinfo (in MB)
        # Format: MEMORY column shows available memory per node
        result = subprocess.run(
            ["sinfo", "-N", "-h", "-o", "%m"],
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.returncode == 0 and result.stdout.strip():
            # Get the minimum memory across all nodes (most conservative)
            memories = []
            for line in result.stdout.strip().split('\n'):
                try:
                    mem = int(line.strip())
                    memories.append(mem)
                except ValueError:
                    continue

            if memories:
                min_memory = min(memories)
                _node_memory_cache["memory_mb"] = min_memory
                _node_memory_cache["last_check"] = now
                logger.debug(f"Slurm node memory: {min_memory} MB")
                return min_memory
    except Exception as e:
        logger.warning(f"Could not get Slurm node memory: {e}")

    return None


def calculate_safe_memory(requested_mb: int, safety_buffer: float = 1.2) -> int:
    """
    Calculate safe memory allocation, capped by node capacity.

    Args:
        requested_mb: Requested memory in MB
        safety_buffer: Multiplier for safety margin (e.g., 1.2 = 20% extra)

    Returns:
        Memory allocation in MB that fits within node capacity
    """
    # Apply safety buffer
    with_buffer = int(requested_mb * safety_buffer)

    # Get node capacity
    node_memory = get_slurm_node_memory()

    if node_memory is not None:
        # Leave 5% headroom for system processes
        max_usable = int(node_memory * 0.95)

        if with_buffer > max_usable:
            logger.warning(
                f"Requested memory {with_buffer}MB exceeds node capacity {node_memory}MB. "
                f"Capping to {max_usable}MB (95% of node memory)."
            )
            return max_usable

    return with_buffer


def preflight_memory_check(method_name: str, min_memory_mb: int) -> Tuple[bool, str, Optional[int]]:
    """
    Pre-flight check to verify if a job can run with available resources.

    Returns:
        Tuple of (can_run: bool, message: str, recommended_memory_mb: Optional[int])
        - can_run: True if job can run with available resources
        - message: Human-readable status message
        - recommended_memory_mb: Suggested memory allocation (may be adjusted from min_memory_mb)
    """
    node_memory = get_slurm_node_memory()

    if node_memory is None:
        # Can't determine node memory, proceed optimistically
        return True, "Node memory unknown, proceeding with requested allocation", min_memory_mb

    max_usable = int(node_memory * 0.95)  # 95% of node memory

    # Check if method has learned memory requirements from past jobs
    if method_name in _method_memory_history:
        history = _method_memory_history[method_name]
        learned_max = history.get("max_memory_mb", min_memory_mb)

        # Use learned max if higher than min_memory_mb (method learned it needs more)
        if learned_max > min_memory_mb:
            logger.info(f"Using learned memory for {method_name}: {learned_max}MB (from {history['samples']} samples)")
            min_memory_mb = learned_max

    # Calculate required memory with safety buffer
    required_with_buffer = int(min_memory_mb * 1.2)

    if required_with_buffer > max_usable:
        # Job needs more memory than available
        if min_memory_mb > max_usable:
            # Even base requirement exceeds capacity - this will likely OOM
            return False, (
                f"Method '{method_name}' requires {min_memory_mb}MB but node only has {max_usable}MB available. "
                f"This job will likely fail with OOM. Consider increasing Docker memory allocation."
            ), None
        else:
            # Base fits but buffer doesn't - run with reduced safety margin
            logger.warning(
                f"Method '{method_name}' safety buffer ({required_with_buffer}MB) exceeds node capacity. "
                f"Running with minimal margin at {max_usable}MB."
            )
            return True, f"Running with reduced safety margin ({max_usable}MB)", max_usable

    return True, f"Memory check passed ({required_with_buffer}MB of {max_usable}MB available)", required_with_buffer


def record_job_memory_usage(method_name: str, actual_memory_mb: int, success: bool):
    """
    Record actual memory usage for a completed job to improve future estimates.

    This creates a learning system where the HPC cluster adapts to actual
    workload requirements over time.
    """
    if not success or actual_memory_mb <= 0:
        return

    global _method_memory_history

    if method_name not in _method_memory_history:
        _method_memory_history[method_name] = {
            "avg_memory_mb": actual_memory_mb,
            "max_memory_mb": actual_memory_mb,
            "samples": 1
        }
    else:
        history = _method_memory_history[method_name]
        samples = history["samples"]

        # Update running average (weighted to favor recent data)
        weight = min(samples, 10)  # Cap weight at 10 for stability
        history["avg_memory_mb"] = int(
            (history["avg_memory_mb"] * weight + actual_memory_mb) / (weight + 1)
        )

        # Track maximum seen
        history["max_memory_mb"] = max(history["max_memory_mb"], actual_memory_mb)
        history["samples"] = samples + 1

        logger.debug(
            f"Updated memory history for {method_name}: "
            f"avg={history['avg_memory_mb']}MB, max={history['max_memory_mb']}MB, samples={history['samples']}"
        )


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
    
    # Check for extracted JSON files and inject their values into params
    # This handles $ref extractions that produce JSON with data/text or value fields
    injected_params = {}
    logger.info(f"  Checking {len(file_paths)} file_paths for JSON injection...")
    for input_name, file_path in list(file_paths.items()):
        logger.info(f"    - '{input_name}': {file_path}")
        if isinstance(file_path, str) and file_path.endswith('.json'):
            logger.info(f"      -> JSON file detected, attempting injection")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                    logger.info(f"      -> Parsed JSON, keys: {list(content.keys()) if isinstance(content, dict) else 'not a dict'}")
                    # Check for data/text structure (standard extraction format)
                    if isinstance(content, dict) and 'data' in content and 'text' in content.get('data', {}):
                        value = content['data']['text']
                        injected_params[input_name] = value
                        logger.info(f"  Injected '{input_name}' from data/text into params (length: {len(str(value))})")
                    # Check for value structure (legacy format)
                    elif isinstance(content, dict) and 'value' in content:
                        injected_params[input_name] = content['value']
                        logger.info(f"  Injected '{input_name}' from value into params")
                    else:
                        logger.info(f"      -> No data/text or value structure found")
            except Exception as e:
                logger.warning(f"  Could not parse '{input_name}' as JSON for injection: {e}")

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

        # Merge injected params from extracted JSON files into job parameters
        merged_params = dict(job.parameters) if job.parameters else {}
        if injected_params:
            merged_params.update(injected_params)
            logger.info(f"Merged {len(injected_params)} injected params into job parameters")

        # Sort parameters to ensure consistent ordering
        sorted_params = sort_parameters(merged_params)
        
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

        # ========================================================================
        # RESOURCE ALLOCATION LOGIC
        # ========================================================================
        # Priority order for resources:
        # 1. Method's explicit settings (from method.json "resources" field)
        # 2. System defaults from dshpc.conf
        # 3. Hardcoded conservative fallbacks
        #
        # The goal is to:
        # - Maximize parallelism (more jobs running simultaneously)
        # - Prevent OOM kills (each job gets enough memory)
        # - Let methods specify their minimum requirements
        # ========================================================================

        resources = method_doc.get("resources") if method_doc else None
        if resources is None:
            resources = {}

        # --- CPU ALLOCATION ---
        # Default: 1 CPU per job (maximizes parallelism)
        # Methods can request more if needed
        default_cpus = 1  # Conservative default for parallelism

        # Read from dshpc.conf if available
        config_path = "/config/dshpc.conf"
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as cfg:
                    for line in cfg:
                        line = line.strip()
                        if line.startswith('DEFAULT_CPUS_PER_TASK='):
                            default_cpus = int(line.split('=')[1])
                            break
            except Exception:
                pass

        # Env var takes precedence
        default_cpus = int(os.environ.get("DEFAULT_CPUS_PER_TASK", default_cpus))

        # Use method's cpus setting, or default
        cpus = resources.get("cpus", default_cpus)

        # 0 means use all available CPUs (single job per node)
        if cpus == 0:
            try:
                import multiprocessing
                cpus = multiprocessing.cpu_count()
            except Exception:
                cpus = 8

        f.write(f"#SBATCH --cpus-per-task={cpus}\n")

        # --- MEMORY ALLOCATION ---
        # PRIORITY: AVOID OOM KILLS AT ALL COSTS!
        # Users cannot intervene, so jobs must not fail due to memory.
        #
        # Strategy:
        # 0. memory_override_mb (OOM retry) - highest priority, set after OOM kill
        # 1. memory_mb (explicit override) - used as-is, user knows best
        # 2. min_memory_mb (method's minimum) - add safety buffer, capped by node capacity
        # 3. Default: generous 2GB per CPU, capped by node capacity
        #
        # DYNAMIC: Memory is automatically capped to fit within node capacity.
        # This prevents "Memory specification can not be satisfied" errors.

        # Check for OOM retry memory override from database
        job_doc = jobs_collection.find_one({"job_hash": job_hash})
        memory_override_mb = job_doc.get("memory_override_mb") if job_doc else None
        if memory_override_mb:
            logger.info(f"Using OOM retry memory override: {memory_override_mb}MB")

        memory_mb = resources.get("memory_mb")  # Explicit override (no buffer)
        min_memory_mb = resources.get("min_memory_mb")  # Minimum requirement

        # Default memory per CPU - GENEROUS to prevent OOM
        # 2GB per CPU gives R/Python plenty of headroom
        default_mem_per_cpu = 2048  # 2GB per CPU

        # Safety buffer: add 20% to min_memory_mb to prevent edge-case OOM
        SAFETY_BUFFER = 1.2  # 20% extra

        # Check for exclusive mode FIRST - it takes precedence over memory settings
        exclusive = resources.get("exclusive", False)
        if exclusive:
            # Exclusive mode: request the entire node
            # This prevents other jobs from running concurrently and competing for memory
            f.write("#SBATCH --exclusive\n")
            f.write("#SBATCH --mem=0\n")  # Request all available memory
            safe_memory = get_slurm_node_memory()  # Track actual node memory for learning
            logger.info(f"Job {job_hash}: Running in exclusive mode (entire node reserved, ~{safe_memory}MB)")
        # OOM retry override takes highest priority (when not exclusive)
        elif memory_override_mb is not None:
            # OOM retry - use the override memory, cap to node capacity
            safe_memory = calculate_safe_memory(memory_override_mb, safety_buffer=1.0)
            f.write(f"#SBATCH --mem={safe_memory}M\n")
            # Store actual memory used for tracking
            if job_doc:
                jobs_collection.update_one(
                    {"job_hash": job_hash},
                    {"$set": {"last_memory_mb": safe_memory}}
                )
        elif memory_mb is not None:
            # Explicit memory setting takes precedence - no buffer
            # User explicitly set this, so trust it
            if memory_mb == 0:
                # 0 = all available memory (single job per node)
                f.write("#SBATCH --mem=0\n")
                safe_memory = None  # Can't track "all memory"
            else:
                # Cap explicit memory to node capacity
                safe_memory = calculate_safe_memory(memory_mb, safety_buffer=1.0)
                f.write(f"#SBATCH --mem={safe_memory}M\n")
        elif min_memory_mb is not None:
            # Method specified minimum - add safety buffer, cap to node capacity
            safe_memory = calculate_safe_memory(min_memory_mb, safety_buffer=SAFETY_BUFFER)
            f.write(f"#SBATCH --mem={safe_memory}M\n")
        else:
            # No memory specified - use generous default, cap to node capacity
            calculated_mem = cpus * default_mem_per_cpu
            safe_memory = calculate_safe_memory(calculated_mem, safety_buffer=1.0)
            f.write(f"#SBATCH --mem={safe_memory}M\n")

        # Track memory used for OOM retry mechanism (if not already set by OOM override)
        if memory_override_mb is None and safe_memory is not None and job_doc:
            jobs_collection.update_one(
                {"job_hash": job_hash},
                {"$set": {"last_memory_mb": safe_memory}}
            )

        # Time limit: use method's setting if specified
        time_limit = resources.get("time_limit")
        if time_limit:
            f.write(f"#SBATCH --time={time_limit}\n")

        # GPUs: use method's setting if specified
        gpus = resources.get("gpus")
        if gpus:
            f.write(f"#SBATCH --gpus={gpus}\n")

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

    # Check if job was cancelled/killed
    # This typically happens when:
    # 1. scancel is used
    # 2. Container/service restart during job execution
    # 3. Job was externally terminated
    #
    # Detection criteria:
    # - Slurm explicitly says CA/CANCELLED
    # - No output files exist at all (job never wrote anything)
    # - Exit code file doesn't exist (job was interrupted before completion)
    #   AND slurm doesn't know the job (empty final_state or non-terminal state)
    no_output_files = not os.path.exists(exit_code_path) and not os.path.exists(output_path) and not os.path.exists(error_path)
    exit_code_missing = not os.path.exists(exit_code_path)
    slurm_unknown = final_state in [None, "", "R", "PD", "CG"]  # Slurm doesn't know job or says it's still running

    is_cancelled = (
        final_state in ["CA", "CANCELLED"] or  # Explicit cancel
        no_output_files or  # No traces at all
        (exit_code_missing and slurm_unknown)  # Interrupted before completion and slurm lost track
    )

    if is_cancelled:
        # Job was cancelled or interrupted - mark as CANCELLED to allow resubmission
        logger.warning(f"Job {job_hash} appears cancelled or interrupted (final_state={final_state}, exit_code_exists={os.path.exists(exit_code_path)}). Marking as CANCELLED.")
        try:
            update_job_status(job_hash, JobStatus.CANCELLED, error="Job was cancelled or interrupted by service restart")
            logger.info(f"Job {job_hash} marked as CANCELLED (allows resubmission)")
            return True
        except Exception as e:
            logger.error(f"Error marking cancelled job {job_hash}: {e}")
            return False

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
    # Also extract JSON from output if there are extra lines before it (e.g., logging messages)
    output_has_error = False
    if output:
        try:
            import json
            import re
            
            # Try to extract JSON from output (in case there are logging messages before JSON)
            # Look for JSON object starting with { and ending with }
            json_match = re.search(r'\{.*\}', output, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                output_data = json.loads(json_str)
                # If we extracted JSON, use the cleaned JSON as the output
                output = json_str
            else:
                # No JSON found, try parsing the whole output
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

        # Record memory usage for learning (helps future jobs of same method)
        job_doc = jobs_collection.find_one({"job_hash": job_hash})
        if job_doc and job_status == JobStatus.COMPLETED:
            method_name = job_doc.get("method_name") or job_doc.get("method_hash", "unknown")
            actual_memory = job_doc.get("last_memory_mb") or job_doc.get("memory_override_mb")
            if actual_memory:
                record_job_memory_usage(method_name, actual_memory, success=True)

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