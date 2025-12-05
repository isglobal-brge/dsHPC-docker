"""
Meta-job service for handling chained processing steps.
"""
import uuid
import hashlib
import json
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional
from dshpc_api.models.meta_job import (
    MetaJobStatus, MetaJobRequest, MetaJobResponse, 
    MetaJobInfo, MetaJobStepInfo, CurrentStepInfo
)
from dshpc_api.services.db_service import (
    get_meta_jobs_db, get_files_db, get_jobs_db, get_job_by_hash, get_file_by_hash
)
from dshpc_api.services.job_service import (
    submit_job, find_existing_job, get_job_status,
    get_latest_method_hash,
    RETRIABLE_FAILED_STATUSES,
    NON_RETRIABLE_FAILED_STATUSES,
    STATUS_DESCRIPTIONS,
    COMPLETED_STATUSES,
    IN_PROGRESS_STATUSES
)
from dshpc_api.services.method_service import check_method_functionality
from dshpc_api.utils.parameter_utils import sort_parameters
from dshpc_api.utils.sorting_utils import sort_file_inputs, sort_chain
from dshpc_api.utils.file_input_resolver import resolve_and_inject_file_inputs
from dshpc_api.services.job_service import is_method_unavailable_error, is_file_not_found_error
import asyncio
import logging

logger = logging.getLogger(__name__)


def compute_meta_job_hash(request: MetaJobRequest, chain_function_hashes: List[str]) -> str:
    """
    Compute a deterministic hash for a meta-job based on its inputs and chain.
    
    Args:
        request: Meta-job request
        chain_function_hashes: List of function hashes for each step in chain (in order)
        
    Returns:
        SHA256 hash as hex string
    """
    hash_components = []
    
    # Component 1: Initial file hash(es)
    if request.initial_file_inputs:
        # Multi-file: sort by key for determinism
        sorted_inputs = sort_file_inputs(request.initial_file_inputs)
        for name, file_hash in sorted_inputs.items():
            hash_components.append(f"file_input:{name}:{file_hash}")
    else:
        # Single file (or PARAMS_ONLY)
        hash_components.append(f"initial_file:{request.initial_file_hash}")
    
    # Component 2: Each step in the chain
    for i, (step, function_hash) in enumerate(zip(request.method_chain, chain_function_hashes)):
        hash_components.append(f"step:{i}")
        hash_components.append(f"function:{function_hash}")

        # Sort parameters for determinism
        sorted_params = sort_parameters(step.parameters)
        params_json = json.dumps(sorted_params, sort_keys=True)
        hash_components.append(f"params:{params_json}")

        # Include file_inputs in hash (important for $ref paths)
        # This ensures different $ref paths produce different meta-job hashes
        if step.file_inputs:
            sorted_file_inputs = sort_file_inputs(step.file_inputs)
            file_inputs_json = json.dumps(sorted_file_inputs, sort_keys=True)
            hash_components.append(f"file_inputs:{file_inputs_json}")
    
    # Combine all components
    hash_input = "|".join(hash_components)
    hash_bytes = hash_input.encode('utf-8')
    
    # Compute SHA256
    meta_job_hash = hashlib.sha256(hash_bytes).hexdigest()
    
    logger.debug(f"Computed meta-job hash: {meta_job_hash[:12]}... from {len(hash_components)} components")
    return meta_job_hash


async def extract_and_store_path_from_hash(source_hash: str, path: str, files_db, meta_job_hash: str, step_idx: int, param_name: str) -> str:
    """
    Extract a value from a JSON output file using a path and store it as a new file.
    
    Args:
        source_hash: Hash of the source file containing JSON
        path: Slash-separated path to extract (e.g., "data/text")
        files_db: Database connection
        meta_job_hash: Meta-job hash (for logging/metadata)
        step_idx: Step index (for logging/metadata)
        param_name: Parameter name (for logging/metadata)
        
    Returns:
        Hash of the newly created file with extracted value
    """
    # Get source file
    source_doc = await files_db.files.find_one({"file_hash": source_hash})
    if not source_doc:
        raise ValueError(f"Source file {source_hash} not found for path extraction")
    
    # Decode content
    import base64
    content_bytes = base64.b64decode(source_doc["content"])
    content_str = content_bytes.decode('utf-8')
    
    # Parse JSON
    try:
        data = json.loads(content_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON from {source_hash}: {e}")
    
    # Navigate path
    current = data
    parts = path.split("/")
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            raise ValueError(f"Path {path} not found in output")
    
    # Wrap extracted value in standard format
    # For primitives (string, number), wrap in data/text structure
    # For complex values (dict, list), store as-is
    if isinstance(current, (str, int, float, bool)):
        extracted_data = {"data": {"text": str(current)}}
    else:
        extracted_data = current
    extracted_json = json.dumps(extracted_data, indent=2)
    extracted_bytes = extracted_json.encode('utf-8')

    # Calculate hash
    extracted_hash = hashlib.sha256(extracted_bytes).hexdigest()

    # Check if already exists
    existing = await files_db.files.find_one({"file_hash": extracted_hash})
    if existing:
        logger.debug(f"Extracted value for {path} already exists as {extracted_hash[:8]}...")
        return extracted_hash

    # Store as new file with metadata
    file_doc = {
        "file_hash": extracted_hash,
        "filename": f"extracted_{param_name}.json",
        "size": len(extracted_bytes),
        "content": base64.b64encode(extracted_bytes).decode('utf-8'),
        "uploaded_at": datetime.utcnow(),
        "metadata": {
            "source": "path_extraction",
            "source_hash": source_hash,
            "extracted_path": path,
            "source_node": f"{meta_job_hash}_step_{step_idx}"
        }
    }
    
    await files_db.files.insert_one(file_doc)
    logger.info(f"Extracted {path} from {source_hash[:8]}... -> {extracted_hash[:8]}...")
    
    return extracted_hash


async def submit_meta_job(request: MetaJobRequest) -> Tuple[bool, str, Optional[MetaJobResponse]]:
    """
    Submit a meta-job that chains multiple processing steps.
    
    Args:
        request: Meta-job request with initial file and method chain
        
    Returns:
        Tuple of (success, message, response)
    """
    try:
        logger.info(f"📋 NEW META-JOB SUBMISSION")
        if request.initial_file_inputs:
            logger.info(f"  Initial files: {list(request.initial_file_inputs.keys())}")
        else:
            logger.info(f"  Initial file: {request.initial_file_hash[:8] if request.initial_file_hash else 'none'}...")
        logger.info(f"  Chain length: {len(request.method_chain)} steps")
        for i, step in enumerate(request.method_chain):
            logger.info(f"  Step {i}: {step.method_name} with {len(step.parameters)} params")
        
        # Validate initial file(s) exist AND are completed
        files_db = await get_files_db()
        
        if request.initial_file_inputs:
            # Multi-file: validate ALL files (handles both single files and arrays)
            for name, file_ref in request.initial_file_inputs.items():
                if isinstance(file_ref, list):
                    # Array of files
                    for idx, file_hash_item in enumerate(file_ref):
                        file_doc = await files_db.files.find_one({"file_hash": file_hash_item})
                        if not file_doc:
                            logger.error(f"File '{name}[{idx}]' with hash {file_hash_item} not found")
                            return False, f"File '{name}[{idx}]' with hash {file_hash_item} not found", None
                        
                        # Only check status for uploaded files, not job outputs or path extractions
                        metadata = file_doc.get("metadata") or {}
                        metadata_source = metadata.get("source")
                        is_generated = metadata_source in ["job_output", "path_extraction"]
                        if not is_generated and file_doc.get("status") != "completed":
                            file_status = file_doc.get("status", "unknown")
                            logger.error(f"File '{name}[{idx}]' not completed (status: {file_status})")
                            return False, f"File '{name}[{idx}]' is not ready (status: {file_status}). Please wait for upload to complete.", None
                else:
                    # Single file hash
                    file_doc = await files_db.files.find_one({"file_hash": file_ref})
                    if not file_doc:
                        logger.error(f"File '{name}' with hash {file_ref} not found")
                        return False, f"File '{name}' with hash {file_ref} not found", None
                    
                    # Only check status for uploaded files, not job outputs or path extractions
                    metadata = file_doc.get("metadata") or {}
                    metadata_source = metadata.get("source")
                    is_generated = metadata_source in ["job_output", "path_extraction"]
                    if not is_generated and file_doc.get("status") != "completed":
                        file_status = file_doc.get("status", "unknown")
                        logger.error(f"File '{name}' not completed (status: {file_status})")
                        return False, f"File '{name}' is not ready (status: {file_status}). Please wait for upload to complete.", None
        else:
            # Single file (legacy) or params-only
            # Special case: PARAMS_ONLY_ hash or None means no actual input file needed
            if request.initial_file_hash and not request.initial_file_hash.startswith("PARAMS_ONLY_"):
                file_doc = await files_db.files.find_one({"file_hash": request.initial_file_hash})
                
                if not file_doc:
                    logger.error(f"Initial file {request.initial_file_hash} not found")
                    return False, f"Initial file with hash {request.initial_file_hash} not found", None
                
                # Only check status for uploaded files, not job outputs or path extractions
                metadata = file_doc.get("metadata") or {}
                metadata_source = metadata.get("source")
                is_generated = metadata_source in ["job_output", "path_extraction"]
                if not is_generated and file_doc.get("status") != "completed":
                    file_status = file_doc.get("status", "unknown")
                    logger.error(f"Initial file {request.initial_file_hash} not completed (status: {file_status})")
                    return False, f"Initial file with hash {request.initial_file_hash} is not ready (status: {file_status}). Please wait for upload to complete.", None
        
        # Validate all methods exist and are functional, and collect function hashes
        chain_function_hashes = []
        for step in request.method_chain:
            is_functional, msg = await check_method_functionality(step.method_name)
            if not is_functional:
                return False, f"Method '{step.method_name}' validation failed: {msg}", None
            
            # Get function hash for this method
            function_hash = await get_latest_method_hash(step.method_name)
            if not function_hash:
                return False, f"Method '{step.method_name}' not found or not active", None
            chain_function_hashes.append(function_hash)
        
        # Compute meta-job hash for deduplication
        meta_job_hash = compute_meta_job_hash(request, chain_function_hashes)
        logger.info(f"  Computed hash: {meta_job_hash[:16]}...")
        
        # Check if this exact meta-job already exists
        # Exclude cancelled meta-jobs - they should be treated as non-existent for re-submission
        meta_jobs_db = await get_meta_jobs_db()
        existing_meta_job = await meta_jobs_db.meta_jobs.find_one({
            "meta_job_hash": meta_job_hash,
            "status": {"$nin": ["cancelled", "CANCELLED"]}
        })

        if existing_meta_job:
            existing_status = existing_meta_job.get("status")
            logger.info(f"  ♻️  Found existing meta-job with status: {existing_status}")
            
            if existing_status in [MetaJobStatus.COMPLETED, MetaJobStatus.RUNNING, MetaJobStatus.PENDING]:
                # Return existing meta-job (transparent caching)
                logger.info(f"  ✓ Returning cached meta-job: {meta_job_hash[:16]}...")
                return True, "Meta-job already exists (cached result)", MetaJobResponse(
                    meta_job_hash=meta_job_hash,
                    status=existing_status,
                    estimated_steps=len(request.method_chain),
                    message=f"Existing meta-job returned (status: {existing_status})"
                )
            elif existing_status == MetaJobStatus.FAILED:
                # Check if methods have changed (function_hash mismatch)
                existing_chain = existing_meta_job.get("chain", [])
                methods_changed = False
                
                for i, (existing_step, new_hash) in enumerate(zip(existing_chain, chain_function_hashes)):
                    if existing_step.get("function_hash") != new_hash:
                        methods_changed = True
                        logger.info(f"  Method at step {i} has changed: {existing_step.get('function_hash')[:8]} -> {new_hash[:8]}")
                        break
                
                if not methods_changed:
                    # Check if failure was due to method unavailability
                    error_msg = existing_meta_job.get("error", "Unknown error")
                    failed_method = is_method_unavailable_error(error_msg)
                    
                    if failed_method:
                        # Check if method is now available
                        method_hash = await get_latest_method_hash(failed_method)
                        if method_hash:
                            logger.info(f"  ♻️  Method '{failed_method}' now available - deleting failed meta-job for retry")
                            await meta_jobs_db.meta_jobs.delete_one({"meta_job_hash": meta_job_hash})
                            # Continue to create new meta-job below
                        else:
                            logger.info(f"  ✗ Method '{failed_method}' still not available")
                            return False, f"Meta-job previously failed: {error_msg}", None
                    else:
                        # Check for file not found
                        missing_file_hash = is_file_not_found_error(error_msg)
                        
                        if missing_file_hash:
                            # Check if file now exists
                            files_db = await get_files_db()
                            file_doc = await files_db.files.find_one({"file_hash": missing_file_hash})
                            
                            if file_doc and file_doc.get("status") == "completed":
                                logger.info(f"  ♻️  File {missing_file_hash[:12]}... now available - deleting failed meta-job for retry")
                                await meta_jobs_db.meta_jobs.delete_one({"meta_job_hash": meta_job_hash})
                                # Continue to create new meta-job below
                            else:
                                logger.info(f"  ✗ File {missing_file_hash[:12]}... still not available")
                                return False, f"Meta-job previously failed: {error_msg}", None
                        else:
                            # Not file-related or method-related - fail fast
                            logger.info(f"  ✗ Meta-job previously failed with same configuration")
                            return False, f"Meta-job previously failed: {error_msg}", None
                else:
                    # Methods changed, allow recomputation
                    logger.info(f"  ↻ Methods changed, will recompute")
                    # Delete old failed meta-job and create new one
                    await meta_jobs_db.meta_jobs.delete_one({"meta_job_hash": meta_job_hash})
        
        # Create new meta-job record with hash as ID
        logger.info(f"  Creating new meta-job with hash: {meta_job_hash[:16]}...")
        
        # Initialize chain info
        chain_info = []
        for i, (step, function_hash) in enumerate(zip(request.method_chain, chain_function_hashes)):
            
            # For first step: use initial_file_hash or initial_file_inputs
            step_input = None
            step_file_inputs = None
            
            if i == 0:
                if request.initial_file_inputs:
                    step_file_inputs = request.initial_file_inputs
                else:
                    step_input = request.initial_file_hash
            
            step_info = {
                "step": i,
                "method_name": step.method_name,
                "function_hash": function_hash,
                "parameters": sort_parameters(step.parameters),
                "input_file_hash": step_input,  # Single file or None
                "input_file_inputs": step_file_inputs,  # Multi-file or None
                "output_file_hash": None,
                "job_hash": None,
                "status": "pending",
                "cached": False
            }
            
            # Include file_inputs if defined in step (from chain definition)
            if step.file_inputs is not None:
                step_info["file_inputs"] = step.file_inputs
                logger.info(f"  Step {i} has file_inputs: {list(step.file_inputs.keys())}")
            
            chain_info.append(step_info)
        
        # Create meta-job document with hash as primary ID
        meta_job_doc = {
            "meta_job_hash": meta_job_hash,  # Hash is the primary identifier
            "initial_file_hash": request.initial_file_hash,  # Can be None
            "initial_file_inputs": request.initial_file_inputs,  # Can be None
            "chain": chain_info,
            "status": MetaJobStatus.PENDING,
            "current_step": None,
            "final_job_hash": None,
            "error": None,
            "created_at": datetime.utcnow(),
            "updated_at": None,
            "completed_at": None
        }
        
        await meta_jobs_db.meta_jobs.insert_one(meta_job_doc)
        
        # Start processing asynchronously
        asyncio.create_task(process_meta_job_chain(meta_job_hash))
        
        return True, "Meta-job submitted successfully", MetaJobResponse(
            meta_job_hash=meta_job_hash,
            status=MetaJobStatus.PENDING,
            estimated_steps=len(request.method_chain),
            message="Processing chain asynchronously"
        )
        
    except Exception as e:
        logger.error(f"Error submitting meta-job: {e}")
        return False, f"Error submitting meta-job: {str(e)}", None


async def process_meta_job_chain(meta_job_hash: str):
    """
    Process a meta-job chain asynchronously.
    
    Args:
        meta_job_hash: Hash of the meta-job to process
    """
    meta_jobs_db = await get_meta_jobs_db()
    
    try:
        # Update status to running
        await meta_jobs_db.meta_jobs.update_one(
            {"meta_job_hash": meta_job_hash},
            {"$set": {
                "status": MetaJobStatus.RUNNING,
                "updated_at": datetime.utcnow()
            }}
        )
        
        # Get meta-job document
        meta_job = await meta_jobs_db.meta_jobs.find_one({"meta_job_hash": meta_job_hash})
        if not meta_job:
            logger.error(f"Meta-job {meta_job_hash} not found")
            return
        
        # Initialize with initial file hash or file_inputs
        current_input_hash = meta_job.get("initial_file_hash")
        current_file_inputs = meta_job.get("initial_file_inputs")
        accumulated_file_inputs = {}  # Track file_inputs across chain steps
        
        # Process each step in the chain
        for i, step in enumerate(meta_job["chain"]):
            # Check if step is already completed (recovery scenario)
            # If step.status is 'completed' or 'cached', skip processing and use its output
            step_status = step.get("status")
            if step_status in ["completed", "cached"]:
                output_hash = step.get("output_hash")
                if output_hash:
                    logger.info(f"⏭️ SKIP - Meta-job {meta_job_hash[:16]}... step {i} already '{step_status}', using output: {output_hash[:16]}...")
                    # Update current_input_hash for next step
                    current_input_hash = output_hash
                    continue
                else:
                    # Step marked as completed but no output hash - this is an error, reprocess
                    logger.warning(f"⚠️ Meta-job {meta_job_hash[:16]}... step {i} marked as '{step_status}' but no output_hash, reprocessing...")

            # Update current step
            await meta_jobs_db.meta_jobs.update_one(
                {"meta_job_hash": meta_job_hash},
                {"$set": {
                    "current_step": i,
                    "updated_at": datetime.utcnow()
                }}
            )

            # For first step: use initial file(s), for subsequent steps: use previous output
            if i == 0:
                step_file_hash = current_input_hash
                step_file_inputs = current_file_inputs
                if step_file_inputs:
                    accumulated_file_inputs = step_file_inputs.copy()
            else:
                # Subsequent steps: start with accumulated file_inputs from previous steps
                step_file_hash = current_input_hash
                step_file_inputs = accumulated_file_inputs.copy() if accumulated_file_inputs else None
            
            # Update step with input info
            step["input_file_hash"] = step_file_hash
            step["input_file_inputs"] = step_file_inputs
            
            # Resolve file_inputs $ref to actual hashes
            from dshpc_api.services.db_service import get_files_db
            files_db = await get_files_db()
            
            # Get step's file_inputs definition (may contain $ref)
            step_file_inputs_with_refs = step.get("file_inputs", {})
            resolved_file_inputs = {}
            
            if step_file_inputs_with_refs:
                logger.info(f"Meta-job {meta_job_hash} step {i}: Processing file_inputs: {step_file_inputs_with_refs}")
                for input_name, ref_value in step_file_inputs_with_refs.items():
                    if isinstance(ref_value, list):
                        # Array of files - resolve each element
                        resolved_array = []
                        for idx, item in enumerate(ref_value):
                            if isinstance(item, str) and item.startswith("$ref:prev"):
                                if i == 0:
                                    raise ValueError(f"Cannot use $ref:prev in first step of meta-job")

                                prev_step = meta_job["chain"][i-1]
                                prev_hash = prev_step.get("output_hash")
                                if not prev_hash:
                                    raise ValueError(f"Previous step {i-1} has no output")

                                # Check if path extraction needed
                                if item.startswith("$ref:prev/"):
                                    ref_path = item[10:]  # Remove "$ref:prev/"
                                    from dshpc_api.background.pipeline_orchestrator import extract_and_store_path
                                    extracted_hash = await extract_and_store_path(
                                        prev_hash, ref_path, f"metajob_{meta_job_hash[:12]}", f"{input_name}_{idx}"
                                    )
                                    resolved_array.append(extracted_hash)
                                else:
                                    # Use full prev output
                                    resolved_array.append(prev_hash)
                            elif isinstance(item, str) and item == "$ref:initial":
                                # Reference to meta-job's initial file hash
                                initial_hash = meta_job.get("initial_file_hash")
                                if not initial_hash:
                                    raise ValueError(f"Cannot use $ref:initial - meta-job has no initial_file_hash")
                                resolved_array.append(initial_hash)
                            else:
                                # Direct hash or other reference
                                resolved_array.append(item)
                        resolved_file_inputs[input_name] = resolved_array
                    elif isinstance(ref_value, str) and ref_value.startswith("$ref:prev"):
                        # Single file with $ref:prev
                        if i == 0:
                            raise ValueError(f"Cannot use $ref:prev in first step of meta-job")

                        prev_step = meta_job["chain"][i-1]
                        prev_hash = prev_step.get("output_hash")
                        if not prev_hash:
                            raise ValueError(f"Previous step {i-1} has no output")

                        # Check if path extraction needed
                        if ref_value.startswith("$ref:prev/"):
                            ref_path = ref_value[10:]  # Remove "$ref:prev/"
                            # Extract and store path as new file
                            from dshpc_api.background.pipeline_orchestrator import extract_and_store_path
                            extracted_hash = await extract_and_store_path(
                                prev_hash, ref_path, f"metajob_{meta_job_hash[:12]}", input_name
                            )
                            resolved_file_inputs[input_name] = extracted_hash
                        else:
                            # Use full prev output
                            resolved_file_inputs[input_name] = prev_hash
                    elif isinstance(ref_value, str) and ref_value == "$ref:initial":
                        # Reference to meta-job's initial file hash
                        initial_hash = meta_job.get("initial_file_hash")
                        if not initial_hash:
                            raise ValueError(f"Cannot use $ref:initial - meta-job has no initial_file_hash")
                        resolved_file_inputs[input_name] = initial_hash
                    else:
                        # Direct hash (single file, no ref)
                        resolved_file_inputs[input_name] = ref_value
                
                logger.info(f"Meta-job {meta_job_hash} step {i}: Resolved file_inputs: {resolved_file_inputs}")
            
            # Keep parameters as-is (no resolution)
            resolved_params = step["parameters"].copy()
            
            # Use resolved file_inputs for this job submission
            # If step defines file_inputs, use those (resolved). Otherwise use step_file_inputs (legacy/accumulated)
            lookup_file_inputs = resolved_file_inputs if step_file_inputs_with_refs else step_file_inputs
            
            # Check if this step already exists (deduplication)
            existing_job = await find_existing_job(
                file_hash=step_file_hash,
                file_inputs=lookup_file_inputs,
                function_hash=step["function_hash"],
                parameters=resolved_params
            )
            
            if existing_job and existing_job.get("status") in COMPLETED_STATUSES:
                # Job already completed, use its output
                logger.info(f"✅ CACHE HIT - Meta-job {meta_job_hash} step {i}:")
                logger.info(f"  Method: {step['method_name']}")
                if step_file_inputs:
                    logger.info(f"  Input files: {list(step_file_inputs.keys())}")
                else:
                    logger.info(f"  Input hash: {step_file_hash[:8] if step_file_hash else 'none'}...")
                logger.info(f"  Cached job hash: {existing_job['job_hash']}")
                logger.info(f"  Skipping execution - using cached output")
                
                # Update current step info even for cached jobs
                await update_meta_job_current_step(meta_job_hash, i, existing_job['job_hash'], 
                                                   existing_job['status'], False)
                
                # Get output_file_hash from existing job
                # This is the hash of the file in the files collection
                output_hash = existing_job.get("output_file_hash")
                
                if not output_hash:
                    # Fallback: calculate hash from output content
                    job_output = existing_job.get("output")
                    output_gridfs_id = existing_job.get("output_gridfs_id")
                    
                    if job_output:
                        # Small output stored inline
                        import hashlib
                        output_hash = hashlib.sha256(job_output.encode('utf-8')).hexdigest()
                    elif output_gridfs_id:
                        # Large output in GridFS - use gridfs_id as proxy for hash
                        output_hash = str(output_gridfs_id)
                    else:
                        raise Exception(f"Cached job {existing_job['job_hash']} has no output")
                
                logger.info(f"  Using output_file_hash for next step: {output_hash[:16] if output_hash else 'None'}...")
                
                step["job_hash"] = existing_job["job_hash"]
                step["output_hash"] = output_hash
                step["status"] = "completed"
                step["cached"] = True
                
                # Update chain in database
                await meta_jobs_db.meta_jobs.update_one(
                    {"meta_job_hash": meta_job_hash},
                    {"$set": {
                        f"chain.{i}": step,
                        "updated_at": datetime.utcnow()
                    }}
                )
                
                # Use output hash as input for next step
                current_input_hash = output_hash
                
            else:
                # Need to submit new job
                logger.info(f"🔄 CACHE MISS - Meta-job {meta_job_hash} step {i}:")
                logger.info(f"  Method: {step['method_name']}")
                if lookup_file_inputs:
                    logger.info(f"  Input files: {list(lookup_file_inputs.keys())}")
                else:
                    logger.info(f"  Input hash: {step_file_hash[:8] if step_file_hash else 'none'}...")
                if existing_job:
                    logger.info(f"  Found job but status is: {existing_job.get('status', 'unknown')}")
                else:
                    logger.info(f"  No existing job found - submitting new job")
                
                # Use already-resolved parameters and file inputs
                success, message, job_data = await submit_job(
                    file_hash=step_file_hash,
                    file_inputs=lookup_file_inputs,
                    function_hash=step["function_hash"],
                    parameters=resolved_params
                )
                
                if not success:
                    raise Exception(f"Failed to submit job for step {i}: {message}")
                
                job_hash = job_data["job_hash"]
                step["job_hash"] = job_hash
                step["status"] = "running"
                
                # Update chain in database
                await meta_jobs_db.meta_jobs.update_one(
                    {"meta_job_hash": meta_job_hash},
                    {"$set": {
                        f"chain.{i}": step,
                        "updated_at": datetime.utcnow()
                    }}
                )
                
                # Wait for job to complete with automatic retry on retriable failures
                job_result = await wait_for_job_completion_with_retry(job_hash, meta_job_hash, i)
                
                if job_result["status"] not in COMPLETED_STATUSES:
                    # Should not happen as wait_for_job_completion_with_retry raises exception for failures
                    raise Exception(f"Job {job_hash} failed with status {job_result['status']}: {job_result.get('error', 'Unknown error')}")
                
                # Get output_file_hash from job result
                # This is the hash of the file in the files collection
                output_hash = job_result.get("output_file_hash")
                
                if not output_hash:
                    # Fallback: calculate hash from output content
                    job_output = job_result.get("output")
                    output_gridfs_id = job_result.get("output_gridfs_id")
                    
                    if job_output:
                        # Small output stored inline
                        import hashlib
                        output_hash = hashlib.sha256(job_output.encode('utf-8')).hexdigest()
                    elif output_gridfs_id:
                        # Large output in GridFS
                        output_hash = str(output_gridfs_id)
                    else:
                        raise Exception(f"Job {job_hash} has no output")
                
                logger.info(f"  Job completed, output_file_hash for next step: {output_hash[:16] if output_hash else 'None'}...")
                
                step["output_hash"] = output_hash
                step["status"] = "completed"
                
                # Update chain in database
                await meta_jobs_db.meta_jobs.update_one(
                    {"meta_job_hash": meta_job_hash},
                    {"$set": {
                        f"chain.{i}": step,
                        "updated_at": datetime.utcnow()
                    }}
                )
                
                # Use output hash as input for next step
                current_input_hash = output_hash
        
        # All steps completed successfully
        # Get final job hash from the last step
        final_job_hash = meta_job["chain"][-1]["job_hash"]
        
        # Get output_file_hash from the final job
        jobs_db = await get_jobs_db()
        final_job = await jobs_db.jobs.find_one({"job_hash": final_job_hash})
        final_output_hash = final_job.get("output_file_hash") if final_job else None
        
        logger.info(f"✅ META-JOB COMPLETED - {meta_job_hash}")
        logger.info(f"  Total steps: {len(meta_job['chain'])}")
        cached_count = sum(1 for s in meta_job['chain'] if s.get('cached', False))
        logger.info(f"  Cached steps: {cached_count}/{len(meta_job['chain'])}")
        logger.info(f"  Final job hash: {final_job_hash}")
        logger.info(f"  Final output hash: {final_output_hash[:16] if final_output_hash else 'None'}...")
        
        # Update meta-job as completed with reference to output hash
        await meta_jobs_db.meta_jobs.update_one(
            {"meta_job_hash": meta_job_hash},
            {"$set": {
                "status": MetaJobStatus.COMPLETED,
                "final_job_hash": final_job_hash,
                "final_output_hash": final_output_hash,  # Store reference to output
                "current_step": None,
                "current_step_info": None,  # Clear current step when completed
                "updated_at": datetime.utcnow(),
                "completed_at": datetime.utcnow()
            }}
        )
        
        logger.info(f"Meta-job {meta_job_hash} completed successfully")
        
    except Exception as e:
        logger.error(f"Error processing meta-job {meta_job_hash}: {e}")
        
        # Update meta-job as failed
        await meta_jobs_db.meta_jobs.update_one(
            {"meta_job_hash": meta_job_hash},
            {"$set": {
                "status": MetaJobStatus.FAILED,
                "error": str(e),
                "updated_at": datetime.utcnow()
            }}
        )


async def wait_for_job_completion(job_hash: str, timeout: int = None, interval: int = 5, 
                                  meta_job_hash: str = None, step_index: int = None) -> Dict[str, Any]:
    """
    Wait for a job to complete.
    
    Args:
        job_hash: Job hash to wait for
        timeout: Maximum time to wait in seconds (None for no timeout)
        interval: Polling interval in seconds (default: 5)
        meta_job_hash: Optional meta-job hash for tracking updates
        step_index: Optional step index for tracking updates
        
    Returns:
        Final job data
    """
    start_time = datetime.utcnow()
    
    while True:
        # Check timeout (only if specified)
        if timeout is not None:
            if (datetime.utcnow() - start_time).total_seconds() > timeout:
                raise TimeoutError(f"Job {job_hash} timed out after {timeout} seconds")
        
        # Get job status
        job_data = await get_job_status(job_hash)
        
        if not job_data:
            # Try getting from database directly
            job_data = await get_job_by_hash(job_hash)
        
        if job_data:
            status = job_data.get("status")
            
            # Update meta-job step info if tracking is enabled
            if meta_job_hash is not None and step_index is not None:
                await update_meta_job_current_step(meta_job_hash, step_index, job_hash, status, False)
            
            # Check if job is completed or failed
            if status in ["CD", "F", "CA", "TO", "NF", "OOM", "BF", "DL", "ER"]:
                return job_data
        
        # Wait before next check
        await asyncio.sleep(interval)


async def update_meta_job_current_step(meta_job_hash: str, step_index: int, job_hash: str, 
                                       job_status: str, is_resubmitted: bool):
    """
    Update meta-job with current step information for tracking.
    
    Args:
        meta_job_hash: Meta-job hash
        step_index: Index of the current step (0-based)
        job_hash: Current job hash
        job_status: Current job status
        is_resubmitted: Whether this job is a resubmission
    """
    meta_jobs_db = await get_meta_jobs_db()
    
    # Get step details
    meta_job = await meta_jobs_db.meta_jobs.find_one({"meta_job_hash": meta_job_hash})
    if not meta_job:
        logger.error(f"Meta-job {meta_job_hash} not found when updating current step")
        return
    
    step = meta_job["chain"][step_index]
    
    current_step_info = {
        "step_number": step_index + 1,  # 1-based for user display
        "method_name": step["method_name"],
        "parameters": step["parameters"],
        "job_hash": job_hash,
        "job_status": job_status,
        "status_description": STATUS_DESCRIPTIONS.get(job_status, "Unknown status"),
        "is_resubmitted": is_resubmitted
    }
    
    await meta_jobs_db.meta_jobs.update_one(
        {"meta_job_hash": meta_job_hash},
        {"$set": {
            "current_step": step_index,
            "current_step_info": current_step_info,
            "updated_at": datetime.utcnow()
        }}
    )


async def wait_for_job_completion_with_retry(job_hash: str, meta_job_hash: str, 
                                             step_index: int, max_retries: int = 3) -> Dict[str, Any]:
    """
    Wait for job completion with automatic retry on retriable failures.
    Updates meta-job current_step info during processing.
    
    Args:
        job_hash: Initial job hash to wait for
        meta_job_hash: Meta-job hash for tracking updates
        step_index: Index of this step in the chain (0-based)
        max_retries: Maximum number of retry attempts
        
    Returns:
        Final job data when completed successfully
        
    Raises:
        Exception: If job fails with non-retriable status or exceeds max retries
    """
    retry_count = 0
    current_job_hash = job_hash
    
    while retry_count <= max_retries:
        # Wait for current job to reach terminal state
        # Pass meta_job_hash and step_index so it updates current_step_info during execution
        job_result = await wait_for_job_completion(current_job_hash, 
                                                    meta_job_hash=meta_job_hash, 
                                                    step_index=step_index)
        job_status = job_result.get("status")
        
        # Final update with resubmission flag if applicable
        if retry_count > 0:
            await update_meta_job_current_step(meta_job_hash, step_index, current_job_hash, 
                                               job_status, True)
        
        if job_status in COMPLETED_STATUSES:
            logger.info(f"Meta-job {meta_job_hash} step {step_index}: Job {current_job_hash} completed successfully")
            return job_result
        
        elif job_status in RETRIABLE_FAILED_STATUSES:
            # Resubmit job automatically
            logger.warning(f"Meta-job {meta_job_hash} step {step_index}: Job {current_job_hash} failed with retriable status {job_status}")
            logger.info(f"  Status description: {STATUS_DESCRIPTIONS.get(job_status, 'Unknown')}")
            logger.info(f"  Attempting automatic resubmission (retry {retry_count + 1}/{max_retries})")
            
            # Get job details for resubmission
            old_job = await get_job_by_hash(current_job_hash)
            if not old_job:
                raise Exception(f"Cannot resubmit: Job {current_job_hash} not found in database")
            
            success, message, new_job_data = await submit_job(
                file_hash=old_job["file_hash"],
                function_hash=old_job["function_hash"],
                parameters=old_job["parameters"]
            )
            
            if not success:
                raise Exception(f"Failed to resubmit job after retriable failure: {message}")
            
            current_job_hash = new_job_data["job_hash"]
            retry_count += 1
            logger.info(f"  Successfully resubmitted as job {current_job_hash}")
            
        elif job_status in NON_RETRIABLE_FAILED_STATUSES:
            # Non-retriable failure - fail the meta-job
            error_msg = job_result.get("error", "No error details available")
            logger.error(f"Meta-job {meta_job_hash} step {step_index}: Job {current_job_hash} failed with non-retriable status {job_status}")
            logger.error(f"  Error: {error_msg}")
            raise Exception(f"Job failed with non-retriable status {job_status}: {error_msg}")
        
        else:
            # Unknown status - treat as non-retriable
            logger.error(f"Meta-job {meta_job_hash} step {step_index}: Job {current_job_hash} has unknown status {job_status}")
            raise Exception(f"Job has unknown status: {job_status}")
    
    # Exceeded max retries
    raise Exception(f"Job failed after {max_retries} retry attempts")


async def get_meta_job_info(meta_job_hash: str) -> Optional[MetaJobInfo]:
    """
    Get full information about a meta-job.
    
    When the meta-job is completed, includes the final output from the last job.
    This allows clients to get results without knowing internal file hashes.
    
    Args:
        meta_job_hash: Meta-job hash
        
    Returns:
        MetaJobInfo if found, None otherwise
    """
    meta_jobs_db = await get_meta_jobs_db()
    meta_job = await meta_jobs_db.meta_jobs.find_one({"meta_job_hash": meta_job_hash})
    
    if not meta_job:
        return None
    
    # Convert to MetaJobInfo model
    chain = [MetaJobStepInfo(**step) for step in meta_job["chain"]]
    
    # Get current step info if available
    current_step_info = None
    if meta_job.get("current_step_info"):
        try:
            current_step_info = CurrentStepInfo(**meta_job["current_step_info"])
        except Exception as e:
            logger.error(f"Error parsing current_step_info for meta-job {meta_job_hash}: {e}")
    
    # If meta-job is completed, get the output from files database using final_output_hash
    final_output = None
    if meta_job["status"] == MetaJobStatus.COMPLETED and meta_job.get("final_output_hash"):
        try:
            from dshpc_api.services.db_service import get_output_from_hash
            final_output = await get_output_from_hash(meta_job["final_output_hash"])
        except Exception as e:
            logger.error(f"Error retrieving final output for meta-job {meta_job_hash}: {e}")
            # Don't fail, just return without output
    
    return MetaJobInfo(
        meta_job_hash=meta_job["meta_job_hash"],
        initial_file_hash=meta_job["initial_file_hash"],
        chain=chain,
        status=meta_job["status"],
        current_step=meta_job.get("current_step"),
        current_step_info=current_step_info,  # Include current step tracking info
        final_job_hash=meta_job.get("final_job_hash"),
        final_output=final_output,  # Include the actual output when completed
        error=meta_job.get("error"),
        created_at=meta_job["created_at"],
        updated_at=meta_job.get("updated_at"),
        completed_at=meta_job.get("completed_at")
    )
