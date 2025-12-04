"""
Utility for resolving $ref references in file_inputs and injecting content into parameters.
"""
import json
import base64
from typing import Dict, Any, Optional, Callable
from dshpc_api.config.logging_config import logger


async def resolve_and_inject_file_inputs(
    file_inputs: Optional[Dict[str, str]],
    parameters: Dict[str, Any],
    get_source_hash: Callable[[str], str],
    files_db,
    context: str = "unknown"
) -> Dict[str, Any]:
    """
    Resolve $ref references in file_inputs, read content, and inject into parameters.
    
    Args:
        file_inputs: Dict of input_name -> reference or hash
        parameters: Current parameters dict
        get_source_hash: Function to resolve source ID to output hash (e.g., "prev" -> hash, "node1" -> hash)
        files_db: Files database connection
        context: Context string for logging (e.g., "meta_job:step2", "pipeline:node3")
        
    Returns:
        Enriched parameters dict with file contents injected
    """
    if not file_inputs:
        return parameters
    
    enriched_params = parameters.copy()
    
    for input_name, ref_value in file_inputs.items():
        try:
            # Resolve reference to hash
            if isinstance(ref_value, str) and ref_value.startswith("$ref:"):
                resolved_hash = await resolve_reference(ref_value, get_source_hash, files_db, context)
            else:
                # Direct hash
                resolved_hash = ref_value
            
            # Read and inject content
            content = await read_and_prepare_content(resolved_hash, input_name, files_db, context)
            enriched_params[input_name] = content
            
            logger.debug(f"[{context}] Injected file_input '{input_name}' from {resolved_hash[:12]}...")
            
        except Exception as e:
            logger.error(f"[{context}] Failed to resolve file_input '{input_name}': {e}")
            raise ValueError(f"Failed to resolve file_input '{input_name}': {e}")
    
    return enriched_params


async def resolve_reference(
    ref_value: str,
    get_source_hash: Callable[[str], str],
    files_db,
    context: str
) -> str:
    """
    Resolve a $ref reference to a file hash.
    
    Args:
        ref_value: Reference string like "$ref:prev/data/text" or "$ref:node1"
        get_source_hash: Function to get source output hash
        files_db: Files database connection
        context: Context for logging
        
    Returns:
        Hash of the resolved file
    """
    ref_full = ref_value[5:]  # Remove "$ref:" prefix
    
    if "/" in ref_full:
        # Path-based reference: prev/data/text or node1/results/value
        source_id = ref_full.split("/")[0]
        path = ref_full[len(source_id)+1:]  # Everything after source_id/
        
        # Get source output hash
        source_hash = get_source_hash(source_id)
        if not source_hash:
            raise ValueError(f"Source '{source_id}' not found or has no output")
        
        # Extract path and create new file
        extracted_hash = await extract_and_store_path(source_hash, path, files_db, context, source_id)
        return extracted_hash
    else:
        # Simple reference: prev or node1 (no path)
        source_hash = get_source_hash(ref_full)
        if not source_hash:
            raise ValueError(f"Source '{ref_full}' not found or has no output")
        return source_hash


async def extract_and_store_path(
    source_hash: str,
    path: str,
    files_db,
    context: str,
    source_id: str
) -> str:
    """
    Extract a value from a JSON file using a path and store it as a new file.
    
    Args:
        source_hash: Hash of the source file containing JSON
        path: Slash-separated path to extract (e.g., "data/text")
        files_db: Files database connection
        context: Context for logging
        source_id: Source identifier for metadata
        
    Returns:
        Hash of the newly created file with extracted value
    """
    # Get source file
    source_doc = await files_db.files.find_one({"file_hash": source_hash})
    if not source_doc:
        raise ValueError(f"Source file {source_hash} not found for path extraction")
    
    # Decode content
    content_bytes = base64.b64decode(source_doc["content"])
    content_str = content_bytes.decode('utf-8')
    
    # Parse JSON
    try:
        data = json.loads(content_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON from {source_hash}: {e}")
    
    # Navigate path (slash-separated)
    path_parts = path.split("/")
    result = data
    for part in path_parts:
        if isinstance(result, dict) and part in result:
            result = result[part]
        else:
            raise ValueError(f"Path '{path}' not found in source file {source_hash}")
    
    # Create new JSON with extracted value
    extracted_content = json.dumps(result)
    extracted_bytes = extracted_content.encode('utf-8')
    extracted_base64 = base64.b64encode(extracted_bytes).decode('utf-8')

    # Compute hash for deduplication
    import hashlib
    extracted_hash = hashlib.sha256(extracted_bytes).hexdigest()

    # Check if already exists
    existing = await files_db.files.find_one({"file_hash": extracted_hash})
    if existing:
        logger.debug(f"[{context}] Path extraction '{path}' already exists: {extracted_hash[:12]}...")
        return extracted_hash

    # Store as new file
    file_doc = {
        "file_hash": extracted_hash,
        "content": extracted_base64,
        "status": "completed",
        "metadata": {
            "source": "path_extraction",
            "source_file_hash": source_hash,
            "source_id": source_id,
            "extracted_path": path,
            "context": context,
            "content_type": "application/json",
            "size_bytes": len(extracted_bytes)
        }
    }
    
    await files_db.files.insert_one(file_doc)
    logger.info(f"[{context}] Extracted path '{path}' from {source_hash[:12]}... → {extracted_hash[:12]}...")
    
    return extracted_hash


async def read_and_prepare_content(
    file_hash: str,
    input_name: str,
    files_db,
    context: str,
    size_threshold: int = 1_000_000  # 1MB
) -> Any:
    """
    Read file content and prepare it for injection into parameters.
    
    For small files (< threshold): Return parsed content directly
    For large files (>= threshold): Save to workspace and return path
    
    Args:
        file_hash: Hash of the file to read
        input_name: Name of the input (for workspace file naming)
        files_db: Files database connection
        context: Context for logging
        size_threshold: Size threshold in bytes (default 1MB)
        
    Returns:
        Parsed content (dict/list/str) for small files, or file path for large files
    """
    # Get file
    file_doc = await files_db.files.find_one({"file_hash": file_hash})
    if not file_doc:
        raise ValueError(f"File {file_hash} not found")
    
    # Decode content
    content_bytes = base64.b64decode(file_doc["content"])
    size_bytes = len(content_bytes)
    
    # Check size
    if size_bytes >= size_threshold:
        # Large file: save to workspace and return path
        # TODO: Implement workspace file saving when needed
        logger.warning(f"[{context}] Large file ({size_bytes} bytes) for '{input_name}' - not yet implemented, returning as string")
        # For now, fall through to content parsing
    
    # Small file or fallback: parse and return content
    content_str = content_bytes.decode('utf-8')
    
    # Try to parse as JSON
    try:
        parsed = json.loads(content_str)
        logger.debug(f"[{context}] Parsed '{input_name}' as JSON ({size_bytes} bytes)")
        return parsed
    except json.JSONDecodeError:
        # Not JSON, return as string
        logger.debug(f"[{context}] Returning '{input_name}' as string ({size_bytes} bytes)")
        return content_str

