import os
import base64
import shutil
import hashlib
import tarfile
import tempfile
import json
from typing import Dict, Any, Tuple, Optional, List
from datetime import datetime

from slurm_api.config.db_config import methods_collection
from slurm_api.config.logging_config import logger
from slurm_api.models.method import Method, MethodExecution
from slurm_api.services.file_service import find_file_by_hash

def find_method_by_hash(function_hash: str) -> Optional[Dict[str, Any]]:
    """
    Find a method in the database by its hash.
    
    Args:
        function_hash: The hash of the method to find
        
    Returns:
        The method document if found, None otherwise
    """
    return methods_collection.find_one({"function_hash": function_hash})

def extract_method_bundle(method_bundle: bytes, target_directory: str) -> bool:
    """
    Extract a compressed method bundle to the target directory.
    
    Args:
        method_bundle: The compressed method bundle as bytes
        target_directory: The directory to extract to
        
    Returns:
        True if extraction was successful, False otherwise
    """
    try:
        # Create a temporary file to store the bundle
        with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as temp_file:
            temp_file_path = temp_file.name
            temp_file.write(method_bundle)
        
        # Extract the bundle directly to the target directory
        with tarfile.open(temp_file_path, 'r:gz') as tar:
            # Extract each file to the root of the target directory
            for member in tar.getmembers():
                # Get just the filename, not the full path
                if '/' in member.name:
                    # Get the last part of the path (filename)
                    member.name = member.name.split('/')[-1]
                
                # Extract only if it's a file (not a directory)
                if member.isfile():
                    tar.extract(member, path=target_directory)
        
        # Clean up the temporary file
        os.unlink(temp_file_path)
        
        return True
    except Exception as e:
        logger.error(f"Error extracting method bundle: {e}")
        return False

def download_and_extract_method(function_hash: str, target_directory: str) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """
    Download a method from the database and extract it to the target directory.
    
    Args:
        function_hash: The hash of the method to download
        target_directory: The directory to extract to
        
    Returns:
        Tuple containing:
        - success (bool): Whether the download was successful
        - message (str): Success or error message
        - method_data (Dict): Method data if successful, None otherwise
    """
    try:
        # Find the method in the database
        method_doc = find_method_by_hash(function_hash)
        
        if not method_doc:
            return False, f"Method with hash {function_hash} not found", None
            
        # Create the target directory if it doesn't exist
        os.makedirs(target_directory, exist_ok=True)
        
        # Get method bundle and extract it
        bundle_content = base64.b64decode(method_doc.get("bundle", ""))
        
        if not extract_method_bundle(bundle_content, target_directory):
            return False, "Failed to extract method bundle", None
        
        # Return the method data
        method_data = {k: v for k, v in method_doc.items() if k != "bundle"}
        return True, f"Method downloaded and extracted to {target_directory}", method_data
        
    except Exception as e:
        logger.error(f"Error downloading method {function_hash}: {e}")
        return False, f"Error downloading method: {str(e)}", None

def prepare_method_execution(workspace_dir: str, method_execution: MethodExecution) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """
    Prepare a method for execution in the workspace.
    
    Args:
        workspace_dir: The workspace directory
        method_execution: The method execution parameters
        
    Returns:
        Tuple containing:
        - success (bool): Whether preparation was successful
        - message (str): Success or error message
        - execution_data (Dict): Data needed for execution if successful, None otherwise
    """
    try:
        # Validate method exists
        method_doc = find_method_by_hash(method_execution.function_hash)
        if not method_doc:
            return False, f"Method with hash {method_execution.function_hash} not found", None
        
        # Validate file exists
        file_doc = find_file_by_hash(method_execution.file_hash)
        if not file_doc:
            return False, f"File with hash {method_execution.file_hash} not found", None
        
        # Create method directory in workspace
        method_dir = os.path.join(workspace_dir, "method")
        os.makedirs(method_dir, exist_ok=True)
        
        # Download and extract method
        success, message, method_data = download_and_extract_method(
            method_execution.function_hash, method_dir
        )
        
        if not success:
            return False, message, None
        
        # Create parameter file (for the script to access)
        params_file_path = os.path.join(workspace_dir, "params.json")
        with open(params_file_path, "w") as f:
            json.dump(method_execution.parameters, f)
        
        # Create execution data
        method = Method(**method_data)
        execution_data = {
            "command": method.command,
            "script_path": os.path.join(method_dir, method.script_path),
            "params_file": params_file_path,
            "method_dir": method_dir,
            "method_name": method.name
        }
        
        return True, "Method prepared for execution", execution_data
        
    except Exception as e:
        logger.error(f"Error preparing method execution: {e}")
        return False, f"Error preparing method execution: {str(e)}", None

def compress_method_directory(method_dir: str) -> Tuple[bool, str, Optional[bytes]]:
    """
    Compress a method directory into a tar.gz bundle.
    
    Args:
        method_dir: Path to the method directory
        
    Returns:
        Tuple containing:
        - success (bool): Whether compression was successful
        - message (str): Success or error message
        - bundle (bytes): Compressed bundle if successful, None otherwise
    """
    try:
        # Create a temporary file for the bundle
        with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as temp_file:
            temp_file_path = temp_file.name
        
        # Create a tar.gz archive of just the files in the directory
        with tarfile.open(temp_file_path, 'w:gz') as tar:
            # Add each file in the directory directly (not preserving directory structure)
            for item in os.listdir(method_dir):
                item_path = os.path.join(method_dir, item)
                # Only include files, not directories
                if os.path.isfile(item_path):
                    # Add the file with just its name, not full path
                    tar.add(item_path, arcname=os.path.basename(item_path))
        
        # Read the compressed file
        with open(temp_file_path, 'rb') as f:
            bundle = f.read()
        
        # Clean up the temporary file
        os.unlink(temp_file_path)
        
        return True, "Method directory compressed successfully", bundle
    
    except Exception as e:
        logger.error(f"Error compressing method directory: {e}")
        return False, f"Error compressing method directory: {str(e)}", None

def calculate_directory_hash(directory_path: str) -> str:
    """
    Calculate a hash for a directory by hashing all files recursively.
    
    Args:
        directory_path: Path to the directory
        
    Returns:
        A SHA-256 hash of the directory contents
    """
    if not os.path.isdir(directory_path):
        raise ValueError(f"Path {directory_path} is not a directory")
    
    hasher = hashlib.sha256()
    
    for root, dirs, files in os.walk(directory_path, topdown=True):
        # Sort directories and files for consistent results
        dirs.sort()
        files.sort()
        
        for filename in files:
            filepath = os.path.join(root, filename)
            rel_path = os.path.relpath(filepath, directory_path)
            
            # Hash the relative path
            hasher.update(rel_path.encode())
            
            # Hash the file contents
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    hasher.update(chunk)
    
    return hasher.hexdigest()

def register_method(method_data: Dict[str, Any], method_dir: str) -> Tuple[bool, str, Optional[str]]:
    """
    Register a method in the database.
    
    Args:
        method_data: Method metadata
        method_dir: Path to the method directory
        
    Returns:
        Tuple containing:
        - success (bool): Whether registration was successful
        - message (str): Success or error message
        - function_hash (str): Hash of the method if successful, None otherwise
    """
    try:
        # Check if method directory exists
        if not os.path.isdir(method_dir):
            return False, f"Method directory {method_dir} does not exist", None
        
        # Compress the method directory
        success, message, bundle = compress_method_directory(method_dir)
        if not success:
            return False, message, None
        
        # Calculate hash for the bundle
        bundle_hash = hashlib.sha256(bundle).hexdigest()
        
        # Check if method with this hash already exists
        existing_method = methods_collection.find_one({"function_hash": bundle_hash})
        if existing_method:
            return False, f"Method with hash {bundle_hash} already exists", bundle_hash
        
        # Prepare method document
        now = datetime.utcnow().isoformat()
        method_doc = {
            "name": method_data.get("name", "Unnamed Method"),
            "description": method_data.get("description", ""),
            "command": method_data.get("command", "python"),
            "script_path": method_data.get("script_path", ""),
            "parameters": method_data.get("parameters", []),
            "function_hash": bundle_hash,
            "version": method_data.get("version", "1.0.0"),
            "created_at": now,
            "updated_at": now,
            "bundle": base64.b64encode(bundle).decode('utf-8')
        }
        
        # Insert method document
        methods_collection.insert_one(method_doc)
        
        return True, "Method registered successfully", bundle_hash
        
    except Exception as e:
        logger.error(f"Error registering method: {e}")
        return False, f"Error registering method: {str(e)}", None

def list_available_methods() -> List[Dict[str, Any]]:
    """
    List all available methods in the database.
    
    Returns:
        List of method documents (without the bundle field)
    """
    try:
        methods = list(methods_collection.find({}, {"bundle": 0}))
        for method in methods:
            method["_id"] = str(method["_id"])
        return methods
    except Exception as e:
        logger.error(f"Error listing methods: {e}")
        return [] 