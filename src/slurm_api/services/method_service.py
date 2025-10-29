import os
import base64
import shutil
import hashlib
import tarfile
import tempfile
import json
import subprocess
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
        
        # Extract the bundle preserving directory structure
        with tarfile.open(temp_file_path, 'r:gz') as tar:
            # Extract everything with full path structure
            tar.extractall(path=target_directory)
        
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
        
        # Validate file(s) exist
        # For multi-file, validation already done in prepare_job_script
        # For single file, validate here
        if method_execution.file_hash:
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
        
        # Find the subdirectory created by extractall
        # Assumes the tarball contains a single top-level directory named after the method
        method_name = method_data.get('name', 'Unnamed Method').replace(' ', '_').lower()
        extracted_method_root = os.path.join(method_dir, method_name)
        # Fallback if the naming is different or extraction didn't create a folder
        if not os.path.isdir(extracted_method_root):
            # Check if there's exactly one directory inside method_dir
            subdirs = [d for d in os.listdir(method_dir) if os.path.isdir(os.path.join(method_dir, d))]
            if len(subdirs) == 1:
                extracted_method_root = os.path.join(method_dir, subdirs[0])
            else:
                 # If no subdirectory or multiple, assume files are in the root
                 extracted_method_root = method_dir

        # Build the full script path relative to the extracted method root
        script_path = os.path.join(extracted_method_root, method.script_path)
        
        execution_data = {
            "command": method.command,
            "script_path": script_path,
            "params_file": params_file_path,
            "method_dir": extracted_method_root, # Use the actual root where the script lives
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
        
        # Create a tar.gz archive including the full directory structure
        with tarfile.open(temp_file_path, 'w:gz') as tar:
            # Add the entire directory with its structure
            tar.add(method_dir, arcname=os.path.basename(method_dir))
        
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

def get_system_runtime_info() -> Dict[str, Any]:
    """
    Get information about the current runtime environment.
    
    Returns:
        A dictionary containing runtime information
    """
    runtime_info = {}
    
    try:
        # Get Python version
        try:
            python_version_full = subprocess.check_output(
                ["/opt/venvs/system_python/bin/python", "-c", "import sys; print(sys.version)"],
                universal_newlines=True
            ).strip()
            # Extract only numeric version (e.g., "3.10.12") without build timestamp
            python_version = python_version_full.split()[0]
            runtime_info["python_version"] = python_version
            runtime_info["python_version_full"] = python_version_full  # Keep full version for audit
        except Exception as e:
            logger.warning(f"Error getting Python version: {e}")
            runtime_info["python_version"] = "unknown"
        
        # Get Python packages
        try:
            pip_list = subprocess.check_output(
                ["/opt/venvs/system_python/bin/pip", "list", "--format=json"],
                universal_newlines=True
            )
            python_packages = json.loads(pip_list)
            runtime_info["python_packages"] = {pkg["name"]: pkg["version"] for pkg in python_packages}
        except Exception as e:
            logger.warning(f"Error getting Python packages: {e}")
            runtime_info["python_packages"] = {}
        
        # Get R version
        try:
            r_version_full = subprocess.check_output(
                ["R", "--version"],
                universal_newlines=True
            ).strip().split("\n")[0]
            # Extract only numeric version (e.g., "4.5.1") without release date
            r_version_parts = r_version_full.split()
            if len(r_version_parts) >= 3:
                r_version = r_version_parts[2]  # "R version 4.5.1 ..." -> "4.5.1"
            else:
                r_version = r_version_full
            runtime_info["r_version"] = r_version
            runtime_info["r_version_full"] = r_version_full  # Keep full version for audit
        except Exception as e:
            logger.warning(f"Error getting R version: {e}")
            runtime_info["r_version"] = "unknown"
        
        # Get R packages
        try:
            r_packages_output = subprocess.check_output(
                ["Rscript", "-e", "cat(paste(rownames(installed.packages()), installed.packages()[, 'Version'], sep='=', collapse=';'))"],
                universal_newlines=True
            ).strip()
            if r_packages_output:
                r_packages = {}
                for pkg in r_packages_output.split(';'):
                    if '=' in pkg:
                        name, version = pkg.split('=', 1)
                        r_packages[name] = version
                runtime_info["r_packages"] = r_packages
            else:
                runtime_info["r_packages"] = {}
        except Exception as e:
            logger.warning(f"Error getting R packages: {e}")
            runtime_info["r_packages"] = {}
        
        # Get installed apt packages
        try:
            apt_list = subprocess.check_output(
                ["dpkg-query", "-W", "-f=${Package}=${Version}\n"],
                universal_newlines=True
            ).strip()
            apt_packages = {}
            for line in apt_list.split("\n"):
                if "=" in line:
                    name, version = line.split("=", 1)
                    apt_packages[name] = version
            runtime_info["apt_packages"] = apt_packages
        except Exception as e:
            logger.warning(f"Error getting apt packages: {e}")
            runtime_info["apt_packages"] = {}
            
        # Read from config files as a backup if they exist
        config_files = {
            "python_config": "/tmp/python.json",
            "r_config": "/tmp/r.json",
            "system_deps": "/tmp/system_deps.json"
        }
        
        for key, path in config_files.items():
            if os.path.exists(path):
                try:
                    with open(path, 'r') as f:
                        runtime_info[key] = json.load(f)
                except Exception as e:
                    logger.warning(f"Error reading {key} from {path}: {e}")
                    runtime_info[key] = {}
        
    except Exception as e:
        logger.error(f"Error getting system runtime info: {e}")
    
    return runtime_info

def find_method_by_name(method_name: str, latest: bool = True) -> Optional[Dict[str, Any]]:
    """
    Find a method in the database by its name.
    
    Args:
        method_name: The name of the method to find
        latest: If True, returns the most recent version based on created_at timestamp
        
    Returns:
        The method document if found, None otherwise
    """
    try:
        query = {"name": method_name}
        
        if latest:
            # Find the most recent version of the method
            method = methods_collection.find_one(
                query,
                sort=[("created_at", -1)]  # Sort by created_at in descending order
            )
        else:
            # Find any version of the method
            method = methods_collection.find_one(query)
        
        return method
    except Exception as e:
        logger.error(f"Error finding method by name: {e}")
        return None

def list_method_versions(method_name: str) -> List[Dict[str, Any]]:
    """
    List all versions of a method sorted by creation time (newest first).
    
    Args:
        method_name: The name of the method to list versions for
        
    Returns:
        List of method documents (without the bundle field)
    """
    try:
        methods = list(methods_collection.find(
            {"name": method_name},
            {"bundle": 0},
            sort=[("created_at", -1)]
        ))
        
        for method in methods:
            method["_id"] = str(method["_id"])
            
        return methods
    except Exception as e:
        logger.error(f"Error listing method versions: {e}")
        return []

def mark_all_methods_inactive() -> Tuple[bool, str]:
    """
    Mark all methods in the database as inactive.
    
    Returns:
        Tuple containing:
        - success (bool): Whether the operation was successful
        - message (str): Success or error message
    """
    try:
        result = methods_collection.update_many(
            {},  # Match all documents
            {"$set": {"active": False}}
        )
        
        logger.info(f"Marked {result.modified_count} methods as inactive")
        return True, f"    > Marked {result.modified_count} methods as inactive!"
    except Exception as e:
        logger.error(f"Error marking methods as inactive: {e}")
        return False, f"    > Error marking methods as inactive: {str(e)}!"

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
        
        # Calculate a stable hash based on method content
        hasher = hashlib.sha256()
        
        # Add key metadata to hash - this ensures the hash changes if critical metadata changes
        for key in ["name", "command", "script_path", "version"]:
            if key in method_data:
                hasher.update(str(method_data[key]).encode())
        
        # Hash all script files in the directory recursively
        for root, dirs, files in os.walk(method_dir):
            # Sort directories and files for consistent results
            dirs.sort()
            files.sort()
            
            for filename in files:
                filepath = os.path.join(root, filename)
                rel_path = os.path.relpath(filepath, method_dir)
                
                # Add the relative path to ensure files with same content but different paths hash differently
                hasher.update(rel_path.encode())
                with open(filepath, 'rb') as f:
                    # Read and hash the file content
                    hasher.update(f.read())
        
        # Get and add system runtime information to the hash
        runtime_info = get_system_runtime_info()
        
        # Create a copy of runtime_info for hashing, excluding *_full fields
        # (which contain compilation timestamps that change with each build)
        runtime_info_for_hash = {
            k: v for k, v in runtime_info.items()
            if not k.endswith('_full')
        }
        
        # Add runtime information to hash (without *_full fields)
        runtime_json = json.dumps(runtime_info_for_hash, sort_keys=True)
        hasher.update(runtime_json.encode())
        
        # Log only summary of runtime information (not the full JSON)
        python_version = runtime_info.get('python_version', 'unknown')
        r_version = runtime_info.get('r_version', 'unknown')
        packages_count = {
            'python': len(runtime_info.get('python_packages', {})),
            'r': len(runtime_info.get('r_packages', {})),
            'apt': len(runtime_info.get('apt_packages', {}))
        }
        logger.info(f"\033[0;35mAdding runtime info to method hash\033[0m (Python {python_version}, R {r_version}, {packages_count['python']} Python packages, {packages_count['r']} R packages)")
        
        function_hash = hasher.hexdigest()
        
        # Check if method with this hash already exists
        existing_method = methods_collection.find_one({"function_hash": function_hash})
        
        # Compress the method directory
        success, message, bundle = compress_method_directory(method_dir)
        if not success:
            return False, message, None
        
        # Ensure we have a valid ISO format timestamp
        now = None
        if "created_at" in method_data and method_data["created_at"]:
            try:
                # If provided timestamp is a string, validate it
                if isinstance(method_data["created_at"], str):
                    # Parse and reformat to ensure ISO format
                    now = datetime.fromisoformat(method_data["created_at"]).isoformat()
                elif isinstance(method_data["created_at"], datetime):
                    # Convert datetime object to ISO string
                    now = method_data["created_at"].isoformat()
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid created_at timestamp provided: {e}. Using current time instead.")
                now = None
        
        # If no valid timestamp was provided, use current time
        if not now:
            now = datetime.utcnow().isoformat()
        
        # Prepare method document
        method_doc = {
            "name": method_data.get("name", "Unnamed Method"),
            "description": method_data.get("description", ""),
            "command": method_data.get("command", "python"),
            "script_path": method_data.get("script_path", ""),
            "parameters": method_data.get("parameters", []),
            "function_hash": function_hash,
            "version": method_data.get("version", "1.0.0"),
            "created_at": now,
            "updated_at": datetime.utcnow().isoformat(),  # Always use current time for updated_at
            "bundle": base64.b64encode(bundle).decode('utf-8'),
            "runtime_info": runtime_info,  # Store the runtime info in the method document
            "active": True  # Mark this method as active
        }
        
        # Insert or replace method document
        if existing_method:
            logger.info(f"\033[0;33mMethod with hash {function_hash} already exists\033[0m")
            methods_collection.replace_one({"function_hash": function_hash}, method_doc)
            return True, f"Method with hash {function_hash} updated", function_hash
        else:
            methods_collection.insert_one(method_doc)
            return True, "Method registered successfully", function_hash
        
    except Exception as e:
        logger.error(f"Error registering method: {e}")
        return False, f"Error registering method: {str(e)}", None

def list_available_methods(active_only: bool = False) -> List[Dict[str, Any]]:
    """
    List all available methods in the database.
    
    Args:
        active_only: If True, only return methods marked as active
        
    Returns:
        List of method documents (without the bundle field)
    """
    try:
        query = {"active": True} if active_only else {}
        methods = list(methods_collection.find(query, {"bundle": 0}))
        for method in methods:
            method["_id"] = str(method["_id"])
        return methods
    except Exception as e:
        logger.error(f"Error listing methods: {e}")
        return [] 