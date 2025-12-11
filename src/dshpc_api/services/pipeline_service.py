"""
Pipeline service for DAG orchestration of meta-jobs
"""
import uuid
import asyncio
import hashlib
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Set
from pymongo.database import Database

from dshpc_api.config.logging_config import logger
from dshpc_api.services.db_service import get_jobs_db, get_files_db
from dshpc_api.models.pipeline import (
    PipelineStatus, PipelineNodeStatus, PipelineNodeInfo, PipelineInfo
)
from dshpc_api.utils.parameter_utils import sort_parameters
from dshpc_api.utils.sorting_utils import sort_nodes, sort_dependencies, sort_chain, sort_file_inputs
from dshpc_api.services.job_service import is_method_unavailable_error, is_file_not_found_error


def compute_pipeline_hash(nodes: Dict[str, Any], function_hashes: Dict[str, List[str]]) -> str:
    """
    Compute a deterministic hash for a pipeline based on its complete DAG structure.
    
    Args:
        nodes: Dictionary of node_id -> node definition
        function_hashes: Dictionary of node_id -> list of function hashes for that node's chain
        
    Returns:
        SHA256 hash as hex string
    """
    hash_components = []
    
    # Sort nodes by node_id for determinism
    sorted_node_ids = sorted(nodes.keys())
    
    for node_id in sorted_node_ids:
        node = nodes[node_id]
        hash_components.append(f"node:{node_id}")
        
        # Add input file hash if present
        if node.get("input_file_hash"):
            hash_components.append(f"input_file:{node['input_file_hash']}")
        
        # Add dependencies (sorted)
        deps = sort_dependencies(node.get("dependencies", []))
        if deps:
            hash_components.append(f"deps:{','.join(deps)}")
        
        # Add method chain with function hashes
        node_function_hashes = function_hashes.get(node_id, [])
        for i, (step, func_hash) in enumerate(zip(node["chain"], node_function_hashes)):
            hash_components.append(f"step:{i}")
            hash_components.append(f"method:{step['method_name']}")
            hash_components.append(f"function:{func_hash}")
            
            # Sort parameters for determinism
            sorted_params = sort_parameters(step.get("parameters", {}))
            params_json = json.dumps(sorted_params, sort_keys=True)
            hash_components.append(f"params:{params_json}")
            
            # Add file_inputs if present (sorted for determinism)
            step_file_inputs = step.get("file_inputs")
            if step_file_inputs:
                sorted_file_inputs = sort_file_inputs(step_file_inputs)
                file_inputs_json = json.dumps(sorted_file_inputs, sort_keys=True)
                hash_components.append(f"file_inputs:{file_inputs_json}")
    
    # Combine all components
    hash_input = "|".join(hash_components)
    hash_bytes = hash_input.encode('utf-8')
    
    # Compute SHA256
    pipeline_hash = hashlib.sha256(hash_bytes).hexdigest()
    
    logger.debug(f"Computed pipeline hash: {pipeline_hash[:12]}... from {len(sorted_node_ids)} nodes")
    return pipeline_hash


# Detect circular dependencies in DAG
def detect_cycles(nodes: Dict[str, Any]) -> Optional[List[str]]:
    """
    Detect circular dependencies in the DAG.
    
    Args:
        nodes: Dictionary of node_id -> {dependencies: [...]}
        
    Returns:
        None if no cycles, otherwise list of nodes in cycle
    """
    # Build adjacency list
    graph = {node_id: node.get("dependencies", []) for node_id, node in nodes.items()}
    
    # Track visited nodes and recursion stack
    visited = set()
    rec_stack = set()
    
    def dfs(node: str, path: List[str]) -> Optional[List[str]]:
        visited.add(node)
        rec_stack.add(node)
        path.append(node)
        
        # Check all dependencies
        for dep in graph.get(node, []):
            if dep not in visited:
                cycle = dfs(dep, path[:])
                if cycle:
                    return cycle
            elif dep in rec_stack:
                # Found cycle
                cycle_start = path.index(dep)
                return path[cycle_start:] + [dep]
        
        rec_stack.remove(node)
        return None
    
    # Check each node
    for node_id in graph:
        if node_id not in visited:
            cycle = dfs(node_id, [])
            if cycle:
                return cycle
    
    return None


def validate_single_terminal_node(nodes: Dict[str, Any]) -> None:
    """
    Validate that pipeline has exactly one terminal node (final output node).
    
    A terminal node is a node that no other node depends on.
    
    Args:
        nodes: Dictionary of node_id -> node definition
        
    Raises:
        ValueError: If pipeline doesn't have exactly one terminal node
    """
    # Find all nodes that are dependencies
    all_dependencies = set()
    for node in nodes.values():
        deps = node.get("dependencies", [])
        all_dependencies.update(deps)
    
    # Terminal nodes = nodes NOT in any dependency list
    terminal_nodes = [nid for nid in nodes.keys() if nid not in all_dependencies]
    
    if len(terminal_nodes) == 0:
        raise ValueError(
            "Pipeline validation failed: No terminal node found. "
            "All nodes are dependencies of other nodes (possible circular dependency)."
        )
    
    if len(terminal_nodes) > 1:
        raise ValueError(
            f"Pipeline validation failed: Multiple terminal nodes detected: {terminal_nodes}. "
            f"Pipelines must converge to exactly ONE final output node. "
            f"Add a merge node that depends on all terminal nodes: {terminal_nodes}"
        )
    
    # Exactly one terminal node - valid!
    logger.info(f"Pipeline validation passed: Single terminal node '{terminal_nodes[0]}'")


# Calculate depth levels for topological ordering
def calculate_depth_levels(nodes: Dict[str, Any]) -> Dict[str, int]:
    """
    Calculate depth level for each node (0 = no dependencies, 1 = depends on level 0, etc.)
    
    Args:
        nodes: Dictionary of node_id -> {dependencies: [...]}
        
    Returns:
        Dictionary mapping node_id -> depth_level
    """
    depths = {}
    
    def get_depth(node_id: str) -> int:
        if node_id in depths:
            return depths[node_id]
        
        node_deps = nodes[node_id].get("dependencies", [])
        if not node_deps:
            depths[node_id] = 0
            return 0
        
        # Depth is 1 + max depth of dependencies
        max_dep_depth = max(get_depth(dep) for dep in node_deps)
        depths[node_id] = max_dep_depth + 1
        return depths[node_id]
    
    for node_id in nodes:
        get_depth(node_id)
    
    return depths


# Resolve $ref references in parameters
def resolve_references(params: Dict[str, Any], completed_nodes: Dict[str, str]) -> Dict[str, Any]:
    """
    Resolve $ref:node_id references in parameters.
    
    Args:
        params: Parameters dict that may contain {"$ref": "node_1"} or {"$ref": "node_1/data/field"}
        completed_nodes: Mapping of node_id -> output_hash
        
    Returns:
        Resolved parameters with hashes instead of references
    """
    resolved = {}
    
    for key, value in params.items():
        if isinstance(value, str) and value.startswith("$ref:"):
            # Extract reference: "$ref:node_1" or "$ref:node_1/data/text" or "$ref:prev"
            ref = value[5:]  # Remove "$ref:" prefix
            
            # $ref:prev is an internal meta-job reference (previous step in chain)
            # Don't resolve it here - let the meta-job system handle it
            if ref == "prev" or ref.startswith("prev/"):
                resolved[key] = value  # Keep as-is
                continue
            
            if "/" in ref:
                # Path-based reference: node_1/data/text
                # For now, just use the node output (path extraction happens in orchestrator)
                node_id = ref.split("/")[0]
                if node_id in completed_nodes:
                    resolved[key] = completed_nodes[node_id]
                else:
                    raise ValueError(f"Reference to incomplete node: {node_id}")
            else:
                # Simple reference: node_1
                if ref in completed_nodes:
                    resolved[key] = completed_nodes[ref]
                else:
                    raise ValueError(f"Reference to incomplete node: {ref}")
        elif isinstance(value, dict):
            # Recursively resolve nested dicts
            resolved[key] = resolve_references(value, completed_nodes)
        elif isinstance(value, list):
            # Recursively resolve lists
            resolved[key] = [
                resolve_references(item, completed_nodes) if isinstance(item, dict)
                else (completed_nodes.get(item[5:], item) if isinstance(item, str) and item.startswith("$ref:") else item)
                for item in value
            ]
        else:
            resolved[key] = value
    
    return resolved


async def create_pipeline(pipeline_data: Dict[str, Any]) -> Tuple[str, str]:
    """
    Create a new pipeline in the database with hash-based deduplication.
    
    Args:
        pipeline_data: Pipeline definition with nodes and dependencies
        
    Returns:
        Tuple of (pipeline_hash, message)
    """
    # Validate nodes exist
    nodes = pipeline_data.get("nodes", {})
    if not nodes:
        raise ValueError("Pipeline must have at least one node")
    
    # Detect circular dependencies
    cycle = detect_cycles(nodes)
    if cycle:
        raise ValueError(f"Circular dependency detected: {' → '.join(cycle)}")
    
    # Validate single terminal node
    validate_single_terminal_node(nodes)
    
    # Calculate depth levels
    depth_levels = calculate_depth_levels(nodes)
    
    # Get function hashes for all nodes to compute pipeline hash
    from dshpc_api.services.job_service import get_latest_method_hash
    function_hashes = {}
    
    for node_id, node_def in nodes.items():
        node_function_hashes = []
        for step in node_def["chain"]:
            func_hash = await get_latest_method_hash(step["method_name"])
            if not func_hash:
                raise ValueError(f"Method '{step['method_name']}' not found or not active")
            node_function_hashes.append(func_hash)
        function_hashes[node_id] = node_function_hashes
    
    # Compute pipeline hash for deduplication
    pipeline_hash = compute_pipeline_hash(nodes, function_hashes)
    logger.info(f"Computed pipeline hash: {pipeline_hash[:16]}...")
    
    # Check if this exact pipeline already exists
    # Exclude cancelled pipelines - they should be treated as non-existent for re-submission
    db = await get_jobs_db()
    existing_pipeline = await db.pipelines.find_one({
        "pipeline_hash": pipeline_hash,
        "status": {"$nin": ["cancelled", "CANCELLED"]}
    })
    
    if existing_pipeline:
        existing_status = existing_pipeline.get("status")
        logger.info(f"♻️  Found existing pipeline with status: {existing_status}")
        
        if existing_status in [PipelineStatus.COMPLETED.value, PipelineStatus.RUNNING.value, PipelineStatus.PENDING.value]:
            # Return existing pipeline (transparent caching)
            logger.info(f"✓ Returning cached pipeline: {pipeline_hash[:16]}...")
            return pipeline_hash, f"Existing pipeline returned (status: {existing_status}, {len(nodes)} nodes)"
        elif existing_status == PipelineStatus.FAILED.value:
            # Check if methods have changed
            methods_changed = False
            for node_id, node_def in nodes.items():
                if node_id in existing_pipeline["nodes"]:
                    existing_chain = existing_pipeline["nodes"][node_id]["chain"]
                    if len(existing_chain) != len(node_def["chain"]):
                        methods_changed = True
                        break
                    # Compare function hashes would be ideal, but we don't store them in nodes
                    # For now, just allow retry on failed pipelines
                else:
                    methods_changed = True
                    break
            
            if not methods_changed:
                # Check if failure was due to method unavailability
                error_msg = existing_pipeline.get("error", "")
                
                # Check all nodes for method unavailability errors
                failed_method = is_method_unavailable_error(error_msg)
                
                # Also check individual node errors
                if not failed_method:
                    for node_id, node_data in existing_pipeline.get("nodes", {}).items():
                        node_error = node_data.get("error", "")
                        failed_method = is_method_unavailable_error(node_error)
                        if failed_method:
                            break
                
                if failed_method:
                    # Check if method is now available
                    from dshpc_api.services.job_service import get_latest_method_hash
                    method_hash = await get_latest_method_hash(failed_method)
                    if method_hash:
                        logger.info(f"  ♻️  Method '{failed_method}' now available - deleting failed pipeline for retry")
                        await db.pipelines.delete_one({"pipeline_hash": pipeline_hash})
                        # Continue to create new pipeline below
                    else:
                        logger.info(f"  ✗ Method '{failed_method}' still not available")
                        return pipeline_hash, f"Pipeline previously failed: {error_msg}"
                else:
                    # Check for file not found
                    missing_file_hash = is_file_not_found_error(error_msg)
                    
                    # Also check node errors
                    if not missing_file_hash:
                        for node_id, node_data in existing_pipeline.get("nodes", {}).items():
                            node_error = node_data.get("error", "")
                            missing_file_hash = is_file_not_found_error(node_error)
                            if missing_file_hash:
                                break
                    
                    if missing_file_hash:
                        # Check if file now exists
                        files_db = await get_files_db()
                        file_doc = await files_db.files.find_one({"file_hash": missing_file_hash})
                        
                        if file_doc and file_doc.get("status") == "completed":
                            logger.info(f"  ♻️  File {missing_file_hash[:12]}... now available - deleting failed pipeline for retry")
                            await db.pipelines.delete_one({"pipeline_hash": pipeline_hash})
                            # Continue to create new pipeline below
                        else:
                            logger.info(f"  ✗ File {missing_file_hash[:12]}... still not available")
                            return pipeline_hash, f"Pipeline previously failed: {error_msg}"
                    else:
                        # Not file or method related - check if it's a validation error that might be fixed
                        # If the error is "Unknown error" or a validation error, allow retry
                        error_msg = existing_pipeline.get("error", "Unknown error")
                        if "validation error" in error_msg.lower() or "Unknown error" in error_msg:
                            logger.info(f"  ♻️  Pipeline failed with validation/unknown error - deleting for retry")
                            await db.pipelines.delete_one({"pipeline_hash": pipeline_hash})
                            # Continue to create new pipeline below
                        else:
                            # Not file or method related - fail fast
                            logger.info(f"✗ Pipeline previously failed with same configuration (not file or method related)")
                            raise ValueError(f"Pipeline previously failed: {error_msg}")
            else:
                # Methods may have changed, allow recomputation
                logger.info(f"↻ Pipeline structure changed, will recompute")
                await db.pipelines.delete_one({"pipeline_hash": pipeline_hash})
    
    # Create new pipeline document with hash as primary ID
    logger.info(f"Creating new pipeline with hash: {pipeline_hash[:16]}...")
    
    pipeline_doc = {
        "pipeline_hash": pipeline_hash,
        "status": PipelineStatus.PENDING.value,
        "nodes": {},
        "created_at": datetime.utcnow(),
        "started_at": None,
        "completed_at": None
    }
    
    # Initialize nodes
    for node_id, node_def in nodes.items():
        pipeline_doc["nodes"][node_id] = {
            "node_id": node_id,
            "status": PipelineNodeStatus.WAITING.value,
            "meta_job_hash": None,
            "chain": node_def["chain"],
            "dependencies": node_def.get("dependencies", []),
            "depth_level": depth_levels[node_id],
            "input_file_hash": node_def.get("input_file_hash"),
            "submitted_at": None,
            "completed_at": None,
            "output_hash": None,
            "error": None
        }
    
    # Insert into database
    await db.pipelines.insert_one(pipeline_doc)
    
    logger.info(f"Created pipeline {pipeline_hash[:16]}... with {len(nodes)} nodes")
    
    return pipeline_hash, f"Pipeline created with {len(nodes)} nodes"


async def get_pipeline_status(pipeline_hash: str, auto_requeue: bool = True) -> Optional[Dict[str, Any]]:
    """
    Get pipeline status and information.

    IMPORTANT: If auto_requeue=True (default), non-completed pipelines will be
    automatically requeued for processing. This ensures that failed/cancelled/stalled
    pipelines get another chance to complete when queried.

    Args:
        pipeline_hash: Pipeline hash
        auto_requeue: If True, requeue non-completed pipelines for processing

    Returns:
        Pipeline information or None if not found
    """
    from datetime import timedelta

    db = await get_jobs_db()
    pipeline = await db.pipelines.find_one({"pipeline_hash": pipeline_hash})

    if not pipeline:
        return None

    current_status = pipeline["status"]

    # AUTO-REQUEUE: If pipeline is not completed, reactivate it for processing
    # This handles failed/cancelled/stalled pipelines by giving them another chance
    if auto_requeue and current_status != PipelineStatus.COMPLETED.value:
        should_requeue = False
        requeue_reason = None

        if current_status in [PipelineStatus.FAILED.value, PipelineStatus.CANCELLED.value, "failed", "cancelled"]:
            # Failed or cancelled - always requeue
            should_requeue = True
            requeue_reason = f"requeued from {current_status} on query"
        elif current_status in [PipelineStatus.RUNNING.value, PipelineStatus.PENDING.value, "running", "pending"]:
            # Check if stalled (no updates for 10+ minutes)
            updated_at = pipeline.get("updated_at") or pipeline.get("started_at") or pipeline.get("created_at")
            if updated_at:
                stall_threshold = datetime.utcnow() - timedelta(minutes=10)
                if updated_at < stall_threshold:
                    should_requeue = True
                    requeue_reason = f"requeued from stalled {current_status} (no updates for 10+ min)"

        if should_requeue:
            logger.info(f"🔄 Auto-requeuing pipeline {pipeline_hash}: {requeue_reason}")

            # Reset pipeline to pending status
            await db.pipelines.update_one(
                {"pipeline_hash": pipeline_hash},
                {"$set": {
                    "status": PipelineStatus.PENDING.value,
                    "error": None,
                    "updated_at": datetime.utcnow()
                }}
            )

            # Reset failed/cancelled nodes to waiting
            for node_id, node_data in pipeline["nodes"].items():
                node_status = node_data.get("status")
                if node_status in [PipelineNodeStatus.FAILED.value, "failed", "cancelled"]:
                    await db.pipelines.update_one(
                        {"pipeline_hash": pipeline_hash},
                        {"$set": {
                            f"nodes.{node_id}.status": PipelineNodeStatus.WAITING.value,
                            f"nodes.{node_id}.error": None
                        }}
                    )

            # Trigger orchestrator to process this pipeline
            from dshpc_api.background.pipeline_orchestrator import orchestrate_pipelines
            asyncio.create_task(orchestrate_pipelines())

            # Update local copy for response
            pipeline["status"] = PipelineStatus.PENDING.value
            pipeline["error"] = None
            current_status = PipelineStatus.PENDING.value

    # Calculate stats
    nodes = pipeline["nodes"]
    total = len(nodes)
    completed = sum(1 for n in nodes.values() if n["status"] == PipelineNodeStatus.COMPLETED.value)
    failed = sum(1 for n in nodes.values() if n["status"] == PipelineNodeStatus.FAILED.value)
    running = sum(1 for n in nodes.values() if n["status"] == PipelineNodeStatus.RUNNING.value)
    waiting = sum(1 for n in nodes.values() if n["status"] == PipelineNodeStatus.WAITING.value)
    
    # Find final node (highest depth level with no dependent nodes)
    final_node_id = None
    final_output = None
    if pipeline["status"] == PipelineStatus.COMPLETED.value:
        # Find node(s) that no other node depends on
        all_dependencies = set()
        for node in nodes.values():
            all_dependencies.update(node["dependencies"])
        
        # Nodes not in any dependency list are terminal nodes
        terminal_nodes = [nid for nid in nodes.keys() if nid not in all_dependencies]
        
        if terminal_nodes:
            # Use the one with highest depth level
            final_node_id = max(terminal_nodes, key=lambda nid: nodes[nid]["depth_level"])
            
            # Retrieve output from files database using stored final_output_hash
            if pipeline.get("final_output_hash"):
                try:
                    from dshpc_api.services.db_service import get_output_from_hash
                    final_output = await get_output_from_hash(pipeline["final_output_hash"])
                except Exception as e:
                    logger.error(f"Error retrieving final output for pipeline {pipeline_hash}: {e}")
                    # Don't fail, just return None
                    final_output = None
    
    return {
        "pipeline_hash": pipeline_hash,
        "status": pipeline["status"],
        "nodes": [
            {
                "node_id": nid,
                "status": ndata["status"],
                "meta_job_hash": ndata.get("meta_job_hash"),
                "dependencies": ndata["dependencies"],
                "depth_level": ndata["depth_level"],
                "submitted_at": ndata.get("submitted_at"),
                "completed_at": ndata.get("completed_at"),
                "error": ndata.get("error")
            }
            for nid, ndata in sorted(nodes.items(), key=lambda x: x[1]["depth_level"])
        ],
        "created_at": pipeline["created_at"],
        "started_at": pipeline.get("started_at"),
        "completed_at": pipeline.get("completed_at"),
        "total_nodes": total,
        "completed_nodes": completed,
        "failed_nodes": failed,
        "running_nodes": running,
        "waiting_nodes": waiting,
        "progress_percentage": round(100 * completed / total, 1) if total > 0 else 0,
        "final_node_id": final_node_id,
        "final_output": final_output
    }
