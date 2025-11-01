"""
Pipeline service for DAG orchestration of meta-jobs
"""
import uuid
import asyncio
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Set
from pymongo.database import Database

from dshpc_api.config.logging_config import logger
from dshpc_api.services.db_service import get_jobs_db, get_files_db
from dshpc_api.models.pipeline import (
    PipelineStatus, PipelineNodeStatus, PipelineNodeInfo, PipelineInfo
)


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
            # Extract reference: "$ref:node_1" or "$ref:node_1/data/text"
            ref = value[5:]  # Remove "$ref:" prefix
            
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
    Create a new pipeline in the database.
    
    Args:
        pipeline_data: Pipeline definition with nodes and dependencies
        
    Returns:
        Tuple of (pipeline_id, message)
    """
    # Validate nodes exist
    nodes = pipeline_data.get("nodes", {})
    if not nodes:
        raise ValueError("Pipeline must have at least one node")
    
    # Detect circular dependencies
    cycle = detect_cycles(nodes)
    if cycle:
        raise ValueError(f"Circular dependency detected: {' → '.join(cycle)}")
    
    # Calculate depth levels
    depth_levels = calculate_depth_levels(nodes)
    
    # Generate pipeline ID
    pipeline_id = str(uuid.uuid4())
    
    # Create pipeline document
    pipeline_doc = {
        "pipeline_id": pipeline_id,
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
            "meta_job_id": None,
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
    db = await get_jobs_db()
    await db.pipelines.insert_one(pipeline_doc)
    
    logger.info(f"Created pipeline {pipeline_id} with {len(nodes)} nodes")
    
    return pipeline_id, f"Pipeline created with {len(nodes)} nodes"


async def get_pipeline_status(pipeline_id: str) -> Optional[Dict[str, Any]]:
    """
    Get pipeline status and information.
    
    Args:
        pipeline_id: Pipeline ID
        
    Returns:
        Pipeline information or None if not found
    """
    db = await get_jobs_db()
    pipeline = await db.pipelines.find_one({"pipeline_id": pipeline_id})
    
    if not pipeline:
        return None
    
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
            final_meta_job_id = nodes[final_node_id].get("meta_job_id")
            
            # Retrieve actual output content from the meta-job (same as meta_job_service does)
            if final_meta_job_id:
                try:
                    from dshpc_api.services.meta_job_service import get_meta_job_info
                    
                    # Get the meta-job info which includes final_output
                    meta_job_info = await get_meta_job_info(final_meta_job_id)
                    if meta_job_info:
                        final_output = meta_job_info.final_output
                except Exception as e:
                    logger.error(f"Error retrieving final output for pipeline {pipeline_id}: {e}")
                    # Don't fail, just return None
                    final_output = None
    
    return {
        "pipeline_id": pipeline_id,
        "status": pipeline["status"],
        "nodes": [
            {
                "node_id": nid,
                "status": ndata["status"],
                "meta_job_id": ndata.get("meta_job_id"),
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
