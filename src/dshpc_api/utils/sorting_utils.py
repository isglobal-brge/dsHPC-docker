"""
Sorting utilities for ensuring deterministic ordering of all data structures.

This module provides specialized sorting functions for different data types
used throughout the system. Consistent sorting is critical for:
1. Hash computation (for deduplication)
2. Database queries (for finding existing jobs/meta-jobs/pipelines)
3. Reproducibility (same input -> same output)
"""

from typing import Dict, Any, List, Optional
from collections import OrderedDict
from dshpc_api.utils.parameter_utils import _sort_dict_recursive


def sort_dict_recursive(data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Recursively sort a dictionary by keys at all nesting levels.
    
    Args:
        data: Dictionary to sort (can be None)
        
    Returns:
        OrderedDict with all keys sorted alphabetically at all levels
    """
    if data is None:
        return {}
    
    return _sort_dict_recursive(data)


def sort_file_inputs(file_inputs: Optional[Dict[str, str]]) -> Dict[str, str]:
    """
    Sort file inputs dictionary by key name alphabetically.
    
    Used for multi-file operations to ensure consistent ordering.
    
    Args:
        file_inputs: Dictionary mapping input names to file hashes
        
    Returns:
        OrderedDict with keys sorted alphabetically
    """
    if file_inputs is None:
        return {}
    
    if not isinstance(file_inputs, dict):
        return file_inputs
    
    # File inputs are typically flat (name -> hash), but use recursive sort for safety
    return OrderedDict(sorted(file_inputs.items()))


def sort_nodes(nodes: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Sort pipeline nodes dictionary by node ID alphabetically.
    Also recursively sorts all nested structures within each node.
    
    Args:
        nodes: Dictionary mapping node IDs to node definitions
        
    Returns:
        OrderedDict with node IDs sorted alphabetically and all nested data sorted
    """
    if nodes is None:
        return {}
    
    if not isinstance(nodes, dict):
        return nodes
    
    sorted_nodes = OrderedDict()
    for node_id in sorted(nodes.keys()):
        node = nodes[node_id]
        # Recursively sort all data within the node
        sorted_nodes[node_id] = _sort_dict_recursive(node)
    
    return sorted_nodes


def sort_dependencies(dependencies: Optional[List[str]]) -> List[str]:
    """
    Sort dependencies list alphabetically.
    
    Args:
        dependencies: List of node IDs that this node depends on
        
    Returns:
        Sorted list of dependencies
    """
    if dependencies is None:
        return []
    
    if not isinstance(dependencies, list):
        return dependencies
    
    # Sort dependencies alphabetically
    return sorted(dependencies)


def sort_chain(chain: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Sort all parameters within a method chain.
    The chain order itself is preserved (it's sequential),
    but parameters within each step are sorted recursively.
    
    Args:
        chain: List of method steps, each with method_name and parameters
        
    Returns:
        Chain with all parameters recursively sorted
    """
    if chain is None:
        return []
    
    if not isinstance(chain, list):
        return chain
    
    sorted_chain = []
    for step in chain:
        if isinstance(step, dict):
            sorted_step = _sort_dict_recursive(step)
            sorted_chain.append(sorted_step)
        else:
            sorted_chain.append(step)
    
    return sorted_chain

