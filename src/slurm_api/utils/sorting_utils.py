"""
Sorting utilities for ensuring deterministic ordering of all data structures.

This module provides specialized sorting functions for different data types
used throughout the system. Consistent sorting is critical for:
1. Hash computation (for deduplication)
2. Database queries (for finding existing jobs)
3. Reproducibility (same input -> same output)
"""

from typing import Dict, Any, Optional
from collections import OrderedDict
from slurm_api.utils.parameter_utils import _sort_dict_recursive


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

