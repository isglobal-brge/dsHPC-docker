from typing import Dict, Any, Optional, List, Union
from collections import OrderedDict


def sort_parameters(parameters: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Sort parameters alphabetically by key with RECURSIVE sorting of nested structures.
    
    This is critical for job deduplication as MongoDB's document comparison
    is order-sensitive. By recursively sorting all parameters, we ensure that jobs 
    with the same parameters in different orders (at any nesting level) are treated 
    as the same job.
    
    Args:
        parameters: Dictionary of parameters (can be None)
        
    Returns:
        OrderedDict with parameters sorted alphabetically by key at all nesting levels
    """
    if parameters is None:
        return {}
    
    return _sort_dict_recursive(parameters)


def _sort_dict_recursive(obj: Any) -> Any:
    """
    Recursively sort a dictionary and all nested dictionaries by their keys.
    Also handles lists/tuples by recursively processing their elements.
    
    Args:
        obj: The object to sort (dict, list, tuple, or primitive)
        
    Returns:
        Sorted version of the object
    """
    if isinstance(obj, dict):
        # Sort dictionary keys alphabetically and recursively process values
        sorted_dict = OrderedDict()
        for key in sorted(obj.keys()):
            sorted_dict[key] = _sort_dict_recursive(obj[key])
        return sorted_dict
    
    elif isinstance(obj, list):
        # Recursively process list elements
        return [_sort_dict_recursive(item) for item in obj]
    
    elif isinstance(obj, tuple):
        # Recursively process tuple elements and convert back to tuple
        return tuple(_sort_dict_recursive(item) for item in obj)
    
    else:
        # Primitive types (str, int, float, bool, None) return as-is
        return obj
