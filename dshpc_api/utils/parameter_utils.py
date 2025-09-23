from typing import Dict, Any, Optional
from collections import OrderedDict

def sort_parameters(parameters: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Sort parameters alphabetically by key to ensure consistent ordering.
    
    This is important for job deduplication as MongoDB's document comparison
    is order-sensitive. By sorting parameters, we ensure that jobs with the
    same parameters in different orders are treated as the same job.
    
    Args:
        parameters: Dictionary of parameters (can be None)
        
    Returns:
        OrderedDict with parameters sorted alphabetically by key
    """
    if parameters is None:
        return {}
    
    # Sort parameters by key and return as OrderedDict to maintain order
    return OrderedDict(sorted(parameters.items())) 