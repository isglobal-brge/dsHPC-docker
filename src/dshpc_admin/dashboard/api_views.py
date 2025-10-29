"""
API views for AJAX endpoints.
"""
from django.http import JsonResponse
from .snapshot_utils import get_latest_snapshot, get_method_source


def snapshot_timestamp(request):
    """Return the timestamp of the latest snapshot."""
    snapshot = get_latest_snapshot()
    
    if snapshot and 'timestamp' in snapshot:
        return JsonResponse({
            'timestamp': snapshot['timestamp'].isoformat()
        })
    
    return JsonResponse({
        'timestamp': None
    })


def method_source(request, function_hash):
    """Return source code for a method."""
    source_data = get_method_source(function_hash)
    
    if source_data:
        # Build file tree structure
        files = source_data.get('files', {})
        
        # Organize files by directory
        file_tree = {}
        for filepath, content in files.items():
            parts = filepath.split('/')
            current = file_tree
            
            for i, part in enumerate(parts):
                if i == len(parts) - 1:
                    # File
                    current[part] = {'type': 'file', 'content': content}
                else:
                    # Directory
                    if part not in current:
                        current[part] = {'type': 'dir', 'children': {}}
                    current = current[part]['children']
        
        return JsonResponse({
            'name': source_data.get('name'),
            'files': files,
            'file_tree': file_tree
        })
    
    return JsonResponse({
        'error': 'Method source not available in current snapshot'
    }, status=404)

