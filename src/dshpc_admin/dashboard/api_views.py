"""
API views for AJAX endpoints.
"""
from django.http import JsonResponse
from .snapshot_utils import get_latest_snapshot


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

