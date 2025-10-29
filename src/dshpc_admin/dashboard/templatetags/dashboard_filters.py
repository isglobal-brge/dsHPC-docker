"""
Custom template filters for the dashboard.
"""
import json
from django import template
from django.utils.safestring import mark_safe

register = template.Library()


@register.filter(name='jsonify')
def jsonify(value, indent=2):
    """
    Convert a Python object to a beautified JSON string.
    
    Usage in template:
        {{ my_dict|jsonify }}
        {{ my_dict|jsonify:4 }}  # with indent of 4
    """
    if value is None:
        return ''
    
    if isinstance(value, str):
        try:
            # Try to parse if it's already a JSON string
            value = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return value
    
    try:
        return json.dumps(value, indent=indent, sort_keys=True, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(value)


@register.filter(name='jsonify_html')
def jsonify_html(value, indent=2):
    """
    Convert a Python object to a beautified JSON string wrapped in HTML with syntax highlighting.
    
    Usage in template:
        {{ my_dict|jsonify_html|safe }}
    """
    json_str = jsonify(value, indent)
    if not json_str:
        return ''
    
    # Escape HTML entities
    json_str = json_str.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    
    # Apply syntax highlighting with spans
    highlighted = json_str
    
    # This is a simple regex-based highlighter
    import re
    
    def replacer(match):
        matched_text = match.group(0)
        cls = 'json-number'
        
        if matched_text.startswith('"'):
            if matched_text.endswith(':'):
                cls = 'json-key'
            else:
                cls = 'json-string'
        elif matched_text in ('true', 'false'):
            cls = 'json-boolean'
        elif matched_text == 'null':
            cls = 'json-null'
        
        return f'<span class="{cls}">{matched_text}</span>'
    
    pattern = r'"(?:[^"\\]|\\.)*"(?:\s*:)?|\b(?:true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?'
    highlighted = re.sub(pattern, replacer, highlighted)
    
    html = f'<div class="json-container"><pre>{highlighted}</pre></div>'
    return mark_safe(html)


@register.filter(name='pretty_bytes')
def pretty_bytes(bytes_value):
    """
    Convert bytes to human-readable format.
    
    Usage in template:
        {{ file.size|pretty_bytes }}
    """
    if bytes_value is None:
        return 'N/A'
    
    try:
        bytes_value = float(bytes_value)
    except (TypeError, ValueError):
        return bytes_value
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    
    return f"{bytes_value:.2f} PB"


@register.filter(name='truncate_hash')
def truncate_hash(hash_value, length=8):
    """
    Truncate a hash to specified length with ellipsis.
    
    Usage in template:
        {{ file.hash|truncate_hash:12 }}
    """
    if not hash_value:
        return ''
    
    hash_str = str(hash_value)
    if len(hash_str) <= length:
        return hash_str
    
    return f"{hash_str[:length]}..."


@register.filter(name='status_badge_class')
def status_badge_class(status):
    """
    Return Bootstrap badge class based on status.
    
    Usage in template:
        <span class="badge {{ job.status|status_badge_class }}">{{ job.status }}</span>
    """
    status_str = str(status).upper() if len(str(status)) <= 3 else str(status).lower()
    
    status_map = {
        # Job statuses (Slurm codes - uppercase)
        'PD': 'bg-warning text-dark',   # Pending - yellow
        'R': 'bg-primary',               # Running - blue
        'CG': 'bg-info',                 # Completing - cyan
        'CD': 'bg-success',              # Completed - green
        'F': 'bg-danger',                # Failed - red
        'CA': 'bg-secondary',            # Cancelled - grey
        'TO': 'bg-danger',               # Timeout - red
        # Meta-job statuses (lowercase)
        'pending': 'bg-warning text-dark',
        'running': 'bg-primary',
        'completed': 'bg-success',
        'failed': 'bg-danger',
        'cancelled': 'bg-secondary',
        # File statuses
        'uploading': 'bg-warning text-dark',
        'error': 'bg-danger',
    }
    
    return status_map.get(status_str, 'bg-secondary')


@register.filter(name='status_icon')
def status_icon(status):
    """
    Return Bootstrap icon class based on status.
    
    Usage in template:
        <i class="{{ job.status|status_icon }}"></i>
    """
    status_map = {
        # Job statuses (Slurm codes)
        'PD': 'bi-clock-fill',           # Pending
        'R': 'bi-play-circle-fill',      # Running
        'CG': 'bi-arrow-clockwise',      # Completing
        'CD': 'bi-check-circle-fill',    # Completed
        'F': 'bi-x-circle-fill',         # Failed
        'CA': 'bi-slash-circle',         # Cancelled
        'TO': 'bi-exclamation-triangle-fill',  # Timeout
        # Meta-job statuses
        'pending': 'bi-clock-fill',
        'running': 'bi-play-circle-fill',
        'completed': 'bi-check-circle-fill',
        'failed': 'bi-x-circle-fill',
        'cancelled': 'bi-slash-circle',
        # Generic
        'uploading': 'bi-upload',
        'error': 'bi-exclamation-triangle-fill',
    }
    
    return status_map.get(str(status).upper() if len(str(status)) <= 3 else str(status).lower(), 'bi-info-circle')


@register.filter(name='duration_format')
def duration_format(seconds):
    """
    Format duration in seconds to human-readable format.
    
    Usage in template:
        {{ job.duration|duration_format }}
    """
    if seconds is None:
        return 'N/A'
    
    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        return str(seconds)
    
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


@register.filter(name='lookup')
def lookup(dictionary, key):
    """
    Lookup a value in a dictionary by key.
    
    Usage in template:
        {{ my_dict|lookup:'key_name' }}
    """
    if dictionary is None:
        return None
    
    try:
        return dictionary.get(key)
    except (AttributeError, TypeError):
        return None

