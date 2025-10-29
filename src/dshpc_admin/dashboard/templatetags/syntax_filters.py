"""
Template filters for syntax highlighting.
"""
from django import template
from django.utils.safestring import mark_safe
import re

register = template.Library()


@register.filter(name='syntax_highlight')
def syntax_highlight(code, language=''):
    """
    Apply basic syntax highlighting to code.
    
    Usage:
        {{ code|syntax_highlight:"r" }}
    """
    if not code:
        return ''
    
    # Escape HTML
    code = code.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    
    language = language.lower()
    
    if language in ['r']:
        # R syntax highlighting
        # Comments
        code = re.sub(r'(#.*?)$', r'<span class="code-comment">\1</span>', code, flags=re.MULTILINE)
        # Strings
        code = re.sub(r'("[^"]*")', r'<span class="code-string">\1</span>', code)
        code = re.sub(r"('[^']*')", r'<span class="code-string">\1</span>', code)
        # Keywords
        keywords = r'\b(function|if|else|for|while|return|library|require|TRUE|FALSE|NULL|NA)\b'
        code = re.sub(keywords, r'<span class="code-keyword">\1</span>', code)
        
    elif language in ['python', 'py']:
        # Python syntax highlighting
        code = re.sub(r'(#.*?)$', r'<span class="code-comment">\1</span>', code, flags=re.MULTILINE)
        code = re.sub(r'("[^"]*")', r'<span class="code-string">\1</span>', code)
        code = re.sub(r"('[^']*')", r'<span class="code-string">\1</span>', code)
        keywords = r'\b(def|class|if|elif|else|for|while|return|import|from|try|except|with|as|True|False|None)\b'
        code = re.sub(keywords, r'<span class="code-keyword">\1</span>', code)
    
    return mark_safe(f'<pre class="code-block">{code}</pre>')


@register.filter(name='file_extension')
def file_extension(filename):
    """Get file extension."""
    if '.' in filename:
        return filename.rsplit('.', 1)[1].lower()
    return ''


@register.filter(name='file_icon')
def file_icon(filename):
    """Get icon for file type."""
    ext = file_extension(filename)
    
    icons = {
        'r': 'bi-file-code',
        'py': 'bi-file-code',
        'sh': 'bi-terminal',
        'json': 'bi-filetype-json',
        'yml': 'bi-filetype-yml',
        'yaml': 'bi-filetype-yml',
        'md': 'bi-markdown',
    }
    
    return icons.get(ext, 'bi-file-text')

