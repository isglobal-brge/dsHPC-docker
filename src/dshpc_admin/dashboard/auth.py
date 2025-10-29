"""
Authentication utilities for the admin panel.
"""
from django.shortcuts import redirect
from django.urls import reverse
from functools import wraps


def login_required_simple(view_func):
    """
    Decorator to require login for a view.
    Checks if user is authenticated via session.
    """
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        if not request.session.get('authenticated', False):
            # Store the next URL to redirect after login
            next_url = request.get_full_path()
            return redirect(f"{reverse('login')}?next={next_url}")
        return view_func(request, *args, **kwargs)
    return wrapper

