"""
WSGI config for dshpc_admin project.
"""
import os
from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dshpc_admin.settings')

application = get_wsgi_application()
