"""
Views for the dashboard app.
"""
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.urls import reverse
from django.contrib import messages
from .db_connections import MongoDBConnections, get_stats
from .auth import login_required_simple
import requests
from django.conf import settings


def login_view(request):
    """Login view - only requires password."""
    if request.session.get('authenticated', False):
        return redirect('dashboard')
    
    if request.method == 'POST':
        password = request.POST.get('password', '')
        
        if password == settings.ADMIN_PASSWORD:
            # Set session as authenticated
            request.session['authenticated'] = True
            request.session['username'] = settings.ADMIN_USERNAME
            
            # Redirect to next URL or dashboard
            next_url = request.GET.get('next', reverse('dashboard'))
            return redirect(next_url)
        else:
            messages.error(request, 'Invalid password. Please try again.')
    
    return render(request, 'dashboard/login.html', {'page_title': 'Login'})


def logout_view(request):
    """Logout view."""
    request.session.flush()
    return redirect('login')


@login_required_simple
def dashboard_home(request):
    """Main dashboard view."""
    stats = get_stats()
    
    # Try to load environment-config.json
    env_config = None
    try:
        import json
        import os
        # Config is mounted at /app/environment-config.json in container
        config_path = '/app/environment-config.json'
        
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                env_config = json.load(f)
    except Exception as e:
        # If config doesn't exist or can't be read, just continue without it
        pass
    
    context = {
        'stats': stats,
        'env_config': env_config,
        'page_title': 'Dashboard'
    }
    
    return render(request, 'dashboard/dashboard.html', context)


@login_required_simple
def files_list(request):
    """List all files with advanced filtering."""
    from datetime import datetime
    
    files_db = MongoDBConnections.get_files_db()
    
    # Get filter parameters
    search = request.GET.get('search', '').strip()
    size_min = request.GET.get('size_min', '').strip()
    size_max = request.GET.get('size_max', '').strip()
    date_from = request.GET.get('date_from', '')
    date_to = request.GET.get('date_to', '')
    
    # Pagination
    page = int(request.GET.get('page', 1))
    per_page = int(request.GET.get('per_page', 50))
    
    # Build query
    query = {}
    
    # Search filter (filename or hash)
    if search:
        query['$or'] = [
            {'filename': {'$regex': search, '$options': 'i'}},
            {'file_hash': {'$regex': search, '$options': 'i'}}
        ]
    
    # Size filter
    if size_min or size_max:
        size_query = {}
        if size_min:
            try:
                size_query['$gte'] = int(size_min)
            except ValueError:
                pass
        if size_max:
            try:
                size_query['$lte'] = int(size_max)
            except ValueError:
                pass
        if size_query:
            query['file_size'] = size_query
    
    # Date range filter
    if date_from or date_to:
        date_query = {}
        if date_from:
            try:
                date_query['$gte'] = datetime.strptime(date_from, '%Y-%m-%d')
            except ValueError:
                pass
        if date_to:
            try:
                # Add one day to include the entire end date
                end_date = datetime.strptime(date_to, '%Y-%m-%d')
                from datetime import timedelta
                date_query['$lte'] = end_date + timedelta(days=1)
            except ValueError:
                pass
        if date_query:
            query['upload_date'] = date_query
    
    # Get total count with filters
    total_files = files_db.files.count_documents(query)
    total_pages = max(1, (total_files + per_page - 1) // per_page)
    
    # Ensure page is within bounds
    page = max(1, min(page, total_pages))
    
    # Get files with pagination
    skip = (page - 1) * per_page
    files = list(files_db.files.find(query).sort('upload_date', -1).skip(skip).limit(per_page))
    
    # Convert ObjectId to string and fix status
    for f in files:
        if '_id' in f:
            f['id'] = str(f['_id'])  # Rename to avoid underscore in template
            del f['_id']
        if 'gridfs_id' in f:
            f['gridfs_id'] = str(f['gridfs_id'])
        
        # Ensure status field exists
        if 'status' not in f or f['status'] is None:
            f['status'] = 'completed'  # Default for old files without status
    
    # Generate page range for pagination (with ellipsis)
    page_range = []
    if total_pages <= 10:
        page_range = list(range(1, total_pages + 1))
    else:
        if page <= 4:
            page_range = list(range(1, 6)) + ['...', total_pages]
        elif page >= total_pages - 3:
            page_range = [1, '...'] + list(range(total_pages - 4, total_pages + 1))
        else:
            page_range = [1, '...'] + list(range(page - 1, page + 2)) + ['...', total_pages]
    
    context = {
        'files': files,
        'page': page,
        'total_pages': total_pages,
        'total_files': total_files,
        'per_page': per_page,
        'page_range': page_range,
        'search': search,
        'size_min': size_min,
        'size_max': size_max,
        'date_from': date_from,
        'date_to': date_to,
        'page_title': 'Files'
    }
    
    return render(request, 'dashboard/files_list.html', context)


@login_required_simple
def jobs_list(request):
    """List all jobs with advanced filtering."""
    from datetime import datetime
    
    jobs_db = MongoDBConnections.get_jobs_db()
    methods_db = MongoDBConnections.get_methods_db()
    files_db = MongoDBConnections.get_files_db()
    
    # Get filter and pagination params
    status_filter = request.GET.get('status', '')
    search = request.GET.get('search', '').strip()
    method_filter = request.GET.get('method', '').strip()
    date_from = request.GET.get('date_from', '')
    date_to = request.GET.get('date_to', '')
    page = int(request.GET.get('page', 1))
    per_page = int(request.GET.get('per_page', 50))
    
    # Build query
    query = {}
    
    # Status filter
    if status_filter:
        query['status'] = status_filter
    
    # Search by job_id
    if search:
        query['job_id'] = {'$regex': search, '$options': 'i'}
    
    # Date range filter
    if date_from or date_to:
        date_query = {}
        if date_from:
            try:
                date_query['$gte'] = datetime.strptime(date_from, '%Y-%m-%d')
            except ValueError:
                pass
        if date_to:
            try:
                from datetime import timedelta
                end_date = datetime.strptime(date_to, '%Y-%m-%d')
                date_query['$lte'] = end_date + timedelta(days=1)
            except ValueError:
                pass
        if date_query:
            query['created_at'] = date_query
    
    # Method filter - need to get function_hash first
    method_hashes = []
    if method_filter:
        methods = list(methods_db.methods.find({'name': {'$regex': method_filter, '$options': 'i'}}))
        method_hashes = [m['function_hash'] for m in methods if 'function_hash' in m]
        if method_hashes:
            query['function_hash'] = {'$in': method_hashes}
    
    # Get total count
    total_jobs = jobs_db.jobs.count_documents(query)
    total_pages = max(1, (total_jobs + per_page - 1) // per_page)
    
    # Ensure page is within bounds
    page = max(1, min(page, total_pages))
    
    # Get jobs from database with pagination
    skip = (page - 1) * per_page
    jobs = list(jobs_db.jobs.find(query).sort('created_at', -1).skip(skip).limit(per_page))
    
    # Enrich jobs with method names from methods DB
    for j in jobs:
        if '_id' in j:
            j['id'] = str(j['_id'])
            del j['_id']
        
        # Get method name from function_hash
        if 'function_hash' in j and j['function_hash']:
            method = methods_db.methods.find_one({'function_hash': j['function_hash']})
            if method:
                j['method_name'] = method.get('name', 'unknown')
                j['method_version'] = method.get('version', '')
        
        # Calculate duration if job is completed
        if 'created_at' in j and 'completed_at' in j and j.get('completed_at'):
            duration = (j['completed_at'] - j['created_at']).total_seconds()
            j['duration_seconds'] = duration
        
        # Retrieve output from GridFS if stored there
        if j.get('output_storage') == 'gridfs' and j.get('output_gridfs_id'):
            try:
                from gridfs import GridFS
                from bson import ObjectId
                fs = GridFS(jobs_db, collection='job_outputs')
                
                grid_id = j['output_gridfs_id']
                if isinstance(grid_id, str):
                    grid_id = ObjectId(grid_id)
                
                # Retrieve only first 10000 chars to avoid memory issues
                grid_out = fs.get(grid_id)
                output_bytes = grid_out.read(10000)  # Read first 10KB
                j['output'] = output_bytes.decode('utf-8', errors='replace')
                j['output_truncated'] = True
                grid_out.close()
            except Exception as e:
                j['output'] = f"[Error retrieving output from GridFS: {str(e)}]"
        
        # Retrieve error from GridFS if stored there
        if j.get('error_storage') == 'gridfs' and j.get('error_gridfs_id'):
            try:
                from gridfs import GridFS
                from bson import ObjectId
                fs = GridFS(jobs_db, collection='job_outputs')
                
                grid_id = j['error_gridfs_id']
                if isinstance(grid_id, str):
                    grid_id = ObjectId(grid_id)
                
                grid_out = fs.get(grid_id)
                error_bytes = grid_out.read(10000)  # Read first 10KB
                j['error'] = error_bytes.decode('utf-8', errors='replace')
                j['error_truncated'] = True
                grid_out.close()
            except Exception as e:
                j['error'] = f"[Error retrieving error from GridFS: {str(e)}]"
        
        # Enrich with file info
        if j.get('file_inputs'):
            j['file_info'] = {}
            for name, hash_val in j['file_inputs'].items():
                file_doc = files_db.files.find_one({'file_hash': hash_val})
                if file_doc:
                    j['file_info'][name] = {
                        'filename': file_doc.get('filename', 'unknown'),
                        'size': file_doc.get('file_size', 0)
                    }
        elif j.get('file_hash'):
            file_doc = files_db.files.find_one({'file_hash': j['file_hash']})
            if file_doc:
                j['input_filename'] = file_doc.get('filename', 'unknown')
                j['input_size'] = file_doc.get('file_size', 0)
    
    # Generate page range for pagination
    page_range = []
    if total_pages <= 10:
        page_range = list(range(1, total_pages + 1))
    else:
        if page <= 4:
            page_range = list(range(1, 6)) + ['...', total_pages]
        elif page >= total_pages - 3:
            page_range = [1, '...'] + list(range(total_pages - 4, total_pages + 1))
        else:
            page_range = [1, '...'] + list(range(page - 1, page + 2)) + ['...', total_pages]
    
    context = {
        'jobs': jobs,
        'status_filter': status_filter,
        'search': search,
        'method_filter': method_filter,
        'date_from': date_from,
        'date_to': date_to,
        'page': page,
        'total_pages': total_pages,
        'total_jobs': total_jobs,
        'per_page': per_page,
        'page_range': page_range,
        'page_title': 'Jobs'
    }
    
    return render(request, 'dashboard/jobs_list.html', context)


@login_required_simple
def meta_jobs_list(request):
    """List all meta-jobs with advanced filtering."""
    from datetime import datetime
    
    jobs_db = MongoDBConnections.get_jobs_db()
    methods_db = MongoDBConnections.get_methods_db()
    files_db = MongoDBConnections.get_files_db()
    
    # Get filter parameters
    search = request.GET.get('search', '').strip()
    status_filter = request.GET.get('status', '')
    date_from = request.GET.get('date_from', '')
    date_to = request.GET.get('date_to', '')
    
    # Pagination
    page = int(request.GET.get('page', 1))
    per_page = int(request.GET.get('per_page', 50))
    
    # Build query
    query = {}
    
    # Search by meta_job_id
    if search:
        query['meta_job_id'] = {'$regex': search, '$options': 'i'}
    
    # Status filter
    if status_filter:
        query['status'] = status_filter
    
    # Date range filter
    if date_from or date_to:
        date_query = {}
        if date_from:
            try:
                date_query['$gte'] = datetime.strptime(date_from, '%Y-%m-%d')
            except ValueError:
                pass
        if date_to:
            try:
                from datetime import timedelta
                end_date = datetime.strptime(date_to, '%Y-%m-%d')
                date_query['$lte'] = end_date + timedelta(days=1)
            except ValueError:
                pass
        if date_query:
            query['created_at'] = date_query
    
    # Get total count
    total_meta_jobs = jobs_db.meta_jobs.count_documents(query)
    total_pages = max(1, (total_meta_jobs + per_page - 1) // per_page)
    
    # Ensure page is within bounds
    page = max(1, min(page, total_pages))
    
    # Get meta-jobs with pagination
    skip = (page - 1) * per_page
    meta_jobs = list(jobs_db.meta_jobs.find(query).sort('created_at', -1).skip(skip).limit(per_page))
    
    # Enrich with job details for each step
    for m in meta_jobs:
        if '_id' in m:
            m['id'] = str(m['_id'])
            del m['_id']
        
        # Enrich initial file inputs with filenames
        if m.get('initial_file_inputs'):
            file_names = {}
            for name, hash_val in m['initial_file_inputs'].items():
                file_doc = files_db.files.find_one({'file_hash': hash_val})
                if file_doc:
                    file_names[name] = file_doc.get('filename', name)
                else:
                    file_names[name] = name
            m['initial_file_names'] = file_names
        elif m.get('initial_file_hash'):
            file_doc = files_db.files.find_one({'file_hash': m['initial_file_hash']})
            if file_doc:
                m['initial_file_name'] = file_doc.get('filename', 'unknown')
        
        # For each step, get the actual job
        if 'chain' in m:
            for step in m['chain']:
                if step.get('job_id'):
                    job = jobs_db.jobs.find_one({'job_id': step['job_id']})
                    if job:
                        # Retrieve output from GridFS if stored there
                        if job.get('output_storage') == 'gridfs' and job.get('output_gridfs_id'):
                            try:
                                from gridfs import GridFS
                                from bson import ObjectId
                                fs = GridFS(jobs_db, collection='job_outputs')
                                
                                grid_id = job['output_gridfs_id']
                                if isinstance(grid_id, str):
                                    grid_id = ObjectId(grid_id)
                                
                                grid_out = fs.get(grid_id)
                                output_bytes = grid_out.read(10000)  # Read first 10KB
                                job['output'] = output_bytes.decode('utf-8', errors='replace')
                                job['output_truncated'] = True
                                grid_out.close()
                            except Exception as e:
                                job['output'] = f"[Error retrieving output from GridFS: {str(e)}]"
                        
                        # Retrieve error from GridFS if stored there
                        if job.get('error_storage') == 'gridfs' and job.get('error_gridfs_id'):
                            try:
                                from gridfs import GridFS
                                from bson import ObjectId
                                fs = GridFS(jobs_db, collection='job_outputs')
                                
                                grid_id = job['error_gridfs_id']
                                if isinstance(grid_id, str):
                                    grid_id = ObjectId(grid_id)
                                
                                grid_out = fs.get(grid_id)
                                error_bytes = grid_out.read(10000)
                                job['error'] = error_bytes.decode('utf-8', errors='replace')
                                job['error_truncated'] = True
                                grid_out.close()
                            except Exception as e:
                                job['error'] = f"[Error retrieving error from GridFS: {str(e)}]"
                        
                        step['job_data'] = job
                        # Get method name
                        if job.get('function_hash'):
                            method = methods_db.methods.find_one({'function_hash': job['function_hash']})
                            if method:
                                step['job_data']['method_name'] = method.get('name', 'unknown')
    
    # Generate page range for pagination
    page_range = []
    if total_pages <= 10:
        page_range = list(range(1, total_pages + 1))
    else:
        if page <= 4:
            page_range = list(range(1, 6)) + ['...', total_pages]
        elif page >= total_pages - 3:
            page_range = [1, '...'] + list(range(total_pages - 4, total_pages + 1))
        else:
            page_range = [1, '...'] + list(range(page - 1, page + 2)) + ['...', total_pages]
    
    context = {
        'meta_jobs': meta_jobs,
        'page': page,
        'total_pages': total_pages,
        'total_meta_jobs': total_meta_jobs,
        'per_page': per_page,
        'page_range': page_range,
        'search': search,
        'status_filter': status_filter,
        'date_from': date_from,
        'date_to': date_to,
        'page_title': 'Meta-Jobs'
    }
    
    return render(request, 'dashboard/meta_jobs_list.html', context)


@login_required_simple
def methods_list(request):
    """List all methods with filtering and pagination."""
    methods_db = MongoDBConnections.get_methods_db()
    
    # Get filter parameters
    show_inactive = request.GET.get('show_inactive', 'false') == 'true'
    search = request.GET.get('search', '').strip()
    version_filter = request.GET.get('version', '').strip()
    page = int(request.GET.get('page', 1))
    per_page = int(request.GET.get('per_page', 50))
    
    # Build base query (always get all methods for client-side toggle)
    query = {}
    
    # Search filter
    if search:
        query['$or'] = [
            {'name': {'$regex': search, '$options': 'i'}},
            {'description': {'$regex': search, '$options': 'i'}}
        ]
    
    # Version filter
    if version_filter:
        query['version'] = {'$regex': version_filter, '$options': 'i'}
    
    # Get total count (only active for pagination, but retrieve all for display)
    total_methods = methods_db.methods.count_documents(query)
    total_pages = max(1, (total_methods + per_page - 1) // per_page)
    
    # Ensure page is within bounds
    page = max(1, min(page, total_pages))
    
    # Get methods - always retrieve both active and inactive for client-side toggle
    skip = (page - 1) * per_page
    
    # Get active methods
    active_query = query.copy()
    active_query['active'] = True
    active_methods_cursor = list(methods_db.methods.find(active_query).sort('name', 1).skip(skip).limit(per_page))
    
    # Always get inactive methods too (for same page) - without pagination to keep it simple
    inactive_query = query.copy()
    inactive_query['active'] = {'$ne': True}
    inactive_methods_cursor = list(methods_db.methods.find(inactive_query).sort('name', 1).skip(skip).limit(per_page))
    
    # Convert ObjectId to string
    active_methods = []
    inactive_methods = []
    
    for m in active_methods_cursor:
        if '_id' in m:
            m['id'] = str(m['_id'])
            del m['_id']
        active_methods.append(m)
    
    for m in inactive_methods_cursor:
        if '_id' in m:
            m['id'] = str(m['_id'])
            del m['_id']
        inactive_methods.append(m)
    
    # Generate page range for pagination
    page_range = []
    if total_pages <= 10:
        page_range = list(range(1, total_pages + 1))
    else:
        if page <= 4:
            page_range = list(range(1, 6)) + ['...', total_pages]
        elif page >= total_pages - 3:
            page_range = [1, '...'] + list(range(total_pages - 4, total_pages + 1))
        else:
            page_range = [1, '...'] + list(range(page - 1, page + 2)) + ['...', total_pages]
    
    context = {
        'active_methods': active_methods,
        'inactive_methods': inactive_methods,
        'show_inactive': show_inactive,
        'search': search,
        'version_filter': version_filter,
        'page': page,
        'total_pages': total_pages,
        'total_methods': total_methods,
        'per_page': per_page,
        'page_range': page_range,
        'methods_count': len(active_methods) + len(inactive_methods),
        'page_title': 'Methods'
    }
    
    return render(request, 'dashboard/methods_list.html', context)


@login_required_simple
def slurm_queue(request):
    """Show Slurm queue status."""
    try:
        # Call Slurm API to get queue status
        response = requests.get(f"{settings.SLURM_API_URL}/queue", timeout=5)
        if response.status_code == 200:
            data = response.json()
            # The API returns {"jobs": [...]}
            queue_data = data.get('jobs', []) if isinstance(data, dict) else []
        else:
            queue_data = []
    except Exception as e:
        queue_data = []
    
    # Check if AJAX request
    is_ajax = request.GET.get('ajax', '0') == '1'
    
    context = {
        'queue': queue_data,
        'page_title': 'Slurm'
    }
    
    # Return partial template for AJAX
    if is_ajax:
        return render(request, 'dashboard/slurm_queue_content.html', context)
    
    return render(request, 'dashboard/slurm_queue.html', context)


@login_required_simple
def environment_info(request):
    """Show environment information from Slurm container."""
    import docker
    from django.core.cache import cache
    from django.utils import timezone
    from datetime import timedelta
    
    # Check if this is an AJAX request for data
    is_ajax = request.GET.get('ajax', '0') == '1'
    
    if not is_ajax:
        # Return loading page immediately
        context = {
            'page_title': 'Environment'
        }
        return render(request, 'dashboard/environment.html', context)
    
    # AJAX request - fetch actual data
    container_name = 'dshpc-epiflare-slurm'
    cache_key = 'environment_info_data'
    cache_timeout = 5  # seconds
    
    # Check if we have cached data
    cached_data = cache.get(cache_key)
    last_update = cache.get(f'{cache_key}_timestamp')
    
    # If we have cached data and it's less than 5 seconds old, use it
    if cached_data and last_update:
        time_since_update = (timezone.now() - last_update).total_seconds()
        if time_since_update < cache_timeout:
            cached_data['cached'] = True
            cached_data['cache_age'] = int(time_since_update)
            cached_data['next_refresh'] = int(cache_timeout - time_since_update)
            return JsonResponse(cached_data)
    
    # Otherwise, fetch fresh data
    env_data = {
        'python': {},
        'r': {},
        'system': {},
        'slurm': {},
        'error': None,
        'cached': False
    }
    
    try:
        client = docker.from_env()
        container = client.containers.get(container_name)
        
        # Get Python version and packages
        try:
            python_version = container.exec_run('python3 --version').output.decode('utf-8').strip()
            env_data['python']['version'] = python_version
            
            # Get pip list
            pip_list = container.exec_run('/opt/venvs/system_python/bin/pip list --format=json').output.decode('utf-8')
            import json
            env_data['python']['packages'] = json.loads(pip_list) if pip_list else []
        except Exception as e:
            env_data['python']['error'] = str(e)
        
        # Get R version and packages
        try:
            r_version = container.exec_run('R --version').output.decode('utf-8').split('\n')[0]
            env_data['r']['version'] = r_version
            
            # Get installed R packages
            r_packages_cmd = 'R -s -e "ip <- installed.packages(); cat(jsonlite::toJSON(data.frame(Package=ip[,\'Package\'], Version=ip[,\'Version\'])))"'
            r_packages = container.exec_run(['bash', '-c', r_packages_cmd]).output.decode('utf-8')
            # Extract JSON from R output
            import re
            json_match = re.search(r'\[.*\]', r_packages, re.DOTALL)
            if json_match:
                env_data['r']['packages'] = json.loads(json_match.group(0))
            else:
                env_data['r']['packages'] = []
        except Exception as e:
            env_data['r']['error'] = str(e)
        
        # Get system info
        try:
            # OS information
            os_name = container.exec_run(['bash', '-c', 'cat /etc/os-release | grep "^PRETTY_NAME=" | cut -d\'"\' -f2']).output.decode('utf-8').strip()
            env_data['system']['os_name'] = os_name if os_name else 'Unknown'
            
            # Kernel version
            kernel = container.exec_run('uname -r').output.decode('utf-8').strip()
            env_data['system']['kernel'] = kernel
            
            # Architecture
            arch = container.exec_run('uname -m').output.decode('utf-8').strip()
            env_data['system']['architecture'] = arch
            
            # CPU info
            cpu_info = container.exec_run('nproc').output.decode('utf-8').strip()
            env_data['system']['cpus'] = cpu_info
            
            # Memory info - parse for visualization
            mem_raw = container.exec_run('free -b').output.decode('utf-8')
            mem_lines = mem_raw.strip().split('\n')
            if len(mem_lines) > 1:
                mem_parts = mem_lines[1].split()
                if len(mem_parts) >= 7:
                    env_data['system']['mem_total'] = int(mem_parts[1])
                    env_data['system']['mem_used'] = int(mem_parts[2])
                    env_data['system']['mem_free'] = int(mem_parts[3])
                    env_data['system']['mem_available'] = int(mem_parts[6])
                    env_data['system']['mem_used_pct'] = int((int(mem_parts[2]) / int(mem_parts[1])) * 100)
            
            # Disk info - parse for visualization
            disk_raw = container.exec_run(['bash', '-c', 'df -B1 / | tail -1']).output.decode('utf-8')
            disk_parts = disk_raw.strip().split()
            if len(disk_parts) >= 5:
                env_data['system']['disk_total'] = int(disk_parts[1])
                env_data['system']['disk_used'] = int(disk_parts[2])
                env_data['system']['disk_available'] = int(disk_parts[3])
                env_data['system']['disk_used_pct'] = int(disk_parts[4].replace('%', ''))
            
            # APT packages - get ALL installed packages properly
            apt_cmd = 'dpkg-query -W -f=\'${Package}\t${Version}\n\' | sort'
            apt_output = container.exec_run(['bash', '-c', apt_cmd]).output.decode('utf-8')
            apt_packages = []
            for line in apt_output.strip().split('\n'):
                if line and '\t' in line:
                    parts = line.split('\t')
                    if len(parts) == 2:
                        apt_packages.append({'name': parts[0], 'version': parts[1]})
            env_data['system']['apt_packages'] = apt_packages
            env_data['system']['apt_packages_count'] = len(apt_packages)
        except Exception as e:
            env_data['system']['error'] = str(e)
        
        # Get Slurm configuration
        try:
            slurm_conf = container.exec_run('cat /etc/slurm/slurm.conf').output.decode('utf-8')
            env_data['slurm']['config'] = slurm_conf
            
            # Get Slurm version
            slurm_version = container.exec_run('scontrol --version').output.decode('utf-8').strip()
            env_data['slurm']['version'] = slurm_version
            
            # Get node info
            node_info = container.exec_run('scontrol show node').output.decode('utf-8')
            env_data['slurm']['node_info'] = node_info
        except Exception as e:
            env_data['slurm']['error'] = str(e)
            
    except docker.errors.NotFound:
        env_data['error'] = f"Container '{container_name}' not found"
    except Exception as e:
        env_data['error'] = f"Error accessing container: {str(e)}"
    
    # Cache the data for 5 seconds
    cache.set(cache_key, env_data, timeout=cache_timeout)
    cache.set(f'{cache_key}_timestamp', timezone.now(), timeout=cache_timeout)
    
    # Return JSON for AJAX request
    return JsonResponse(env_data)


@login_required_simple
def logs_viewer(request):
    """View container and job logs."""
    import docker
    
    container_name = request.GET.get('container', 'dshpc-epiflare-slurm')
    lines = int(request.GET.get('lines', 100))
    is_ajax = request.GET.get('ajax', '0') == '1'
    
    # Use OrderedDict to maintain order
    from collections import OrderedDict
    containers = OrderedDict([
        ('dshpc-epiflare-slurm', 'Slurm Service'),
        ('dshpc-epiflare-api', 'API Server'),
        ('dshpc-epiflare-admin', 'Admin Panel')
    ])
    
    # Get logs using Docker SDK
    logs = ""
    error = None
    
    try:
        client = docker.from_env()
        container = client.containers.get(container_name)
        
        # Get logs (last N lines)
        log_bytes = container.logs(tail=lines, timestamps=True)
        logs_raw = log_bytes.decode('utf-8', errors='replace')
        
        # For AJAX: strip ANSI for now (we'll add proper parsing later)
        # For HTML: keep as-is with proper rendering
        import re
        ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        logs = logs_raw  # Keep ANSI codes
        
    except docker.errors.NotFound:
        error = f"Container '{container_name}' not found"
        logs = f"Container not found. Available: {', '.join(containers.keys())}"
    except Exception as e:
        error = f"Error: {str(e)}"
        logs = f"Error getting logs: {str(e)}"
    
    # Return JSON for AJAX requests
    if is_ajax:
        return JsonResponse({
            'logs': logs,
            'container': container_name,
            'container_name': containers.get(container_name, container_name),
            'error': error
        })
    
    # Return HTML for normal requests
    context = {
        'containers': containers,
        'selected_container': container_name,
        'logs': logs,
        'error': error,
        'lines': lines,
        'page_title': 'Logs'
    }
    
    return render(request, 'dashboard/logs_viewer.html', context)

