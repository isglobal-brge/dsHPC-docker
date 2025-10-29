"""
Views for the dashboard app.
"""
from django.shortcuts import render
from django.http import JsonResponse
from .db_connections import MongoDBConnections, get_stats
import requests
from django.conf import settings


def dashboard_home(request):
    """Main dashboard view."""
    stats = get_stats()
    
    context = {
        'stats': stats,
        'page_title': 'Dashboard'
    }
    
    return render(request, 'dashboard/dashboard.html', context)


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


def methods_list(request):
    """List all methods with filtering and pagination."""
    methods_db = MongoDBConnections.get_methods_db()
    
    # Get filter parameters
    show_inactive = request.GET.get('show_inactive', 'false') == 'true'
    search = request.GET.get('search', '').strip()
    version_filter = request.GET.get('version', '').strip()
    page = int(request.GET.get('page', 1))
    per_page = int(request.GET.get('per_page', 50))
    
    # Build query
    query = {} if show_inactive else {'active': True}
    
    # Search filter
    if search:
        query['$or'] = [
            {'name': {'$regex': search, '$options': 'i'}},
            {'description': {'$regex': search, '$options': 'i'}}
        ]
    
    # Version filter
    if version_filter:
        query['version'] = {'$regex': version_filter, '$options': 'i'}
    
    # Get total count
    total_methods = methods_db.methods.count_documents(query)
    total_pages = max(1, (total_methods + per_page - 1) // per_page)
    
    # Ensure page is within bounds
    page = max(1, min(page, total_pages))
    
    # Get methods from database - active first, then by name
    skip = (page - 1) * per_page
    methods = list(methods_db.methods.find(query).sort([('active', -1), ('name', 1)]).skip(skip).limit(per_page))
    
    # Convert ObjectId to string and separate active/inactive
    active_methods = []
    inactive_methods = []
    
    for m in methods:
        if '_id' in m:
            m['id'] = str(m['_id'])
            del m['_id']
        
        if m.get('active', False):
            active_methods.append(m)
        else:
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
        'methods_count': len(methods),
        'page_title': 'Methods'
    }
    
    return render(request, 'dashboard/methods_list.html', context)


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
        'page_title': 'Logs Viewer'
    }
    
    return render(request, 'dashboard/logs_viewer.html', context)

