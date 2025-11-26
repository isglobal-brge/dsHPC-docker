"""
Views for the dashboard app.
"""
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.urls import reverse
from django.contrib import messages
from .db_connections import MongoDBConnections, get_stats
from .auth import login_required_simple
from .snapshot_utils import get_container_status, get_system_resources, get_job_logs, get_environment_info, get_slurm_queue, get_latest_snapshot
import requests
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


def login_view(request):
    """Login view - only requires password."""
    if request.session.get('authenticated', False):
        return redirect('dashboard')
    
    # Load environment config for environment name
    env_name = "High-Performance Computing"
    try:
        import json
        import os
        config_path = '/app/environment-config.json'
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = json.load(f)
                environment_name = config.get('environment_name', env_name)
                # Truncate if too long
                env_name = environment_name if len(environment_name) <= 30 else environment_name[:27] + '...'
    except:
        pass
    
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
    
    return render(request, 'dashboard/login.html', {
        'page_title': 'Login',
        'env_name': env_name
    })


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
    
    # Load dsHPC authors from local file
    dshpc_authors = []
    try:
        import json
        import os
        authors_path = os.path.join(os.path.dirname(__file__), '..', 'authors-data.json')
        if os.path.exists(authors_path):
            with open(authors_path, 'r') as f:
                authors_data = json.load(f)
                dshpc_authors = authors_data.get('dshpc_authors', [])
    except:
        pass
    
    # Get docker prefix for container names
    docker_prefix = env_config.get('docker_stack_prefix', 'dshpc') if env_config else 'dshpc'
    
    context = {
        'stats': stats,
        'env_config': env_config,
        'dshpc_authors': dshpc_authors,
        'docker_prefix': docker_prefix,
        'page_title': 'Overview'
    }
    
    return render(request, 'dashboard/dashboard.html', context)


def container_status(request):
    """Get container status from latest snapshot."""
    snapshot_data = get_container_status()
    
    # Add display names and icons
    display_info = {
        'slurm': {'displayName': 'Slurm Service', 'icon': 'cpu'},
        'api': {'displayName': 'API Server', 'icon': 'server'},
        'admin': {'displayName': 'Admin Panel', 'icon': 'speedometer2'},
        'jobs': {'displayName': 'Jobs Database', 'icon': 'database'},
        'files': {'displayName': 'Files Database', 'icon': 'database'},
        'methods': {'displayName': 'Methods Database', 'icon': 'database'}
    }
    
    enriched_containers = []
    for container in snapshot_data['containers']:
        # Extract suffix (api, slurm, jobs, etc.) from name
        parts = container['name'].split('-')
        suffix = parts[-1] if parts else 'unknown'
        
        info = display_info.get(suffix, {'displayName': container['name'], 'icon': 'box'})
        
        enriched_containers.append({
            **container,
            'displayName': info['displayName'],
            'icon': info['icon']
        })
    
    return JsonResponse({
        'containers': enriched_containers,
        'timestamp': snapshot_data['timestamp'],
        'age_seconds': snapshot_data['age_seconds']
    })


def get_env_config():
    """Helper to load environment config."""
    try:
        import json
        import os
        config_path = '/app/environment-config.json'
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
    except:
        pass
    return {}


@login_required_simple
def files_list(request):
    """List all files with advanced filtering."""
    from datetime import datetime
    from django.utils import timezone
    
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
        
        # Add content preview for ALL files (text and binary)
        try:
            content_bytes = None
            content_type = f.get('content_type', '')
            
            # Get content based on storage type
            if f.get('storage_type') == 'gridfs' and f.get('gridfs_id'):
                try:
                    from gridfs import GridFS
                    from bson import ObjectId
                    fs = GridFS(files_db, collection='fs')
                    
                    grid_id = f['gridfs_id']
                    if isinstance(grid_id, str):
                        grid_id = ObjectId(grid_id)
                    
                    # Read first 10KB for preview
                    grid_out = fs.get(grid_id)
                    content_bytes = grid_out.read(10000)
                    grid_out.close()
                except Exception as e:
                    logger.debug(f"Could not load GridFS content for {f.get('filename')}: {e}")
            
            elif f.get('storage_type') in ['mongodb', 'inline'] and f.get('content'):
                import base64
                try:
                    # Decode base64 content
                    content_bytes = base64.b64decode(f.get('content'))[:10000]
                except Exception as e:
                    logger.debug(f"Could not decode content for {f.get('filename')}: {e}")
            
            # Generate preview
            if content_bytes:
                # Try to decode as text first
                is_text = content_type.startswith('text/') or content_type in ['application/json', 'application/x-r', '']
                
                if is_text:
                    try:
                        content_str = content_bytes.decode('utf-8', errors='replace')
                        max_preview = 5000
                        f['content_preview'] = content_str[:max_preview]
                        f['content_truncated'] = len(content_bytes) > max_preview
                        f['preview_type'] = 'text'
                    except:
                        is_text = False
                
                if not is_text:
                    # Show hex dump for binary files
                    hex_lines = []
                    for i in range(0, min(512, len(content_bytes)), 16):
                        chunk = content_bytes[i:i+16]
                        hex_part = ' '.join(f'{b:02x}' for b in chunk)
                        ascii_part = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
                        hex_lines.append(f'{i:08x}  {hex_part:<48}  {ascii_part}')
                    
                    f['content_preview'] = '\n'.join(hex_lines)
                    f['content_truncated'] = len(content_bytes) > 512
                    f['preview_type'] = 'hex'
        except Exception as e:
            logger.debug(f"Error adding preview for {f.get('filename')}: {e}")
    
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
    
    # Get snapshot timestamp
    snapshot = get_latest_snapshot()
    snapshot_time = snapshot['timestamp'] if snapshot else timezone.now()
    
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
        'snapshot_time': snapshot_time,
        'env_config': get_env_config(),
        'page_title': 'Files'
    }
    
    return render(request, 'dashboard/files_list.html', context)


@login_required_simple  
def jobs_list(request):
    """List all jobs with advanced filtering."""
    from datetime import datetime
    from django.utils import timezone
    
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
    
    # Search by job_hash
    if search:
        query['job_hash'] = {'$regex': search, '$options': 'i'}
    
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
                
                # Retrieve first 10KB
                grid_out = fs.get(grid_id)
                output_bytes = grid_out.read(10000)
                grid_out.close()
                
                # Try to decode as text, fallback to hex
                try:
                    j['output'] = output_bytes.decode('utf-8', errors='strict')
                    j['output_type'] = 'text'
                except UnicodeDecodeError:
                    # Binary data - show hex dump
                    hex_lines = []
                    for i in range(0, min(512, len(output_bytes)), 16):
                        chunk = output_bytes[i:i+16]
                        hex_part = ' '.join(f'{b:02x}' for b in chunk)
                        ascii_part = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
                        hex_lines.append(f'{i:08x}  {hex_part:<48}  {ascii_part}')
                    j['output'] = '\n'.join(hex_lines)
                    j['output_type'] = 'hex'
                
                j['output_truncated'] = True
            except Exception as e:
                j['output'] = f"[Error retrieving output from GridFS: {str(e)}]"
                j['output_type'] = 'error'
        
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
                error_bytes = grid_out.read(10000)
                grid_out.close()
                
                # Try to decode as text, fallback to hex
                try:
                    j['error'] = error_bytes.decode('utf-8', errors='strict')
                    j['error_type'] = 'text'
                except UnicodeDecodeError:
                    # Binary data - show hex dump
                    hex_lines = []
                    for i in range(0, min(512, len(error_bytes)), 16):
                        chunk = error_bytes[i:i+16]
                        hex_part = ' '.join(f'{b:02x}' for b in chunk)
                        ascii_part = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
                        hex_lines.append(f'{i:08x}  {hex_part:<48}  {ascii_part}')
                    j['error'] = '\n'.join(hex_lines)
                    j['error_type'] = 'hex'
                
                j['error_truncated'] = True
            except Exception as e:
                j['error'] = f"[Error retrieving error from GridFS: {str(e)}]"
                j['error_type'] = 'error'
        
        # Parse file_inputs arrays (if stored as strings)
        if j.get('file_inputs'):
            for input_name, input_value in list(j['file_inputs'].items()):
                if isinstance(input_value, str) and input_value.startswith('['):
                    try:
                        import json
                        j['file_inputs'][input_name] = json.loads(input_value.replace("'", '"'))
                    except:
                        pass  # Keep as string if parse fails
        
        # Enrich with file info (handle both single files and arrays)
        if j.get('file_inputs'):
            j['file_info'] = {}
            for name, hash_val in j['file_inputs'].items():
                if isinstance(hash_val, list):
                    # Array of files - enrich each one
                    j['file_info'][name] = []
                    for h in hash_val:
                        file_doc = files_db.files.find_one({'file_hash': h})
                        if file_doc:
                            j['file_info'][name].append({
                                'filename': file_doc.get('filename', 'unknown'),
                                'size': file_doc.get('file_size', 0),
                                'hash': h
                            })
                else:
                    # Single file
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
    
    # Get snapshot timestamp for last update indicator
    snapshot = get_latest_snapshot()
    snapshot_time = snapshot['timestamp'] if snapshot else timezone.now()
    
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
        'snapshot_time': snapshot_time,
        'env_config': get_env_config(),
        'page_title': 'Jobs'
    }
    
    return render(request, 'dashboard/jobs_list.html', context)


@login_required_simple
def meta_jobs_list(request):
    """List all meta-jobs with advanced filtering."""
    from datetime import datetime
    from django.utils import timezone
    
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
        
        # Enrich initial file inputs with filenames (handle both single files and arrays)
        if m.get('initial_file_inputs'):
            file_names = {}
            for name, hash_val in m['initial_file_inputs'].items():
                if isinstance(hash_val, list):
                    # Array - leave as list for template to handle
                    file_names[name] = [f"File {i}" for i in range(len(hash_val))]
                else:
                    # Single file
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
        
        # For each step, get the actual job and debug file_inputs
        if 'chain' in m:
            for step in m['chain']:
                # Check file_inputs - PyMongo should return arrays as lists
                if step.get('file_inputs'):
                    print(f"DEBUG file_inputs: {step['file_inputs']}")
                    for input_name, input_value in step['file_inputs'].items():
                        print(f"  {input_name}: type={type(input_value).__name__}, is_list={isinstance(input_value, list)}")
                        if not isinstance(input_value, list) and str(input_value).startswith('['):
                            print(f"  WARNING: Array stored as string, needs conversion!")
                    # file_inputs should already be lists from PyMongo
                if step.get('job_hash'):
                    job = jobs_db.jobs.find_one({'job_hash': step['job_hash']})
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
                                output_bytes = grid_out.read(10000)
                                grid_out.close()
                                
                                # Try to decode as text, fallback to hex
                                try:
                                    job['output'] = output_bytes.decode('utf-8', errors='strict')
                                    job['output_type'] = 'text'
                                except UnicodeDecodeError:
                                    # Binary - show hex dump
                                    hex_lines = []
                                    for i in range(0, min(512, len(output_bytes)), 16):
                                        chunk = output_bytes[i:i+16]
                                        hex_part = ' '.join(f'{b:02x}' for b in chunk)
                                        ascii_part = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
                                        hex_lines.append(f'{i:08x}  {hex_part:<48}  {ascii_part}')
                                    job['output'] = '\n'.join(hex_lines)
                                    job['output_type'] = 'hex'
                                
                                job['output_truncated'] = True
                            except Exception as e:
                                job['output'] = f"[Error retrieving output from GridFS: {str(e)}]"
                                job['output_type'] = 'error'
                        
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
                                grid_out.close()
                                
                                # Try to decode as text
                                try:
                                    job['error'] = error_bytes.decode('utf-8', errors='strict')
                                    job['error_type'] = 'text'
                                except UnicodeDecodeError:
                                    # Binary - show hex dump
                                    hex_lines = []
                                    for i in range(0, min(512, len(error_bytes)), 16):
                                        chunk = error_bytes[i:i+16]
                                        hex_part = ' '.join(f'{b:02x}' for b in chunk)
                                        ascii_part = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
                                        hex_lines.append(f'{i:08x}  {hex_part:<48}  {ascii_part}')
                                    job['error'] = '\n'.join(hex_lines)
                                    job['error_type'] = 'hex'
                                
                                job['error_truncated'] = True
                            except Exception as e:
                                job['error'] = f"[Error retrieving error from GridFS: {str(e)}]"
                                job['error_type'] = 'error'
                        
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
    
    # Get snapshot timestamp
    snapshot = get_latest_snapshot()
    snapshot_time = snapshot['timestamp'] if snapshot else timezone.now()
    
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
        'snapshot_time': snapshot_time,
        'env_config': get_env_config(),
        'page_title': 'Meta-Jobs'
    }
    
    return render(request, 'dashboard/meta_jobs_list.html', context)


@login_required_simple
def pipelines_list(request):
    """Show pipelines list from snapshot."""
    snapshot_data = get_latest_snapshot()
    
    if not snapshot_data or 'pipelines' not in snapshot_data:
        context = {
            'pipelines': [],
            'total_pipelines': 0,
            'snapshot_time': None,
            'page': 1,
            'total_pages': 1,
            'page_range': [],
            'per_page': 25,
            'env_config': get_env_config(),
            'page_title': 'Pipelines'
        }
        return render(request, 'dashboard/pipelines_list.html', context)
    
    pipelines_data = snapshot_data['pipelines']
    
    # Parse filters
    status_filter = request.GET.get('status', '')
    search = request.GET.get('search', '')
    date_from = request.GET.get('date_from', '')
    date_to = request.GET.get('date_to', '')
    page = int(request.GET.get('page', 1))
    per_page = int(request.GET.get('per_page', 25))
    
    # Filter pipelines
    filtered_pipelines = []
    for pipeline in pipelines_data:
        # Status filter
        if status_filter and pipeline.get('status') != status_filter:
            continue
        
        # Search filter
        if search:
            search_lower = search.lower()
            if not (search_lower in pipeline.get('pipeline_id', '').lower()):
                continue
        
        # Date filters
        if date_from or date_to:
            from datetime import datetime
            created_at = pipeline.get('created_at')
            if isinstance(created_at, str):
                try:
                    created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                except:
                    continue
            
            if date_from:
                try:
                    date_from_obj = datetime.strptime(date_from, '%Y-%m-%d')
                    if created_at.replace(tzinfo=None) < date_from_obj:
                        continue
                except:
                    pass
            
            if date_to:
                try:
                    from datetime import timedelta
                    date_to_obj = datetime.strptime(date_to, '%Y-%m-%d') + timedelta(days=1)
                    if created_at.replace(tzinfo=None) > date_to_obj:
                        continue
                except:
                    pass
        
        # Calculate pipeline statistics and organize by layers
        pipeline = process_pipeline_for_display(pipeline)
        filtered_pipelines.append(pipeline)
    
    # Sort by created_at descending
    filtered_pipelines.sort(
        key=lambda x: x.get('created_at') or '',
        reverse=True
    )
    
    # Pagination
    total_pipelines = len(filtered_pipelines)
    total_pages = (total_pipelines + per_page - 1) // per_page
    page = max(1, min(page, total_pages)) if total_pages > 0 else 1
    
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    pipelines_page = filtered_pipelines[start_idx:end_idx]
    
    # Generate page range for pagination
    page_range = []
    if total_pages <= 7:
        page_range = list(range(1, total_pages + 1))
    else:
        if page <= 4:
            page_range = list(range(1, 6)) + ['...', total_pages]
        elif page >= total_pages - 3:
            page_range = [1, '...'] + list(range(total_pages - 4, total_pages + 1))
        else:
            page_range = [1, '...'] + list(range(page - 1, page + 2)) + ['...', total_pages]
    
    context = {
        'pipelines': pipelines_page,
        'total_pipelines': total_pipelines,
        'snapshot_time': snapshot_data.get('timestamp'),
        'page': page,
        'total_pages': total_pages,
        'page_range': page_range,
        'per_page': per_page,
        'status_filter': status_filter,
        'search': search,
        'date_from': date_from,
        'date_to': date_to,
        'env_config': get_env_config(),
        'page_title': 'Pipelines'
    }
    
    return render(request, 'dashboard/pipelines_list.html', context)


def process_pipeline_for_display(pipeline):
    """Process a pipeline to calculate statistics and organize nodes by layers."""
    nodes = pipeline.get('nodes', {})
    
    # Count node statuses
    status_counts = {
        'waiting': 0,
        'running': 0,
        'completed': 0,
        'failed': 0,
        'cancelled': 0
    }
    
    for node_id, node_data in nodes.items():
        status = node_data.get('status', 'waiting')
        if status in status_counts:
            status_counts[status] += 1
    
    pipeline['total_nodes'] = len(nodes)
    pipeline['waiting_nodes'] = status_counts['waiting']
    pipeline['running_nodes'] = status_counts['running']
    pipeline['completed_nodes'] = status_counts['completed']
    pipeline['failed_nodes'] = status_counts['failed']
    pipeline['cancelled_nodes'] = status_counts['cancelled']
    
    # Calculate depth levels (layers) for DAG visualization
    # This is a topological sort that groups nodes by depth
    depths = {}
    
    def calculate_depth(node_id):
        if node_id in depths:
            return depths[node_id]
        
        node = nodes.get(node_id, {})
        dependencies = node.get('dependencies', [])
        
        if not dependencies:
            depths[node_id] = 0
            return 0
        
        max_dep_depth = max((calculate_depth(dep) for dep in dependencies), default=-1)
        depths[node_id] = max_dep_depth + 1
        return depths[node_id]
    
    # Calculate depth for all nodes
    for node_id in nodes.keys():
        calculate_depth(node_id)
    
    # Organize nodes by layers
    max_depth = max(depths.values()) if depths else 0
    layers = []
    
    for layer_num in range(max_depth + 1):
        layer_nodes = []
        for node_id, depth in depths.items():
            if depth == layer_num:
                node_data = nodes[node_id].copy()
                node_data['node_id'] = node_id
                layer_nodes.append(node_data)
        
        # Sort nodes in layer by node_id for consistency
        layer_nodes.sort(key=lambda x: x['node_id'])
        layers.append({'layer': layer_num, 'nodes': layer_nodes})
    
    pipeline['max_depth'] = max_depth + 1
    pipeline['layers'] = layers
    
    # Generate display name based on pipeline structure
    if layers:
        # Get root nodes (first layer)
        root_nodes = sorted([n['node_id'] for n in layers[0]['nodes']]) if layers else []
        # Get leaf nodes (last layer)
        leaf_nodes = sorted([n['node_id'] for n in layers[-1]['nodes']]) if layers else []
        
        if len(root_nodes) > 0 and len(leaf_nodes) > 0:
            # Format: "root1, root2 → ... → leaf1, leaf2" or "root → leaf" if same
            if len(layers) == 1:
                # Single layer
                pipeline['display_name'] = ', '.join(root_nodes)
            else:
                # Multiple layers
                root_str = ', '.join(root_nodes[:3])  # Show max 3 roots
                if len(root_nodes) > 3:
                    root_str += f', +{len(root_nodes) - 3}'
                
                leaf_str = ', '.join(leaf_nodes[:3])  # Show max 3 leaves
                if len(leaf_nodes) > 3:
                    leaf_str += f', +{len(leaf_nodes) - 3}'
                
                pipeline['display_name'] = f"{root_str} → ... → {leaf_str}"
        else:
            pipeline['display_name'] = f"{len(nodes)} node(s)"
    else:
        pipeline['display_name'] = "Empty Pipeline"
    
    return pipeline


@login_required_simple
def methods_list(request):
    """List all methods with filtering and pagination."""
    from django.utils import timezone
    
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
    
    # Get active methods query
    active_query = query.copy()
    active_query['active'] = True
    
    # Get total count of active methods only (for display)
    active_methods_count = methods_db.methods.count_documents(active_query)
    total_pages = max(1, (active_methods_count + per_page - 1) // per_page)
    
    # Ensure page is within bounds
    page = max(1, min(page, total_pages))
    
    # Get methods - always retrieve both active and inactive for client-side toggle
    skip = (page - 1) * per_page
    
    # Get active methods
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
    
    # Get snapshot timestamp
    snapshot = get_latest_snapshot()
    snapshot_time = snapshot['timestamp'] if snapshot else timezone.now()
    
    context = {
        'active_methods': active_methods,
        'inactive_methods': inactive_methods,
        'show_inactive': show_inactive,
        'search': search,
        'version_filter': version_filter,
        'page': page,
        'total_pages': total_pages,
        'total_methods': active_methods_count,  # Only count active methods
        'per_page': per_page,
        'page_range': page_range,
        'methods_count': len(active_methods) + len(inactive_methods),
        'snapshot_time': snapshot_time,
        'env_config': get_env_config(),
        'page_title': 'Methods'
    }
    
    return render(request, 'dashboard/methods_list.html', context)


@login_required_simple
def slurm_queue(request):
    """Show Slurm queue status from snapshot."""
    # Get data from snapshot
    snapshot_data = get_slurm_queue()
    
    # Check if AJAX request
    is_ajax = request.GET.get('ajax', '0') == '1'
    
    # Return JSON for AJAX
    if is_ajax:
        return JsonResponse({
            'queue': snapshot_data['queue'],
            'age_seconds': snapshot_data['age_seconds']
        }, safe=False)
    
    context = {
        'queue': snapshot_data['queue'],
        'snapshot_time': snapshot_data['timestamp'],
        'env_config': get_env_config(),
        'page_title': 'Slurm'
    }
    
    return render(request, 'dashboard/slurm_queue.html', context)


@login_required_simple
def slurm_job_logs(request, slurm_id):
    """Get logs for a specific Slurm job from snapshot."""
    # Try to get from snapshot first
    log_data = get_job_logs(slurm_id=slurm_id)
    
    if log_data:
        return JsonResponse({
            'stdout': log_data.get('slurm_output'),
            'system_output': log_data.get('system_output'),
            'system_error': log_data.get('system_error'),
            'from_snapshot': True,
            'error': None
        })
    
    # If not in snapshot, job might be too old or completed
    return JsonResponse({
        'error': 'Logs not available in current snapshot. Job may have completed or logs may have been archived.'
    })


@login_required_simple
def environment_info(request):
    """Show environment information from snapshot."""
    # Check if this is an AJAX request for data
    is_ajax = request.GET.get('ajax', '0') == '1'
    
    if not is_ajax:
        # Return loading page immediately
        context = {
            'page_title': 'Environment',
            'env_config': get_env_config()
        }
        return render(request, 'dashboard/environment.html', context)
    
    # AJAX request - get data from snapshot
    env_data = get_environment_info()
    
    # Return JSON for AJAX request
    return JsonResponse(env_data)


@login_required_simple
def logs_viewer(request):
    """View container and job logs."""
    import docker
    
    # Get docker prefix from environment config
    import json
    import os
    docker_prefix = 'dshpc'
    try:
        config_path = '/app/environment-config.json'
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = json.load(f)
                docker_prefix = config.get('docker_stack_prefix', 'dshpc')
    except:
        pass
    
    # Build container names dynamically
    from collections import OrderedDict
    containers = OrderedDict([
        (f'{docker_prefix}-slurm', 'Slurm Service'),
        (f'{docker_prefix}-api', 'API Server'),
        (f'{docker_prefix}-admin', 'Admin Panel')
    ])
    
    container_name = request.GET.get('container', f'{docker_prefix}-slurm')
    lines = int(request.GET.get('lines', 100))
    is_ajax = request.GET.get('ajax', '0') == '1'
    
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
        'env_config': get_env_config(),
        'page_title': 'Logs'
    }
    
    return render(request, 'dashboard/logs_viewer.html', context)

