"""
Background monitoring worker that collects system data and stores snapshots in MongoDB.
"""
import asyncio
import time
import os
import json
from datetime import datetime
from pymongo import MongoClient
import docker
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MonitorWorker:
    """Background worker that monitors system and stores snapshots."""

    def __init__(self):
        # MongoDB connections
        self.mongo_jobs_uri = os.environ.get('MONGO_JOBS_URI', 'mongodb://localhost:27017/')
        self.mongo_files_uri = os.environ.get('MONGO_FILES_URI', 'mongodb://localhost:27017/')
        self.mongo_methods_uri = os.environ.get('MONGO_METHODS_URI', 'mongodb://localhost:27017/')
        self.mongo_jobs_db_name = os.environ.get('MONGO_JOBS_DB', 'dshpc-jobs')
        self.mongo_files_db_name = os.environ.get('MONGO_FILES_DB', 'dshpc-files')
        self.mongo_methods_db_name = os.environ.get('MONGO_METHODS_DB', 'dshpc-methods')

        self.jobs_client = None
        self.files_client = None
        self.methods_client = None
        self.jobs_db = None
        self.files_db = None
        self.methods_db = None
        self.snapshots_collection = None

        # Docker client
        self.docker_client = None

        # Config
        self.docker_prefix = 'dshpc'
        self.load_config()
    
    def load_config(self):
        """Load docker prefix from environment-config.json"""
        try:
            config_path = '/app/environment-config.json'
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config = json.load(f)
                    self.docker_prefix = config.get('docker_stack_prefix', 'dshpc')
        except Exception as e:
            logger.warning(f"Could not load config: {e}")
    
    def connect(self):
        """Connect to MongoDB databases and Docker."""
        try:
            self.jobs_client = MongoClient(self.mongo_jobs_uri)
            self.jobs_db = self.jobs_client[self.mongo_jobs_db_name]
            self.snapshots_collection = self.jobs_db['system_snapshots']
            logger.info(f"Connected to Jobs DB: {self.mongo_jobs_db_name}")

            self.files_client = MongoClient(self.mongo_files_uri)
            self.files_db = self.files_client[self.mongo_files_db_name]
            logger.info(f"Connected to Files DB: {self.mongo_files_db_name}")

            self.methods_client = MongoClient(self.mongo_methods_uri)
            self.methods_db = self.methods_client[self.mongo_methods_db_name]
            logger.info(f"Connected to Methods DB: {self.mongo_methods_db_name}")

            # Aliases for backward compatibility with existing code
            self.client = self.jobs_client
            self.db = self.jobs_db
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

        try:
            self.docker_client = docker.from_env()
            logger.info("Connected to Docker")
        except Exception as e:
            logger.error(f"Failed to connect to Docker: {e}")
            raise
    
    def collect_container_status(self):
        """Collect status of all containers."""
        containers_data = []
        
        container_names = [
            {'name': f'{self.docker_prefix}-slurm', 'type': 'service'},
            {'name': f'{self.docker_prefix}-api', 'type': 'service'},
            {'name': f'{self.docker_prefix}-admin', 'type': 'service'},
            {'name': f'{self.docker_prefix}-jobs', 'type': 'database'},
            {'name': f'{self.docker_prefix}-files', 'type': 'database'},
            {'name': f'{self.docker_prefix}-methods', 'type': 'database'}
        ]
        
        for container_info in container_names:
            try:
                container = self.docker_client.containers.get(container_info['name'])
                containers_data.append({
                    'name': container_info['name'],
                    'type': container_info['type'],
                    'status': container.status,
                    'state': 'running' if container.status == 'running' else 'stopped'
                })
            except docker.errors.NotFound:
                containers_data.append({
                    'name': container_info['name'],
                    'type': container_info['type'],
                    'status': 'not_found',
                    'state': 'stopped'
                })
            except Exception as e:
                logger.error(f"Error checking container {container_info['name']}: {e}")
        
        return containers_data
    
    def collect_system_resources(self):
        """Collect system resource usage from slurm container."""
        resources = {}
        
        try:
            container = self.docker_client.containers.get(f'{self.docker_prefix}-slurm')
            
            # CPU count
            result = container.exec_run('nproc')
            if result.exit_code == 0:
                resources['cpus'] = int(result.output.decode('utf-8').strip())
            
            # Memory
            result = container.exec_run('free -b')
            if result.exit_code == 0:
                lines = result.output.decode('utf-8').strip().split('\n')
                if len(lines) > 1:
                    parts = lines[1].split()
                    if len(parts) >= 7:
                        resources['mem_total'] = int(parts[1])
                        resources['mem_used'] = int(parts[2])
                        resources['mem_free'] = int(parts[3])
                        resources['mem_available'] = int(parts[6])
                        resources['mem_used_pct'] = int((int(parts[2]) / int(parts[1])) * 100)
            
            # Disk
            result = container.exec_run(['bash', '-c', 'df -B1 / | tail -1'])
            if result.exit_code == 0:
                parts = result.output.decode('utf-8').strip().split()
                if len(parts) >= 5:
                    resources['disk_total'] = int(parts[1])
                    resources['disk_used'] = int(parts[2])
                    resources['disk_available'] = int(parts[3])
                    resources['disk_used_pct'] = int(parts[4].replace('%', ''))
        
        except Exception as e:
            logger.error(f"Error collecting system resources: {e}")
        
        return resources
    
    def collect_slurm_job_logs(self):
        """Collect logs from active Slurm jobs."""
        logs_data = []
        
        try:
            container = self.docker_client.containers.get(f'{self.docker_prefix}-slurm')
            
            # Get list of active jobs from MongoDB
            jobs = list(self.db.jobs.find({
                'status': {'$in': ['PD', 'R', 'CG']},  # Pending, Running, Completing
                'slurm_id': {'$ne': None}
            }))
            
            for job in jobs:
                slurm_id = job.get('slurm_id')
                job_hash = job.get('job_hash')
                
                if not slurm_id:
                    continue
                
                log_entry = {
                    'slurm_id': slurm_id,
                    'job_hash': job_hash,
                    'timestamp': datetime.utcnow()
                }
                
                # Try to read slurm output (last 500 lines)
                try:
                    result = container.exec_run(['bash', '-c', f'tail -n 500 /app/slurm-{slurm_id}.out 2>/dev/null || echo ""'])
                    if result.exit_code == 0:
                        output = result.output.decode('utf-8', errors='replace').strip()
                        if output:
                            # Additional safeguard: limit to 50k characters max
                            if len(output) > 50000:
                                output = output[-50000:]  # Keep last 50k chars
                                log_entry['slurm_output_truncated'] = True
                            log_entry['slurm_output'] = output
                            log_entry['slurm_output_lines'] = len(output.split('\n'))
                except Exception as e:
                    logger.debug(f"Could not read slurm output for {slurm_id}: {e}")
                
                # Try to read system output (last 500 lines)
                try:
                    result = container.exec_run(['bash', '-c', f'tail -n 500 /tmp/output_{job_hash}.txt 2>/dev/null || echo ""'])
                    if result.exit_code == 0:
                        output = result.output.decode('utf-8', errors='replace').strip()
                        if output:
                            # Additional safeguard: limit to 50k characters max
                            if len(output) > 50000:
                                output = output[-50000:]
                                log_entry['system_output_truncated'] = True
                            log_entry['system_output'] = output
                            log_entry['system_output_lines'] = len(output.split('\n'))
                except Exception as e:
                    logger.debug(f"Could not read system output for {job_hash}: {e}")
                
                # Try to read system error (last 500 lines)
                try:
                    result = container.exec_run(['bash', '-c', f'tail -n 500 /tmp/error_{job_hash}.txt 2>/dev/null || echo ""'])
                    if result.exit_code == 0:
                        output = result.output.decode('utf-8', errors='replace').strip()
                        if output:
                            # Additional safeguard: limit to 50k characters max
                            if len(output) > 50000:
                                output = output[-50000:]
                                log_entry['system_error_truncated'] = True
                            log_entry['system_error'] = output
                            log_entry['system_error_lines'] = len(output.split('\n'))
                except Exception as e:
                    logger.debug(f"Could not read system error for {job_hash}: {e}")
                
                # Only add if we got some logs
                if 'slurm_output' in log_entry or 'system_output' in log_entry or 'system_error' in log_entry:
                    logs_data.append(log_entry)
        
        except Exception as e:
            logger.error(f"Error collecting job logs: {e}")
        
        return logs_data
    
    def collect_environment_info(self):
        """Collect environment information (Python, R, System, Slurm)."""
        env_data = {
            'python': {},
            'r': {},
            'system': {},
            'slurm': {}
        }
        
        try:
            container = self.docker_client.containers.get(f'{self.docker_prefix}-slurm')
            
            # Python version and packages
            try:
                python_version = container.exec_run('python3 --version').output.decode('utf-8').strip()
                env_data['python']['version'] = python_version
                
                pip_list = container.exec_run('/opt/venvs/system_python/bin/pip list --format=json').output.decode('utf-8')
                env_data['python']['packages'] = json.loads(pip_list) if pip_list else []
            except Exception as e:
                env_data['python']['error'] = str(e)
            
            # R version and packages
            try:
                r_version = container.exec_run('R --version').output.decode('utf-8').split('\n')[0]
                env_data['r']['version'] = r_version
                
                r_packages_cmd = 'R -s -e "ip <- installed.packages(); cat(jsonlite::toJSON(data.frame(Package=ip[,\'Package\'], Version=ip[,\'Version\'])))"'
                r_packages = container.exec_run(['bash', '-c', r_packages_cmd]).output.decode('utf-8')
                
                import re
                json_match = re.search(r'\[.*\]', r_packages, re.DOTALL)
                if json_match:
                    env_data['r']['packages'] = json.loads(json_match.group(0))
            except Exception as e:
                env_data['r']['error'] = str(e)
            
            # OS and system info
            try:
                os_name = container.exec_run(['bash', '-c', 'cat /etc/os-release | grep "^PRETTY_NAME=" | cut -d\'"\' -f2']).output.decode('utf-8').strip()
                env_data['system']['os_name'] = os_name if os_name else 'Unknown'
                
                kernel = container.exec_run('uname -r').output.decode('utf-8').strip()
                env_data['system']['kernel'] = kernel
                
                arch = container.exec_run('uname -m').output.decode('utf-8').strip()
                env_data['system']['architecture'] = arch
                
                # APT packages - get ALL packages
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
            
            # Slurm config
            try:
                slurm_conf = container.exec_run('cat /etc/slurm/slurm.conf').output.decode('utf-8')
                env_data['slurm']['config'] = slurm_conf

                slurm_version = container.exec_run('scontrol --version').output.decode('utf-8').strip()
                env_data['slurm']['version'] = slurm_version

                node_info = container.exec_run('scontrol show node').output.decode('utf-8')
                env_data['slurm']['node_info'] = node_info

                # Parse live node specs for easy display
                node_specs = {}
                for line in node_info.split('\n'):
                    line = line.strip()
                    for field in line.split():
                        if '=' in field:
                            key, value = field.split('=', 1)
                            if key in ['CPUTot', 'RealMemory', 'FreeMem', 'AllocMem', 'State', 'Gres']:
                                node_specs[key] = value
                env_data['slurm']['node_specs'] = node_specs

                # Get partition info for memory limits
                partition_info = container.exec_run('scontrol show partition').output.decode('utf-8')
                env_data['slurm']['partition_info'] = partition_info

                # Parse partition specs
                partition_specs = {}
                for line in partition_info.split('\n'):
                    line = line.strip()
                    for field in line.split():
                        if '=' in field:
                            key, value = field.split('=', 1)
                            if key in ['DefMemPerCPU', 'MaxMemPerCPU', 'MaxTime', 'PartitionName']:
                                partition_specs[key] = value
                env_data['slurm']['partition_specs'] = partition_specs

            except Exception as e:
                env_data['slurm']['error'] = str(e)
            
            # Startup scripts
            try:
                startup_scripts = {}
                script_paths = {
                    'pre-install.sh': '/environment/startup/pre-install.sh',
                    'pre-startup.sh': '/environment/startup/pre-startup.sh',
                    'post-install.sh': '/environment/startup/post-install.sh'
                }
                
                for script_name, script_path in script_paths.items():
                    try:
                        result = container.exec_run(['bash', '-c', f'cat {script_path} 2>/dev/null || echo ""'])
                        script_content = result.output.decode('utf-8')
                        startup_scripts[script_name] = script_content if script_content else ""
                    except Exception as e:
                        logger.warning(f"Error reading startup script {script_name}: {e}")
                        startup_scripts[script_name] = ""
                
                env_data['startup_scripts'] = startup_scripts
            except Exception as e:
                logger.warning(f"Error collecting startup scripts: {e}")
                env_data['startup_scripts'] = {}
                
        except docker.errors.NotFound:
            env_data['error'] = f"Container '{self.docker_prefix}-slurm' not found"
        except Exception as e:
            logger.error(f"Error collecting environment info: {e}")
            env_data['error'] = str(e)
        
        return env_data
    
    def collect_slurm_queue(self):
        """Collect current Slurm queue status with full enrichment."""
        queue_data = []
        
        try:
            import requests
            # Get slurm API URL from environment
            slurm_api_url = os.environ.get('SLURM_API_URL', f'http://{self.docker_prefix}-slurm:8000')
            
            response = requests.get(f"{slurm_api_url}/queue", timeout=3)
            if response.status_code == 200:
                data = response.json()
                queue_data = data.get('jobs', []) if isinstance(data, dict) else []
                
                # Get references to other collections
                methods_client = self.client[os.environ.get('MONGO_METHODS_DB', 'dshpc-methods')]
                files_client = self.client[os.environ.get('MONGO_FILES_DB', 'dshpc-files')]
                
                # Enrich with MongoDB data (jobs, methods, files)
                for slurm_job in queue_data:
                    slurm_id = slurm_job.get('job_id')
                    if slurm_id:
                        job_doc = self.db.jobs.find_one({'slurm_id': slurm_id})
                        if job_doc:
                            slurm_job['db_job_hash'] = str(job_doc.get('job_hash', ''))
                            slurm_job['db_status'] = job_doc.get('status')
                            slurm_job['created_at'] = job_doc.get('created_at')
                            slurm_job['function_hash'] = job_doc.get('function_hash')
                            slurm_job['parameters'] = job_doc.get('parameters')
                            
                            # Get method name
                            if job_doc.get('function_hash'):
                                method = methods_client.methods.find_one({'function_hash': job_doc['function_hash']})
                                if method:
                                    slurm_job['method_name'] = method.get('name')
                                    slurm_job['method_version'] = method.get('version')
                            
                            # Get input file info
                            if job_doc.get('file_hash'):
                                file_doc = files_client.files.find_one({'file_hash': job_doc['file_hash']})
                                if file_doc:
                                    slurm_job['input_filename'] = file_doc.get('filename')
                                    slurm_job['input_size'] = file_doc.get('file_size')
                            elif job_doc.get('file_inputs'):
                                slurm_job['file_inputs'] = job_doc['file_inputs']
                                slurm_job['file_info'] = {}
                                for name, hash_val in job_doc['file_inputs'].items():
                                    file_doc = files_client.files.find_one({'file_hash': hash_val})
                                    if file_doc:
                                        slurm_job['file_info'][name] = {
                                            'filename': file_doc.get('filename'),
                                            'size': file_doc.get('file_size')
                                        }
        except Exception as e:
            logger.error(f"Error collecting Slurm queue: {e}")
        
        return queue_data
    
    def collect_method_sources(self):
        """Collect source code from active methods."""
        methods_source = []
        
        try:
            import base64
            import tarfile
            import io
            
            # Connect to methods database
            methods_uri = os.environ.get('MONGO_METHODS_URI', 'mongodb://localhost:27017/')
            methods_db_name = os.environ.get('MONGO_METHODS_DB', 'dshpc-methods')
            
            from pymongo import MongoClient
            methods_client = MongoClient(methods_uri)
            methods_db = methods_client[methods_db_name]
            
            # Get active methods with bundles
            methods = list(methods_db.methods.find({'active': True, 'bundle': {'$exists': True}}).limit(10))
            
            for method in methods:
                try:
                    method_data = {
                        'name': method.get('name'),
                        'function_hash': method.get('function_hash'),
                        'files': {}
                    }
                    
                    # Decode bundle
                    bundle_b64 = method.get('bundle')
                    if not bundle_b64:
                        continue
                    
                    bundle_bytes = base64.b64decode(bundle_b64)
                    
                    # Extract tarball and read files
                    with tarfile.open(fileobj=io.BytesIO(bundle_bytes), mode='r:gz') as tar:
                        for member in tar.getmembers():
                            if member.isfile():
                                # Process ALL files (not just specific extensions)
                                try:
                                    file_obj = tar.extractfile(member)
                                    if file_obj:
                                        # Efficiently read only first 100KB without loading entire file
                                        truncate_at = 100000  # 100KB - maximum size to collect
                                        original_size = member.size
                                        
                                        # Read only what we need (max 100KB)
                                        content_bytes = file_obj.read(truncate_at)
                                        
                                        # Try to decode as text, fallback to hex dump for binary
                                        try:
                                            # Attempt UTF-8 decode
                                            content = content_bytes.decode('utf-8')
                                            preview_type = 'text'
                                        except UnicodeDecodeError:
                                            # Binary file - create hex dump preview
                                            hex_lines = []
                                            for i in range(0, min(1024, len(content_bytes)), 16):  # Max 1KB hex preview
                                                chunk = content_bytes[i:i+16]
                                                hex_part = ' '.join(f'{b:02x}' for b in chunk)
                                                ascii_part = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
                                                hex_lines.append(f'{i:08x}  {hex_part:<48}  {ascii_part}')
                                            content = '\n'.join(hex_lines)
                                            if len(content_bytes) > 1024:
                                                content += f'\n\n... (binary file, showing first 1KB as hex dump)'
                                            preview_type = 'hex'
                                        
                                        file_info = {
                                            'content': content,
                                            'truncated': original_size > truncate_at,
                                            'original_size': original_size,
                                            'preview_type': preview_type
                                        }
                                        
                                        method_data['files'][member.name] = file_info
                                except Exception as e:
                                    # Robust error handling - log but don't crash
                                    logger.debug(f"Could not process {member.name}: {e}")
                                    # Store error info so user knows file exists but couldn't be read
                                    try:
                                        method_data['files'][member.name] = {
                                            'content': f'[Error reading file: {str(e)}]',
                                            'truncated': False,
                                            'original_size': member.size if hasattr(member, 'size') else 0,
                                            'preview_type': 'error'
                                        }
                                    except:
                                        pass  # If even error storage fails, just skip
                    
                    if method_data['files']:
                        methods_source.append(method_data)
                        
                except Exception as e:
                    logger.error(f"Error processing method {method.get('name')}: {e}")
        
        except Exception as e:
            logger.error(f"Error collecting method sources: {e}")
        
        return methods_source
    
    def collect_pipelines(self):
        """Collect pipeline data from MongoDB with enriched meta-job information."""
        pipelines_data = []
        
        try:
            # Get all pipelines (limit to recent 200 for snapshot efficiency)
            pipelines = list(self.db.pipelines.find({}).sort('created_at', -1).limit(200))
            
            for pipeline in pipelines:
                # Convert ObjectId to string for JSON serialization
                if '_id' in pipeline:
                    del pipeline['_id']
                
                # Enrich nodes with full meta-job information
                nodes = pipeline.get('nodes', {})
                for node_id, node_data in nodes.items():
                    meta_job_hash = node_data.get('meta_job_hash')
                    if meta_job_hash:
                        try:
                            # Fetch meta-job data
                            meta_job = self.db.meta_jobs.find_one({'meta_job_hash': meta_job_hash})
                            if meta_job:
                                # Remove _id from meta_job
                                if '_id' in meta_job:
                                    del meta_job['_id']
                                
                                # Enrich initial input files with content previews
                                initial_file_inputs = meta_job.get('initial_file_inputs', {})
                                if initial_file_inputs:
                                    initial_file_info = {}
                                    for name, file_hash in initial_file_inputs.items():
                                        try:
                                            file_doc = self.db.files.find_one({'file_hash': file_hash})
                                            if file_doc:
                                                import base64
                                                file_info = {
                                                    'filename': file_doc.get('filename', 'unknown'),
                                                    'size': file_doc.get('size', 0)
                                                }
                                                
                                                # Get content preview (first 2000 chars)
                                                if 'content' in file_doc:
                                                    try:
                                                        content_bytes = base64.b64decode(file_doc['content'])
                                                        content_str = content_bytes.decode('utf-8')[:2000]
                                                        file_info['content_preview'] = content_str
                                                    except Exception:
                                                        file_info['content_preview'] = None
                                                
                                                initial_file_info[name] = file_info
                                        except Exception as e:
                                            logger.debug(f"Could not load file info for {file_hash}: {e}")
                                    
                                    if initial_file_info:
                                        meta_job['initial_file_info'] = initial_file_info
                                
                                # Enrich chain steps with job data (including outputs)
                                chain = meta_job.get('chain', [])
                                for step in chain:
                                    job_hash = step.get('job_hash')
                                    if job_hash:
                                        job = self.db.jobs.find_one({'job_hash': job_hash})
                                        if job:
                                            # Remove _id from job
                                            if '_id' in job:
                                                del job['_id']
                                            
                                            # Load output from GridFS if needed
                                            if job.get('output_gridfs_id') and not job.get('output'):
                                                try:
                                                    import gridfs
                                                    fs = gridfs.GridFS(self.db._database)
                                                    grid_out = fs.get(job['output_gridfs_id'])
                                                    # Get first 10KB as preview
                                                    output_content = grid_out.read(10240).decode('utf-8')
                                                    job['output'] = output_content
                                                    job['output_truncated'] = True
                                                except Exception as e:
                                                    logger.debug(f"Could not load GridFS output for {job_hash}: {e}")
                                            
                                            # Truncate very long outputs that are directly in the document
                                            if job.get('output') and len(job['output']) > 10240:
                                                job['output'] = job['output'][:10240]
                                                job['output_truncated'] = True
                                            
                                            # Load file_info for each file input (needed for extraction path metadata)
                                            if job.get('file_inputs'):
                                                job['file_info'] = {}
                                                for input_name, file_hash in job['file_inputs'].items():
                                                    try:
                                                        file_doc = self.db.files.find_one({'file_hash': file_hash})
                                                        if file_doc:
                                                            file_info = {
                                                                'filename': file_doc.get('filename', 'unknown'),
                                                                'size': file_doc.get('size', 0)
                                                            }
                                                            # Include metadata if available (contains extraction path info)
                                                            if 'metadata' in file_doc:
                                                                file_info['metadata'] = file_doc['metadata']
                                                            
                                                            job['file_info'][input_name] = file_info
                                                    except Exception as e:
                                                        logger.debug(f"Could not load file_info for {file_hash}: {e}")
                                            
                                            step['job_data'] = job
                                            
                                            # Debug logging to see what we have
                                            has_output = 'output' in job and job['output']
                                            logger.debug(f"Job {job_hash}: status={job.get('status')}, has_output={has_output}, output_length={len(job.get('output', ''))}")
                                
                                # Add enriched meta_job_info to node
                                node_data['meta_job_info'] = meta_job
                        except Exception as e:
                            logger.error(f"Error enriching node {node_id} with meta-job {meta_job_hash}: {e}")
                
                pipelines_data.append(pipeline)
                
            logger.debug(f"Collected {len(pipelines_data)} pipelines")
            
        except Exception as e:
            logger.error(f"Error collecting pipelines: {e}")
        
        return pipelines_data

    def collect_jobs_list(self, limit=50):
        """Collect pre-enriched jobs list for admin panel with output/error previews."""
        from gridfs import GridFS
        from bson import ObjectId

        OUTPUT_PREVIEW_SIZE = 500

        jobs_data = {'items': [], 'total': 0}
        try:
            jobs_collection = self.jobs_db['jobs']
            methods_collection = self.methods_db['methods']
            files_collection = self.files_db['files']

            # Initialize GridFS for output/error loading
            output_fs = GridFS(self.jobs_db, collection='outputs')
            error_fs = GridFS(self.jobs_db, collection='errors')

            # Get total count
            jobs_data['total'] = jobs_collection.count_documents({})

            # Get jobs sorted by created_at desc
            jobs = list(jobs_collection.find().sort('created_at', -1).limit(limit))

            # Batch load methods and files
            function_hashes = set()
            file_hashes = set()
            for j in jobs:
                if j.get('function_hash'):
                    function_hashes.add(j['function_hash'])
                if j.get('file_inputs'):
                    for v in j['file_inputs'].values():
                        if isinstance(v, list):
                            file_hashes.update(v)
                        elif v:
                            file_hashes.add(v)
                if j.get('file_hash'):
                    file_hashes.add(j['file_hash'])

            methods_lookup = {}
            if function_hashes:
                for m in methods_collection.find({'function_hash': {'$in': list(function_hashes)}}):
                    methods_lookup[m['function_hash']] = {'name': m.get('name'), 'version': m.get('version')}

            files_lookup = {}
            if file_hashes:
                for f in files_collection.find({'file_hash': {'$in': list(file_hashes)}}):
                    files_lookup[f['file_hash']] = {'filename': f.get('filename'), 'file_size': f.get('file_size')}

            # Enrich jobs with output/error previews
            for j in jobs:
                job = {
                    'id': str(j['_id']),
                    'job_hash': j.get('job_hash'),
                    'status': j.get('status'),
                    'slurm_job_id': j.get('slurm_job_id'),
                    'slurm_id': j.get('slurm_id'),
                    'created_at': j.get('created_at'),
                    'completed_at': j.get('completed_at'),
                    'has_output': bool(j.get('output') or j.get('output_gridfs_id')),
                    'has_error': bool(j.get('error') or j.get('error_gridfs_id')),
                    'function_hash': j.get('function_hash'),
                    'file_inputs': j.get('file_inputs'),
                }

                # Pre-load output preview
                output_content = j.get('output')
                if j.get('output_gridfs_id') and not output_content:
                    try:
                        grid_id = j['output_gridfs_id']
                        if isinstance(grid_id, str):
                            grid_id = ObjectId(grid_id)
                        grid_out = output_fs.get(grid_id)
                        output_content = grid_out.read(OUTPUT_PREVIEW_SIZE * 2).decode('utf-8', errors='replace')
                        grid_out.close()
                        job['output_storage'] = 'gridfs'
                    except Exception as e:
                        logger.debug(f"Could not load GridFS output: {e}")

                if output_content:
                    job['output'] = output_content[:OUTPUT_PREVIEW_SIZE]
                    job['output_truncated'] = len(output_content) > OUTPUT_PREVIEW_SIZE

                # Pre-load error preview
                error_content = j.get('error')
                if j.get('error_gridfs_id') and not error_content:
                    try:
                        grid_id = j['error_gridfs_id']
                        if isinstance(grid_id, str):
                            grid_id = ObjectId(grid_id)
                        grid_out = error_fs.get(grid_id)
                        error_content = grid_out.read(OUTPUT_PREVIEW_SIZE * 2).decode('utf-8', errors='replace')
                        grid_out.close()
                    except Exception as e:
                        logger.debug(f"Could not load GridFS error: {e}")

                if error_content:
                    job['error'] = error_content[:OUTPUT_PREVIEW_SIZE]
                    job['error_truncated'] = len(error_content) > OUTPUT_PREVIEW_SIZE

                # Method info
                if j.get('function_hash') and j['function_hash'] in methods_lookup:
                    job['method_name'] = methods_lookup[j['function_hash']]['name']
                    job['method_version'] = methods_lookup[j['function_hash']]['version']

                # Duration
                if j.get('created_at') and j.get('completed_at'):
                    job['duration_seconds'] = (j['completed_at'] - j['created_at']).total_seconds()

                # File info (simplified)
                if j.get('file_inputs'):
                    job['file_info'] = {}
                    for name, hash_val in j['file_inputs'].items():
                        if isinstance(hash_val, list):
                            job['file_info'][name] = [files_lookup.get(h, {}).get('filename', 'unknown') for h in hash_val]
                        elif hash_val in files_lookup:
                            job['file_info'][name] = files_lookup[hash_val].get('filename', 'unknown')

                jobs_data['items'].append(job)

            logger.debug(f"Collected {len(jobs_data['items'])} jobs with previews for snapshot")
        except Exception as e:
            logger.error(f"Error collecting jobs list: {e}")
        return jobs_data

    def collect_meta_jobs_list(self, limit=50):
        """Collect pre-enriched meta-jobs list for admin panel."""
        meta_jobs_data = {'items': [], 'total': 0}
        try:
            meta_jobs_collection = self.jobs_db['meta_jobs']
            methods_collection = self.methods_db['methods']

            meta_jobs_data['total'] = meta_jobs_collection.count_documents({})
            meta_jobs = list(meta_jobs_collection.find().sort('created_at', -1).limit(limit))

            # Batch load methods
            function_hashes = set(m.get('function_hash') for m in meta_jobs if m.get('function_hash'))
            methods_lookup = {}
            if function_hashes:
                for m in methods_collection.find({'function_hash': {'$in': list(function_hashes)}}):
                    methods_lookup[m['function_hash']] = {'name': m.get('name'), 'version': m.get('version')}

            for mj in meta_jobs:
                meta_job = {
                    'id': str(mj['_id']),
                    'meta_job_hash': mj.get('meta_job_hash'),
                    'status': mj.get('status'),
                    'created_at': mj.get('created_at'),
                    'completed_at': mj.get('completed_at'),
                    'job_hashes': mj.get('job_hashes', []),
                    'job_count': len(mj.get('job_hashes', [])),
                }
                if mj.get('function_hash') and mj['function_hash'] in methods_lookup:
                    meta_job['method_name'] = methods_lookup[mj['function_hash']]['name']
                if mj.get('created_at') and mj.get('completed_at'):
                    meta_job['duration_seconds'] = (mj['completed_at'] - mj['created_at']).total_seconds()

                meta_jobs_data['items'].append(meta_job)

            logger.debug(f"Collected {len(meta_jobs_data['items'])} meta-jobs for snapshot")
        except Exception as e:
            logger.error(f"Error collecting meta-jobs list: {e}")
        return meta_jobs_data

    def collect_files_list(self, limit=50):
        """Collect pre-processed files list for admin panel with content previews."""
        import base64
        from gridfs import GridFS

        files_data = {'items': [], 'total': 0}
        try:
            files_collection = self.files_db['files']

            files_data['total'] = files_collection.count_documents({})
            files = list(files_collection.find().sort('upload_date', -1).limit(limit))

            # Initialize GridFS for preview loading
            fs = GridFS(self.files_db, collection='fs')

            # Preview settings - small to keep snapshots fast
            TEXT_PREVIEW_SIZE = 500
            HEX_PREVIEW_SIZE = 64
            BINARY_EXTENSIONS = ['.nii', '.nii.gz', '.gz', '.zip', '.tar', '.png', '.jpg', '.jpeg', '.gif', '.dcm', '.nrrd']

            for f in files:
                filename = f.get('filename', '').lower()
                file_size = f.get('file_size', 0)
                storage_type = f.get('storage_type', 'inline')

                file_item = {
                    'id': str(f['_id']),
                    'file_hash': f.get('file_hash'),
                    'filename': f.get('filename'),
                    'file_size': file_size,
                    'content_type': f.get('content_type'),
                    'upload_date': f.get('upload_date'),
                    'storage_type': storage_type,
                    'status': f.get('status', 'completed'),
                    'gridfs_id': str(f['gridfs_id']) if f.get('gridfs_id') else None,
                }

                # Pre-compute content preview based on file type
                is_binary = any(filename.endswith(ext) for ext in BINARY_EXTENSIONS)
                is_large = file_size > 100000  # > 100KB

                if is_binary:
                    # Binary file: hex preview (first 64 bytes)
                    content_bytes = None
                    try:
                        if storage_type == 'gridfs' and f.get('gridfs_id'):
                            from bson import ObjectId
                            grid_id = f['gridfs_id']
                            if isinstance(grid_id, str):
                                grid_id = ObjectId(grid_id)
                            grid_out = fs.get(grid_id)
                            content_bytes = grid_out.read(HEX_PREVIEW_SIZE)
                            grid_out.close()
                        elif storage_type in ['mongodb', 'inline'] and f.get('content'):
                            content_bytes = base64.b64decode(f.get('content'))[:HEX_PREVIEW_SIZE]
                    except Exception as e:
                        logger.debug(f"Could not load hex preview for {filename}: {e}")

                    if content_bytes:
                        file_item['content_preview'] = ' '.join(f'{b:02x}' for b in content_bytes)
                        file_item['preview_type'] = 'hex'
                        file_item['content_truncated'] = True
                    else:
                        file_item['content_preview'] = '[Binary file - click to view details]'
                        file_item['preview_type'] = 'binary'

                elif is_large:
                    # Large text file: placeholder only
                    file_item['content_preview'] = f'[Large file ({file_size:,} bytes) - click to view]'
                    file_item['preview_type'] = 'large'

                else:
                    # Small text file: text preview
                    content_bytes = None
                    try:
                        if storage_type == 'gridfs' and f.get('gridfs_id'):
                            from bson import ObjectId
                            grid_id = f['gridfs_id']
                            if isinstance(grid_id, str):
                                grid_id = ObjectId(grid_id)
                            grid_out = fs.get(grid_id)
                            content_bytes = grid_out.read(TEXT_PREVIEW_SIZE)
                            grid_out.close()
                        elif storage_type in ['mongodb', 'inline'] and f.get('content'):
                            content_bytes = base64.b64decode(f.get('content'))[:TEXT_PREVIEW_SIZE]
                    except Exception as e:
                        logger.debug(f"Could not load text preview for {filename}: {e}")

                    if content_bytes:
                        try:
                            file_item['content_preview'] = content_bytes.decode('utf-8', errors='replace')
                            file_item['content_truncated'] = file_size > TEXT_PREVIEW_SIZE
                            file_item['preview_type'] = 'text'
                        except:
                            file_item['content_preview'] = '[Binary content]'
                            file_item['preview_type'] = 'binary'

                files_data['items'].append(file_item)

            logger.debug(f"Collected {len(files_data['items'])} files with previews for snapshot")
        except Exception as e:
            logger.error(f"Error collecting files list: {e}")
        return files_data

    def cache_document_previews(self):
        """
        PERFORMANCE OPTIMIZATION: Cache previews directly in MongoDB documents.

        This runs before snapshot collection and updates documents that have GridFS content
        but no cached preview. The preview is stored directly in the document so views
        can display it without reading from GridFS.

        Updates:
        - jobs: output_preview, error_preview (from GridFS)
        - files: content_preview, preview_type (from GridFS)
        """
        from gridfs import GridFS
        from bson import ObjectId

        PREVIEW_SIZE = 500
        HEX_PREVIEW_SIZE = 64
        BINARY_EXTENSIONS = ['.nii', '.nii.gz', '.gz', '.zip', '.tar', '.png', '.jpg', '.jpeg', '.gif', '.dcm', '.nrrd']

        try:
            # ========== JOBS: Cache output/error previews ==========
            jobs_collection = self.jobs_db['jobs']
            output_fs = GridFS(self.jobs_db, collection='outputs')
            error_fs = GridFS(self.jobs_db, collection='errors')

            # Find jobs with GridFS output but no cached preview
            jobs_need_output_cache = list(jobs_collection.find({
                'output_gridfs_id': {'$exists': True, '$ne': None},
                'output_preview': {'$exists': False}
            }).limit(50))  # Process 50 per cycle to avoid overload

            for job in jobs_need_output_cache:
                try:
                    grid_id = job['output_gridfs_id']
                    if isinstance(grid_id, str):
                        grid_id = ObjectId(grid_id)
                    grid_out = output_fs.get(grid_id)
                    content = grid_out.read(PREVIEW_SIZE * 2).decode('utf-8', errors='replace')
                    grid_out.close()

                    preview = content[:PREVIEW_SIZE]
                    truncated = len(content) > PREVIEW_SIZE

                    jobs_collection.update_one(
                        {'_id': job['_id']},
                        {'$set': {
                            'output_preview': preview,
                            'output_preview_truncated': truncated
                        }}
                    )
                except Exception as e:
                    logger.debug(f"Could not cache output preview for job {job.get('job_hash')}: {e}")

            # Find jobs with GridFS error but no cached preview
            jobs_need_error_cache = list(jobs_collection.find({
                'error_gridfs_id': {'$exists': True, '$ne': None},
                'error_preview': {'$exists': False}
            }).limit(50))

            for job in jobs_need_error_cache:
                try:
                    grid_id = job['error_gridfs_id']
                    if isinstance(grid_id, str):
                        grid_id = ObjectId(grid_id)
                    grid_out = error_fs.get(grid_id)
                    content = grid_out.read(PREVIEW_SIZE * 2).decode('utf-8', errors='replace')
                    grid_out.close()

                    preview = content[:PREVIEW_SIZE]
                    truncated = len(content) > PREVIEW_SIZE

                    jobs_collection.update_one(
                        {'_id': job['_id']},
                        {'$set': {
                            'error_preview': preview,
                            'error_preview_truncated': truncated
                        }}
                    )
                except Exception as e:
                    logger.debug(f"Could not cache error preview for job {job.get('job_hash')}: {e}")

            cached_jobs = len(jobs_need_output_cache) + len(jobs_need_error_cache)
            if cached_jobs > 0:
                logger.info(f"Cached previews for {cached_jobs} job documents")

            # ========== FILES: Cache content previews ==========
            files_collection = self.files_db['files']
            files_fs = GridFS(self.files_db, collection='fs')

            # Find files with GridFS content but no cached preview
            files_need_cache = list(files_collection.find({
                'storage_type': 'gridfs',
                'gridfs_id': {'$exists': True, '$ne': None},
                'content_preview': {'$exists': False}
            }).limit(50))

            for f in files_need_cache:
                try:
                    filename = f.get('filename', '').lower()
                    file_size = f.get('file_size', 0)

                    grid_id = f['gridfs_id']
                    if isinstance(grid_id, str):
                        grid_id = ObjectId(grid_id)

                    is_binary = any(filename.endswith(ext) for ext in BINARY_EXTENSIONS)

                    if is_binary:
                        # Binary: hex preview
                        grid_out = files_fs.get(grid_id)
                        content_bytes = grid_out.read(HEX_PREVIEW_SIZE)
                        grid_out.close()

                        preview = ' '.join(f'{b:02x}' for b in content_bytes)
                        files_collection.update_one(
                            {'_id': f['_id']},
                            {'$set': {
                                'content_preview': preview,
                                'preview_type': 'hex',
                                'content_preview_truncated': True
                            }}
                        )
                    else:
                        # Text: text preview
                        grid_out = files_fs.get(grid_id)
                        content_bytes = grid_out.read(PREVIEW_SIZE)
                        grid_out.close()

                        try:
                            preview = content_bytes.decode('utf-8', errors='replace')
                            files_collection.update_one(
                                {'_id': f['_id']},
                                {'$set': {
                                    'content_preview': preview,
                                    'preview_type': 'text',
                                    'content_preview_truncated': file_size > PREVIEW_SIZE
                                }}
                            )
                        except:
                            files_collection.update_one(
                                {'_id': f['_id']},
                                {'$set': {
                                    'content_preview': '[Binary content]',
                                    'preview_type': 'binary',
                                    'content_preview_truncated': True
                                }}
                            )
                except Exception as e:
                    logger.debug(f"Could not cache content preview for file {f.get('filename')}: {e}")

            if len(files_need_cache) > 0:
                logger.info(f"Cached previews for {len(files_need_cache)} file documents")

        except Exception as e:
            logger.error(f"Error caching document previews: {e}")

    def store_snapshot(self):
        """Collect all data and store a snapshot in MongoDB."""
        try:
            # First, cache previews directly in documents for fast pagination
            self.cache_document_previews()
            snapshot = {
                'timestamp': datetime.utcnow(),
                'containers': self.collect_container_status(),
                'system_resources': self.collect_system_resources(),
                'job_logs': self.collect_slurm_job_logs(),
                'environment': self.collect_environment_info(),
                'slurm_queue': self.collect_slurm_queue(),
                'method_sources': self.collect_method_sources(),
                'pipelines': self.collect_pipelines(),
                # Pre-enriched lists for fast admin panel loading
                'jobs_list': self.collect_jobs_list(limit=50),
                'meta_jobs_list': self.collect_meta_jobs_list(limit=50),
                'files_list': self.collect_files_list(limit=50),
            }
            
            # Store snapshot
            self.snapshots_collection.insert_one(snapshot)
            
            # Clean old snapshots (keep last 100)
            count = self.snapshots_collection.count_documents({})
            if count > 100:
                # Delete oldest
                old_snapshots = list(self.snapshots_collection.find().sort('timestamp', 1).limit(count - 100))
                for old in old_snapshots:
                    self.snapshots_collection.delete_one({'_id': old['_id']})
            
            logger.info(f"Snapshot stored successfully at {snapshot['timestamp']}")
            return True
            
        except Exception as e:
            logger.error(f"Error storing snapshot: {e}")
            return False
    
    async def run(self, interval=10):
        """Run the monitoring loop."""
        logger.info(f"Starting monitor worker with {interval}s interval")
        
        while True:
            try:
                self.store_snapshot()
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
            
            await asyncio.sleep(interval)


async def main():
    """Main entry point for the worker."""
    worker = MonitorWorker()
    worker.connect()
    await worker.run(interval=10)  # Run every 10 seconds


if __name__ == '__main__':
    asyncio.run(main())

