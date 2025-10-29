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
        # MongoDB connection for snapshots
        self.mongo_uri = os.environ.get('MONGO_JOBS_URI', 'mongodb://localhost:27017/')
        self.mongo_db_name = os.environ.get('MONGO_JOBS_DB', 'dshpc-jobs')
        self.client = None
        self.db = None
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
        """Connect to MongoDB and Docker."""
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.mongo_db_name]
            self.snapshots_collection = self.db['system_snapshots']
            logger.info(f"Connected to MongoDB: {self.mongo_db_name}")
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
                job_id = job.get('job_id')
                
                if not slurm_id:
                    continue
                
                log_entry = {
                    'slurm_id': slurm_id,
                    'job_id': job_id,
                    'timestamp': datetime.utcnow()
                }
                
                # Try to read slurm output (last 500 lines)
                try:
                    result = container.exec_run(['bash', '-c', f'tail -n 500 /app/slurm-{slurm_id}.out 2>/dev/null || echo ""'])
                    if result.exit_code == 0:
                        output = result.output.decode('utf-8', errors='replace').strip()
                        if output:
                            log_entry['slurm_output'] = output
                            log_entry['slurm_output_lines'] = len(output.split('\n'))
                except Exception as e:
                    logger.debug(f"Could not read slurm output for {slurm_id}: {e}")
                
                # Try to read system output (last 500 lines)
                try:
                    result = container.exec_run(['bash', '-c', f'tail -n 500 /tmp/output_{job_id}.txt 2>/dev/null || echo ""'])
                    if result.exit_code == 0:
                        output = result.output.decode('utf-8', errors='replace').strip()
                        if output:
                            log_entry['system_output'] = output
                            log_entry['system_output_lines'] = len(output.split('\n'))
                except Exception as e:
                    logger.debug(f"Could not read system output for {job_id}: {e}")
                
                # Try to read system error (last 500 lines)
                try:
                    result = container.exec_run(['bash', '-c', f'tail -n 500 /tmp/error_{job_id}.txt 2>/dev/null || echo ""'])
                    if result.exit_code == 0:
                        output = result.output.decode('utf-8', errors='replace').strip()
                        if output:
                            log_entry['system_error'] = output
                            log_entry['system_error_lines'] = len(output.split('\n'))
                except Exception as e:
                    logger.debug(f"Could not read system error for {job_id}: {e}")
                
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
            except Exception as e:
                env_data['slurm']['error'] = str(e)
                
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
                            slurm_job['db_job_id'] = str(job_doc.get('job_id', ''))
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
    
    def store_snapshot(self):
        """Collect all data and store a snapshot in MongoDB."""
        try:
            snapshot = {
                'timestamp': datetime.utcnow(),
                'containers': self.collect_container_status(),
                'system_resources': self.collect_system_resources(),
                'job_logs': self.collect_slurm_job_logs(),
                'environment': self.collect_environment_info(),
                'slurm_queue': self.collect_slurm_queue()
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

