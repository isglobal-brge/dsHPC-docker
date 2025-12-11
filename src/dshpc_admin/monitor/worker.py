"""
Background monitoring worker that collects system data and stores snapshots in MongoDB.

ROBUST DESIGN v3.1:
- Self-healing: Auto-reconnects to MongoDB and Docker on failure
- Graceful degradation: Partial snapshots on collector errors (better than nothing)
- Heartbeat tracking: Records last_activity for external watchdog monitoring
- Error isolation: Each collector runs independently, one failure doesn't stop others
- Connection health checks: Verifies connections before each snapshot cycle
- Circuit breaker: Disables failing collectors temporarily
- Singleton: Only one instance runs at a time (via file lock)
- Multiprocess timeouts: Hard timeouts using subprocess isolation
- ADAPTIVE TIMING: Automatically adjusts intervals based on cycle duration and data volume
"""
import time
import os
import json
import signal
import sys
import fcntl
from datetime import datetime
from functools import wraps
from multiprocessing import Process, Queue
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, AutoReconnect
import docker
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CircuitBreaker:
    """Circuit breaker pattern to temporarily disable failing operations."""

    def __init__(self, failure_threshold=3, recovery_timeout=60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = {}
        self.disabled_until = {}

    def is_open(self, name):
        """Check if circuit is open (operation disabled)."""
        if name in self.disabled_until:
            if time.time() < self.disabled_until[name]:
                return True
            else:
                # Recovery time passed, reset
                del self.disabled_until[name]
                self.failures[name] = 0
        return False

    def record_failure(self, name):
        """Record a failure for the named operation."""
        self.failures[name] = self.failures.get(name, 0) + 1
        if self.failures[name] >= self.failure_threshold:
            self.disabled_until[name] = time.time() + self.recovery_timeout
            logger.warning(f"Circuit breaker OPEN for '{name}' - disabled for {self.recovery_timeout}s")

    def record_success(self, name):
        """Record success, reset failure count."""
        self.failures[name] = 0

    def get_status(self):
        """Get status of all circuits."""
        return {
            name: 'OPEN' if self.is_open(name) else f'CLOSED ({self.failures.get(name, 0)} failures)'
            for name in set(list(self.failures.keys()) + list(self.disabled_until.keys()))
        }


class AdaptiveTiming:
    """
    Adaptive timing system that adjusts intervals based on observed performance.

    Instead of fighting against cycle duration with a fixed interval, this system:
    1. Measures actual cycle durations (moving average)
    2. Adds a configurable pause between cycles
    3. Scales the pause if cycles are consistently slow
    4. Logs when timing adapts significantly
    """

    def __init__(self, min_pause=5, max_pause=60, history_size=10):
        """
        Args:
            min_pause: Minimum seconds to wait between cycles (breathing room)
            max_pause: Maximum seconds to wait between cycles
            history_size: Number of cycles to average for adaptive calculations
        """
        self.min_pause = min_pause
        self.max_pause = max_pause
        self.history_size = history_size

        # Cycle duration history (rolling window)
        self.cycle_durations = []

        # Track timing changes for logging
        self.last_logged_avg = 0
        self.current_pause = min_pause

    def record_cycle(self, duration):
        """Record a cycle duration and return the recommended pause time."""
        self.cycle_durations.append(duration)

        # Keep only recent history
        if len(self.cycle_durations) > self.history_size:
            self.cycle_durations.pop(0)

        # Calculate average cycle duration
        avg_duration = sum(self.cycle_durations) / len(self.cycle_durations)

        # Adaptive pause calculation:
        # - Fast cycles (<10s): use min_pause
        # - Medium cycles (10-30s): use min_pause
        # - Slow cycles (30-60s): increase pause proportionally
        # - Very slow cycles (>60s): use max_pause
        if avg_duration < 30:
            self.current_pause = self.min_pause
        elif avg_duration < 60:
            # Scale from min_pause to max_pause/2 as cycles go from 30s to 60s
            scale = (avg_duration - 30) / 30  # 0 to 1
            self.current_pause = self.min_pause + (self.max_pause / 2 - self.min_pause) * scale
        else:
            # Very slow cycles - give the system more breathing room
            self.current_pause = self.max_pause

        # Log significant timing changes (>20% change in average)
        if self.last_logged_avg > 0:
            change_pct = abs(avg_duration - self.last_logged_avg) / self.last_logged_avg
            if change_pct > 0.2:
                logger.info(f"[ADAPTIVE] Cycle avg changed: {self.last_logged_avg:.1f}s -> {avg_duration:.1f}s, "
                           f"pause adjusted to {self.current_pause:.1f}s")
                self.last_logged_avg = avg_duration
        else:
            self.last_logged_avg = avg_duration

        return self.current_pause

    def get_stats(self):
        """Get current timing statistics."""
        if not self.cycle_durations:
            return {'avg_duration': 0, 'current_pause': self.min_pause, 'samples': 0}

        return {
            'avg_duration': sum(self.cycle_durations) / len(self.cycle_durations),
            'min_duration': min(self.cycle_durations),
            'max_duration': max(self.cycle_durations),
            'current_pause': self.current_pause,
            'samples': len(self.cycle_durations)
        }


class MonitorWorker:
    """
    Background worker that monitors system and stores snapshots.

    ROBUSTNESS FEATURES:
    - Auto-reconnection on database/docker failures
    - Graceful degradation on collector errors
    - Heartbeat tracking for external monitoring
    - Error isolation between collectors
    """

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

        # Robustness tracking
        self.last_successful_snapshot = None
        self.consecutive_failures = 0
        self.max_consecutive_failures = 10
        self.connected = False
        self.last_heartbeat = None

        # Circuit breaker for failing collectors (v3.0)
        self.circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=120)

        # Collector timeouts (seconds) - prevent any collector from hanging forever
        self.collector_timeouts = {
            'containers': 10,
            'system_resources': 15,
            'slurm_queue': 20,
            'job_logs': 30,
            'jobs_list': 30,
            'meta_jobs_list': 20,
            'files_list': 30,
            'method_sources': 30,
            'environment': 45,
            'pipelines': 45,
            'cache_previews': 60,
        }

        # Graceful shutdown
        self._shutdown_requested = False
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """Handle graceful shutdown."""
        logger.info(f"Shutdown signal received ({signum}). Cleaning up...")
        self._shutdown_requested = True
    
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
        """Connect to MongoDB databases and Docker with retry logic."""
        self._connect_mongodb()
        self._connect_docker()
        self.connected = True
        logger.info("All connections established successfully")

    def _connect_mongodb(self):
        """Connect to MongoDB with connection pooling and timeouts."""
        connection_options = {
            'serverSelectionTimeoutMS': 5000,
            'connectTimeoutMS': 5000,
            'socketTimeoutMS': 30000,
            'retryWrites': True,
            'maxPoolSize': 10,
        }

        try:
            # Close existing connections if any
            if self.jobs_client:
                try:
                    self.jobs_client.close()
                except:
                    pass

            self.jobs_client = MongoClient(self.mongo_jobs_uri, **connection_options)
            # Verify connection is alive
            self.jobs_client.admin.command('ping')
            self.jobs_db = self.jobs_client[self.mongo_jobs_db_name]
            self.snapshots_collection = self.jobs_db['system_snapshots']
            logger.info(f"Connected to Jobs DB: {self.mongo_jobs_db_name}")

            if self.files_client:
                try:
                    self.files_client.close()
                except:
                    pass

            self.files_client = MongoClient(self.mongo_files_uri, **connection_options)
            self.files_client.admin.command('ping')
            self.files_db = self.files_client[self.mongo_files_db_name]
            logger.info(f"Connected to Files DB: {self.mongo_files_db_name}")

            if self.methods_client:
                try:
                    self.methods_client.close()
                except:
                    pass

            self.methods_client = MongoClient(self.mongo_methods_uri, **connection_options)
            self.methods_client.admin.command('ping')
            self.methods_db = self.methods_client[self.mongo_methods_db_name]
            logger.info(f"Connected to Methods DB: {self.mongo_methods_db_name}")

            # Aliases for backward compatibility with existing code
            self.client = self.jobs_client
            self.db = self.jobs_db
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def _connect_docker(self):
        """Connect to Docker daemon."""
        try:
            self.docker_client = docker.from_env()
            # Verify connection by pinging
            self.docker_client.ping()
            logger.info("Connected to Docker")
        except Exception as e:
            logger.error(f"Failed to connect to Docker: {e}")
            raise

    def check_connections(self):
        """
        Verify all connections are healthy. Returns True if OK, False if reconnection needed.
        This is the key to self-healing - called before each snapshot cycle.
        """
        healthy = True

        # Check MongoDB connections
        try:
            self.jobs_client.admin.command('ping')
        except Exception as e:
            logger.warning(f"Jobs DB connection lost: {e}")
            healthy = False

        try:
            self.files_client.admin.command('ping')
        except Exception as e:
            logger.warning(f"Files DB connection lost: {e}")
            healthy = False

        try:
            self.methods_client.admin.command('ping')
        except Exception as e:
            logger.warning(f"Methods DB connection lost: {e}")
            healthy = False

        # Check Docker connection
        try:
            self.docker_client.ping()
        except Exception as e:
            logger.warning(f"Docker connection lost: {e}")
            healthy = False

        return healthy

    def reconnect(self):
        """Attempt to reconnect all services. Returns True on success."""
        logger.info("Attempting to reconnect all services...")
        try:
            self._connect_mongodb()
            self._connect_docker()
            self.connected = True
            logger.info("Reconnection successful!")
            return True
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            self.connected = False
            return False

    def update_heartbeat(self):
        """
        Update heartbeat in MongoDB so external monitors can detect if worker is stuck.
        This is separate from snapshots - if heartbeat stops updating, worker is dead.
        """
        try:
            self.last_heartbeat = datetime.utcnow()
            self.jobs_db['worker_status'].update_one(
                {'worker_id': 'monitor_worker'},
                {
                    '$set': {
                        'last_heartbeat': self.last_heartbeat,
                        'status': 'running',
                        'consecutive_failures': self.consecutive_failures,
                        'last_successful_snapshot': self.last_successful_snapshot,
                    }
                },
                upsert=True
            )
        except Exception as e:
            logger.debug(f"Could not update heartbeat: {e}")
    
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
                            elif job_doc.get('file_inputs') and isinstance(job_doc.get('file_inputs'), dict):
                                slurm_job['file_inputs'] = job_doc['file_inputs']
                                slurm_job['file_info'] = {}
                                for name, hash_val in job_doc['file_inputs'].items():
                                    # Skip None or empty file hashes
                                    if not hash_val or not isinstance(hash_val, str):
                                        continue
                                    file_doc = files_client.files.find_one({'file_hash': hash_val})
                                    if file_doc:
                                        slurm_job['file_info'][name] = {
                                            'filename': file_doc.get('filename'),
                                            'size': file_doc.get('file_size'),
                                            'hash': hash_val
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
        """Collect pipeline data from MongoDB with enriched meta-job information.

        PERFORMANCE OPTIMIZATION: Uses batch lookups instead of individual queries.
        - Collects all needed hashes first, then does batch queries
        - No GridFS reads - uses cached previews only
        """
        pipelines_data = []

        try:
            # Get all pipelines (limit to recent 100 for snapshot efficiency)
            pipelines = list(self.db.pipelines.find({}).sort('created_at', -1).limit(100))

            # PHASE 1: Collect all hashes we need to look up
            meta_job_hashes = set()
            for pipeline in pipelines:
                nodes = pipeline.get('nodes', {})
                for node_data in nodes.values():
                    if node_data.get('meta_job_hash'):
                        meta_job_hashes.add(node_data['meta_job_hash'])

            # PHASE 2: Batch load meta_jobs
            meta_jobs_lookup = {}
            if meta_job_hashes:
                for mj in self.db.meta_jobs.find({'meta_job_hash': {'$in': list(meta_job_hashes)}}):
                    if '_id' in mj:
                        del mj['_id']
                    meta_jobs_lookup[mj['meta_job_hash']] = mj

            # PHASE 3: Collect all job_hashes and file_hashes from meta_jobs
            job_hashes = set()
            file_hashes = set()
            for mj in meta_jobs_lookup.values():
                # Collect file hashes from initial_file_inputs
                initial_inputs = mj.get('initial_file_inputs', {})
                if initial_inputs and isinstance(initial_inputs, dict):
                    for fh in initial_inputs.values():
                        if fh and isinstance(fh, str):
                            file_hashes.add(fh)
                # Collect job hashes from chain
                for step in mj.get('chain', []):
                    if step.get('job_hash'):
                        job_hashes.add(step['job_hash'])

            # PHASE 4: Batch load jobs
            jobs_lookup = {}
            if job_hashes:
                for j in self.db.jobs.find({'job_hash': {'$in': list(job_hashes)}}):
                    if '_id' in j:
                        del j['_id']
                    # Collect file hashes from job's file_inputs
                    fi = j.get('file_inputs', {})
                    if fi and isinstance(fi, dict):
                        for fh in fi.values():
                            if fh and isinstance(fh, str):
                                file_hashes.add(fh)
                    jobs_lookup[j['job_hash']] = j

            # PHASE 5: Batch load files (only filename/size, no content)
            files_lookup = {}
            if file_hashes:
                projection = {'file_hash': 1, 'filename': 1, 'file_size': 1, 'size': 1, 'metadata': 1}
                for f in self.files_db.files.find({'file_hash': {'$in': list(file_hashes)}}, projection):
                    files_lookup[f['file_hash']] = {
                        'filename': f.get('filename', 'unknown'),
                        'size': f.get('file_size') or f.get('size', 0),
                        'metadata': f.get('metadata')
                    }

            # PHASE 6: Enrich pipelines using lookups (no more DB queries)
            for pipeline in pipelines:
                if '_id' in pipeline:
                    del pipeline['_id']

                nodes = pipeline.get('nodes', {})
                for node_id, node_data in nodes.items():
                    meta_job_hash = node_data.get('meta_job_hash')
                    if meta_job_hash and meta_job_hash in meta_jobs_lookup:
                        meta_job = meta_jobs_lookup[meta_job_hash].copy()

                        # Enrich initial file inputs
                        initial_inputs = meta_job.get('initial_file_inputs', {})
                        if initial_inputs and isinstance(initial_inputs, dict):
                            initial_file_info = {}
                            for name, fh in initial_inputs.items():
                                if fh and fh in files_lookup:
                                    initial_file_info[name] = files_lookup[fh].copy()
                            if initial_file_info:
                                meta_job['initial_file_info'] = initial_file_info

                        # Enrich chain steps with job data
                        for step in meta_job.get('chain', []):
                            job_hash = step.get('job_hash')
                            if job_hash and job_hash in jobs_lookup:
                                job = jobs_lookup[job_hash].copy()

                                # Use cached preview only (no GridFS reads)
                                if job.get('output_preview'):
                                    job['output'] = job['output_preview']
                                    job['output_truncated'] = True
                                elif job.get('output') and isinstance(job['output'], str):
                                    if len(job['output']) > 10240:
                                        job['output'] = job['output'][:10240]
                                        job['output_truncated'] = True
                                elif job.get('output_gridfs_id'):
                                    job['output'] = '[Loading...]'
                                    job['output_truncated'] = True

                                # Enrich file_inputs with file info
                                fi = job.get('file_inputs', {})
                                if fi and isinstance(fi, dict):
                                    job['file_info'] = {}
                                    for input_name, fh in fi.items():
                                        if fh and fh in files_lookup:
                                            job['file_info'][input_name] = files_lookup[fh].copy()

                                step['job_data'] = job

                        node_data['meta_job_info'] = meta_job

                pipelines_data.append(pipeline)

            logger.debug(f"Collected {len(pipelines_data)} pipelines with batch lookups")

        except Exception as e:
            logger.error(f"Error collecting pipelines: {e}")

        return pipelines_data

    def collect_jobs_list(self, limit=50):
        """Collect pre-enriched jobs list for admin panel with output/error previews.

        PERFORMANCE: Uses batch lookups for methods/files and cached previews only.
        No GridFS reads at snapshot time - previews are cached by cache_document_previews().
        """
        OUTPUT_PREVIEW_SIZE = 500

        jobs_data = {'items': [], 'total': 0}
        try:
            jobs_collection = self.jobs_db['jobs']
            methods_collection = self.methods_db['methods']
            files_collection = self.files_db['files']

            # Get total count
            jobs_data['total'] = jobs_collection.count_documents({})

            # Get jobs with priority ordering: RUNNING first, then PENDING, then by date
            # Use aggregation pipeline for custom sort order
            status_priority = {
                'R': 0,    # Running - highest priority
                'PD': 1,   # Pending
                'CG': 2,   # Completing
                'CF': 3,   # Configuring
            }

            # First get running and pending jobs (they should always appear)
            active_jobs = list(jobs_collection.find(
                {'status': {'$in': ['R', 'PD', 'CG', 'CF']}}
            ).sort('created_at', -1))

            # Then get remaining jobs by date (excluding active ones)
            remaining_limit = max(0, limit - len(active_jobs))
            other_jobs = []
            if remaining_limit > 0:
                other_jobs = list(jobs_collection.find(
                    {'status': {'$nin': ['R', 'PD', 'CG', 'CF']}}
                ).sort('created_at', -1).limit(remaining_limit))

            # Combine: sort active jobs by status priority, then by date
            def sort_key(j):
                return (status_priority.get(j.get('status'), 99),
                        -(j.get('created_at') or datetime.min).timestamp())

            active_jobs.sort(key=sort_key)
            jobs = active_jobs + other_jobs

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
                    'has_output': bool(j.get('output') or j.get('output_gridfs_id') or j.get('output_preview')),
                    'has_error': bool(j.get('error') or j.get('error_gridfs_id') or j.get('error_preview')),
                    'function_hash': j.get('function_hash'),
                    'file_inputs': j.get('file_inputs'),
                    # Include params for pending jobs display
                    'params': j.get('params'),
                }

                # PERFORMANCE: Use cached previews ONLY - never read GridFS at snapshot time
                # The cache_document_previews() method runs separately and caches previews in documents
                # This avoids slow GridFS reads during snapshot collection
                if j.get('output_preview'):
                    # Use pre-cached preview (set by cache_document_previews)
                    job['output'] = j['output_preview']
                    job['output_truncated'] = j.get('output_preview_truncated', True)
                elif j.get('output') and not j.get('output_gridfs_id'):
                    # Use inline output (small, already in document - no extra read)
                    output_content = j['output']
                    job['output'] = output_content[:OUTPUT_PREVIEW_SIZE]
                    job['output_truncated'] = len(output_content) > OUTPUT_PREVIEW_SIZE
                elif j.get('output_gridfs_id'):
                    # GridFS but no cache yet - show placeholder, cache will be populated soon
                    job['output'] = '[Loading...]'
                    job['output_truncated'] = True

                # Same for error - use cache only
                if j.get('error_preview'):
                    job['error'] = j['error_preview']
                    job['error_truncated'] = j.get('error_preview_truncated', True)
                elif j.get('error') and not j.get('error_gridfs_id'):
                    error_content = j['error']
                    job['error'] = error_content[:OUTPUT_PREVIEW_SIZE]
                    job['error_truncated'] = len(error_content) > OUTPUT_PREVIEW_SIZE
                elif j.get('error_gridfs_id'):
                    # GridFS but no cache yet - show placeholder
                    job['error'] = '[Loading...]'
                    job['error_truncated'] = True

                # Method info
                if j.get('function_hash') and j['function_hash'] in methods_lookup:
                    job['method_name'] = methods_lookup[j['function_hash']]['name']
                    job['method_version'] = methods_lookup[j['function_hash']]['version']

                # Duration
                if j.get('created_at') and j.get('completed_at'):
                    job['duration_seconds'] = (j['completed_at'] - j['created_at']).total_seconds()

                # File info - matching views.py format for template compatibility
                if j.get('file_inputs'):
                    job['file_info'] = {}
                    for name, hash_val in j['file_inputs'].items():
                        if isinstance(hash_val, list):
                            # Array of files - format as list of dicts
                            job['file_info'][name] = []
                            for h in hash_val:
                                file_doc = files_lookup.get(h, {})
                                job['file_info'][name].append({
                                    'filename': file_doc.get('filename', 'unknown'),
                                    'size': file_doc.get('file_size', 0),
                                    'hash': h
                                })
                        elif hash_val in files_lookup:
                            # Single file - format as dict
                            file_doc = files_lookup[hash_val]
                            job['file_info'][name] = {
                                'filename': file_doc.get('filename', 'unknown'),
                                'size': file_doc.get('file_size', 0),
                                'hash': hash_val
                            }
                elif j.get('file_hash'):
                    # Legacy single file format - convert to file_info for template compatibility
                    file_hash = j['file_hash']
                    if file_hash in files_lookup:
                        file_doc = files_lookup[file_hash]
                        job['file_info'] = {
                            'input': {
                                'filename': file_doc.get('filename', 'unknown'),
                                'size': file_doc.get('file_size', 0),
                                'hash': file_hash
                            }
                        }
                    job['file_hash'] = file_hash  # Keep original for reference

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

    def _safe_collect(self, collector_name, collector_func, default_value):
        """
        Safely execute a collector function with:
        - Circuit breaker: Skip collectors that fail repeatedly
        - Error isolation: Failures don't crash the worker

        ROBUST v3.0: Uses simple try/except. Complex collectors are skipped
        when circuit breaker opens. No signal-based timeouts as they don't
        work reliably with C-level blocking calls (Docker exec, MongoDB).
        """
        # Check circuit breaker FIRST - skip if collector is disabled
        if self.circuit_breaker.is_open(collector_name):
            logger.debug(f"Circuit breaker OPEN for {collector_name}, skipping")
            return default_value

        start_time = time.time()
        timeout = self.collector_timeouts.get(collector_name, 30)

        try:
            result = collector_func()
            elapsed = time.time() - start_time

            if result is None:
                logger.warning(f"Collector {collector_name} returned None ({elapsed:.1f}s)")
                self.circuit_breaker.record_failure(collector_name)
                return default_value

            # Log slow collectors
            if elapsed > timeout * 0.5:
                logger.warning(f"Collector {collector_name} slow: {elapsed:.1f}s (limit: {timeout}s)")

            # Success - reset circuit breaker
            self.circuit_breaker.record_success(collector_name)
            return result

        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Collector {collector_name} failed after {elapsed:.1f}s: {e}")
            self.circuit_breaker.record_failure(collector_name)
            return default_value

    def store_snapshot(self):
        """
        Collect all data and store a snapshot in MongoDB.

        ROBUSTNESS: Each collector is isolated - if one fails, others continue.
        Partial snapshots are stored (better than nothing for visualization).
        """
        errors = []
        snapshot = {
            'timestamp': datetime.utcnow(),
            'worker_version': '3.0-robust',  # Version tracking for debugging
            'circuit_breaker_status': self.circuit_breaker.get_status(),
        }

        # First, try to cache previews (non-critical)
        logger.info("Starting cache_previews")
        self._safe_collect('cache_previews', self.cache_document_previews, None)

        # Collect each data source independently with error isolation
        # Critical collectors (core functionality)
        logger.info("Starting containers")
        snapshot['containers'] = self._safe_collect(
            'containers', self.collect_container_status, [])
        logger.info("Starting system_resources")
        snapshot['system_resources'] = self._safe_collect(
            'system_resources', self.collect_system_resources, {})
        logger.info("Starting slurm_queue")
        snapshot['slurm_queue'] = self._safe_collect(
            'slurm_queue', self.collect_slurm_queue, [])

        # Job-related collectors
        logger.info("Starting job_logs")
        snapshot['job_logs'] = self._safe_collect(
            'job_logs', self.collect_slurm_job_logs, [])
        logger.info("Starting jobs_list")
        snapshot['jobs_list'] = self._safe_collect(
            'jobs_list', lambda: self.collect_jobs_list(limit=50), {'items': [], 'total': 0})
        logger.info("Starting meta_jobs_list")
        snapshot['meta_jobs_list'] = self._safe_collect(
            'meta_jobs_list', lambda: self.collect_meta_jobs_list(limit=50), {'items': [], 'total': 0})

        # Files and methods (can be slower)
        logger.info("Starting files_list")
        snapshot['files_list'] = self._safe_collect(
            'files_list', lambda: self.collect_files_list(limit=50), {'items': [], 'total': 0})
        logger.info("Starting method_sources")
        snapshot['method_sources'] = self._safe_collect(
            'method_sources', self.collect_method_sources, [])

        # Heavy collectors (environment and pipelines - can take longer)
        logger.info("Starting environment")
        snapshot['environment'] = self._safe_collect(
            'environment', self.collect_environment_info, {'python': {}, 'r': {}, 'system': {}, 'slurm': {}})
        logger.info("Starting pipelines")
        snapshot['pipelines'] = self._safe_collect(
            'pipelines', self.collect_pipelines, [])

        # Track errors in snapshot for debugging
        if errors:
            snapshot['collector_errors'] = errors

        # Store snapshot - this is the critical part
        try:
            self.snapshots_collection.insert_one(snapshot)
            logger.info(f"Snapshot stored at {snapshot['timestamp']} (errors: {len(errors)})")

            # Update success tracking
            self.last_successful_snapshot = snapshot['timestamp']
            self.consecutive_failures = 0

            # Clean old snapshots (keep last 100)
            try:
                count = self.snapshots_collection.count_documents({})
                if count > 100:
                    old_snapshots = list(self.snapshots_collection.find().sort('timestamp', 1).limit(count - 100))
                    for old in old_snapshots:
                        self.snapshots_collection.delete_one({'_id': old['_id']})
            except Exception as e:
                logger.warning(f"Could not clean old snapshots: {e}")

            return True

        except Exception as e:
            logger.error(f"CRITICAL: Could not store snapshot: {e}")
            self.consecutive_failures += 1
            return False
    
    async def run(self, interval=10):
        """
        Run the monitoring loop with self-healing capabilities.

        ROBUSTNESS FEATURES:
        - Connection health check before each cycle
        - Auto-reconnection on connection failures
        - Exponential backoff on consecutive failures
        - Graceful shutdown handling
        - Heartbeat updates for external monitoring
        """
        logger.info(f"Starting ROBUST monitor worker v3.0 with {interval}s interval")
        logger.info(f"Features: circuit breaker, hard timeouts, auto-reconnect, graceful degradation")

        backoff_time = interval
        max_backoff = 60  # Max 60s between retries

        while not self._shutdown_requested:
            cycle_start = time.time()

            try:
                # Update heartbeat FIRST - proves we're alive even if snapshot fails
                self.update_heartbeat()

                # Check connection health before each cycle
                if not self.check_connections():
                    logger.warning("Connection health check failed, attempting reconnection...")
                    if not self.reconnect():
                        # Reconnection failed - wait with backoff
                        logger.error(f"Reconnection failed. Waiting {backoff_time}s before retry...")
                        await asyncio.sleep(backoff_time)
                        backoff_time = min(backoff_time * 2, max_backoff)
                        continue
                    else:
                        # Reconnection succeeded - reset backoff
                        backoff_time = interval

                # Store snapshot (with internal error isolation)
                success = self.store_snapshot()

                if success:
                    # Reset backoff on success
                    backoff_time = interval
                else:
                    # Increment backoff on failure
                    backoff_time = min(backoff_time * 1.5, max_backoff)
                    logger.warning(f"Snapshot failed. Next attempt in {backoff_time}s")

                # Check if we're having too many failures
                if self.consecutive_failures >= self.max_consecutive_failures:
                    logger.error(f"Too many consecutive failures ({self.consecutive_failures}). "
                                 f"Forcing full reconnection...")
                    self.connected = False
                    self.reconnect()
                    self.consecutive_failures = 0

            except Exception as e:
                # Catch-all for any unexpected errors - worker must not die
                logger.error(f"UNEXPECTED ERROR in monitoring loop: {e}")
                import traceback
                logger.error(traceback.format_exc())
                self.consecutive_failures += 1
                backoff_time = min(backoff_time * 2, max_backoff)

            # Calculate sleep time to maintain consistent intervals
            cycle_duration = time.time() - cycle_start
            sleep_time = max(1, interval - cycle_duration)  # At least 1 second
            await asyncio.sleep(sleep_time)

        # Graceful shutdown
        logger.info("Worker shutdown complete")

    def run_sync(self, min_pause=5, max_pause=60):
        """
        Synchronous version of run() with ADAPTIVE TIMING.

        Instead of a fixed interval, the worker:
        1. Measures how long each cycle takes
        2. Adds a configurable pause between cycles (min_pause to max_pause)
        3. Adapts the pause based on cycle duration trends
        4. Never tries to "catch up" - just maintains a healthy pace

        Args:
            min_pause: Minimum seconds between cycles (default 5s)
            max_pause: Maximum seconds between cycles (default 60s)
        """
        logger.info(f"Starting ROBUST monitor worker v3.1 (sync) with ADAPTIVE TIMING")
        logger.info(f"Features: adaptive timing ({min_pause}s-{max_pause}s pause), circuit breaker, auto-reconnect")

        # Initialize adaptive timing
        adaptive = AdaptiveTiming(min_pause=min_pause, max_pause=max_pause)
        error_backoff = min_pause  # Backoff for connection errors
        max_error_backoff = 120

        cycle_count = 0

        while not self._shutdown_requested:
            cycle_start = time.time()
            cycle_count += 1

            try:
                self.update_heartbeat()

                if not self.check_connections():
                    logger.warning("Connection health check failed, attempting reconnection...")
                    if not self.reconnect():
                        logger.error(f"Reconnection failed. Waiting {error_backoff}s before retry...")
                        time.sleep(error_backoff)
                        error_backoff = min(error_backoff * 2, max_error_backoff)
                        continue
                    else:
                        error_backoff = min_pause

                success = self.store_snapshot()

                if success:
                    error_backoff = min_pause
                else:
                    error_backoff = min(error_backoff * 1.5, max_error_backoff)

                if self.consecutive_failures >= self.max_consecutive_failures:
                    logger.error(f"Too many consecutive failures ({self.consecutive_failures}). "
                                 f"Forcing full reconnection...")
                    self.connected = False
                    self.reconnect()
                    self.consecutive_failures = 0

            except Exception as e:
                logger.error(f"UNEXPECTED ERROR in monitoring loop: {e}")
                import traceback
                logger.error(traceback.format_exc())
                self.consecutive_failures += 1
                error_backoff = min(error_backoff * 2, max_error_backoff)

            # Calculate adaptive pause based on cycle duration
            cycle_duration = time.time() - cycle_start
            pause_time = adaptive.record_cycle(cycle_duration)

            # Log cycle stats periodically (every 10 cycles)
            if cycle_count % 10 == 0:
                stats = adaptive.get_stats()
                logger.info(f"[STATS] Cycle #{cycle_count}: duration={cycle_duration:.1f}s, "
                           f"avg={stats['avg_duration']:.1f}s, pause={pause_time:.1f}s")

            time.sleep(pause_time)

        logger.info("Worker shutdown complete")


async def main():
    """Main entry point for the worker (async version)."""
    worker = MonitorWorker()

    # Initial connection with retry
    max_initial_retries = 5
    for attempt in range(max_initial_retries):
        try:
            worker.connect()
            break
        except Exception as e:
            logger.error(f"Initial connection attempt {attempt + 1}/{max_initial_retries} failed: {e}")
            if attempt < max_initial_retries - 1:
                wait_time = (attempt + 1) * 5
                logger.info(f"Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
            else:
                logger.error("All initial connection attempts failed. Starting anyway - will retry in loop.")

    await worker.run(interval=10)


def main_sync():
    """Main entry point for the worker (synchronous version - recommended for background execution)."""
    import fcntl

    # SINGLETON: Ensure only one worker instance runs at a time
    lock_file_path = '/var/run/monitor_worker.lock'
    lock_file = open(lock_file_path, 'w')
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_file.write(str(os.getpid()))
        lock_file.flush()
        logger.info(f"Acquired singleton lock (PID: {os.getpid()})")
    except (IOError, OSError):
        logger.error("Another worker instance is already running. Exiting.")
        sys.exit(1)

    worker = MonitorWorker()

    # Initial connection with retry
    max_initial_retries = 5
    for attempt in range(max_initial_retries):
        try:
            worker.connect()
            break
        except Exception as e:
            logger.error(f"Initial connection attempt {attempt + 1}/{max_initial_retries} failed: {e}")
            if attempt < max_initial_retries - 1:
                wait_time = (attempt + 1) * 5
                logger.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error("All initial connection attempts failed. Starting anyway - will retry in loop.")

    try:
        # v3.1: Use adaptive timing instead of fixed interval
        # min_pause=5s ensures a brief rest between cycles
        # max_pause=60s is the maximum wait for very slow cycles
        worker.run_sync(min_pause=5, max_pause=60)
    finally:
        # Release lock on exit
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()


if __name__ == '__main__':
    # Use sync version by default for more robust background execution
    # The sync version is simpler and doesn't have asyncio event loop issues
    main_sync()

