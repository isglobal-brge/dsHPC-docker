#!/bin/bash

# Define colors for output
GREEN="\033[1;32m"
CYAN="\033[1;36m"
PURPLE="\033[0;35m"
BOLD="\033[1m"
YELLOW="\033[1;33m"
ORANGE="\033[0;33m"
BLUE="\033[1;34m"
RED="\033[1;31m"
NC="\033[0m" # No Color

echo -e "${BLUE}┌────────────────────────────────────────────────────┐${NC}"
echo -e "${BLUE}│${NC} ${BLUE}              __       __  __ ____   ______       ${NC} ${BLUE}│${NC}"
echo -e "${BLUE}│${NC} ${BLUE}         ____/ /_____ / / / // __ \ / ____/       ${NC} ${BLUE}│${NC}"
echo -e "${BLUE}│${NC} ${BLUE}        / __  // ___// /_/ // /_/ // /            ${NC} ${BLUE}│${NC}"
echo -e "${BLUE}│${NC} ${BLUE}       / /_/ /(__  )/ __  // ____// /____         ${NC} ${BLUE}│${NC}"
echo -e "${BLUE}│${NC} ${BLUE}       \__,_//____//_/ /_//_/     \_____/         ${NC} ${BLUE}│${NC}"
echo -e "${BLUE}│${NC} ${BLUE}                                                  ${NC} ${BLUE}│${NC}"
echo -e "${BLUE}└────────────────────────────────────────────────────┘${NC}"
echo -e ""
echo -e "${BOLD}Welcome to ${YELLOW}High-Performance Computing for DataSHIELD${NC}${BOLD}!${NC}"
echo -e ""
echo -e "${BOLD}${CYAN}>> Starting services...${NC}"

# Handle dshpc.conf - check if custom config exists, otherwise create default
if [ -f /config/dshpc.conf ]; then
    echo -e "${GREEN}>> Using custom dshpc.conf from /config${NC}"
else
    echo -e "${YELLOW}>> No custom dshpc.conf found, creating default configuration${NC}"
    cat > /config/dshpc.conf << 'EOF'
# ============================================================================
# dsHPC Job Resource Defaults
# ============================================================================
# These values are used when a method doesn't specify its own resource requirements.
#
# IMPORTANT: For methods that need more resources (like deep learning),
# specify min_memory_mb in the method's method.json file:
#
#   "resources": {
#       "min_memory_mb": 4096,  // Method needs at least 4GB
#       "cpus": 2               // Method needs 2 CPUs
#   }
#
# ============================================================================

# Default CPUs per task
# Set to 1 to maximize parallelism (more jobs running simultaneously)
# Methods that need more CPUs should specify it in their method.json
DEFAULT_CPUS_PER_TASK=1

# Default memory per CPU in MB
# With DEFAULT_CPUS_PER_TASK=1, each job gets 2048 MB (2GB) by default
# This is generous to avoid OOM kills - users can't intervene remotely
# Heavy methods should still specify min_memory_mb (gets +20% safety buffer)
# DEFAULT_MEM_PER_CPU=2048

# Default time limit (optional, format: HH:MM:SS)
# DEFAULT_TIME_LIMIT=01:00:00
EOF
fi
# Show current settings
echo -e "${CYAN}   DEFAULT_CPUS_PER_TASK=$(grep '^DEFAULT_CPUS_PER_TASK=' /config/dshpc.conf | cut -d'=' -f2)${NC}"

# Function to reload slurm configuration
reload_slurm_config() {
    if [ -f /config/slurm.conf ]; then
        # Check if config has changed
        if ! cmp -s /config/slurm.conf /etc/slurm/slurm.conf; then
            echo -e "${YELLOW}>> Detected slurm.conf changes, reloading configuration...${NC}"
            cp /config/slurm.conf /etc/slurm/slurm.conf
            # Reconfigure slurm without restarting (preserves running jobs)
            scontrol reconfigure 2>/dev/null && echo -e "${GREEN}>> Slurm configuration reloaded successfully${NC}" || true
        fi
    fi
}

# Handle slurm.conf - ALWAYS regenerate to detect current system resources
# This ensures memory changes (e.g., Docker Desktop settings) are detected on restart
echo -e "${CYAN}>> Detecting system resources for Slurm configuration...${NC}"

# Check if user has a CUSTOM slurm.conf (with custom settings beyond auto-detection)
USER_CUSTOM_SLURM=false
if [ -f /config/slurm.conf ]; then
    # Check if it's a user-customized file (has a marker comment)
    if grep -q "# USER_CUSTOMIZED=true" /config/slurm.conf 2>/dev/null; then
        USER_CUSTOM_SLURM=true
        echo -e "${GREEN}>> Using user-customized slurm.conf from /config${NC}"
        cp /config/slurm.conf /etc/slurm/slurm.conf
    fi
fi

if [ "$USER_CUSTOM_SLURM" = false ]; then
    echo -e "${YELLOW}>> Auto-generating slurm.conf based on current system resources${NC}"
    # Detect system resources
    DETECTED_CPUS=$(nproc 2>/dev/null || echo 8)
    DETECTED_MEMORY_KB=$(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}' || echo 16777216)
    TOTAL_MEMORY_MB=$((DETECTED_MEMORY_KB / 1024))

    # CONSERVATIVE MEMORY ALLOCATION:
    # Reserve memory for system services (MongoDB, Slurm daemons, OS, etc.)
    # - Base reservation: 2 GB for OS/Slurm
    # - Per-container reservation: ~500 MB each for 3 MongoDB instances = 1.5 GB
    # - Buffer for spikes: 500 MB
    # Total reserved: ~4 GB (4096 MB)
    SYSTEM_RESERVED_MB=4096

    # If system has less than 8 GB, reserve 50% for system instead
    if [ $TOTAL_MEMORY_MB -lt 8192 ]; then
        SYSTEM_RESERVED_MB=$((TOTAL_MEMORY_MB / 2))
    fi

    # Memory available for Slurm jobs
    SLURM_MEMORY_MB=$((TOTAL_MEMORY_MB - SYSTEM_RESERVED_MB))

    # Ensure at least 1 GB for jobs
    if [ $SLURM_MEMORY_MB -lt 1024 ]; then
        SLURM_MEMORY_MB=1024
    fi

    # MEMORY PER CPU ALLOCATION:
    # PRIORITY: AVOID OOM KILLS! Users cannot intervene remotely.
    # Default to 2048 MB (2GB) per CPU - generous to prevent OOM
    # Methods that need more should specify min_memory_mb (gets +20% buffer)
    # With DEFAULT_CPUS_PER_TASK=1, each job gets 2GB by default
    DEF_MEM_PER_CPU=2048

    # Calculate max parallel jobs based on available memory
    # With 1 CPU per job (default) and 2048 MB per CPU
    MEM_PER_JOB=$((DEF_MEM_PER_CPU * 1))  # 1 CPU per job default
    MAX_PARALLEL_JOBS=$((SLURM_MEMORY_MB / MEM_PER_JOB))

    # Ensure at least 1 job can run
    if [ $MAX_PARALLEL_JOBS -lt 1 ]; then
        MAX_PARALLEL_JOBS=1
        # Recalculate DEF_MEM_PER_CPU if not enough memory
        DEF_MEM_PER_CPU=$((SLURM_MEMORY_MB / 2))
    fi

    # Max memory per CPU is 2x the default (for memory-intensive jobs)
    MAX_MEM_PER_CPU=$((DEF_MEM_PER_CPU * 2))

    echo -e "${CYAN}>> Detected resources: ${DETECTED_CPUS} CPUs, ${TOTAL_MEMORY_MB} MB total RAM${NC}"
    echo -e "${CYAN}>> Memory allocation: ${SYSTEM_RESERVED_MB} MB reserved for system, ${SLURM_MEMORY_MB} MB for jobs${NC}"
    echo -e "${CYAN}>> Default job: 1 CPU, ${DEF_MEM_PER_CPU} MB RAM (2GB) → max ~${MAX_PARALLEL_JOBS} parallel jobs${NC}"
    echo -e "${CYAN}>> OOM prevention: generous defaults + 20% safety buffer on min_memory_mb${NC}"

    cat > /etc/slurm/slurm.conf << EOF
ClusterName=${CLUSTER_NAME:-dshpc-slurm}
SlurmctldHost=localhost

# LOGGING
SlurmctldLogFile=/var/log/slurm/slurmctld.log
SlurmdLogFile=/var/log/slurm/slurmd.log
SlurmdDebug=debug5
SlurmctldDebug=debug5

# SCHEDULER - Allow multiple jobs to run simultaneously
SelectType=select/cons_tres
SelectTypeParameters=CR_Core_Memory

# COMPUTE NODES - Auto-configured based on system resources
# Total system: ${DETECTED_CPUS} CPUs, ${TOTAL_MEMORY_MB} MB RAM
# Reserved for system (MongoDB, Slurm, OS): ${SYSTEM_RESERVED_MB} MB
# Available for jobs: ${SLURM_MEMORY_MB} MB
# Default job: 1 CPU, ${DEF_MEM_PER_CPU} MB RAM
# Estimated max parallel jobs: ${MAX_PARALLEL_JOBS}
NodeName=localhost CPUs=${DETECTED_CPUS} RealMemory=${SLURM_MEMORY_MB} TmpDisk=100000 State=UNKNOWN
PartitionName=debug Nodes=localhost Default=YES MaxTime=INFINITE State=UP DefMemPerCPU=${DEF_MEM_PER_CPU} MaxMemPerCPU=${MAX_MEM_PER_CPU}

# PROCESS TRACKING
ProctrackType=proctrack/linuxproc
EOF
fi  # End of USER_CUSTOM_SLURM check

# Copy environment configuration files if they exist
if [ -f /environment/python.json ]; then
    echo -e "${GREEN}>> Using python.json from /environment${NC}"
    cp /environment/python.json /tmp/python.json
fi

if [ -f /environment/r.json ]; then
    echo -e "${GREEN}>> Using r.json from /environment${NC}"
    cp /environment/r.json /tmp/r.json
fi

if [ -f /environment/system_deps.json ]; then
    echo -e "${GREEN}>> Using system_deps.json from /environment${NC}"
    cp /environment/system_deps.json /tmp/system_deps.json
fi

# Create Munge key if it doesn't exist
if [ ! -f /etc/munge/munge.key ]; then
    dd if=/dev/urandom bs=1 count=1024 > /etc/munge/munge.key 2>/dev/null
    chown munge:munge /etc/munge/munge.key
    chmod 400 /etc/munge/munge.key
fi

# Start Munge
service munge start >/dev/null 2>&1
echo -e "${GREEN}>> Munge service started!${NC}"

# Clean up cluster name file if it exists and doesn't match
if [ -f /var/spool/clustername ]; then
    STORED_CLUSTER=$(cat /var/spool/clustername)
    EXPECTED_CLUSTER=$(grep "^ClusterName=" /etc/slurm/slurm.conf | cut -d'=' -f2)
    
    if [ "$STORED_CLUSTER" != "$EXPECTED_CLUSTER" ]; then
        echo -e "${YELLOW}>> Cluster name mismatch detected (${STORED_CLUSTER} -> ${EXPECTED_CLUSTER})${NC}"
        echo -e "${YELLOW}>> Cleaning Slurm state files to prevent corruption...${NC}"
        rm -f /var/spool/clustername
        rm -f /var/spool/job_state*
        rm -f /var/spool/node_state*
        echo -e "${GREEN}>> State files cleaned!${NC}"
    fi
fi

# Start Slurm services
echo -e "${CYAN}>> Starting slurmctld...${NC}"
service slurmctld start >/dev/null 2>&1
SLURMCTLD_EXIT=$?

echo -e "${CYAN}>> Starting slurmd...${NC}"
service slurmd start >/dev/null 2>&1
SLURMD_EXIT=$?

if [ $SLURMCTLD_EXIT -eq 0 ] && [ $SLURMD_EXIT -eq 0 ]; then
    echo -e "${GREEN}>> Slurm services started successfully!${NC}"
else
    echo -e "${RED}>> Warning: Some Slurm services may have failed to start${NC}"
    echo -e "${YELLOW}>> Checking service status...${NC}"
    service slurmctld status || true
    service slurmd status || true
fi

# Wait for Slurm services to fully initialize
sleep 5

# Verify slurmctld is actually running
if ! pgrep -x slurmctld > /dev/null; then
    echo -e "${RED}>> ERROR: slurmctld is not running! Attempting to restart...${NC}"
    # Check logs for errors
    tail -20 /var/log/slurm/slurmctld.log | grep -i "fatal\|error" || true
    
    # Try to start again
    service slurmctld start
    sleep 3
    
    if ! pgrep -x slurmctld > /dev/null; then
        echo -e "${RED}>> CRITICAL: slurmctld failed to start. Job submission will not work.${NC}"
        echo -e "${YELLOW}>> Check /var/log/slurm/slurmctld.log for details${NC}"
    else
        echo -e "${GREEN}>> slurmctld started successfully on second attempt${NC}"
    fi
fi

# Check if the node exists and conditionally set to IDLE state only if needed
if scontrol show node=localhost &>/dev/null; then
    # Get current node state
    NODE_STATE=$(scontrol show node=localhost | grep "State=" | awk -F "=" '{print $2}' | awk '{print $1}')
    
    # Check if node needs to be set to IDLE (not already IDLE and not running jobs)
    if [[ "$NODE_STATE" =~ ^(DOWN|DRAIN|DRAINING|FAIL|FAILING|MAINT|UNKNOWN).*$ ]]; then
        echo -e "${GREEN}>> Node localhost found in $NODE_STATE state! Setting to IDLE...${NC}"
        scontrol update NodeName=localhost State=IDLE >/dev/null 2>&1
    else
        echo -e "${GREEN}>> Node localhost already in $NODE_STATE state!${NC}"
    fi
else
    echo -e "${ORANGE}>> Node localhost not found in Slurm configuration! Skipping state check...${NC}"
fi

# Load methods from the methods directory
echo -e ""
echo -e "${BOLD}${CYAN}>> Loading methods...${NC}"
bash /load-methods.sh
LOAD_METHODS_EXIT_CODE=$?

if [ $LOAD_METHODS_EXIT_CODE -ne 0 ]; then
    echo -e "\033[1;31mERROR: Method loading failed with exit code $LOAD_METHODS_EXIT_CODE. Aborting system startup.\033[0m"
    exit 1
fi

# Start configuration monitor in background (checks for slurm.conf changes every 30s)
(
    while true; do
        sleep 30
        reload_slurm_config
    done
) &
echo -e "${GREEN}>> Configuration monitor started (checks /config/slurm.conf every 30s)${NC}"

# Start FastAPI application using API_PYTHON environment
cd /app
echo -e ""
echo -e "${BOLD}${CYAN}>> Starting API server...${NC}"
/opt/venvs/api_python/bin/uvicorn slurm_api.main:app --host 0.0.0.0 --port 8000 --reload --log-level warning --no-access-log 