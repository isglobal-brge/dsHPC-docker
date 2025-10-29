#!/bin/bash

# Define colors for output
GREEN="\033[1;32m"
CYAN="\033[1;36m"
PURPLE="\033[0;35m"
BOLD="\033[1m"
YELLOW="\033[1;33m"
ORANGE="\033[0;33m"
BLUE="\033[1;34m"
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

# Handle slurm.conf - check if custom config exists, otherwise create default
if [ -f /config/slurm.conf ]; then
    echo -e "${GREEN}>> Using custom slurm.conf from /config${NC}"
    cp /config/slurm.conf /etc/slurm/slurm.conf
else
    echo -e "${YELLOW}>> No custom slurm.conf found, creating default configuration${NC}"
    cat > /etc/slurm/slurm.conf << EOF
ClusterName=${CLUSTER_NAME:-dshpc-slurm}
SlurmctldHost=localhost

# LOGGING
SlurmctldLogFile=/var/log/slurm/slurmctld.log
SlurmdLogFile=/var/log/slurm/slurmd.log
SlurmdDebug=debug5
SlurmctldDebug=debug5

# COMPUTE NODES
NodeName=localhost CPUs=8 Boards=1 SocketsPerBoard=1 CoresPerSocket=8 ThreadsPerCore=1 State=UNKNOWN
PartitionName=debug Nodes=localhost Default=YES MaxTime=INFINITE State=UP

# PROCESS TRACKING
ProctrackType=proctrack/linuxproc
EOF
fi

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

# Start FastAPI application using API_PYTHON environment
cd /app
echo -e ""
echo -e "${BOLD}${CYAN}>> Starting API server...${NC}"
/opt/venvs/api_python/bin/uvicorn slurm_api.main:app --host 0.0.0.0 --port 8000 --reload --log-level warning --no-access-log 