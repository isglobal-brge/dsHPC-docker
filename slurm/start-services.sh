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

echo -e "${BLUE}┌────────────────────────────────────────┐${NC}"
echo -e "${BLUE}│${NC} ${BLUE}        __       __  __ ____   ______ ${NC} ${BLUE}│${NC}"
echo -e "${BLUE}│${NC} ${BLUE}   ____/ /_____ / / / // __ \ / ____/ ${NC} ${BLUE}│${NC}"
echo -e "${BLUE}│${NC} ${BLUE}  / __  // ___// /_/ // /_/ // /      ${NC} ${BLUE}│${NC}"
echo -e "${BLUE}│${NC} ${BLUE} / /_/ /(__  )/ __  // ____// /___    ${NC} ${BLUE}│${NC}"
echo -e "${BLUE}│${NC} ${BLUE} \__,_//____//_/ /_//_/     \____/    ${NC} ${BLUE}│${NC}"
echo -e "${BLUE}│${NC} ${BLUE}                                      ${NC} ${BLUE}│${NC}"
echo -e "${BLUE}└────────────────────────────────────────┘${NC}"
echo -e ""
echo -e "${BOLD}Welcome to High-Performance Computing for ${YELLOW}DataSHIELD${NC}${BOLD}!${NC}"
echo -e ""
echo -e "${BOLD}${CYAN}>> Starting services...${NC}"

# Create Munge key if it doesn't exist
if [ ! -f /etc/munge/munge.key ]; then
    dd if=/dev/urandom bs=1 count=1024 > /etc/munge/munge.key 2>/dev/null
    chown munge:munge /etc/munge/munge.key
    chmod 400 /etc/munge/munge.key
fi

# Start Munge
service munge start >/dev/null 2>&1
echo -e "${GREEN}>> Munge service started!${NC}"

# Start Slurm services
service slurmctld start >/dev/null 2>&1
service slurmd start >/dev/null 2>&1
echo -e "${GREEN}>> Slurm services started!${NC}"

# Wait for Slurm services to fully initialize
sleep 5

# Ensure the node is set to IDLE state and ready for jobs
scontrol update NodeName=localhost State=IDLE >/dev/null 2>&1
echo -e "${GREEN}>> Node set to IDLE state!${NC}"

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