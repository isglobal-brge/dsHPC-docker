#!/bin/bash

# Create Munge key if it doesn't exist
if [ ! -f /etc/munge/munge.key ]; then
    dd if=/dev/urandom bs=1 count=1024 > /etc/munge/munge.key
    chown munge:munge /etc/munge/munge.key
    chmod 400 /etc/munge/munge.key
fi

# Start Munge
service munge start

# Start Slurm services
service slurmctld start
service slurmd start

# Wait for Slurm services to fully initialize
sleep 5

# Check if the localhost node exists before trying to update its state
if sinfo -h -n localhost &>/dev/null; then
    # Check the current node state and set to IDLE only if it's DOWN, DRAINED, or in another non-working state
    NODE_STATE=$(sinfo -h -n localhost -o "%t" | tr -d '[:space:]')
    if [ "$NODE_STATE" != "idle" ] && [ "$NODE_STATE" != "alloc" ] && [ "$NODE_STATE" != "mix" ]; then
        echo "Node is in $NODE_STATE state. Setting to IDLE state..."
        scontrol update NodeName=localhost State=IDLE
        echo "Node set to IDLE state"
    else
        echo "Node is already in working state ($NODE_STATE). No state change needed."
    fi
else
    echo "Node 'localhost' not found in Slurm configuration. Skipping node state update."
fi

# Load methods from the methods directory
echo "Loading methods..."
bash /load-methods.sh
LOAD_METHODS_EXIT_CODE=$?

if [ $LOAD_METHODS_EXIT_CODE -ne 0 ]; then
    echo "ERROR: Method loading failed with exit code $LOAD_METHODS_EXIT_CODE. Aborting system startup."
    exit 1
fi

# Start FastAPI application using API_PYTHON environment
cd /app
/opt/venvs/api_python/bin/uvicorn slurm_api.main:app --host 0.0.0.0 --port 8000 --reload 