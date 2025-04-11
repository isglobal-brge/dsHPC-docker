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

# Load methods from the methods directory
echo "Loading methods..."
bash /load-methods.sh

# Start FastAPI application using API_PYTHON environment
cd /app
/opt/venvs/api_python/bin/uvicorn slurm_api.main:app --host 0.0.0.0 --port 8000 --reload 