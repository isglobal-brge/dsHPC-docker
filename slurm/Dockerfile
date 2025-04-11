FROM ubuntu:22.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary packages
RUN apt-get update && apt-get install -y \
    slurmd \
    slurmctld \
    slurm-client \
    munge \
    python3 \
    python3-pip \
    python3-venv \
    cgroup-tools \
    && rm -rf /var/lib/apt/lists/*

# Setup system Python (default Python for Slurm and users)
RUN python3 -m pip install --upgrade pip

# Setup separate Python environment for the API
RUN python3 -m venv /opt/api_venv
ENV API_PYTHON=/opt/api_venv/bin/python
ENV API_PIP=/opt/api_venv/bin/pip

# Install Python dependencies in the API environment
RUN $API_PIP install --upgrade pip && \
    $API_PIP install fastapi uvicorn pymongo

# Create necessary directories and set permissions
RUN mkdir -p /var/run/munge \
    && mkdir -p /var/run/slurm \
    && mkdir -p /var/spool/slurm \
    && mkdir -p /var/log/slurm \
    && mkdir -p /etc/munge \
    && mkdir -p /var/spool/slurmd \
    && chown munge:munge /etc/munge \
    && chown munge:munge /var/run/munge \
    && chown slurm:slurm /var/run/slurm \
    && chown slurm:slurm /var/spool/slurm \
    && chown slurm:slurm /var/log/slurm \
    && chown slurm:slurm /var/spool/slurmd

# Copy configuration files and API
COPY slurm/slurm.conf /etc/slurm/slurm.conf
COPY slurm_api /app/slurm_api
COPY slurm/start-services.sh /start-services.sh

# Make the startup script executable
RUN chmod +x /start-services.sh

# Expose port for FastAPI
EXPOSE 8000

# Start services
CMD ["/start-services.sh"] 