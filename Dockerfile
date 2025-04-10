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
    cgroup-tools \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip3 install fastapi uvicorn

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
COPY slurm.conf /etc/slurm/slurm.conf
COPY api.py /app/api.py
COPY start-services.sh /start-services.sh

# Make the startup script executable
RUN chmod +x /start-services.sh

# Expose port for FastAPI
EXPOSE 8000

# Start services
CMD ["/start-services.sh"] 