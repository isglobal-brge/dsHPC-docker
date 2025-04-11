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
    git \
    curl \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    jq \
    cgroup-tools \
    && rm -rf /var/lib/apt/lists/*

# Install pyenv for managing Python versions
RUN curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash
ENV PYENV_ROOT="/root/.pyenv"
ENV PATH="$PYENV_ROOT/bin:$PATH"
RUN echo 'eval "$(pyenv init -)"' >> /root/.bashrc

# Copy the Python configuration file
COPY config/python.json /tmp/python.json

# Install Python version from config 
RUN PYTHON_VERSION=$(jq -r '.python_version' /tmp/python.json) && \
    pyenv install $PYTHON_VERSION && \
    pyenv global $PYTHON_VERSION

# Create Python virtual environments
RUN mkdir -p /opt/venvs

# Create API Python environment (fixed version)
RUN python3 -m venv /opt/venvs/api_python
RUN /opt/venvs/api_python/bin/pip install --upgrade pip
RUN /opt/venvs/api_python/bin/pip install fastapi==0.95.1 uvicorn==0.22.0 pymongo==4.3.3

# Create System Python environment from config
RUN PYTHON_VERSION=$(jq -r '.python_version' /tmp/python.json) && \
    $PYENV_ROOT/versions/$PYTHON_VERSION/bin/python -m venv /opt/venvs/system_python
RUN /opt/venvs/system_python/bin/pip install --upgrade pip

# Install requirements from config
RUN jq -r '.requirements | to_entries[] | "\(.key) \(.value)"' /tmp/python.json > /tmp/requirements.txt && \
    /opt/venvs/system_python/bin/pip install -r /tmp/requirements.txt

# Add system_python to PATH and make it the default
ENV PATH="/opt/venvs/system_python/bin:${PATH}"
RUN echo 'source /opt/venvs/system_python/bin/activate' >> /root/.bashrc

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