FROM ubuntu:22.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Copy configuration files
COPY config/python.json /tmp/python.json
COPY config/r.json /tmp/r.json
COPY config/system_deps.json /tmp/system_deps.json

# Install base packages
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

# Install system dependencies from config file if not empty
RUN apt-get update && \
    if [ $(jq '.apt_packages | length' /tmp/system_deps.json) -gt 0 ]; then \
        jq -r '.apt_packages[]' /tmp/system_deps.json | xargs apt-get install -y; \
    else \
        echo "No additional system packages specified, skipping..."; \
    fi && \
    rm -rf /var/lib/apt/lists/*

# Install pyenv for managing Python versions
RUN curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash
ENV PYENV_ROOT="/root/.pyenv"
ENV PATH="$PYENV_ROOT/bin:$PATH"
RUN echo 'eval "$(pyenv init -)"' >> /root/.bashrc

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

# Install python libraries from config if not empty
RUN if [ $(jq '.libraries | length' /tmp/python.json) -gt 0 ]; then \
        jq -r '.libraries | to_entries[] | if .value == null or .value == "" then .key else "\(.key)==\(.value)" end' /tmp/python.json > /tmp/requirements.txt && \
        /opt/venvs/system_python/bin/pip install -r /tmp/requirements.txt; \
    else \
        echo "No Python libraries specified, skipping..."; \
    fi

# Add system_python to PATH and make it the default
ENV PATH="/opt/venvs/system_python/bin:${PATH}"
RUN echo 'source /opt/venvs/system_python/bin/activate' >> /root/.bashrc

# Install R from CRAN
RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common \
    dirmngr \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 \
    && add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/" \
    && apt-get update \
    && R_VERSION=$(jq -r '.r_version' /tmp/r.json) \
    && apt-get install -y --no-install-recommends r-base r-base-dev \
    && R --version | grep -q "R version" \
    && rm -rf /var/lib/apt/lists/*

# Install BiocManager and other essential R packages
RUN R -s -e "install.packages(c('BiocManager', 'jsonlite'), repos='https://cloud.r-project.org/')"

# Copy the R package installation script
COPY slurm/install-r-packages.sh /usr/local/bin/install-r-packages.sh
RUN chmod +x /usr/local/bin/install-r-packages.sh

# Install R packages from config if not empty
RUN if [ $(jq '.packages | length' /tmp/r.json) -gt 0 ]; then \
        jq -r '.packages | keys[]' /tmp/r.json > /tmp/r_packages.txt && \
        install-r-packages.sh; \
    else \
        echo "No R packages specified, skipping..."; \
    fi

# Create necessary directories and set permissions
RUN mkdir -p /var/run/munge \
    && mkdir -p /var/run/slurm \
    && mkdir -p /var/spool/slurm \
    && mkdir -p /var/log/slurm \
    && mkdir -p /etc/munge \
    && mkdir -p /var/spool/slurmd \
    && mkdir -p /workspace \
    && mkdir -p /methods \
    && mkdir -p /methods/commands \
    && mkdir -p /methods/scripts \
    && mkdir -p /app/scripts \
    && mkdir -p /app/slurm/scripts \
    && chown munge:munge /etc/munge \
    && chown munge:munge /var/run/munge \
    && chown slurm:slurm /var/run/slurm \
    && chown slurm:slurm /var/spool/slurm \
    && chown slurm:slurm /var/log/slurm \
    && chown slurm:slurm /var/spool/slurmd \
    && chmod 777 /workspace \
    && chmod 777 /methods

# Copy configuration files and API
COPY slurm_api /app/slurm_api
COPY slurm/start-services.sh /start-services.sh
COPY slurm/load-methods.sh /load-methods.sh
COPY slurm/scripts /app/slurm/scripts

# Make the startup scripts executable
RUN chmod +x /start-services.sh \
    && chmod +x /load-methods.sh \
    && chmod +x /app/slurm/scripts/register_method.py

# Expose port for FastAPI
EXPOSE 8000

# Start services
CMD ["/start-services.sh"] 