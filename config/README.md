# Configuration Files

This directory contains configuration files used to customize the Docker environment for the Slurm API service.

## Overview

Three configuration files control different aspects of the environment:

1. `python.json` - Configures Python environment and packages
2. `r.json` - Configures R environment and packages
3. `system_deps.json` - Configures additional system dependencies installed via `apt-get`

Each file can be customized to meet your specific requirements without modifying the Dockerfile.

## File Formats

### python.json

Controls the Python version and packages installed in the system Python environment:

```json
{
  "python_version": "3.8.10",
  "libraries": {
    "numpy": ">=1.22.0",
    "pandas": ">=1.3.5"
  }
}
```

- `python_version`: The specific Python version to install via pyenv
- `libraries`: Key-value pairs of Python packages and their version requirements

### r.json

Controls the R version and packages installed:

```json
{
  "r_version": "4.4.0",
  "packages": {
    "ggplot2": ">=3.4.0",
    "dplyr": ">=1.0.10"
  }
}
```

- `r_version`: The R version to install
- `packages`: Key-value pairs of R packages and their version requirements

### system_deps.json

Controls additional system packages installed via apt-get:

```json
{
  "apt_packages": [
    "libssl-dev",
    "git",
    "wget"
  ]
}
```

- `apt_packages`: List of packages to install with apt-get

## Usage

The Docker container build process reads these configuration files and installs the specified software. You can:

1. Modify any configuration file to add, remove, or update packages
2. Leave sections empty (e.g., empty `apt_packages` array) to skip installing those components
3. Rebuild the container with `docker-compose build` after changes

The configuration allows for a flexible and reproducible environment without modifying the Dockerfile directly. 