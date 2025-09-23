# Setup Tools

This directory contains tools for setting up and configuring dsHPC environments.

## Files

### environment-config.json

Configuration file that defines the environment settings:

```json
{
  "environment_name": "dsHPC",
  "display_name": "High-Performance Computing for DataSHIELD", 
  "docker_stack_prefix": "dshpc",
  "base_repository": "https://github.com/isglobal-brge/dsHPC-docker.git",
  "default_api_port": 8001,
  "description": "DataSHIELD High-Performance Computing Environment"
}
```

**Configuration Options:**
- `environment_name`: Short name used for directories and references
- `display_name`: Full name shown in the setup process
- `docker_stack_prefix`: Prefix for Docker container and network names
- `base_repository`: Git repository URL to clone from
- `default_api_port`: Default port for the API service
- `description`: Brief description of the environment

### setup-dshpc-environment.sh

Main setup script that automates the environment deployment process.

## Usage

### Prerequisites

The user should have a directory with their environment configuration:

```
my-project/
├── environment/
│   ├── methods/
│   │   ├── commands/
│   │   └── scripts/
│   ├── python.json
│   ├── r.json
│   └── system_deps.json
└── setup-dshpc-environment.sh
```

### Running the Setup

**Run the setup script**:
```bash
./setup-dshpc-environment.sh
```

### What the Script Does

1. **Validates** the environment directory structure
2. **Clones** the base repository from GitHub
3. **Copies** user's environment configuration
4. **Generates** a `.env` file with random API key
5. **Builds** Docker images (optional)
6. **Provides** startup instructions

### Generated Structure

After running the script:

```
my-project/
├── environment/              # Original user config
├── dsHPC-docker/            # Cloned and configured repository
│   ├── .env                 # Generated environment variables
│   ├── environment/         # User's config copied here
│   ├── config/             # User's config copied here (if exists)
│   ├── docker-compose.yml  # Ready to use
│   └── src/                # Source code
└── setup-dshpc-environment.sh
```

## Environment Variables

The script generates a `.env` file with these variables:

```bash
# API Configuration
DSHPC_API_EXTERNAL_PORT=8001
DSHPC_API_KEY=randomly-generated-32-char-key

# Docker Stack Configuration  
COMPOSE_PROJECT_NAME=dshpc

# Logging Configuration
LOG_LEVEL=WARNING
```

## Customization

To customize for different environments:

1. **Modify `environment-config.json`** with your settings
2. **Update the base repository URL** to point to your fork
3. **Change naming conventions** by updating the docker prefix
4. **Adjust default ports** and other settings

## Dependencies

**Required:**
- `git` - for cloning the repository
- `docker` and `docker-compose` - for building and running

**Optional:**
- `jq` - for JSON parsing (has fallback)
- `openssl` - for secure key generation (has fallback)
