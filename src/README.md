# Source Code Directory

This directory contains all source code for the dsHPC system components.

## Directory Structure

### dshpc_api/
Main API service that provides the external interface for the system
- Handles job submission and management
- File upload/download operations
- Method registration and execution

### slurm/
Slurm container configuration and scripts
- Dockerfile for the Slurm compute environment
- Service startup scripts
- Method loading scripts
- R package installation utilities

### slurm_api/
Internal API service running within the Slurm container
- Interfaces with Slurm for job management
- Handles method execution
- Manages job lifecycle

## Architecture

The system follows a microservices architecture:
1. **dshpc_api**: External-facing API (port 8001)
2. **slurm_api**: Internal Slurm API (port 8000)
3. **MongoDB**: Distributed data storage for jobs, files, and methods

## Development

All source code is mounted as volumes during development for hot-reloading:
- Changes to Python code are automatically reloaded
- Container restart required for Dockerfile changes

## Language

All code and comments should be in English for international collaboration.
