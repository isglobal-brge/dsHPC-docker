# Docker Network Configuration for dsHPC

This document explains the network configuration of the dsHPC system, focusing on security and customization.

## Network Architecture

The dsHPC system uses a two-network approach for security:

1. **Internal Network (`dshpc-internal-network`)**: 
   - All services are connected to this network
   - This network is marked as `internal: true` which means it's not accessible from the host
   - Services can communicate with each other within this network
   - Used by MongoDB databases and the Slurm service

2. **Public Network (`dshpc-public-network`)**:
   - Only the dshpc-api service is connected to this network
   - This network allows exposing services to the host machine
   - The dshpc-api serves as the only entry point to the system

## Port Configuration

The external port configuration can be customized using environment variables:

### External Ports (Configurable)
- `DSHPC_API_EXTERNAL_PORT`: The port that the API is accessible on from outside (default: 8001)

### Internal Ports (Fixed)
The internal ports are fixed for simplicity:
- API: 8001
- Slurm API: 8000
- MongoDB instances: 27017

## Customizing the External Port

To change the external port that exposes the API, use one of these methods:

### Method 1: Using Environment Variables Directly (Recommended)

```bash
# To change the external API port to 9000
DSHPC_API_EXTERNAL_PORT=9000 docker-compose up -d
```

### Method 2: Using .env File

Create or modify a `.env` file in the project root:
```
DSHPC_API_EXTERNAL_PORT=9000
```

Then use:
```bash
docker-compose --env-file .env up -d
```

> **Note**: In some Docker Compose versions, automatic loading of the `.env` file may not work consistently. Using the environment variables directly (Method 1) is more reliable.

## Security Benefits

This architecture provides several security benefits:

1. **Minimal Attack Surface**: Only the main API is exposed, not the databases or internal services
2. **Network Isolation**: Internal services can't be accessed from outside the Docker network
3. **Single Entry Point**: All requests must go through the API, which can implement proper authentication and authorization

## Accessing the Services

- **API**: http://localhost:[DSHPC_API_EXTERNAL_PORT] (by default: http://localhost:8001)
- **Other Services**: Not directly accessible from outside

## Viewing Logs

To view logs for any service, use:

```bash
docker-compose logs [service-name]
```

Example:
```bash
docker-compose logs dshpc-api
```

## Debugging Network Issues

If you need to debug network issues:

1. Check that all services are running: `docker-compose ps`
2. Check the logs for each service: `docker-compose logs [service-name]`
3. If necessary, you can temporarily expose internal services for debugging by adding port mappings in the `docker-compose.yml` file
4. Use Docker's built-in commands like `docker network inspect dshpc-internal-network` to view the network configuration 