# Slurm API Service

A modern, RESTful API service for interacting with Slurm workload manager. This service allows users to submit jobs to Slurm, monitor job status, and query the queue through a clean HTTP interface.

## Features

- Submit Slurm jobs through a simple REST API
- Monitor job status in real-time
- Query Slurm queue information
- Retrieve job details and output
- Persistent storage of job information in MongoDB
- Background process for job status monitoring

## Architecture

The application is built with:
- **FastAPI**: Modern, high-performance web framework for building APIs
- **MongoDB**: Document database for storing job information
- **Docker**: Containerization for easy deployment
- **Slurm**: HPC workload manager

## API Endpoints

### Submit a Job

```
POST /submit
```

Submit a new job to Slurm.

**Request Body**:
```json
{
  "script": "#!/bin/bash\n#SBATCH --time=00:10:00\n#SBATCH --ntasks=1\n\necho 'Hello World'\nsleep 10",
  "name": "test-job",
  "parameters": {
    "time": "00:10:00",
    "ntasks": 1,
    "partition": "debug"
  }
}
```

- `script`: The content of the Slurm batch script (required)
- `name`: Name for the job (optional)
- `parameters`: Additional parameters to pass to Slurm (optional)

**Response**:
```json
{
  "message": "Job submitted successfully",
  "job_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### Get Queue Status

```
GET /queue
```

Get the current Slurm queue status.

**Response**:
```json
{
  "jobs": [
    {
      "job_id": "12345",
      "name": "test-job",
      "user": "slurm",
      "state": "RUNNING",
      "time": "00:01:30",
      "nodes": "1",
      "nodelist": "node01"
    }
  ]
}
```

### Get Job Information

```
GET /job/{job_id}
```

Get detailed information about a specific job.

**Path Parameters**:
- `job_id`: The ID of the job to retrieve

**Response**:
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "slurm_id": "12345",
  "name": "test-job",
  "status": "RUNNING",
  "submit_time": "2023-05-15T10:30:00",
  "start_time": "2023-05-15T10:31:00",
  "end_time": null,
  "script": "#!/bin/bash\n#SBATCH --time=00:10:00\n...",
  "parameters": {
    "time": "00:10:00",
    "ntasks": 1
  },
  "output": "Hello World\n"
}
```

### Health Check

```
GET /health
```

Simple health check endpoint to verify the API is running.

**Response**:
```json
{
  "status": "ok"
}
```

## Job Status Values

Jobs can have the following status values:

- `PD`: Pending - Job is queued and waiting for resources
- `R`: Running - Job is currently running
- `CD`: Completed - Job completed successfully
- `F`: Failed - Job failed
- `CA`: Cancelled - Job was cancelled by user or system
- `TO`: Timeout - Job reached time limit
- `NF`: Node Failure - Job failed due to node failure
- `OOM`: Out of Memory - Job experienced out of memory error
- `S`: Suspended - Job has been suspended
- `ST`: Stopped - Job has been stopped
- `BF`: Boot Failure - Job failed during node boot
- `DL`: Deadline - Job terminated on deadline
- `CG`: Completing - Job is in the process of completing
- `CF`: Configuring - Job is in the process of configuring
- `PR`: Preempted - Job was preempted by another job

## Setup and Installation

### Prerequisites

- Docker and Docker Compose
- Available ports: 8000 (API) and 27017 (MongoDB)

### Quick Start

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/slurm-api.git
   cd slurm-api
   ```

2. Start the services:
   ```bash
   docker-compose up -d
   ```

3. The API will be available at http://localhost:8000

### Environment Variables

- `MONGO_URI`: MongoDB connection URI (default: mongodb://mongo:27017/)
- `MONGO_DB`: MongoDB database name (default: outputs)

## Examples

### Submit a Simple Job

```bash
curl -X POST http://localhost:8000/submit \
  -H "Content-Type: application/json" \
  -d '{
    "script": "#!/bin/bash\n#SBATCH --time=00:05:00\n\necho \"Hello from Slurm\"\nsleep 30",
    "name": "hello-job"
  }'
```

### Check Job Status

```bash
curl -X GET http://localhost:8000/job/550e8400-e29b-41d4-a716-446655440000
```

### Check Queue Status

```bash
curl -X GET http://localhost:8000/queue
```

## Development

### Project Structure

```
slurm_api/
├── api/
│   └── endpoints.py    # API endpoint definitions
├── background/
│   └── tasks.py        # Background tasks for job monitoring
├── config/
│   ├── db_config.py    # MongoDB configuration
│   └── logging_config.py # Logging configuration
├── models/
│   └── job.py          # Pydantic models for jobs
├── services/
│   ├── job_service.py  # Job management service
│   └── slurm_service.py # Slurm interaction service
├── utils/
│   ├── db_utils.py     # Database utility functions
│   └── hash_utils.py   # Utility functions for hashing
└── main.py             # FastAPI application entry point
```

### Running Tests

```bash
docker-compose run slurm python -m pytest
```

## License

See the [LICENSE](LICENSE) file for details. 