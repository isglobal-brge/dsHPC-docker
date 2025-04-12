# Multi-job API

This document describes the multi-job API in the dsHPC system, which allows batch processing of multiple job configurations with a single request.

## Overview

The multi-job API builds on the single job endpoint by allowing you to:

1. Specify multiple job configurations in a single request
2. Process all jobs in parallel
3. Get consolidated results with statistics about job processing

This is particularly useful for scenarios where you need to run the same method across multiple datasets, or multiple methods across the same dataset, or any combination of methods and datasets.

## API Endpoint

```
POST /simulate-jobs
```

### Request Format

```json
{
  "jobs": [
    {
      "file_hash": "hash_of_file_1",
      "method_name": "Example Method",
      "parameters": {
        "param1": "value1",
        "param2": "value2"
      }
    },
    {
      "file_hash": "hash_of_file_2",
      "method_name": "Another Method",
      "parameters": {
        "threshold": 0.5,
        "verbose": true
      }
    }
    // Additional job configurations...
  ]
}
```

### Response Format

```json
{
  "results": [
    {
      "job_id": "unique_job_id_1",
      "new_status": "CD",
      "old_status": null,
      "output": "Job output if available",
      "message": "Completed job found",
      "file_hash": "hash_of_file_1",
      "method_name": "Example Method",
      "function_hash": "hash_of_method_function",
      "parameters": {
        "param1": "value1",
        "param2": "value2"
      }
    },
    {
      "job_id": "unique_job_id_2",
      "new_status": "PD",
      "old_status": null,
      "output": null,
      "message": "New job submitted",
      "file_hash": "hash_of_file_2",
      "method_name": "Another Method",
      "function_hash": "hash_of_another_method",
      "parameters": {
        "threshold": 0.5,
        "verbose": true
      }
    }
    // Additional job results...
  ],
  "total_jobs": 2,
  "successful_submissions": 1,
  "failed_submissions": 0,
  "completed_jobs": 1,
  "in_progress_jobs": 1,
  "resubmitted_jobs": 0
}
```

## Job Status Logic

For each job configuration, the system:

1. Finds the most recent hash for the specified method name
2. Checks if a job with the same file, method, and parameters already exists
3. Based on existing job status:
   - `COMPLETED`: Returns the job status and output
   - `IN_PROGRESS`: Returns the job status without output
   - `FAILED`: Submits a new job and returns both statuses
   - `NONEXISTENT`: Submits a new job and returns the status

## Statistics

The response includes the following statistics:

- `total_jobs`: Total number of job configurations processed
- `successful_submissions`: Number of new jobs successfully submitted
- `failed_submissions`: Number of job submissions that failed
- `completed_jobs`: Number of jobs that were already completed
- `in_progress_jobs`: Number of jobs that are currently in progress
- `resubmitted_jobs`: Number of jobs that were resubmitted after failure

## Error Handling

If a method with the specified name doesn't exist, the system returns a result with:

```json
{
  "job_id": null,
  "new_status": null,
  "message": "Method 'Method Name' not found",
  "file_hash": "hash_of_file",
  "method_name": "Method Name",
  "function_hash": null,
  "parameters": {}
}
```

## Usage Examples

### Python Example

```python
import aiohttp
import asyncio
import json

async def simulate_multiple_jobs():
    api_url = "http://localhost:8001"
    
    job_configs = [
        {
            "file_hash": "file_hash_1",
            "method_name": "Example Method",
            "parameters": {"param1": "value1"}
        },
        {
            "file_hash": "file_hash_2",
            "method_name": "Another Method",
            "parameters": {"threshold": 0.5}
        }
    ]
    
    payload = {"jobs": job_configs}
    
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{api_url}/simulate-jobs",
            json=payload
        ) as response:
            return await response.json()

result = asyncio.run(simulate_multiple_jobs())
print(json.dumps(result, indent=2))
```

### curl Example

```bash
curl -X POST "http://localhost:8001/simulate-jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "jobs": [
      {
        "file_hash": "file_hash_1",
        "method_name": "Example Method",
        "parameters": {"param1": "value1"}
      },
      {
        "file_hash": "file_hash_2",
        "method_name": "Another Method",
        "parameters": {"threshold": 0.5}
      }
    ]
  }'
```

## Additional Notes

- The parameters field is optional and can be omitted if the method doesn't require parameters.
- Job configurations are processed in parallel for better performance.
- Each job follows the same logic as the single job endpoint, but all jobs are processed in a single request.
- If a method name doesn't exist, the system will still process other valid job configurations. 