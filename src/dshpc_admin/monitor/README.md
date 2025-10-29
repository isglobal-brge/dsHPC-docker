# System Monitoring Worker

## Overview

Background monitoring service that collects system data periodically and stores snapshots in MongoDB.

## Architecture

```
Monitor Worker (Background Process)
    ↓ Every 5 seconds
    ├── Collect Container Status (Docker API)
    ├── Collect System Resources (CPU, RAM, Disk)
    ├── Collect Active Job Logs (Slurm + /tmp files)
    ↓
MongoDB Collection: system_snapshots
    ↓
Admin Panel Views (Fast reads, no blocking)
```

## Benefits

- **No blocking**: Admin panel reads from MongoDB (fast)
- **No race conditions**: Worker handles file reading safely
- **Resilient**: If data collection fails, shows last known state
- **Historical**: Can track changes over time
- **Scalable**: Independent from web requests

## Data Collected

### Containers
- Status of all 6 containers (api, slurm, admin, 3x mongodb)
- Running/stopped state
- Updated every 5 seconds

### System Resources
- CPU count
- Memory usage (total, used, available, percentage)
- Disk usage (total, used, available, percentage)
- From slurm container

### Job Logs
- Active jobs only (PD, R, CG states)
- Last 500 lines from:
  - `/app/slurm-{slurm_id}.out` - Slurm output
  - `/tmp/output_{job_id}.txt` - Job output
  - `/tmp/error_{job_id}.txt` - System messages
- Safe reading with error handling

## Snapshot Retention

- Keeps last 100 snapshots
- Auto-cleanup of old snapshots
- ~8 hours of history at 5s intervals

## Configuration

- Interval: 10 seconds (configurable in worker.py)
- Log lines: 500 (configurable with tail -n)
- Retention: 100 snapshots (configurable)
- Dashboard refresh: 15 seconds

## Startup

Worker starts automatically with the admin container via entrypoint script.

