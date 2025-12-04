# Configuration Directory

This directory contains runtime configuration files for the dsHPC system.

## dshpc.conf

The dsHPC configuration file defines default resource settings for jobs.

```bash
# Default CPUs per task (when method doesn't specify)
DEFAULT_CPUS_PER_TASK=2

# Default memory per task in MB (optional)
# DEFAULT_MEMORY_MB=4000

# Default time limit (optional, format: HH:MM:SS)
# DEFAULT_TIME_LIMIT=01:00:00
```

Methods can override these defaults by specifying `resources` in their `method.yaml`:

```yaml
name: my_method
resources:
  cpus: 4
  memory_mb: 8000
  time_limit: "02:00:00"
```

## slurm.conf

The Slurm configuration file (`slurm.conf`) defines the cluster configuration.

- If you provide a custom `slurm.conf` file in this directory, it will be used by the system
- If no `slurm.conf` is present, a default configuration will be created automatically

### Default Configuration

If no custom configuration is provided, the system creates a default `slurm.conf` with:
- ClusterName: dshpc-slurm
- Single node: localhost with 8 CPUs
- Debug partition with unlimited time
- Debug logging enabled

### Custom Configuration

To use a custom Slurm configuration:
1. Place your `slurm.conf` file in this directory
2. The file will be copied to `/etc/slurm/slurm.conf` when the container starts
3. Restart the container for changes to take effect

## Environment Configuration

Environment configuration files (python.json, r.json, system_deps.json) have been moved to the `environment/` directory. See the environment directory README for details.