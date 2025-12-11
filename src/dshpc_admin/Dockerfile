FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY src/dshpc_admin/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY src/dshpc_admin/ .

# Expose port
EXPOSE 8000

# Create robust entrypoint script with worker auto-restart
# NOTE: Using /proc check instead of pgrep (not available in slim image)
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Robust worker management with auto-restart\n\
# The worker itself is now self-healing (v2.0), but we still need external watchdog\n\
# in case the Python process itself crashes\n\
\n\
is_worker_running() {\n\
    # Check /proc for worker process (pgrep not available in slim)\n\
    for p in /proc/[0-9]*/cmdline; do\n\
        if [ -f "$p" ] && grep -q "monitor/worker.py" "$p" 2>/dev/null; then\n\
            return 0\n\
        fi\n\
    done\n\
    return 1\n\
}\n\
\n\
start_worker() {\n\
    echo "[$(date)] Starting monitoring worker v2.0 (robust)..."\n\
    python /app/monitor/worker.py >> /var/log/worker.log 2>&1 &\n\
    WORKER_PID=$!\n\
    echo "[$(date)] Worker started with PID: $WORKER_PID"\n\
    echo $WORKER_PID > /var/run/worker.pid\n\
}\n\
\n\
# Worker watchdog - runs in background and restarts worker if it dies\n\
worker_watchdog() {\n\
    echo "[$(date)] Watchdog started - monitoring worker process"\n\
    local consecutive_restarts=0\n\
    local max_restarts=10\n\
    local restart_window_start=$(date +%s)\n\
    \n\
    while true; do\n\
        sleep 30  # Check every 30 seconds\n\
        \n\
        if ! is_worker_running; then\n\
            echo "[$(date)] Worker not running! Checking restart count..." >> /var/log/worker.log\n\
            \n\
            # Reset counter if more than 10 minutes since first restart\n\
            current_time=$(date +%s)\n\
            if [ $((current_time - restart_window_start)) -gt 600 ]; then\n\
                consecutive_restarts=0\n\
                restart_window_start=$current_time\n\
            fi\n\
            \n\
            consecutive_restarts=$((consecutive_restarts + 1))\n\
            \n\
            if [ $consecutive_restarts -le $max_restarts ]; then\n\
                echo "[$(date)] Restart #$consecutive_restarts - starting worker..." >> /var/log/worker.log\n\
                sleep 5  # Brief delay before restart\n\
                start_worker\n\
            else\n\
                echo "[$(date)] CRITICAL: Too many restarts ($consecutive_restarts). Waiting 5 minutes..." >> /var/log/worker.log\n\
                sleep 300  # Wait 5 minutes before trying again\n\
                consecutive_restarts=0\n\
                restart_window_start=$(date +%s)\n\
            fi\n\
        else\n\
            # Worker is running - reset restart counter if stable for 5+ minutes\n\
            current_time=$(date +%s)\n\
            if [ $((current_time - restart_window_start)) -gt 300 ] && [ $consecutive_restarts -gt 0 ]; then\n\
                echo "[$(date)] Worker stable for 5+ minutes. Resetting restart counter." >> /var/log/worker.log\n\
                consecutive_restarts=0\n\
            fi\n\
        fi\n\
    done\n\
}\n\
\n\
echo "Running Django migrations..."\n\
python manage.py migrate --noinput\n\
\n\
echo "Collecting static files..."\n\
python manage.py collectstatic --noinput || true\n\
\n\
# Create log file and pid directory\n\
touch /var/log/worker.log\n\
mkdir -p /var/run\n\
\n\
# Start worker\n\
start_worker\n\
\n\
# Start watchdog in background\n\
worker_watchdog &\n\
WATCHDOG_PID=$!\n\
echo "Worker watchdog started with PID: $WATCHDOG_PID"\n\
\n\
echo "Starting Django server..."\n\
exec python manage.py runserver 0.0.0.0:8000\n\
' > /entrypoint.sh && chmod +x /entrypoint.sh

# Run entrypoint
CMD ["/entrypoint.sh"]

