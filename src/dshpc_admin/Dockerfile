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
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Robust worker management with auto-restart\n\
start_worker() {\n\
    echo "[$(date)] Starting monitoring worker..."\n\
    python /app/monitor/worker.py >> /var/log/worker.log 2>&1 &\n\
    WORKER_PID=$!\n\
    echo "[$(date)] Worker started with PID: $WORKER_PID"\n\
}\n\
\n\
# Worker watchdog - runs in background and restarts worker if it dies\n\
worker_watchdog() {\n\
    while true; do\n\
        # Check if worker is running\n\
        if ! pgrep -f "monitor/worker.py" > /dev/null 2>&1; then\n\
            echo "[$(date)] Worker died! Restarting in 5 seconds..." >> /var/log/worker.log\n\
            sleep 5\n\
            start_worker\n\
        fi\n\
        sleep 30  # Check every 30 seconds\n\
    done\n\
}\n\
\n\
echo "Running Django migrations..."\n\
python manage.py migrate --noinput\n\
\n\
echo "Collecting static files..."\n\
python manage.py collectstatic --noinput || true\n\
\n\
# Create log file\n\
touch /var/log/worker.log\n\
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

