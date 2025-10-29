FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY src/dshpc_admin/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY src/dshpc_admin/ .

# Expose port
EXPOSE 8000

# Create entrypoint script
RUN echo '#!/bin/bash\n\
set -e\n\
echo "Running Django migrations..."\n\
python manage.py migrate --noinput\n\
echo "Collecting static files..."\n\
python manage.py collectstatic --noinput || true\n\
echo "Starting monitoring worker in background..."\n\
python /app/monitor/worker.py &\n\
MONITOR_PID=$!\n\
echo "Monitor worker started with PID: $MONITOR_PID"\n\
echo "Starting Django server..."\n\
python manage.py runserver 0.0.0.0:8000 &\n\
DJANGO_PID=$!\n\
echo "Django server started with PID: $DJANGO_PID"\n\
wait $DJANGO_PID\n\
' > /entrypoint.sh && chmod +x /entrypoint.sh

# Run entrypoint
CMD ["/entrypoint.sh"]

