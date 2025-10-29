FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY src/dshpc_admin/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY src/dshpc_admin/ .

# Collect static files
RUN python manage.py collectstatic --noinput || true

# Expose port
EXPOSE 8000

# Run Django development server
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]

