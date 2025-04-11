FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY dshpc_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY dshpc_api /app/dshpc_api

# Expose port
EXPOSE 8001

# Start the API
CMD ["uvicorn", "dshpc_api.main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"] 