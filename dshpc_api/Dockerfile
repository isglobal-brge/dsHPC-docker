FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY dshpc_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY dshpc_api /app/dshpc_api

# Expose port
EXPOSE 8001

# Create a startup script with proper port indication and consistent color scheme
RUN echo '#!/bin/bash' > /app/start.sh && \
    echo '# Define colors for nice output' >> /app/start.sh && \
    echo 'GREEN="\033[1;32m"' >> /app/start.sh && \
    echo 'YELLOW="\033[1;33m"' >> /app/start.sh && \
    echo 'CYAN="\033[1;36m"' >> /app/start.sh && \
    echo 'ORANGE="\033[0;33m"' >> /app/start.sh && \
    echo 'BOLD="\033[1m"' >> /app/start.sh && \
    echo 'NC="\033[0m"' >> /app/start.sh && \
    echo '' >> /app/start.sh && \
    echo 'echo -e "    > Starting API service..."' >> /app/start.sh && \
    echo 'PORT=${DSHPC_API_EXTERNAL_PORT:-8001}' >> /app/start.sh && \
    echo 'uvicorn dshpc_api.main:app --host 0.0.0.0 --port 8001 --reload --log-level warning --no-access-log & PID=$!' >> /app/start.sh && \
    echo 'sleep 2' >> /app/start.sh && \
    echo 'echo -e "${GREEN}>> API service ready!${NC}"' >> /app/start.sh && \
    echo 'echo -e "${GREEN}>> API available on port: ${GREEN}${BOLD}${NC}${BOLD}${YELLOW}${PORT}${NC}"' >> /app/start.sh && \
    echo 'echo -e ""' >> /app/start.sh && \
    echo 'echo -e "${BOLD}All systems are ${GREEN}ready${NC}${BOLD}! Enjoy using ${YELLOW}dsHPC${NC}${BOLD}!"' >> /app/start.sh && \
    echo 'wait $PID' >> /app/start.sh && \
    chmod +x /app/start.sh

# Start the API with reduced logging and better colors
CMD ["/app/start.sh"] 