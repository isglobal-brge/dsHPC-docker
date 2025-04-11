#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

API_URL="http://localhost:8000"

echo -e "${BLUE}Testing Python versions in different environments${NC}"

# Submit the job
echo -e "\n${GREEN}Submitting job to check Python version${NC}"
RESPONSE=$(curl -s -X POST "${API_URL}/submit" \
  -H "Content-Type: application/json" \
  -d '{
    "script": "#!/bin/bash\necho \"Python version in job:\"\npython --version\necho \"PATH in job:\"\necho \$PATH",
    "name": "python_version_test",
    "parameters": {
      "time": "1:00"
    },
    "file_hash": "version_test_file_hash",
    "function_hash": "version_test_function_hash"
  }')

echo "Response: $RESPONSE"
JOB_ID=$(echo $RESPONSE | grep -o '"job_id":"[^"]*' | cut -d'"' -f4)

if [ ! -z "$JOB_ID" ]; then
    echo -e "${GREEN}✓ Job submission successful. Job ID: $JOB_ID${NC}"
    
    # Wait for the job to complete
    echo -e "\n${GREEN}Waiting for job to complete...${NC}"
    sleep 10
    
    # Get job details
    echo -e "\n${GREEN}Getting job output:${NC}"
    JOB_DETAILS=$(curl -s -X GET "${API_URL}/job/${JOB_ID}")
    
    # Display job details
    echo -e "${BLUE}Job details:${NC}"
    echo "$JOB_DETAILS" | python3 -m json.tool
    
    # Compare with container Python versions
    echo -e "\n${GREEN}Comparing with container Python versions:${NC}"
    docker exec -it dshpc-slurm bash -c "echo 'API Python:' && /opt/venvs/api_python/bin/python --version && echo 'System Python:' && python --version"
else
    echo -e "${RED}✗ Job submission failed${NC}"
fi

echo -e "\n${GREEN}Test completed!${NC}" 