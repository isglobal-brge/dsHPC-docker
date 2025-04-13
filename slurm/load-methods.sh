#!/bin/bash
# Script to load all methods in the methods directory into the database

# Define colors for pretty output
GREEN="\033[0;32m"
BLUE="\033[0;34m"
RED="\033[0;31m"
CYAN="\033[1;36m"
YELLOW="\033[1;33m"
PURPLE="\033[0;35m"
ORANGE="\033[0;33m"
BOLD="\033[1m"
NC="\033[0m" # No Color

# Check if METHODS_DIR environment variable is set
if [ -z "${METHODS_DIR}" ]; then
    echo -e "METHODS_DIR environment variable not set, using default /methods"
    METHODS_DIR="/methods"
fi

# First, mark all methods in the database as inactive
echo -e "    > Updating method status..."
result=$(/opt/venvs/api_python/bin/python -c "
import sys
sys.path.append('/app')
from slurm_api.services.method_service import mark_all_methods_inactive
success, message = mark_all_methods_inactive()
print(message)
")
# Always print the "marked X methods as inactive" message in GREEN
echo -e "${GREEN}${result}${NC}"

# Check if the methods directory exists
if [ ! -d "${METHODS_DIR}" ]; then
    echo -e "Methods directory ${METHODS_DIR} does not exist, creating it"
    mkdir -p "${METHODS_DIR}"
    exit 0
fi

# Check if the methods/commands directory exists
if [ ! -d "${METHODS_DIR}/commands" ]; then
    mkdir -p "${METHODS_DIR}/commands" >/dev/null 2>&1
fi

# Check if the methods/scripts directory exists
if [ ! -d "${METHODS_DIR}/scripts" ]; then
    mkdir -p "${METHODS_DIR}/scripts" >/dev/null 2>&1
fi

echo -e "${BOLD}${CYAN}>> Loading methods from commands directory...${NC}"

# Check if there are any json files in the commands directory
if [ ! "$(ls -A ${METHODS_DIR}/commands/*.json 2>/dev/null)" ]; then
    echo -e "No command files found"
    exit 0
fi

# Check for duplicate method names using temporary files
temp_dir=$(mktemp -d)
method_list_file="${temp_dir}/method_list.txt"
has_duplicates=false

touch "${method_list_file}"

for command_file in ${METHODS_DIR}/commands/*.json; do
    # Extract the actual method name from the JSON file - only get the first match
    method_name=$(grep -m 1 -o '"name"[ \t]*:[ \t]*"[^"]*"' "${command_file}" | head -1 | sed 's/.*"name"[ \t]*:[ \t]*"\([^"]*\)".*/\1/')
    
    if [ -z "$method_name" ]; then
        echo -e "${RED}ERROR: Could not extract method name from ${command_file}${NC}"
        rm -rf "${temp_dir}"
        exit 1
    fi
    
    echo -e "    ${BOLD}${CYAN}> Found method:${NC} ${method_name}"
    
    # If this method name is already in the list
    if grep -q "^${method_name}|" "${method_list_file}"; then
        # Get the file that had this method name first
        first_file=$(grep "^${method_name}|" "${method_list_file}" | head -1 | cut -d'|' -f2)
        
        echo -e "${RED}ERROR: Duplicate method name detected: '${method_name}'${NC}"
        echo -e "${RED}  - First occurrence: ${first_file}${NC}"
        echo -e "${RED}  - Duplicate: ${command_file}${NC}"
        has_duplicates=true
    else
        # Add this method name to our list
        echo "${method_name}|${command_file}" >> "${method_list_file}"
    fi
done

if [ "$has_duplicates" = true ]; then
    echo -e "${RED}ERROR: Duplicate method names found. Aborting.${NC}"
    rm -rf "${temp_dir}"
    exit 1
fi

rm -rf "${temp_dir}"

# For each command file in the commands directory
for command_file in ${METHODS_DIR}/commands/*.json; do
    # Get the command name from the file name
    command_name=$(basename "${command_file}" .json)
    echo -e "    ${BOLD}${CYAN}> Processing command:${NC} ${command_name}"
    
    # Check if the script directory exists
    if [ ! -d "${METHODS_DIR}/scripts/${command_name}" ]; then
        echo -e "  Script directory for ${command_name} not found, skipping"
        continue
    fi
    
    # Register the method - using api_python environment
    /opt/venvs/api_python/bin/python /app/slurm/scripts/register_method.py "${METHODS_DIR}/scripts/${command_name}" "${command_file}" >/dev/null
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}      ✓ Method registered successfully!${NC}"
    else
        echo -e "${RED}      ✗ Failed to register method${NC}"
    fi
done

echo -e "\033[1;32m>> Methods loading complete!\033[0m"
echo -e ""
echo -e "\033[1;33m>> Active methods summary:\033[0m"
/opt/venvs/api_python/bin/python -c "
import sys
sys.path.append('/app')
from slurm_api.services.method_service import list_available_methods
active_methods = list_available_methods(active_only=True)
print(f'   \033[1;33mTotal active methods:\033[0m {len(active_methods)}')
for method in active_methods:
    print(f'    + \033[1;33m{method[\"name\"]}\033[0m (hash: {method[\"function_hash\"][:8]}...)')" 