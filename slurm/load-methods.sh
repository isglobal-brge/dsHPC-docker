#!/bin/bash
# Script to load all methods in the methods directory into the database

# Check if METHODS_DIR environment variable is set
if [ -z "${METHODS_DIR}" ]; then
    echo "METHODS_DIR environment variable not set, using default /methods"
    METHODS_DIR="/methods"
fi

# First, mark all methods in the database as inactive
echo "Marking all existing methods as inactive..."
/opt/venvs/api_python/bin/python -c "
import sys
sys.path.append('/app')
from slurm_api.services.method_service import mark_all_methods_inactive
success, message = mark_all_methods_inactive()
print(message)
"

# Check if the methods directory exists
if [ ! -d "${METHODS_DIR}" ]; then
    echo "Methods directory ${METHODS_DIR} does not exist, creating it"
    mkdir -p "${METHODS_DIR}"
    exit 0
fi

# Check if the methods/commands directory exists
if [ ! -d "${METHODS_DIR}/commands" ]; then
    echo "Commands directory ${METHODS_DIR}/commands does not exist, creating it"
    mkdir -p "${METHODS_DIR}/commands"
fi

# Check if the methods/scripts directory exists
if [ ! -d "${METHODS_DIR}/scripts" ]; then
    echo "Scripts directory ${METHODS_DIR}/scripts does not exist, creating it"
    mkdir -p "${METHODS_DIR}/scripts"
fi

echo "Loading methods from ${METHODS_DIR}/commands..."

# Check if there are any json files in the commands directory
if [ ! "$(ls -A ${METHODS_DIR}/commands/*.json 2>/dev/null)" ]; then
    echo "No command files found in ${METHODS_DIR}/commands"
    exit 0
fi

# Check for duplicate method names using temporary files
echo "Checking for duplicate method names..."
temp_dir=$(mktemp -d)
method_list_file="${temp_dir}/method_list.txt"
has_duplicates=false

touch "${method_list_file}"

for command_file in ${METHODS_DIR}/commands/*.json; do
    # Extract the actual method name from the JSON file - only get the first match
    method_name=$(grep -m 1 -o '"name"[ \t]*:[ \t]*"[^"]*"' "${command_file}" | head -1 | sed 's/.*"name"[ \t]*:[ \t]*"\([^"]*\)".*/\1/')
    
    if [ -z "$method_name" ]; then
        echo "ERROR: Could not extract method name from ${command_file}"
        rm -rf "${temp_dir}"
        exit 1
    fi
    
    echo "Found method: '${method_name}' in ${command_file}"
    
    # If this method name is already in the list
    if grep -q "^${method_name}|" "${method_list_file}"; then
        # Get the file that had this method name first
        first_file=$(grep "^${method_name}|" "${method_list_file}" | head -1 | cut -d'|' -f2)
        
        echo "ERROR: Duplicate method name detected: '${method_name}'"
        echo "  - First occurrence: ${first_file}"
        echo "  - Duplicate: ${command_file}"
        has_duplicates=true
    else
        # Add this method name to our list
        echo "${method_name}|${command_file}" >> "${method_list_file}"
    fi
done

if [ "$has_duplicates" = true ]; then
    echo "ERROR: Duplicate method names found. Aborting."
    rm -rf "${temp_dir}"
    exit 1
fi

rm -rf "${temp_dir}"

# For each command file in the commands directory
for command_file in ${METHODS_DIR}/commands/*.json; do
    # Get the command name from the file name
    command_name=$(basename "${command_file}" .json)
    echo "Processing command: ${command_name}"
    
    # Check if the script directory exists
    if [ ! -d "${METHODS_DIR}/scripts/${command_name}" ]; then
        echo "Script directory for ${command_name} not found, skipping"
        continue
    fi
    
    # Register the method - using api_python environment
    /opt/venvs/api_python/bin/python /app/slurm/scripts/register_method.py "${METHODS_DIR}/scripts/${command_name}" "${command_file}"
done

echo "Methods loading complete"
echo "Active methods summary:"
/opt/venvs/api_python/bin/python -c "
import sys
sys.path.append('/app')
from slurm_api.services.method_service import list_available_methods
active_methods = list_available_methods(active_only=True)
print(f'Total active methods: {len(active_methods)}')
for method in active_methods:
    print(f'- {method[\"name\"]} (hash: {method[\"function_hash\"][:8]}...)')" 