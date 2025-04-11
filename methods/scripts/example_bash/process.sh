#!/bin/bash
# Example Bash method for processing text files
# This script demonstrates how to create a Bash method that processes a file
# and returns results in JSON format

# Function to output JSON
output_json() {
    local status="$1"
    local message="$2"
    local content="$3"
    local line_count="$4"
    local grep_results="$5"
    
    # Escape special characters in strings for JSON
    content=$(echo "$content" | sed 's/\\/\\\\/g' | sed 's/"/\\"/g' | sed 's/\n/\\n/g' | tr -d '\r')
    message=$(echo "$message" | sed 's/\\/\\\\/g' | sed 's/"/\\"/g' | sed 's/\n/\\n/g' | tr -d '\r')
    grep_results=$(echo "$grep_results" | sed 's/\\/\\\\/g' | sed 's/"/\\"/g' | sed 's/\n/\\n/g' | tr -d '\r')
    
    # Build and output the JSON
    cat << EOF
{
  "status": "$status",
  "message": "$message",
  "processed_content": "$content",
  "line_count": $line_count,
  "grep_results": "$grep_results"
}
EOF
}

# Check if we have required arguments
if [ $# -lt 2 ]; then
    output_json "error" "Usage: $0 <input_file> <params_file>" "" 0 ""
    exit 1
fi

INPUT_FILE="$1"
PARAMS_FILE="$2"

# Check if the input file exists
if [ ! -f "$INPUT_FILE" ]; then
    output_json "error" "Error: Input file $INPUT_FILE does not exist" "" 0 ""
    exit 1
fi

# Check if the params file exists
if [ ! -f "$PARAMS_FILE" ]; then
    output_json "error" "Error: Parameters file $PARAMS_FILE does not exist" "" 0 ""
    exit 1
fi

# Read parameters (using jq which should be installed in the container)
COUNT_LINES=$(jq -r '.count_lines // false' "$PARAMS_FILE")
GREP_PATTERN=$(jq -r '.grep_pattern // ""' "$PARAMS_FILE")

# Read file content
FILE_CONTENT=$(cat "$INPUT_FILE")

# Process based on parameters
LINE_COUNT=0
if [ "$COUNT_LINES" = "true" ]; then
    LINE_COUNT=$(echo "$FILE_CONTENT" | wc -l)
fi

# Grep for pattern if provided
GREP_RESULTS=""
if [ -n "$GREP_PATTERN" ]; then
    GREP_RESULTS=$(grep -n "$GREP_PATTERN" "$INPUT_FILE" || echo "Pattern not found")
fi

# Output results in JSON format
output_json "success" "File processed successfully" "$FILE_CONTENT" "$LINE_COUNT" "$GREP_RESULTS"
exit 0 