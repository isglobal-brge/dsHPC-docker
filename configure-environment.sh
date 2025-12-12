#!/bin/bash

# Colors for output
GREEN="\033[1;32m"
BLUE="\033[1;34m"
YELLOW="\033[1;33m"
RED="\033[1;31m"
CYAN="\033[1;36m"
BOLD="\033[1m"
NC="\033[0m" # No Color

# Configuration file path
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/environment-config.json"

# Function to extract JSON values
get_config_value() {
    local key=$1
    if command -v jq &> /dev/null; then
        jq -r ".$key" "$CONFIG_FILE" 2>/dev/null || echo ""
    else
        # Fallback for systems without jq
        grep "\"$key\"" "$CONFIG_FILE" | sed 's/.*"'$key'"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/'
    fi
}

# Load configuration
ENV_NAME=$(get_config_value "environment_name")
DISPLAY_NAME=$(get_config_value "display_name")
DOCKER_PREFIX=$(get_config_value "docker_stack_prefix")
BASE_REPO=$(get_config_value "base_repository")
DEFAULT_PORT=$(get_config_value "default_api_port")
DEFAULT_ADMIN_PORT=$(get_config_value "default_admin_port")
DESCRIPTION=$(get_config_value "description")

# Fallback values if config file is missing or malformed
ENV_NAME=${ENV_NAME:-"dsHPC"}
DISPLAY_NAME=${DISPLAY_NAME:-"High-Performance Computing Environment"}
DOCKER_PREFIX=${DOCKER_PREFIX:-"dshpc"}
BASE_REPO=${BASE_REPO:-"https://github.com/isglobal-brge/dsHPC-core.git"}
DEFAULT_PORT=${DEFAULT_PORT:-8001}
DEFAULT_ADMIN_PORT=${DEFAULT_ADMIN_PORT:-8002}
DESCRIPTION=${DESCRIPTION:-"High-Performance Computing Environment"}

# Generate random API key
generate_api_key() {
    if command -v openssl &> /dev/null; then
        openssl rand -base64 32 | tr -d "=+/" | cut -c1-32
    else
        # Fallback using /dev/urandom
        cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1
    fi
}

# Print banner
print_banner() {
    local title="$DISPLAY_NAME"
    local desc="$DESCRIPTION"
    
    # Configuration
    local max_line_width=60  # Max characters per line
    local max_desc_lines=3   # Max lines for description
    local box_width=64       # Fixed box width
    local content_width=$((box_width - 2))
    
    # Truncate title if too long
    if [[ ${#title} -gt 60 ]]; then
        title="${title:0:57}..."
    fi
    
    # Title
    local title_len=${#title}
    local title_padding=$(( (content_width - title_len) / 2 ))
    
    # Split description into lines if too long
    local desc_lines=()
    if [[ ${#desc} -gt $max_line_width ]]; then
        # Split into words
        local words=($desc)
        local current_line=""
        
        for word in "${words[@]}"; do
            if [[ ${#current_line} -eq 0 ]]; then
                current_line="$word"
            elif [[ $((${#current_line} + 1 + ${#word})) -le $max_line_width ]]; then
                current_line="$current_line $word"
            else
                # Line would be too long, save current and start new
                desc_lines+=("$current_line")
                current_line="$word"
                
                # Check if we've reached max lines
                if [[ ${#desc_lines[@]} -ge $max_desc_lines ]]; then
                    desc_lines[$((max_desc_lines - 1))]="${desc_lines[$((max_desc_lines - 1))]}..."
                    break
                fi
            fi
        done
        
        # Add last line if we haven't reached limit
        if [[ ${#desc_lines[@]} -lt $max_desc_lines ]] && [[ -n "$current_line" ]]; then
            desc_lines+=("$current_line")
        fi
    else
        desc_lines=("$desc")
    fi
    
    # Generate horizontal line
    local hline=$(printf '─%.0s' $(seq 1 $box_width))
    
    # Print box
    echo -e "${BLUE}┌${hline}┐${NC}"
    
    # Print title (centered)
    printf "${BLUE}│${NC} %*s${BOLD}${CYAN}%s${NC}%*s ${BLUE}│${NC}\n" \
           $title_padding "" "$title" $((content_width - title_len - title_padding)) ""
    
    # Print empty separator line between title and description
    printf "${BLUE}│${NC} %*s ${BLUE}│${NC}\n" $content_width ""
    
    # Print description lines (centered)
    for line in "${desc_lines[@]}"; do
        local line_len=${#line}
        local line_padding=$(( (content_width - line_len) / 2 ))
        printf "${BLUE}│${NC} %*s%s%*s ${BLUE}│${NC}\n" \
               $line_padding "" "$line" $((content_width - line_len - line_padding)) ""
    done
    
    echo -e "${BLUE}└${hline}┘${NC}"
    echo
}

# Validate environment directory
validate_environment() {
    echo -e "${CYAN}🔍 Validating environment directory...${NC}"
    
    local errors=0
    
    if [[ ! -d "environment" ]]; then
        echo -e "${RED}❌ Error: 'environment/' directory not found${NC}"
        errors=$((errors + 1))
    else
        echo -e "${GREEN}✓ Found environment/ directory${NC}"
        
        # Check required files
        local required_files=("python.json" "r.json" "system_deps.json")
        for file in "${required_files[@]}"; do
            if [[ ! -f "environment/$file" ]]; then
                echo -e "${RED}❌ Missing: environment/$file${NC}"
                errors=$((errors + 1))
            else
                echo -e "${GREEN}✓ Found environment/$file${NC}"
            fi
        done
        
        # Check methods directory
        if [[ ! -d "environment/methods" ]]; then
            echo -e "${YELLOW}⚠️  Warning: environment/methods/ not found, creating...${NC}"
            mkdir -p environment/methods/{commands,scripts}
            echo -e "${GREEN}✓ Created environment/methods/ structure${NC}"
        else
            echo -e "${GREEN}✓ Found environment/methods/ directory${NC}"
        fi
    fi
    
    if [[ $errors -gt 0 ]]; then
        echo
        echo -e "${RED}Please ensure you have the following structure:${NC}"
        echo -e "${YELLOW}environment/${NC}"
        echo -e "${YELLOW}├── methods/${NC}"
        echo -e "${YELLOW}│   ├── commands/${NC}"
        echo -e "${YELLOW}│   └── scripts/${NC}"
        echo -e "${YELLOW}├── python.json${NC}"
        echo -e "${YELLOW}├── r.json${NC}"
        echo -e "${YELLOW}└── system_deps.json${NC}"
        echo
        exit 1
    fi
    
    echo
}

# Clone or update repository
setup_repository() {
    echo -e "${CYAN}📥 Setting up $ENV_NAME repository...${NC}"
    
    # =============================================================================
    # PRESERVATION POLICY:
    # - PRESERVED (User configuration): environment/, .env, .gitignore, config/slurm.conf
    # - UPDATED (Repository files): src/, docker-compose.yml, Dockerfiles, etc.
    # =============================================================================
    
    # Save user's files that should be preserved
    local user_env_exists=false
    local user_env_file_exists=false
    local user_gitignore_exists=false
    local user_slurm_conf_exists=false
    local override_slurm_conf=false
    local temp_user_env=""
    local temp_env_file=""
    local temp_gitignore_file=""
    local temp_slurm_conf_file=""
    
    echo -e "${YELLOW}Preserving your custom configuration files...${NC}"
    
    # Preserve environment/ directory (user's methods and dependencies)
    if [[ -d "environment" ]]; then
        user_env_exists=true
        temp_user_env=$(mktemp -d)
        echo -e "${CYAN}  • Preserving environment/ directory (user configuration)${NC}"
        cp -r environment/* "$temp_user_env/" 2>/dev/null || true
    fi
    
    # Preserve .env file (API keys and local configuration)
    if [[ -f ".env" ]]; then
        user_env_file_exists=true
        temp_env_file=$(mktemp)
        echo -e "${CYAN}  • Preserving .env file (API keys)${NC}"
        cp ".env" "$temp_env_file"
    fi
    
    # Preserve .gitignore (user's local ignore rules)
    if [[ -f ".gitignore" ]]; then
        user_gitignore_exists=true
        temp_gitignore_file=$(mktemp)
        echo -e "${CYAN}  • Preserving .gitignore (local ignore rules)${NC}"
        cp ".gitignore" "$temp_gitignore_file"
    fi
    
    # Check for existing Slurm configuration
    if [[ -f "config/slurm.conf" ]]; then
        user_slurm_conf_exists=true
        echo -e "${YELLOW}⚠️  Existing Slurm configuration detected: config/slurm.conf${NC}"
        echo -e "${CYAN}This file may contain custom resource settings.${NC}"
        read -p "Do you want to override it with repository defaults? (y/N): " -n 1 -r
        echo
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            override_slurm_conf=true
            echo -e "${YELLOW}  • Will update config/slurm.conf from repository${NC}"
        else
            temp_slurm_conf_file=$(mktemp)
            echo -e "${CYAN}  • Preserving config/slurm.conf (custom Slurm configuration)${NC}"
            cp "config/slurm.conf" "$temp_slurm_conf_file"
        fi
    fi
    
    # Check if base repository needs updating by cloning to temp directory and comparing
    echo -e "${YELLOW}Checking base repository for updates...${NC}"
    local temp_check_dir=$(mktemp -d)
    local needs_update=false
    
    # Always clone fresh to check for updates
    git clone "$BASE_REPO" "$temp_check_dir/${ENV_NAME}-docker-check" &>/dev/null
    
    if [[ $? -eq 0 ]]; then
        # If we have existing repository files, compare with fresh clone
        if [[ -f "docker-compose.yml" ]] || [[ -f "Dockerfile" ]] || [[ -d ".git" ]]; then
            echo -e "${CYAN}Comparing with base repository...${NC}"
            # Simple check: compare a key file's modification time or content
            if [[ -f "docker-compose.yml" ]] && [[ -f "$temp_check_dir/${ENV_NAME}-docker-check/docker-compose.yml" ]]; then
                if ! cmp -s "docker-compose.yml" "$temp_check_dir/${ENV_NAME}-docker-check/docker-compose.yml"; then
                    needs_update=true
                    echo -e "${YELLOW}Base repository has updates available${NC}"
                else
                    echo -e "${GREEN}✓ Repository is up to date${NC}"
                fi
            else
                needs_update=true
                echo -e "${YELLOW}Base repository files missing, will update${NC}"
            fi
        else
            needs_update=true
            echo -e "${YELLOW}No base repository files found, will clone${NC}"
        fi
        
        rm -rf "$temp_check_dir"
    else
        echo -e "${RED}❌ Could not access base repository for update check${NC}"
        rm -rf "$temp_check_dir"
        exit 1
    fi
    
    if [[ "$needs_update" == true ]]; then
        echo -e "Updating from: $BASE_REPO"
        echo -e "${YELLOW}Updating repository contents to current directory...${NC}"
        
        # Clone to temporary directory first
        local temp_dir=$(mktemp -d)
        git clone "$BASE_REPO" "$temp_dir/${ENV_NAME}-docker"
        
        if [[ $? -eq 0 ]]; then
            # Save the original directory
            local original_dir=$(pwd)
            
            # CRITICAL: Remove directories that MUST be fully updated from repository
            # This ensures clean update without merge conflicts
            echo -e "${YELLOW}Removing old repository files to ensure clean update...${NC}"
            
            # Remove src/ completely to ensure full update
            if [[ -d "$original_dir/src" ]]; then
                echo -e "${CYAN}  • Removing old src/ directory${NC}"
                rm -rf "$original_dir/src"
            fi
            
            # Remove config/ directory if user wants to override slurm.conf or it doesn't exist
            if [[ "$override_slurm_conf" == true ]] || [[ "$user_slurm_conf_exists" == false ]]; then
                if [[ -d "$original_dir/config" ]]; then
                    echo -e "${CYAN}  • Removing old config/ directory${NC}"
                    rm -rf "$original_dir/config"
                fi
            fi
            
            # Move ALL contents from temp directory to current directory
            cd "$temp_dir/${ENV_NAME}-docker"
            
            echo -e "${YELLOW}Copying repository files...${NC}"
            
            # Move regular files and directories
            for item in *; do
                if [[ -e "$item" ]]; then
                    echo -e "${CYAN}  • Updating: $item${NC}"
                    # Use cp -rf to handle directories properly, then remove source
                    cp -rf "$item" "$original_dir/"
                fi
            done
            
            # Also move hidden files (excluding .git, .env, .gitignore - already preserved)
            for item in .[^.]*; do
                if [[ "$item" != ".git" && "$item" != ".env" && "$item" != ".gitignore" && -e "$item" ]]; then
                    echo -e "${CYAN}  • Updating: $item${NC}"
                    cp -rf "$item" "$original_dir/"
                fi
            done
            
            # Return to original directory
            cd "$original_dir"
            rm -rf "$temp_dir"
            echo -e "${GREEN}✓ Repository contents updated in current directory${NC}"
        else
            echo -e "${RED}❌ Failed to clone repository${NC}"
            rm -rf "$temp_dir"
            exit 1
        fi
    else
        echo -e "${GREEN}✓ Base repository is already up to date${NC}"
    fi
    
    # Restore preserved user files
    echo -e "${YELLOW}Restoring your preserved configuration...${NC}"
    
    # Restore environment/ directory (user's methods and dependencies)
    if [[ "$user_env_exists" == true ]]; then
        echo -e "${CYAN}  • Restoring environment/ directory${NC}"
        # Copy user's environment files over the repo's defaults
        cp -r "$temp_user_env"/* environment/ 2>/dev/null || true
        rm -rf "$temp_user_env"
    fi
    
    # Restore .env file (API keys and local configuration)
    if [[ "$user_env_file_exists" == true ]]; then
        echo -e "${CYAN}  • Restoring .env file${NC}"
        cp "$temp_env_file" ".env"
        rm -f "$temp_env_file"
    fi
    
    # Restore .gitignore (user's local ignore rules)
    if [[ "$user_gitignore_exists" == true ]]; then
        echo -e "${CYAN}  • Restoring .gitignore${NC}"
        cp "$temp_gitignore_file" ".gitignore"
        rm -f "$temp_gitignore_file"
    fi
    
    # Restore config/slurm.conf if user chose to preserve it
    if [[ "$user_slurm_conf_exists" == true ]] && [[ "$override_slurm_conf" == false ]]; then
        # Ensure config directory exists
        mkdir -p config
        echo -e "${CYAN}  • Restoring config/slurm.conf (custom Slurm configuration)${NC}"
        cp "$temp_slurm_conf_file" "config/slurm.conf"
        rm -f "$temp_slurm_conf_file"
    fi
    
    echo -e "${GREEN}✓ User configuration restored successfully${NC}"
    echo
    echo -e "${BOLD}${CYAN}Update Summary:${NC}"
    echo -e "${GREEN}✓ UPDATED from repository:${NC}"
    echo -e "  • src/ directory (all source code)"
    echo -e "  • docker-compose.yml"
    echo -e "  • Dockerfiles and build scripts"
    if [[ "$override_slurm_conf" == true ]] || [[ "$user_slurm_conf_exists" == false ]]; then
        echo -e "  • config/ directory (including slurm.conf)"
    else
        echo -e "  • config/ directory (README and templates)"
    fi
    echo
    echo -e "${YELLOW}✓ PRESERVED (your configuration):${NC}"
    echo -e "  • environment/ directory (methods, dependencies)"
    echo -e "  • .env file (API keys)"
    echo -e "  • .gitignore (local ignore rules)"
    if [[ "$user_slurm_conf_exists" == true ]] && [[ "$override_slurm_conf" == false ]]; then
        echo -e "  • config/slurm.conf (custom Slurm configuration)"
    fi
    
    echo
}

# Setup environment configuration
setup_environment() {
    echo -e "${CYAN}📋 Setting up environment configuration...${NC}"
    
    # Check if environment directory exists
    if [[ ! -d "environment" ]]; then
        echo -e "${YELLOW}Creating environment directory structure...${NC}"
        mkdir -p environment/methods/{commands,scripts}
        
        # Create default configuration files if they don't exist
        if [[ ! -f "environment/python.json" ]]; then
            echo '{"python_version": "3.10.0", "libraries": {}}' > environment/python.json
        fi
        if [[ ! -f "environment/r.json" ]]; then
            echo '{"r_version": "4.3.0", "packages": {}}' > environment/r.json
        fi
        if [[ ! -f "environment/system_deps.json" ]]; then
            echo '{"apt_packages": []}' > environment/system_deps.json
        fi
        echo -e "${GREEN}✓ Environment directory created with defaults${NC}"
    else
        echo -e "${GREEN}✓ Environment configuration ready${NC}"
    fi
    
    # Config directory is optional
    if [[ -d "config" ]]; then
        echo -e "${GREEN}✓ Config directory found${NC}"
    fi
    
    echo
}

# Generate environment file
generate_env_file() {
    echo -e "${CYAN}⚙️  Generating environment configuration...${NC}"
    
    # Check if .env already exists
    if [[ -f ".env" ]]; then
        echo -e "${GREEN}✓ Environment file already exists, preserving it${NC}"
        echo -e "${CYAN}💡 To regenerate API key, delete .env and run setup again${NC}"
        
        # Extract current API key for display
        local current_api_key=$(grep "API_KEY=" .env | cut -d'=' -f2)
        if [[ -n "$current_api_key" ]]; then
            echo -e "${YELLOW}📝 Current API Key: $current_api_key${NC}"
        fi
    else
        local api_key=$(generate_api_key)
        local admin_password=$(generate_api_key)
        # Convert to uppercase in a bash 3.2 compatible way
        local docker_prefix_upper=$(echo "$DOCKER_PREFIX" | tr '[:lower:]' '[:upper:]')
        
        # Create .env file in current directory
        cat > ".env" << EOF
# $DISPLAY_NAME Environment Configuration
# Generated on $(date)

# API Configuration
API_EXTERNAL_PORT=$DEFAULT_PORT
API_KEY=$api_key

# Admin Panel Configuration
ADMIN_EXTERNAL_PORT=$DEFAULT_ADMIN_PORT
ADMIN_PASSWORD=$admin_password

# Docker Stack Configuration
COMPOSE_PROJECT_NAME=${DOCKER_PREFIX}

# Logging Configuration
LOG_LEVEL=WARNING
EOF
        
        echo -e "${GREEN}✓ Environment file created: .env${NC}"
        echo -e "${YELLOW}📝 API Port: $DEFAULT_PORT${NC}"
        echo -e "${YELLOW}📝 Admin Panel Port: $DEFAULT_ADMIN_PORT${NC}"
        echo -e "${YELLOW}📝 Generated API Key: $api_key${NC}"
        echo -e "${YELLOW}📝 Generated Admin Password: $admin_password${NC}"
        echo -e "${RED}⚠️  IMPORTANT: Save the admin password securely!${NC}"
        echo -e "${CYAN}💡 You can modify these settings in .env${NC}"
    fi
    
    echo
}

# Get nested config value (for resource_requirements)
get_nested_config_value() {
    local key=$1
    local subkey=$2
    if command -v jq &> /dev/null; then
        jq -r ".$key.$subkey // empty" "$CONFIG_FILE" 2>/dev/null || echo ""
    else
        echo ""
    fi
}

# =============================================================================
# GPU DETECTION AND CONFIGURATION
# =============================================================================
# Comprehensive GPU detection for all supported platforms:
# - Linux + NVIDIA (full support via nvidia-container-toolkit)
# - Linux + AMD (full support via ROCm/amd-container-toolkit)
# - Windows WSL2 + NVIDIA (full support via Docker Desktop)
# - macOS (no GPU support in containers - Metal not compatible)
# =============================================================================

# Global GPU configuration variables
GPU_TYPE="none"           # none, nvidia, amd, intel
GPU_COUNT=0
GPU_NAMES=""
GPU_MEMORY=""
GPU_DOCKER_READY=false
GPU_CUDA_VERSION=""
GPU_ROCM_VERSION=""

detect_gpu_comprehensive() {
    echo -e "${CYAN}🔍 Detecting GPU configuration...${NC}"

    local os_type=$(uname -s)
    local is_wsl=false

    # Detect WSL
    if [[ -f /proc/version ]] && grep -qi "microsoft\|wsl" /proc/version 2>/dev/null; then
        is_wsl=true
        echo -e "${CYAN}  Running in WSL2 environment${NC}"
    fi

    # ==========================================================================
    # macOS Detection (No container GPU support)
    # ==========================================================================
    if [[ "$os_type" == "Darwin" ]]; then
        echo -e "${YELLOW}  macOS detected - GPU passthrough to Docker containers is not supported${NC}"

        # Still detect for informational purposes
        if system_profiler SPDisplaysDataType 2>/dev/null | grep -q "Apple M"; then
            local apple_gpu=$(system_profiler SPDisplaysDataType 2>/dev/null | grep "Chipset Model" | head -1 | sed 's/.*: //')
            echo -e "${CYAN}  Host GPU: ${apple_gpu} (Metal - not available in containers)${NC}"
            echo -e "${YELLOW}  Note: Apple Silicon uses Metal framework, incompatible with CUDA/ROCm${NC}"
        fi

        GPU_TYPE="none"
        GPU_COUNT=0
        GPU_DOCKER_READY=false
        return 0
    fi

    # ==========================================================================
    # NVIDIA GPU Detection (Linux / WSL2)
    # ==========================================================================
    if command -v nvidia-smi &> /dev/null; then
        echo -e "${CYAN}  Checking NVIDIA GPU...${NC}"

        # Get GPU count
        local nvidia_count=$(nvidia-smi --query-gpu=count --format=csv,noheader 2>/dev/null | head -1)

        if [[ -n "$nvidia_count" ]] && [[ "$nvidia_count" -gt 0 ]]; then
            GPU_TYPE="nvidia"
            GPU_COUNT=$nvidia_count
            GPU_NAMES=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | tr '\n' ', ' | sed 's/,$//')
            GPU_MEMORY=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader 2>/dev/null | head -1)

            # Get CUDA version
            GPU_CUDA_VERSION=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader 2>/dev/null | head -1)

            echo -e "${GREEN}  ✓ NVIDIA GPU(s) detected: ${GPU_COUNT}x ${GPU_NAMES}${NC}"
            echo -e "${GREEN}    Memory: ${GPU_MEMORY}, Driver: ${GPU_CUDA_VERSION}${NC}"

            # Test Docker GPU passthrough
            test_docker_gpu_nvidia
        fi
    fi

    # ==========================================================================
    # AMD GPU Detection (Linux only, not WSL)
    # ==========================================================================
    if [[ "$GPU_TYPE" == "none" ]] && [[ "$is_wsl" == false ]]; then
        # Check for AMD GPU via /dev/kfd (ROCm kernel interface)
        if [[ -e /dev/kfd ]]; then
            echo -e "${CYAN}  Checking AMD GPU (ROCm)...${NC}"

            # Try rocm-smi if available
            if command -v rocm-smi &> /dev/null; then
                local amd_count=$(rocm-smi --showid 2>/dev/null | grep -c "GPU" || echo 0)
                if [[ "$amd_count" -gt 0 ]]; then
                    GPU_TYPE="amd"
                    GPU_COUNT=$amd_count
                    GPU_NAMES=$(rocm-smi --showproductname 2>/dev/null | grep "Card" | head -1 | sed 's/.*: //' || echo "AMD GPU")

                    # Get ROCm version
                    GPU_ROCM_VERSION=$(cat /opt/rocm/.info/version 2>/dev/null || echo "unknown")

                    echo -e "${GREEN}  ✓ AMD GPU(s) detected: ${GPU_COUNT}x ${GPU_NAMES}${NC}"
                    echo -e "${GREEN}    ROCm version: ${GPU_ROCM_VERSION}${NC}"

                    test_docker_gpu_amd
                fi
            elif [[ -d /dev/dri ]]; then
                # Fallback: check render nodes
                local render_count=$(ls /dev/dri/renderD* 2>/dev/null | wc -l)
                if [[ "$render_count" -gt 0 ]]; then
                    # Check if it's actually AMD via lspci
                    if command -v lspci &> /dev/null && lspci | grep -qi "amd.*vga\|radeon"; then
                        GPU_TYPE="amd"
                        GPU_COUNT=$render_count
                        GPU_NAMES="AMD GPU (detected via /dev/dri)"
                        echo -e "${GREEN}  ✓ AMD GPU detected via render nodes${NC}"
                        test_docker_gpu_amd
                    fi
                fi
            fi
        fi
    fi

    # ==========================================================================
    # Intel GPU Detection (Experimental)
    # ==========================================================================
    if [[ "$GPU_TYPE" == "none" ]] && [[ "$is_wsl" == false ]]; then
        if command -v xpu-smi &> /dev/null || [[ -d /dev/dri ]] && command -v lspci &> /dev/null && lspci | grep -qi "intel.*vga"; then
            echo -e "${YELLOW}  Intel GPU detected - experimental support only${NC}"
            # Not enabling by default due to immature container support
        fi
    fi

    # ==========================================================================
    # Summary
    # ==========================================================================
    if [[ "$GPU_DOCKER_READY" == true ]]; then
        echo -e "${GREEN}  ✓ GPU is ready for Docker containers${NC}"
    elif [[ "$GPU_TYPE" != "none" ]]; then
        echo -e "${YELLOW}  ⚠️  GPU detected but Docker passthrough not available${NC}"
        echo -e "${YELLOW}     See GPU_USAGE.md for setup instructions${NC}"
    else
        echo -e "${CYAN}  No compatible GPU detected for container use${NC}"
    fi

    # Export for other functions
    export GPU_TYPE
    export GPU_COUNT
    export GPU_NAMES
    export GPU_MEMORY
    export GPU_DOCKER_READY
    export GPU_CUDA_VERSION
    export GPU_ROCM_VERSION
}

test_docker_gpu_nvidia() {
    echo -e "${CYAN}  Testing Docker NVIDIA GPU passthrough...${NC}"

    if ! command -v docker &> /dev/null; then
        echo -e "${YELLOW}    Docker not found, skipping GPU test${NC}"
        GPU_DOCKER_READY=false
        return 1
    fi

    # Check if nvidia-container-toolkit is configured
    local docker_runtime_check=$(docker info 2>/dev/null | grep -i "nvidia" || echo "")

    # Try running a minimal NVIDIA container
    # Use a lightweight test that doesn't require pulling large images
    local gpu_test_result
    gpu_test_result=$(docker run --rm --gpus all \
        nvidia/cuda:12.0-base-ubuntu22.04 \
        nvidia-smi --query-gpu=name --format=csv,noheader 2>&1)
    local test_exit=$?

    if [[ $test_exit -eq 0 ]] && [[ -n "$gpu_test_result" ]] && [[ ! "$gpu_test_result" =~ "error" ]]; then
        echo -e "${GREEN}    ✓ Docker GPU passthrough working${NC}"
        GPU_DOCKER_READY=true
        return 0
    else
        echo -e "${YELLOW}    ✗ Docker GPU passthrough failed${NC}"

        # Provide specific guidance
        if [[ -z "$docker_runtime_check" ]]; then
            echo -e "${YELLOW}    nvidia-container-toolkit may not be installed${NC}"
            echo -e "${CYAN}    Install with: ${NC}"
            echo -e "${CYAN}      curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg${NC}"
            echo -e "${CYAN}      sudo apt-get update && sudo apt-get install -y nvidia-container-toolkit${NC}"
            echo -e "${CYAN}      sudo nvidia-ctk runtime configure --runtime=docker${NC}"
            echo -e "${CYAN}      sudo systemctl restart docker${NC}"
        fi

        GPU_DOCKER_READY=false
        return 1
    fi
}

test_docker_gpu_amd() {
    echo -e "${CYAN}  Testing Docker AMD GPU passthrough...${NC}"

    if ! command -v docker &> /dev/null; then
        echo -e "${YELLOW}    Docker not found, skipping GPU test${NC}"
        GPU_DOCKER_READY=false
        return 1
    fi

    # Test ROCm container access
    local gpu_test_result
    gpu_test_result=$(docker run --rm \
        --device=/dev/kfd \
        --device=/dev/dri \
        --security-opt seccomp=unconfined \
        rocm/rocm-terminal:latest \
        rocm-smi --showid 2>&1)
    local test_exit=$?

    if [[ $test_exit -eq 0 ]] && [[ "$gpu_test_result" =~ "GPU" ]]; then
        echo -e "${GREEN}    ✓ Docker AMD GPU passthrough working${NC}"
        GPU_DOCKER_READY=true
        return 0
    else
        echo -e "${YELLOW}    ✗ Docker AMD GPU passthrough failed${NC}"
        echo -e "${YELLOW}    Ensure ROCm kernel driver is installed on host${NC}"
        echo -e "${CYAN}    See: https://rocm.docs.amd.com/projects/install-on-linux/en/latest/${NC}"
        GPU_DOCKER_READY=false
        return 1
    fi
}

# Detect system resources
detect_system_resources() {
    echo -e "${CYAN}🔍 Detecting system resources...${NC}"

    local cpus=8
    local memory_mb=16000

    # Detect CPUs
    if [[ "$(uname)" == "Darwin" ]]; then
        # macOS
        cpus=$(sysctl -n hw.ncpu 2>/dev/null || echo 8)
        # Get memory in bytes and convert to MB
        memory_bytes=$(sysctl -n hw.memsize 2>/dev/null || echo 17179869184)
        memory_mb=$((memory_bytes / 1024 / 1024))
    else
        # Linux
        cpus=$(nproc 2>/dev/null || grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 8)
        # Get memory in KB and convert to MB
        memory_kb=$(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}' || echo 16777216)
        memory_mb=$((memory_kb / 1024))
    fi

    # Docker Desktop typically has less memory than the system
    # Check Docker's actual memory limit
    if command -v docker &> /dev/null; then
        docker_memory=$(docker run --rm alpine sh -c 'cat /proc/meminfo | grep MemTotal' 2>/dev/null | awk '{print $2}' || echo 0)
        if [[ $docker_memory -gt 0 ]]; then
            docker_memory_mb=$((docker_memory / 1024))
            # Use Docker's memory if it's less than system memory
            if [[ $docker_memory_mb -lt $memory_mb ]]; then
                memory_mb=$docker_memory_mb
                echo -e "${YELLOW}  Docker memory limit detected: ${memory_mb} MB${NC}"
            fi
        fi
    fi

    # Leave some memory for the system (use 90% of available)
    memory_mb=$((memory_mb * 90 / 100))

    # Run comprehensive GPU detection
    detect_gpu_comprehensive

    # Summary
    local gpu_summary="no GPU"
    if [[ "$GPU_DOCKER_READY" == true ]]; then
        gpu_summary="${GPU_COUNT} ${GPU_TYPE} GPU(s) ready"
    elif [[ "$GPU_TYPE" != "none" ]]; then
        gpu_summary="${GPU_COUNT} ${GPU_TYPE} GPU(s) detected (not Docker-ready)"
    fi

    echo -e "${GREEN}✓ System resources: ${cpus} CPUs, ${memory_mb} MB RAM, ${gpu_summary}${NC}"

    # Export for use in other functions
    export DETECTED_CPUS=$cpus
    export DETECTED_MEMORY=$memory_mb
    export DETECTED_GPUS=$GPU_COUNT

    # Check resource requirements from environment-config.json
    local min_memory_gb=$(get_nested_config_value "resource_requirements" "min_memory_gb")
    local recommended_memory_gb=$(get_nested_config_value "resource_requirements" "recommended_memory_gb")
    local min_cpus=$(get_nested_config_value "resource_requirements" "min_cpus")
    local recommended_cpus=$(get_nested_config_value "resource_requirements" "recommended_cpus")
    local resource_notes=$(get_nested_config_value "resource_requirements" "notes")

    # Track if requirements are met
    local requirements_met=true

    # Validate against requirements if specified
    if [[ -n "$min_memory_gb" ]]; then
        local min_memory_mb=$((min_memory_gb * 1024))
        local memory_gb=$((memory_mb / 1024))

        if [[ $memory_mb -lt $min_memory_mb ]]; then
            echo
            echo -e "${RED}╔══════════════════════════════════════════════════════════════╗${NC}"
            echo -e "${RED}║  ⚠️  INSUFFICIENT MEMORY FOR THIS ENVIRONMENT                 ║${NC}"
            echo -e "${RED}╠══════════════════════════════════════════════════════════════╣${NC}"
            echo -e "${RED}║  Current:  ${memory_gb} GB                                           ║${NC}"
            echo -e "${RED}║  Minimum:  ${min_memory_gb} GB                                          ║${NC}"
            if [[ -n "$recommended_memory_gb" ]]; then
                echo -e "${YELLOW}║  Recommended: ${recommended_memory_gb} GB                                       ║${NC}"
            fi
            echo -e "${RED}╠══════════════════════════════════════════════════════════════╣${NC}"
            if [[ -n "$resource_notes" ]]; then
                echo -e "${CYAN}║  ${resource_notes:0:60}${NC}"
            fi
            echo -e "${YELLOW}║  On Docker Desktop: Settings → Resources → Memory           ║${NC}"
            echo -e "${RED}╚══════════════════════════════════════════════════════════════╝${NC}"
            echo
            requirements_met=false
        elif [[ -n "$recommended_memory_gb" ]] && [[ $memory_mb -lt $((recommended_memory_gb * 1024)) ]]; then
            echo -e "${YELLOW}⚠️  Memory below recommended: ${memory_gb} GB (recommended: ${recommended_memory_gb} GB)${NC}"
            if [[ -n "$resource_notes" ]]; then
                echo -e "${CYAN}   Note: ${resource_notes}${NC}"
            fi
        fi
    fi

    if [[ -n "$min_cpus" ]] && [[ $cpus -lt $min_cpus ]]; then
        echo -e "${RED}⚠️  WARNING: Insufficient CPUs! Current: ${cpus} | Minimum: ${min_cpus}${NC}"
        requirements_met=false
    fi

    # If requirements not met, ask user to confirm
    if [[ "$requirements_met" == "false" ]]; then
        echo
        echo -e "${YELLOW}The system does not meet minimum requirements.${NC}"
        echo -e "${YELLOW}Some workloads may fail due to insufficient resources.${NC}"
        echo
        read -p "Continue anyway? [y/N]: " confirm
        if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
            echo -e "${RED}Setup aborted. Please increase Docker resources and try again.${NC}"
            exit 1
        fi
        echo -e "${YELLOW}⚠️  Continuing with insufficient resources...${NC}"
    fi

    echo
}

# Configure Slurm with detected resources
configure_slurm() {
    echo -e "${CYAN}⚙️  Configuring Slurm with detected resources...${NC}"

    if [[ ! -d "config" ]]; then
        mkdir -p config
    fi

    # Only create slurm.conf if it doesn't exist
    if [[ -f "config/slurm.conf" ]]; then
        echo -e "${GREEN}✓ Slurm configuration already exists: config/slurm.conf${NC}"
        echo -e "${CYAN}💡 To regenerate, delete config/slurm.conf and run setup again${NC}"

        # Show current configuration resources if available
        if grep -q "CPUs=" "config/slurm.conf" 2>/dev/null; then
            local current_cpus=$(grep "CPUs=" "config/slurm.conf" | sed 's/.*CPUs=\([0-9]*\).*/\1/')
            local current_mem=$(grep "RealMemory=" "config/slurm.conf" | sed 's/.*RealMemory=\([0-9]*\).*/\1/')
            local current_gpu=$(grep "Gres=" "config/slurm.conf" | sed 's/.*Gres=\([^ ]*\).*/\1/' || echo "none")
            echo -e "${CYAN}Current config: ${current_cpus} CPUs, ${current_mem} MB RAM, GPU: ${current_gpu}${NC}"
            echo -e "${CYAN}Detected resources: ${DETECTED_CPUS} CPUs, ${DETECTED_MEMORY} MB RAM${NC}"
        fi
    else
        echo -e "${YELLOW}Creating new Slurm configuration...${NC}"

        # Build GPU configuration if available
        local gres_types=""
        local gres_config=""
        local node_gres=""

        if [[ "$GPU_DOCKER_READY" == true ]] && [[ "$GPU_COUNT" -gt 0 ]]; then
            echo -e "${GREEN}  Configuring Slurm for ${GPU_COUNT} ${GPU_TYPE} GPU(s)${NC}"
            gres_types="GresTypes=gpu"

            if [[ "$GPU_TYPE" == "nvidia" ]]; then
                node_gres="Gres=gpu:nvidia:${GPU_COUNT}"
            elif [[ "$GPU_TYPE" == "amd" ]]; then
                node_gres="Gres=gpu:amd:${GPU_COUNT}"
            else
                node_gres="Gres=gpu:${GPU_COUNT}"
            fi

            # Create gres.conf
            configure_slurm_gres
        fi

        # Create slurm.conf with detected resources
        cat > "config/slurm.conf" << EOF
ClusterName=${DOCKER_PREFIX}-slurm
SlurmctldHost=localhost

# LOGGING
SlurmctldLogFile=/var/log/slurm/slurmctld.log
SlurmdLogFile=/var/log/slurm/slurmd.log
SlurmdDebug=debug5
SlurmctldDebug=debug5

# SCHEDULER - Allow multiple jobs to run simultaneously
SelectType=select/cons_tres
SelectTypeParameters=CR_Core_Memory
EOF

        # Add GPU configuration if available
        if [[ -n "$gres_types" ]]; then
            cat >> "config/slurm.conf" << EOF

# GPU RESOURCES
${gres_types}
EOF
        fi

        # Add node and partition configuration
        local node_line="NodeName=localhost CPUs=${DETECTED_CPUS} RealMemory=${DETECTED_MEMORY} TmpDisk=100000"
        if [[ -n "$node_gres" ]]; then
            node_line="${node_line} ${node_gres}"
        fi
        node_line="${node_line} State=UNKNOWN"

        cat >> "config/slurm.conf" << EOF

# COMPUTE NODES - Auto-configured based on system resources
# Detected: ${DETECTED_CPUS} CPUs, ${DETECTED_MEMORY} MB RAM, ${GPU_COUNT} GPU(s)
${node_line}
PartitionName=debug Nodes=localhost Default=YES MaxTime=INFINITE State=UP DefMemPerCPU=$((DETECTED_MEMORY / DETECTED_CPUS)) MaxMemPerCPU=$((DETECTED_MEMORY / DETECTED_CPUS * 2))

# PROCESS TRACKING
ProctrackType=proctrack/linuxproc
EOF

        local gpu_msg=""
        if [[ "$GPU_DOCKER_READY" == true ]] && [[ "$GPU_COUNT" -gt 0 ]]; then
            gpu_msg=", ${GPU_COUNT} ${GPU_TYPE} GPU(s)"
        fi
        echo -e "${GREEN}✓ Slurm configured with: ${DETECTED_CPUS} CPUs, ${DETECTED_MEMORY} MB RAM${gpu_msg}${NC}"
    fi

    echo
}

# Configure Slurm GRES (Generic Resources) for GPU
configure_slurm_gres() {
    echo -e "${CYAN}  Creating gres.conf for GPU resources...${NC}"

    cat > "config/gres.conf" << 'EOF'
# =============================================================================
# Slurm Generic Resources (GRES) Configuration
# Auto-generated by configure-environment.sh
# =============================================================================
# This file defines GPU resources available to Slurm jobs.
#
# IMPORTANT: This configuration is validated at container startup.
# If the actual GPU configuration differs, start-services.sh will
# regenerate this file based on detected hardware.
# =============================================================================

EOF

    if [[ "$GPU_TYPE" == "nvidia" ]]; then
        cat >> "config/gres.conf" << EOF
# NVIDIA GPU Configuration
# AutoDetect=nvml will use NVIDIA Management Library for automatic detection
# This is the recommended approach as it handles GPU topology automatically
AutoDetect=nvml

# Manual configuration (used as fallback if AutoDetect fails)
# Uncomment and modify if you need explicit device mapping:
EOF
        # Add manual entries for each GPU
        for ((i=0; i<GPU_COUNT; i++)); do
            echo "# NodeName=localhost Name=gpu Type=nvidia File=/dev/nvidia${i}" >> "config/gres.conf"
        done

    elif [[ "$GPU_TYPE" == "amd" ]]; then
        cat >> "config/gres.conf" << EOF
# AMD GPU Configuration (ROCm)
# AMD GPUs use /dev/dri/renderD* devices
# AutoDetect=rsmi uses ROCm SMI for detection (requires ROCm 5.0+)
AutoDetect=rsmi

# Manual configuration (used as fallback if AutoDetect fails)
EOF
        # Add manual entries - AMD uses renderD128, renderD129, etc.
        for ((i=0; i<GPU_COUNT; i++)); do
            local render_id=$((128 + i))
            echo "# NodeName=localhost Name=gpu Type=amd File=/dev/dri/renderD${render_id}" >> "config/gres.conf"
        done
    fi

    echo -e "${GREEN}  ✓ gres.conf created${NC}"
}

# Configure docker-compose with dynamic prefix and GPU support
configure_docker_compose() {
    echo -e "${CYAN}⚙️  Configuring Docker Compose with prefix: $DOCKER_PREFIX${NC}"

    if [[ -f "docker-compose.yml" ]]; then
        # Replace the template placeholder with the actual prefix
        if command -v sed &> /dev/null; then
            # Use sed to replace the placeholder
            sed -i.bak "s/__DOCKER_PREFIX__/$DOCKER_PREFIX/g" docker-compose.yml
            rm -f docker-compose.yml.bak 2>/dev/null
            echo -e "${GREEN}✓ Docker Compose configured with prefix: $DOCKER_PREFIX${NC}"
        else
            echo -e "${YELLOW}⚠️  sed not available, docker-compose.yml may need manual configuration${NC}"
        fi

        # Configure GPU support if available
        if [[ "$GPU_DOCKER_READY" == true ]] && [[ "$GPU_COUNT" -gt 0 ]]; then
            configure_docker_compose_gpu
        fi
    else
        echo -e "${RED}❌ docker-compose.yml not found${NC}"
    fi

    echo
}

# Add GPU configuration to docker-compose.yml
configure_docker_compose_gpu() {
    echo -e "${CYAN}  Adding GPU configuration to docker-compose.yml...${NC}"

    # Check if GPU is already configured
    if grep -q "deploy:" docker-compose.yml && grep -q "capabilities:" docker-compose.yml; then
        echo -e "${YELLOW}  GPU configuration already present in docker-compose.yml${NC}"
        return 0
    fi

    # Create GPU configuration based on type
    local gpu_config=""

    if [[ "$GPU_TYPE" == "nvidia" ]]; then
        # NVIDIA GPU configuration using deploy.resources.reservations
        gpu_config=$(cat << 'GPUEOF'
    # GPU CONFIGURATION (auto-generated)
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: all
              capabilities: [gpu]
GPUEOF
)
    elif [[ "$GPU_TYPE" == "amd" ]]; then
        # AMD GPU requires device passthrough
        gpu_config=$(cat << 'GPUEOF'
    # GPU CONFIGURATION (auto-generated) - AMD ROCm
    # AMD GPUs require explicit device mapping
    devices:
      - /dev/kfd:/dev/kfd
      - /dev/dri:/dev/dri
    security_opt:
      - seccomp:unconfined
    group_add:
      - video
      - render
GPUEOF
)
    fi

    if [[ -n "$gpu_config" ]]; then
        # Find the slurm service and add GPU config after 'privileged: true'
        # This is a safe insertion point that exists in the current docker-compose.yml
        if grep -q "privileged: true" docker-compose.yml; then
            # Create a temporary file with the GPU config inserted
            local temp_file=$(mktemp)

            # Use awk to insert GPU config after 'privileged: true' in the slurm service
            awk -v gpu="$gpu_config" '
            /privileged: true/ {
                print
                # Check if this is likely the slurm service (has slurm in recent lines)
                if (in_slurm_service) {
                    print gpu
                    gpu_added = 1
                }
                next
            }
            /__DOCKER_PREFIX__-slurm:|'$DOCKER_PREFIX'-slurm:/ {
                in_slurm_service = 1
            }
            /^  [a-zA-Z_-]+:$/ && !/__DOCKER_PREFIX__-slurm:|'$DOCKER_PREFIX'-slurm:/ {
                in_slurm_service = 0
            }
            { print }
            ' docker-compose.yml > "$temp_file"

            # Check if awk succeeded
            if [[ -s "$temp_file" ]]; then
                mv "$temp_file" docker-compose.yml
                echo -e "${GREEN}  ✓ GPU configuration added to docker-compose.yml${NC}"
            else
                rm -f "$temp_file"
                echo -e "${YELLOW}  ⚠️  Could not automatically add GPU config${NC}"
                echo -e "${YELLOW}  Please manually add the following to the slurm service:${NC}"
                echo "$gpu_config"
            fi
        else
            echo -e "${YELLOW}  ⚠️  Could not find insertion point for GPU config${NC}"
            echo -e "${YELLOW}  Please manually add the following to the slurm service:${NC}"
            echo "$gpu_config"
        fi
    fi
}

# Build Docker images
build_images() {
    echo -e "${CYAN}🔨 Building Docker images...${NC}"

    echo -e "Building images for $DISPLAY_NAME..."
    echo -e "${YELLOW}Using --no-cache to ensure fresh build with latest configuration...${NC}"
    echo -e "${CYAN}💡 This ensures Python/R dependencies from environment config are properly installed${NC}"

    # Prepare GPU build arguments
    local build_args=""
    if [[ "$GPU_DOCKER_READY" == true ]] && [[ "$GPU_TYPE" != "none" ]]; then
        echo -e "${GREEN}  Building with ${GPU_TYPE} GPU support${NC}"
        build_args="--build-arg GPU_TYPE=${GPU_TYPE}"

        if [[ "$GPU_TYPE" == "nvidia" ]]; then
            # Use a compatible CUDA version
            build_args="${build_args} --build-arg CUDA_VERSION=12.0"
            echo -e "${CYAN}  CUDA version: 12.0${NC}"
        elif [[ "$GPU_TYPE" == "amd" ]]; then
            build_args="${build_args} --build-arg ROCM_VERSION=6.0"
            echo -e "${CYAN}  ROCm version: 6.0${NC}"
        fi
    else
        echo -e "${CYAN}  Building CPU-only image (no GPU support)${NC}"
        build_args="--build-arg GPU_TYPE=none"
    fi

    # Build with GPU args
    if docker-compose build --no-cache --parallel $build_args; then
        echo -e "${GREEN}✓ Docker images built successfully${NC}"

        if [[ "$GPU_DOCKER_READY" == true ]] && [[ "$GPU_TYPE" != "none" ]]; then
            echo -e "${GREEN}  GPU support: ${GPU_TYPE} (${GPU_COUNT} GPU(s))${NC}"
        fi
    else
        echo -e "${RED}❌ Failed to build Docker images${NC}"
        echo -e "${YELLOW}💡 You can try building manually later with:${NC}"

        if [[ "$GPU_DOCKER_READY" == true ]] && [[ "$GPU_TYPE" != "none" ]]; then
            echo -e "${YELLOW}   docker-compose build --no-cache ${build_args}${NC}"
        else
            echo -e "${YELLOW}   docker-compose build --no-cache${NC}"
        fi
    fi

    echo
}

# Generate startup instructions
generate_instructions() {
    echo -e "${GREEN}🎉 Setup completed successfully!${NC}"
    echo
    echo -e "${BOLD}${CYAN}Ready to Start:${NC}"
    echo -e "${YELLOW}1.${NC} Start the services:"
    echo -e "   ${CYAN}docker-compose up${NC}"
    echo -e "   ${CYAN}# Or run in background: docker-compose up -d${NC}"
    echo
    echo -e "${YELLOW}2.${NC} Access the services:"
    echo -e "   ${CYAN}API: http://localhost:$DEFAULT_PORT${NC}"
    echo -e "   ${CYAN}Admin Panel: http://localhost:$DEFAULT_ADMIN_PORT${NC}"
    echo
    echo -e "${BOLD}${CYAN}Configuration Files:${NC}"
    echo -e "• ${YELLOW}Environment variables:${NC} .env"
    echo -e "• ${YELLOW}API Authentication:${NC} Use X-API-Key header with generated key"
    echo -e "• ${YELLOW}Admin Panel:${NC} Login with generated password at http://localhost:$DEFAULT_ADMIN_PORT"
    echo -e "• ${YELLOW}Methods:${NC} environment/methods/"
    echo -e "• ${YELLOW}Dependencies:${NC} environment/*.json"
    echo
    echo -e "${BOLD}${CYAN}Useful Commands:${NC}"
    echo -e "• ${YELLOW}View logs:${NC} docker-compose logs -f"
    echo -e "• ${YELLOW}Stop services:${NC} docker-compose down"
    echo -e "• ${YELLOW}Rebuild after config changes:${NC} docker-compose build --no-cache"
    echo -e "• ${YELLOW}Check status:${NC} docker-compose ps"
    echo -e "• ${YELLOW}Clean rebuild:${NC} docker-compose down && docker-compose build --no-cache && docker-compose up"
    echo
    echo -e "${RED}⚠️  IMPORTANT:${NC} ${BOLD}Always use --no-cache when rebuilding${NC}"
    echo -e "${CYAN}   This ensures Python/R dependencies from environment/ are properly installed${NC}"
    echo
}

# Main execution
main() {
    print_banner
    
    echo -e "${BOLD}Setting up $DISPLAY_NAME environment...${NC}"
    echo -e "Base repository: $BASE_REPO"
    echo -e "Docker prefix: $DOCKER_PREFIX"
    echo
    
    validate_environment
    setup_repository
    setup_environment
    detect_system_resources
    configure_slurm
    configure_docker_compose
    generate_env_file
    
    # Ask user if they want to build images now
    echo -e "${YELLOW}Do you want to build Docker images now? This may take several minutes.${NC}"
    echo -e "${CYAN}Note: Images will be built with --no-cache to ensure dependencies from your environment config are properly installed.${NC}"
    echo -e "${CYAN}You can also build them later with: docker-compose build --no-cache${NC}"
    read -p "Build images now? (y/N): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        build_images
    else
        echo -e "${YELLOW}Skipping image build. You can build later when ready.${NC}"
        echo -e "${RED}⚠️  IMPORTANT: Always use --no-cache when building to ensure environment dependencies are installed:${NC}"
        echo -e "${YELLOW}   docker-compose build --no-cache${NC}"
        echo
    fi
    
    generate_instructions
}

# Run main function
main "$@"
