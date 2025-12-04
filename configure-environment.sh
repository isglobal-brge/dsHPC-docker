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
    
    echo -e "${GREEN}✓ Detected: ${cpus} CPUs, ${memory_mb} MB RAM available for Slurm${NC}"
    
    # Export for use in other functions
    export DETECTED_CPUS=$cpus
    export DETECTED_MEMORY=$memory_mb
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
            echo -e "${CYAN}Current config: ${current_cpus} CPUs, ${current_mem} MB RAM${NC}"
            echo -e "${CYAN}Detected resources: ${DETECTED_CPUS} CPUs, ${DETECTED_MEMORY} MB RAM${NC}"
        fi
    else
        echo -e "${YELLOW}Creating new Slurm configuration...${NC}"
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
SelectTypeParameters=CR_Core

# COMPUTE NODES - Auto-configured based on system resources
# Detected: ${DETECTED_CPUS} CPUs, ${DETECTED_MEMORY} MB RAM
NodeName=localhost CPUs=${DETECTED_CPUS} RealMemory=${DETECTED_MEMORY} TmpDisk=100000 State=UNKNOWN
PartitionName=debug Nodes=localhost Default=YES MaxTime=INFINITE State=UP DefMemPerCPU=$((DETECTED_MEMORY / DETECTED_CPUS)) MaxMemPerCPU=$((DETECTED_MEMORY / DETECTED_CPUS * 2))

# PROCESS TRACKING
ProctrackType=proctrack/linuxproc
EOF
        echo -e "${GREEN}✓ Slurm configured with: ${DETECTED_CPUS} CPUs, ${DETECTED_MEMORY} MB RAM${NC}"
    fi
    
    echo
}

# Configure docker-compose with dynamic prefix
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
    else
        echo -e "${RED}❌ docker-compose.yml not found${NC}"
    fi
    
    echo
}

# Build Docker images
build_images() {
    echo -e "${CYAN}🔨 Building Docker images...${NC}"
    
    echo -e "Building images for $DISPLAY_NAME..."
    echo -e "${YELLOW}Using --no-cache to ensure fresh build with latest configuration...${NC}"
    echo -e "${CYAN}💡 This ensures Python/R dependencies from environment config are properly installed${NC}"
    if docker-compose build --no-cache --parallel; then
        echo -e "${GREEN}✓ Docker images built successfully${NC}"
    else
        echo -e "${RED}❌ Failed to build Docker images${NC}"
        echo -e "${YELLOW}💡 You can try building manually later with:${NC}"
        echo -e "${YELLOW}   docker-compose build --no-cache${NC}"
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
