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
DESCRIPTION=$(get_config_value "description")

# Fallback values if config file is missing or malformed
ENV_NAME=${ENV_NAME:-"dsHPC"}
DISPLAY_NAME=${DISPLAY_NAME:-"High-Performance Computing Environment"}
DOCKER_PREFIX=${DOCKER_PREFIX:-"dshpc"}
BASE_REPO=${BASE_REPO:-"https://github.com/isglobal-brge/dsHPC-docker.git"}
DEFAULT_PORT=${DEFAULT_PORT:-8001}
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
    echo -e "${BLUE}┌────────────────────────────────────────────────────┐${NC}"
    echo -e "${BLUE}│${NC} ${BOLD}${CYAN}$DISPLAY_NAME Setup${NC} ${BLUE}│${NC}"
    echo -e "${BLUE}│${NC} ${DESCRIPTION}${NC} ${BLUE}│${NC}"
    echo -e "${BLUE}└────────────────────────────────────────────────────┘${NC}"
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
    
    local repo_dir="${ENV_NAME}-docker"
    
    if [[ -d "$repo_dir" ]]; then
        echo -e "${YELLOW}Repository already exists, updating...${NC}"
        cd "$repo_dir"
        git pull origin main || git pull origin master
        cd ..
        echo -e "${GREEN}✓ Repository updated${NC}"
    else
        echo -e "Cloning from: $BASE_REPO"
        git clone "$BASE_REPO" "$repo_dir"
        if [[ $? -eq 0 ]]; then
            echo -e "${GREEN}✓ Repository cloned successfully${NC}"
        else
            echo -e "${RED}❌ Failed to clone repository${NC}"
            exit 1
        fi
    fi
    
    echo
}

# Copy environment configuration
copy_environment() {
    echo -e "${CYAN}📋 Copying environment configuration...${NC}"
    
    local repo_dir="${ENV_NAME}-docker"
    
    # Backup existing environment if it exists
    if [[ -d "$repo_dir/environment" ]]; then
        echo -e "${YELLOW}Backing up existing environment...${NC}"
        mv "$repo_dir/environment" "$repo_dir/environment.backup.$(date +%Y%m%d_%H%M%S)"
    fi
    
    # Copy user's environment
    cp -r environment "$repo_dir/"
    echo -e "${GREEN}✓ Environment configuration copied${NC}"
    
    # Copy config directory if it exists
    if [[ -d "config" ]]; then
        cp -r config "$repo_dir/"
        echo -e "${GREEN}✓ Config directory copied${NC}"
    fi
    
    echo
}

# Generate environment file
generate_env_file() {
    echo -e "${CYAN}⚙️  Generating environment configuration...${NC}"
    
    local repo_dir="${ENV_NAME}-docker"
    local api_key=$(generate_api_key)
    
    # Create .env file
    cat > "$repo_dir/.env" << EOF
# $DISPLAY_NAME Environment Configuration
# Generated on $(date)

# API Configuration
${DOCKER_PREFIX^^}_API_EXTERNAL_PORT=$DEFAULT_PORT
${DOCKER_PREFIX^^}_API_KEY=$api_key

# Docker Stack Configuration
COMPOSE_PROJECT_NAME=${DOCKER_PREFIX}

# Logging Configuration
LOG_LEVEL=WARNING
EOF
    
    echo -e "${GREEN}✓ Environment file created: $repo_dir/.env${NC}"
    echo -e "${YELLOW}📝 Generated API Key: $api_key${NC}"
    echo -e "${CYAN}💡 You can modify these settings in $repo_dir/.env${NC}"
    
    echo
}

# Build Docker images
build_images() {
    echo -e "${CYAN}🔨 Building Docker images...${NC}"
    
    local repo_dir="${ENV_NAME}-docker"
    
    cd "$repo_dir"
    
    echo -e "Building images for $DISPLAY_NAME..."
    if docker-compose build --parallel; then
        echo -e "${GREEN}✓ Docker images built successfully${NC}"
    else
        echo -e "${RED}❌ Failed to build Docker images${NC}"
        echo -e "${YELLOW}💡 You can try building manually later with:${NC}"
        echo -e "${YELLOW}   cd $repo_dir && docker-compose build${NC}"
    fi
    
    cd ..
    echo
}

# Generate startup instructions
generate_instructions() {
    local repo_dir="${ENV_NAME}-docker"
    
    echo -e "${GREEN}🎉 Setup completed successfully!${NC}"
    echo
    echo -e "${BOLD}${CYAN}Next Steps:${NC}"
    echo -e "${YELLOW}1.${NC} Navigate to the project directory:"
    echo -e "   ${CYAN}cd $repo_dir${NC}"
    echo
    echo -e "${YELLOW}2.${NC} Start the services:"
    echo -e "   ${CYAN}docker-compose up${NC}"
    echo -e "   ${CYAN}# Or run in background: docker-compose up -d${NC}"
    echo
    echo -e "${YELLOW}3.${NC} Access the API:"
    echo -e "   ${CYAN}http://localhost:$DEFAULT_PORT${NC}"
    echo
    echo -e "${BOLD}${CYAN}Configuration Files:${NC}"
    echo -e "• ${YELLOW}Environment variables:${NC} $repo_dir/.env"
    echo -e "• ${YELLOW}API Authentication:${NC} Use X-API-Key header with generated key"
    echo -e "• ${YELLOW}Methods:${NC} $repo_dir/environment/methods/"
    echo -e "• ${YELLOW}Dependencies:${NC} $repo_dir/environment/*.json"
    echo
    echo -e "${BOLD}${CYAN}Useful Commands:${NC}"
    echo -e "• ${YELLOW}View logs:${NC} docker-compose logs -f"
    echo -e "• ${YELLOW}Stop services:${NC} docker-compose down"
    echo -e "• ${YELLOW}Rebuild after changes:${NC} docker-compose build"
    echo -e "• ${YELLOW}Check status:${NC} docker-compose ps"
    echo
}

# Main execution
main() {
    print_banner
    
    echo -e "${BOLD}Setting up $DISPLAY_NAME environment...${NC}"
    echo -e "Repository: $BASE_REPO"
    echo -e "Docker prefix: $DOCKER_PREFIX"
    echo
    
    validate_environment
    setup_repository
    copy_environment
    generate_env_file
    
    # Ask user if they want to build images now
    echo -e "${YELLOW}Do you want to build Docker images now? This may take several minutes.${NC}"
    echo -e "${CYAN}You can also build them later with: cd ${ENV_NAME}-docker && docker-compose build${NC}"
    read -p "Build images now? (y/N): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        build_images
    else
        echo -e "${YELLOW}Skipping image build. You can build later when ready.${NC}"
        echo
    fi
    
    generate_instructions
}

# Run main function
main "$@"
