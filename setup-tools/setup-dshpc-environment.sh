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
    
    # Save user's files that should be preserved
    local user_env_exists=false
    local user_env_file_exists=false
    local user_readme_exists=false
    local user_license_exists=false
    local user_env_config_exists=false
    local user_setup_script_exists=false
    local temp_user_env=""
    local temp_env_file=""
    local temp_readme_file=""
    local temp_license_file=""
    local temp_env_config_file=""
    local temp_setup_script_file=""
    
    echo -e "${YELLOW}Preserving your custom files...${NC}"
    
    if [[ -d "environment" ]]; then
        user_env_exists=true
        temp_user_env=$(mktemp -d)
        echo -e "${CYAN}  • Preserving environment/ directory${NC}"
        cp -r environment/* "$temp_user_env/" 2>/dev/null || true
    fi
    
    if [[ -f ".env" ]]; then
        user_env_file_exists=true
        temp_env_file=$(mktemp)
        echo -e "${CYAN}  • Preserving .env file${NC}"
        cp ".env" "$temp_env_file"
    fi
    
    if [[ -f "README.md" ]]; then
        user_readme_exists=true
        temp_readme_file=$(mktemp)
        echo -e "${CYAN}  • Preserving README.md${NC}"
        cp "README.md" "$temp_readme_file"
    fi
    
    if [[ -f "LICENSE" ]]; then
        user_license_exists=true
        temp_license_file=$(mktemp)
        echo -e "${CYAN}  • Preserving LICENSE${NC}"
        cp "LICENSE" "$temp_license_file"
    fi
    
    if [[ -f "environment-config.json" ]]; then
        user_env_config_exists=true
        temp_env_config_file=$(mktemp)
        echo -e "${CYAN}  • Preserving environment-config.json${NC}"
        cp "environment-config.json" "$temp_env_config_file"
    fi
    
    if [[ -f "setup-dshpc-environment.sh" ]]; then
        user_setup_script_exists=true
        temp_setup_script_file=$(mktemp)
        echo -e "${CYAN}  • Preserving setup-dshpc-environment.sh${NC}"
        cp "setup-dshpc-environment.sh" "$temp_setup_script_file"
    fi
    
    # Check if we're already in a git repository
    if [[ -d ".git" ]]; then
        echo -e "${YELLOW}Git repository already exists, updating...${NC}"
        git pull origin main || git pull origin master || echo -e "${YELLOW}Could not update repository${NC}"
        echo -e "${GREEN}✓ Repository updated${NC}"
    else
        echo -e "Cloning from: $BASE_REPO"
        echo -e "${YELLOW}Cloning repository contents to current directory...${NC}"
        
        # Clone to temporary directory first
        local temp_dir=$(mktemp -d)
        git clone "$BASE_REPO" "$temp_dir/${ENV_NAME}-docker"
        
        if [[ $? -eq 0 ]]; then
            # Save the original directory
            local original_dir=$(pwd)
            
            # Move ALL contents from temp directory to current directory
            cd "$temp_dir/${ENV_NAME}-docker"
            
            # Move regular files and directories
            for item in *; do
                if [[ -e "$item" ]]; then
                    # Simply overwrite existing files (user files are already preserved)
                    mv "$item" "$original_dir/"
                fi
            done
            
            # Also move hidden files like .gitignore
            for item in .[^.]*; do
                if [[ "$item" != ".git" && -e "$item" ]]; then
                    # Simply overwrite existing files (user files are already preserved)
                    mv "$item" "$original_dir/"
                fi
            done
            
            # Return to original directory
            cd "$original_dir"
            rm -rf "$temp_dir"
            echo -e "${GREEN}✓ Repository contents copied to current directory${NC}"
        else
            echo -e "${RED}❌ Failed to clone repository${NC}"
            rm -rf "$temp_dir"
            exit 1
        fi
    fi
    
    # Restore all preserved user files
    echo -e "${YELLOW}Restoring your preserved files...${NC}"
    
    if [[ "$user_env_exists" == true ]]; then
        echo -e "${CYAN}  • Restoring environment/ directory${NC}"
        # Copy user's environment files over the repo's defaults
        cp -r "$temp_user_env"/* environment/ 2>/dev/null || true
        rm -rf "$temp_user_env"
    fi
    
    if [[ "$user_env_file_exists" == true ]]; then
        echo -e "${CYAN}  • Restoring .env file${NC}"
        cp "$temp_env_file" ".env"
        rm -f "$temp_env_file"
    fi
    
    if [[ "$user_readme_exists" == true ]]; then
        echo -e "${CYAN}  • Restoring README.md${NC}"
        cp "$temp_readme_file" "README.md"
        rm -f "$temp_readme_file"
    fi
    
    if [[ "$user_license_exists" == true ]]; then
        echo -e "${CYAN}  • Restoring LICENSE${NC}"
        cp "$temp_license_file" "LICENSE"
        rm -f "$temp_license_file"
    fi
    
    if [[ "$user_env_config_exists" == true ]]; then
        echo -e "${CYAN}  • Restoring environment-config.json${NC}"
        cp "$temp_env_config_file" "environment-config.json"
        rm -f "$temp_env_config_file"
    fi
    
    if [[ "$user_setup_script_exists" == true ]]; then
        echo -e "${CYAN}  • Restoring setup-dshpc-environment.sh${NC}"
        cp "$temp_setup_script_file" "setup-dshpc-environment.sh"
        rm -f "$temp_setup_script_file"
    fi
    
    echo -e "${GREEN}✓ User files restored successfully${NC}"
    
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
        # Convert to uppercase in a bash 3.2 compatible way
        local docker_prefix_upper=$(echo "$DOCKER_PREFIX" | tr '[:lower:]' '[:upper:]')
        
        # Create .env file in current directory
        cat > ".env" << EOF
# $DISPLAY_NAME Environment Configuration
# Generated on $(date)

# API Configuration
${docker_prefix_upper}_API_EXTERNAL_PORT=$DEFAULT_PORT
${docker_prefix_upper}_API_KEY=$api_key

# Docker Stack Configuration
COMPOSE_PROJECT_NAME=${DOCKER_PREFIX}

# Logging Configuration
LOG_LEVEL=WARNING
EOF
        
        echo -e "${GREEN}✓ Environment file created: .env${NC}"
        echo -e "${YELLOW}📝 Generated API Key: $api_key${NC}"
        echo -e "${CYAN}💡 You can modify these settings in .env${NC}"
    fi
    
    echo
}

# Build Docker images
build_images() {
    echo -e "${CYAN}🔨 Building Docker images...${NC}"
    
    echo -e "Building images for $DISPLAY_NAME..."
    echo -e "${YELLOW}Using --no-cache to ensure fresh build...${NC}"
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
    echo -e "${YELLOW}2.${NC} Access the API:"
    echo -e "   ${CYAN}http://localhost:$DEFAULT_PORT${NC}"
    echo
    echo -e "${BOLD}${CYAN}Configuration Files:${NC}"
    echo -e "• ${YELLOW}Environment variables:${NC} .env"
    echo -e "• ${YELLOW}API Authentication:${NC} Use X-API-Key header with generated key"
    echo -e "• ${YELLOW}Methods:${NC} environment/methods/"
    echo -e "• ${YELLOW}Dependencies:${NC} environment/*.json"
    echo
    echo -e "${BOLD}${CYAN}Useful Commands:${NC}"
    echo -e "• ${YELLOW}View logs:${NC} docker-compose logs -f"
    echo -e "• ${YELLOW}Stop services:${NC} docker-compose down"
    echo -e "• ${YELLOW}Rebuild after changes:${NC} docker-compose build --no-cache"
    echo -e "• ${YELLOW}Check status:${NC} docker-compose ps"
    echo -e "• ${YELLOW}Clean rebuild:${NC} docker-compose down && docker-compose build --no-cache && docker-compose up"
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
    setup_environment
    generate_env_file
    
    # Ask user if they want to build images now
    echo -e "${YELLOW}Do you want to build Docker images now? This may take several minutes.${NC}"
    echo -e "${CYAN}You can also build them later with: docker-compose build --no-cache${NC}"
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
