#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DOCKERFILE="$SCRIPT_DIR/Dockerfile"
IMAGE_NAME="steam-pipeline"
CONTAINER_NAME="steam-container" 
CRON_SCHEDULE="0 7 * * *"

# Function to check if cron job exists
check_cron_exists() {
    crontab -l 2>/dev/null | grep -q "$SCRIPT_DIR"
    return $?
}

# Function to create docker management script
create_docker_script() {
    DOCKER_SCRIPT="$SCRIPT_DIR/docker_manager.sh"
    
    cat > "$DOCKER_SCRIPT" << 'EOF'
#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DOCKERFILE="$SCRIPT_DIR/Dockerfile"
IMAGE_NAME="your-image-name"
CONTAINER_NAME="your-container-name"

# Check if Docker image exists
if ! docker image inspect "$IMAGE_NAME" &> /dev/null; then
    sudo echo "Image does not exist. Building from Dockerfile..."
    if [ -f "$DOCKERFILE" ]; then
        sudo docker build -t "$IMAGE_NAME" -f "$DOCKERFILE" "$SCRIPT_DIR"
    else
        sudo echo "Error: Dockerfile not found at $DOCKERFILE"
        exit 1
    fi
else
    sudo echo "Image $IMAGE_NAME exists."
fi

# Stop and remove container if it's already running
if sudo docker ps -a | grep -q "$CONTAINER_NAME"; then
    sudo echo "Stopping and removing existing container..."
    sudo docker stop "$CONTAINER_NAME" &> /dev/null
    sudo docker rm "$CONTAINER_NAME" &> /dev/null
fi

# Run the container
sudo echo "Starting container $CONTAINER_NAME from image $IMAGE_NAME..."
sudo docker run -d --name "$CONTAINER_NAME" "$IMAGE_NAME"
sudo echo "Container started successfully."
EOF

    sudo chmod +x "$DOCKER_SCRIPT"
    sudo echo "Created docker management script at $DOCKER_SCRIPT"
}

# Function to add a cronjob to run the docker script
add_cron_job() {
    # Create docker management script if it doesn't exist
    create_docker_script
    
    # Create a temporary file with current crontab
    sudo crontab -l 2>/dev/null > /tmp/current_crontab
    
    # Add new cron job to the temporary file
    sudo echo "$CRON_SCHEDULE $SCRIPT_DIR/docker_manager.sh" >> /tmp/current_crontab
    
    # Install new crontab from the temporary file
    sudo crontab /tmp/current_crontab
    
    # Remove the temporary file
    sudo rm /tmp/current_crontab
    
    sudo echo "Cron job added successfully."
}

# Main script execution
sudo echo "Checking for Dockerfile..."
if [ ! -f "$DOCKERFILE" ]; then
    sudo echo "Error: Dockerfile not found at $DOCKERFILE"
    exit 1
fi

sudo echo "Checking for existing cron job..."
if check_cron_exists; then
    sudo echo "Cron job already exists. No changes made."
else
    sudo echo "No existing cron job found. Adding new cron job..."
    add_cron_job
fi