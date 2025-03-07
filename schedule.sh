#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DOCKERFILE="$SCRIPT_DIR/dockerfile"
IMAGE_NAME="steam-pipeline"
CONTAINER_NAME="steam-container"
CRON_SCHEDULE="0 7 * * *"

# Function to check if cron job exists (for current user)
check_cron_exists() {
    crontab -l 2>/dev/null | grep -q "sudo docker run $IMAGE_NAME"
}

# Function to add a cronjob to run the docker script (for current user)
add_cron_job() {
    echo "Adding new cron job..."
    (crontab -l 2>/dev/null; echo "$CRON_SCHEDULE sudo docker run "$IMAGE_NAME"") | crontab -
    echo "Cron job added successfully."
}

# Main script execution
echo "Checking for Dockerfile..."
if [ ! -f "$DOCKERFILE" ]; then
    echo "Error: Dockerfile not found at $DOCKERFILE"
    exit 1
fi

echo "Checking for existing cron job..."
if check_cron_exists; then
    echo "Cron job already exists. No changes made."
else
    echo "No existing cron job found. Adding new cron job..."
    add_cron_job
fi