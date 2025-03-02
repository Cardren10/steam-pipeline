#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DOCKERFILE="$SCRIPT_DIR/dockerfile"
IMAGE_NAME="steam-pipeline"
CONTAINER_NAME="steam-container"

# Check if Docker image exists
if ! sudo docker image inspect "$IMAGE_NAME" &> /dev/null; then
    echo "Image does not exist. Building from Dockerfile..."
    if [ -f "$DOCKERFILE" ]; then
        sudo docker build -t "$IMAGE_NAME" -f "$DOCKERFILE" "$SCRIPT_DIR"
    else
        echo "Error: Dockerfile not found at $DOCKERFILE"
        exit 1
    fi
else
    echo "Image $IMAGE_NAME exists."
fi

# Stop and remove container if it's already running
if sudo docker ps --filter "name=$CONTAINER_NAME" --format "{{.Names}}" | grep -q "^$CONTAINER_NAME$"; then
    echo "Stopping and removing existing container..."
    sudo docker stop "$CONTAINER_NAME" &> /dev/null
    sudo docker rm "$CONTAINER_NAME" &> /dev/null
fi

# Run the container
echo "Starting container $CONTAINER_NAME from image $IMAGE_NAME..."
sudo docker run -d --rm --name "$CONTAINER_NAME" "$IMAGE_NAME"
echo "Container started successfully."