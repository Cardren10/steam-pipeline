#!/bin/bash

# Get the absolute path of the directory containing the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PYTHON_SCRIPT="$SCRIPT_DIR/main.py"
CRON_SCHEDULE="0 7 * * *"

# Function to check if cron job exists
check_cron_exists() {
    crontab -l 2>/dev/null | grep -q "$PYTHON_SCRIPT"
    return $?
}

# Function to add a cronjob to run main assuming it does not exist.
add_cron_job() {

}

# Main script execution
echo "Checking for main.py..."
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "Error: main.py not found at $PYTHON_SCRIPT"
    exit 1
fi

echo "Checking for existing cron job..."
if check_cron_exists; then
    echo "Cron job already exists. No changes made."
else
    echo "No existing cron job found. Adding new cron job..."
    add_cron_job
fi

# Verify current crontab
echo -e "\nCurrent crontab contents:"
crontab -l