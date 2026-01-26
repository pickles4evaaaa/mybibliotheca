#!/bin/bash

# ==============================================================================
# BIBLIOTHECA LIBRARY SYNC
# ==============================================================================
# This script synchronizes book inventories between all users on a 
# Bibliotheca instance. It respects "Read" status (keeping it private) 
# while ensuring every user has a copy of every book in the system.
#
# USAGE:
#   ./run_sync_safe.sh            (Standard mode: Checks for user activity first)
#   ./run_sync_safe.sh -override  (Force mode: Ignores activity check)
# ==============================================================================

# --- [USER CONFIGURATION REQUIRED] ---
# Container Name (Check via 'docker ps')
CONTAINER_NAME="bibliotheca"

# The PHYSICAL path on your host machine where 'books.db' lives.
# (Run: docker inspect -f '{{ range .Mounts }}Source: {{.Source}}{{ "\n" }}{{ end }}' bibliotheca)
HOST_DATA_PATH="/path/to/your/bibliotheca/data"

# How many minutes of log silence required before sync runs?
IDLE_MINUTES=10
# -------------------------------------

# Automatically detect the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/sync.log"
PYTHON_SCRIPT="$SCRIPT_DIR/sync_all.py"

# Redirect ALL output to the log file from this point forward
exec 1>>"$LOG_FILE" 2>&1

echo "==================================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting Sync Process..."

# Safety Check: Ensure the user set the path
if [[ "$HOST_DATA_PATH" == *"/path/to/your"* ]]; then
    echo "ERROR: You must edit this script and set HOST_DATA_PATH to your actual data folder."
    echo "Aborting."
    exit 1
fi

# 1. CHECK FOR OVERRIDE FLAG
FORCE_RUN=false
if [ "$1" == "-override" ]; then
    FORCE_RUN=true
    echo "OVERRIDE DETECTED: Skipping activity check."
fi

# 2. CHECK FOR ACTIVE USERS
if [ "$FORCE_RUN" = false ]; then
    echo "Checking for user activity..."
    # Check logs for the last X minutes
    ACTIVITY_COUNT=$(docker logs --since "${IDLE_MINUTES}m" $CONTAINER_NAME 2>&1 | grep -v "healthcheck" | grep -v "127.0.0.1" | grep -v "Scrape" | wc -l)

    if [ "$ACTIVITY_COUNT" -gt 0 ]; then
        echo "ABORTING SYNC: System is active ($ACTIVITY_COUNT log entries)."
        exit 0
    fi
    echo "System appears idle."
fi

echo "Proceeding with sync..."

# 3. STOP CONTAINER (Releases the SQLite file lock)
echo "Stopping container..."
docker stop $CONTAINER_NAME

# 4. RUN SYNC
echo "Running Python Sync Script..."

# We mount the HOST_DATA_PATH to /data inside the container
# We mount the python script dynamically from the script directory
docker run --rm \
  -u 0 \
  -v "$HOST_DATA_PATH":/data \
  -v "$PYTHON_SCRIPT":/app/sync_all.py \
  python:3.11-slim \
  python /app/sync_all.py

# 5. RESTART CONTAINER
echo "Restarting container..."
docker start $CONTAINER_NAME

echo "Sync Finished."
echo "==================================================="