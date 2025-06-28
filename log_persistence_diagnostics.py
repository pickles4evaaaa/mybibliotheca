#!/usr/bin/env python3
"""
Persistence Diagnostics Logger

This script can be run inside the Docker container to log database state
and help diagnose persistence issues. Can be called from docker-entrypoint.sh
or run manually inside the container.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [PERSISTENCE_CHECK] %(message)s'
)
logger = logging.getLogger(__name__)

def log_environment_info():
    """Log container environment information."""
    logger.info("=== CONTAINER ENVIRONMENT ===")
    logger.info(f"Hostname: {os.getenv('HOSTNAME', 'N/A')}")
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info(f"Process ID: {os.getpid()}")
    logger.info(f"Python executable: {sys.executable}")
    logger.info(f"Python version: {sys.version}")
    
    # Log important environment variables
    env_vars = [
        'KUZU_DB_PATH', 'GRAPH_DATABASE_ENABLED', 'SECRET_KEY',
        'WORKERS', 'AUTO_MIGRATE', 'KUZU_FORCE_RESET'
    ]
    logger.info("Environment variables:")
    for var in env_vars:
        value = os.getenv(var, 'NOT_SET')
        # Mask sensitive values
        if 'SECRET' in var or 'KEY' in var:
            value = '***MASKED***' if value != 'NOT_SET' else 'NOT_SET'
        logger.info(f"  {var}: {value}")

def log_filesystem_info():
    """Log filesystem and mount information."""
    logger.info("=== FILESYSTEM INFO ===")
    
    # Check data directory
    data_path = Path("/app/data")
    logger.info(f"Data directory exists: {data_path.exists()}")
    if data_path.exists():
        logger.info(f"Data directory permissions: {oct(data_path.stat().st_mode)[-3:]}")
        logger.info(f"Data directory owner: {data_path.stat().st_uid}:{data_path.stat().st_gid}")
        
        # List contents
        try:
            contents = list(data_path.iterdir())
            logger.info(f"Data directory contents: {len(contents)} items")
            for item in contents:
                size = item.stat().st_size if item.is_file() else "DIR"
                logger.info(f"  - {item.name} ({size} bytes)")
        except Exception as e:
            logger.error(f"Could not list data directory: {e}")
    
    # Check Kuzu directory specifically
    kuzu_path = Path(os.getenv('KUZU_DB_PATH', '/app/data/kuzu'))
    logger.info(f"KuzuDB directory exists: {kuzu_path.exists()}")
    if kuzu_path.exists():
        logger.info(f"KuzuDB directory permissions: {oct(kuzu_path.stat().st_mode)[-3:]}")
        
        # List Kuzu files
        try:
            kuzu_files = list(kuzu_path.glob("*"))
            logger.info(f"KuzuDB files: {len(kuzu_files)}")
            total_size = 0
            for file in kuzu_files:
                if file.is_file():
                    size = file.stat().st_size
                    total_size += size
                    logger.info(f"  - {file.name} ({size} bytes, modified: {datetime.fromtimestamp(file.stat().st_mtime)})")
            logger.info(f"Total KuzuDB size: {total_size} bytes")
        except Exception as e:
            logger.error(f"Could not list KuzuDB directory: {e}")

def log_mount_info():
    """Log Docker mount information."""
    logger.info("=== MOUNT INFO ===")
    
    try:
        # Check if we can read mount info
        with open('/proc/mounts', 'r') as f:
            mounts = f.readlines()
        
        # Look for app-related mounts
        app_mounts = [m for m in mounts if '/app' in m]
        logger.info(f"App-related mounts: {len(app_mounts)}")
        for mount in app_mounts:
            logger.info(f"  {mount.strip()}")
            
    except Exception as e:
        logger.info(f"Could not read mount info: {e}")
    
    # Check disk usage
    try:
        import shutil
        total, used, free = shutil.disk_usage('/app/data')
        logger.info(f"Disk usage for /app/data:")
        logger.info(f"  Total: {total // (1024**3)} GB")
        logger.info(f"  Used: {used // (1024**3)} GB") 
        logger.info(f"  Free: {free // (1024**3)} GB")
    except Exception as e:
        logger.info(f"Could not get disk usage: {e}")

def log_container_lifecycle():
    """Log container lifecycle information."""
    logger.info("=== CONTAINER LIFECYCLE ===")
    
    # Check for container marker
    marker_path = Path("/app/data/kuzu/.container_marker")
    if marker_path.exists():
        logger.info("Container marker found - this is a restart")
        try:
            with open(marker_path, 'r') as f:
                marker_content = f.read().strip()
            logger.info(f"Marker content: {marker_content}")
        except Exception as e:
            logger.error(f"Could not read container marker: {e}")
    else:
        logger.info("No container marker found - this is a fresh start")
        
        # Create marker for future runs
        try:
            marker_path.parent.mkdir(parents=True, exist_ok=True)
            with open(marker_path, 'w') as f:
                f.write(f"Container created: {datetime.now()}\n")
                f.write(f"Hostname: {os.getenv('HOSTNAME', 'unknown')}\n")
            logger.info("Created container marker for future runs")
        except Exception as e:
            logger.error(f"Could not create container marker: {e}")

def main():
    """Main diagnostic function."""
    logger.info("Starting persistence diagnostics...")
    logger.info(f"Diagnostic time: {datetime.now()}")
    
    try:
        log_environment_info()
        log_filesystem_info()
        log_mount_info()
        log_container_lifecycle()
        
        logger.info("=== DIAGNOSTICS COMPLETE ===")
        logger.info("Check logs above for any persistence-related issues")
        
    except Exception as e:
        logger.error(f"Diagnostic script failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
