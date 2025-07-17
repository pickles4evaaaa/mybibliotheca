"""
Application initialization utilities for fresh deployments.
"""

import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

def ensure_data_directories() -> bool:
    """
    Ensure all necessary data directories exist.
    Returns True if successful, False otherwise.
    """
    try:
        # Get the project root directory
        project_root = Path(__file__).parent.parent.parent
        
        # Define required directories
        required_dirs = [
            'data',
            'data/kuzu',
            'data/flask_sessions',
            'data/covers',
            'backups',
            'migration_backups',
            'flask_session'
        ]
        
        for directory in required_dirs:
            dir_path = project_root / directory
            dir_path.mkdir(parents=True, exist_ok=True)
            
            # Create .gitkeep if it doesn't exist
            gitkeep_path = dir_path / '.gitkeep'
            if not gitkeep_path.exists():
                gitkeep_path.write_text('')
        
        logger.info("✅ All required data directories exist")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create data directories: {e}")
        return False

def check_fresh_install() -> bool:
    """
    Check if this is a fresh installation (no existing database).
    Returns True if it's a fresh install, False if database exists.
    """
    try:
        # Check if KuzuDB directory exists and has content
        kuzu_path = Path(__file__).parent.parent.parent / 'data' / 'kuzu' / 'bibliotheca.db'
        
        if not kuzu_path.exists():
            logger.info("🆕 Fresh installation detected - no existing database")
            return True
        
        # Check if it's a directory with files (proper KuzuDB format)
        if kuzu_path.is_dir():
            files = list(kuzu_path.glob('*'))
            if len(files) == 0:
                logger.info("🆕 Fresh installation detected - empty database directory")
                return True
            else:
                logger.info(f"📚 Existing database found with {len(files)} files")
                return False
        else:
            # It's a file - this is from old KuzuDB version
            logger.warning("⚠️  Old database format detected (file instead of directory)")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error checking installation status: {e}")
        return True  # Assume fresh install on error

def initialize_fresh_app() -> bool:
    """
    Initialize the application for a fresh deployment.
    Returns True if successful, False otherwise.
    """
    try:
        logger.info("🚀 Initializing fresh application...")
        
        # Ensure data directories exist
        if not ensure_data_directories():
            return False
        
        # Check if this is a fresh install
        is_fresh = check_fresh_install()
        
        if is_fresh:
            logger.info("🆕 Fresh installation - database will be created on first connection")
        else:
            logger.info("📚 Existing database found - will attempt to connect")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize fresh application: {e}")
        return False
