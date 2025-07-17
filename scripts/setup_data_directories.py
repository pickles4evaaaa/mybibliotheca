#!/usr/bin/env python3
"""
Setup script to create the necessary data directory structure for fresh installations.
This ensures the data directories exist without including actual database files.
"""

import os
from pathlib import Path

def setup_data_directories():
    """Create the necessary data directory structure for the application."""
    
    # Get the project root (parent of scripts directory)
    project_root = Path(__file__).parent.parent
    
    # Define directories that should exist
    directories = [
        'data',
        'data/kuzu',
        'data/flask_sessions',
        'data/covers',
        'backups',
        'migration_backups',
        'flask_session'
    ]
    
    print("📁 Setting up data directories...")
    
    for directory in directories:
        dir_path = project_root / directory
        dir_path.mkdir(parents=True, exist_ok=True)
        
        # Create a .gitkeep file to preserve directory structure in git
        gitkeep_path = dir_path / '.gitkeep'
        if not gitkeep_path.exists():
            gitkeep_path.write_text('')
            print(f"   ✅ Created {directory}/ with .gitkeep")
        else:
            print(f"   ✅ {directory}/ already exists")
    
    print("\n🎉 Data directory structure setup complete!")
    print("📝 Note: Database files will be created automatically when the application starts")
    print("🔐 Database files are excluded from git to protect user data")

if __name__ == "__main__":
    setup_data_directories()
