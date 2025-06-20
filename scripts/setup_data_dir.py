#!/usr/bin/env python3
"""
Cross-platform setup script to ensure data directory and database file exist with proper permissions.
This ensures parity between Docker and standalone execution across Windows, macOS, and Linux.
"""

import os
import stat
import platform
from pathlib import Path

def setup_data_directory():
    """Ensure data directory exists with proper permissions for standalone execution"""
    
    # Get the directory where this script is located (project root)
    project_root = Path(__file__).parent.absolute()
    data_dir = project_root / 'data'
    db_path = data_dir / 'books.db'
    
    system = platform.system()
    print(f"🔧 Setting up data directory for standalone execution on {system}...")
    print("   This ensures parity with Docker environment setup")
    
    # Create data directory if it doesn't exist
    try:
        if system == "Windows":
            # On Windows, just create the directory without specific mode
            data_dir.mkdir(exist_ok=True)
        else:
            # On Unix-like systems, set proper permissions
            data_dir.mkdir(mode=0o755, exist_ok=True)
        print(f"✅ Data directory created/verified: {data_dir}")
    except Exception as e:
        print(f"⚠️  Could not create data directory: {e}")
        return False
    
    # Create empty database file if it doesn't exist (matches Docker setup)
    try:
        if not db_path.exists():
            if system == "Windows":
                # On Windows, just create the file
                db_path.touch()
            else:
                # On Unix-like systems, set proper permissions
                db_path.touch(mode=0o664)
            print(f"✅ Database file created: {db_path}")
        else:
            print(f"✅ Database file exists: {db_path}")
    except Exception as e:
        print(f"⚠️  Could not create database file: {e}")
        return False
    
    # Set permissions (only on Unix-like systems)
    if system != "Windows":
        try:
            # Set directory permissions (755 = rwxr-xr-x)
            data_dir.chmod(0o755)
            
            # Set database file permissions (664 = rw-rw-r--)
            if db_path.exists():
                db_path.chmod(0o664)
                
            print("✅ Permissions set correctly (755 for directory, 664 for database)")
            
        except PermissionError as e:
            print(f"⚠️  Could not set permissions: {e}")
            print("   This is normal on some systems and won't affect functionality")
        except Exception as e:
            print(f"⚠️  Permission error: {e}")
    else:
        print("✅ Running on Windows - skipping Unix permission settings")
    
    # Verify the setup matches what config.py expects
    try:
        from config import Config
        config_db_path = Path(Config.DATABASE_PATH)
        
        if config_db_path.resolve() == db_path.resolve():
            print("✅ Database path matches config.py expectations")
        else:
            print(f"⚠️  Path mismatch - Config expects: {config_db_path}")
            print(f"   But we created: {db_path}")
    except Exception as e:
        print(f"⚠️  Could not verify config path: {e}")
    
    print(f"\n🎉 Data directory setup complete for {system}!")
    print(f"   Data directory: {data_dir}")
    print(f"   Database path: {db_path}")
    
    if system != "Windows":
        print(f"   Permissions: directory=755, database=664")
    else:
        print(f"   Permissions: Using Windows default file permissions")
    
    print("\nYou can now run Bibliotheca using:")
    if system == "Windows":
        print("   python -m gunicorn -w 4 -b 0.0.0.0:5054 run:app")
    else:
        print("   gunicorn -w 4 -b 0.0.0.0:5054 run:app")
    
    return True

if __name__ == "__main__":
    setup_data_directory()
