#!/usr/bin/env python3
"""
Run script specifically for KuzuDB version.

KuzuDB doesn't support concurrent access from multiple processes,
so this script ensures the application runs with a single worker.
"""

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from app import create_app

# This app is intended to be run via Gunicorn with a single worker for KuzuDB
app = create_app()

if __name__ == '__main__':
    import os
    import sys

    # Force single worker for KuzuDB compatibility
    command = [
        "gunicorn",
        "-w", "1",  # Single worker for KuzuDB
        "-b", "0.0.0.0:5054",
        "--timeout", "300",  # 5 minute timeout for bulk operations
        "run_kuzu:app"
    ]

    print("🚀 Launching Gunicorn with KuzuDB-compatible configuration...")
    print(f"⚠️  Note: Running with single worker due to KuzuDB concurrency limitations")
    print(f"Command: {' '.join(command)}")
    
    try:
        os.execvp(command[0], command)
    except FileNotFoundError:
        print("Error: 'gunicorn' command not found.", file=sys.stderr)
        print("Please install Gunicorn: pip install gunicorn", file=sys.stderr)
        sys.exit(1)
