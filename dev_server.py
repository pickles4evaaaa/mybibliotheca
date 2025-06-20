#!/usr/bin/env python3
"""
Development server for testing Bibliotheca functionality.
This bypasses the Gunicorn requirement for local testing.
"""

import os
import sys

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import create_app

def run_dev_server():
    """Run the Flask development server."""
    app = create_app()
    
    print("🧪 Starting Bibliotheca Development Server")
    print("=" * 50)
    print("NOTE: This is for testing only. Production uses Gunicorn.")
    print("")
    print("Access the application at: http://localhost:5001")
    print("Custom Metadata Import Test: http://localhost:5001/import-books")
    print("")
    print("⚠️  Redis connection may fail - some features will be limited")
    print("")
    
    try:
        app.run(
            host='0.0.0.0',
            port=5001,
            debug=True,
            use_reloader=False  # Avoid reloader issues in dev mode
        )
    except Exception as e:
        print(f"❌ Error starting development server: {e}")
        print("This is expected if Redis is not running.")
        print("The custom metadata models and UI can still be inspected.")

if __name__ == '__main__':
    run_dev_server()
