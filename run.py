from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

"""Application entry point.

Performs a schema preflight (additive column auto-upgrade with backup) before
creating the Flask app so migrations happen deterministically at startup.
"""

# Import triggers preflight side-effect (safe no-op if nothing to change)
from app.startup import schema_preflight  # noqa: F401

from app import create_app

# This app is intended to be run via Gunicorn only
app = create_app()
if __name__ == '__main__':
    import os
    import sys

    command = [
        "gunicorn",
        "-w", "1",
        "-b", "0.0.0.0:5054",
        "run:app"
    ]

    print(f"🚀 Launching Gunicorn with command: {' '.join(command)}")
    try:
        os.execvp(command[0], command)
    except FileNotFoundError:
        print("Error: 'gunicorn' command not found.", file=sys.stderr)
        print("Please install Gunicorn: pip install gunicorn", file=sys.stderr)
        sys.exit(1)
