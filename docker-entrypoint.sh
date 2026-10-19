#!/bin/bash
set -e

echo "🚀 Starting MyBibliotheca..."
echo "📅 Container startup time: $(date)"
echo "🔍 Container environment:"
echo "  - HOSTNAME: $HOSTNAME"
echo "  - PWD: $PWD"
echo "  - USER: $(whoami)"

# Generate a secure secret key if not provided
if [ -z "$SECRET_KEY" ]; then
    echo "⚠️  No SECRET_KEY provided, generating a random one..."
    export SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
    echo "🔑 Generated SECRET_KEY for this session"
fi

# Detect the durable backend without importing the app package (which would
# initialize Flask and may open Kuzu). A malformed state must fail closed so
# the entrypoint never touches a database whose authority is uncertain.
echo "📁 Preparing application data directory..."
mkdir -p /app/data
PERSISTENCE_STATE_PATH=${MYBIBLIOTHECA_PERSISTENCE_STATE_PATH:-${APP_DATA_DIR:-/app/data}/persistence_state.json}
ACTIVE_BACKEND=$(python3 - "$PERSISTENCE_STATE_PATH" <<'PY'
import json
import os
import sys
from pathlib import Path

state_path = Path(sys.argv[1])
try:
    state = json.loads(state_path.read_text(encoding="utf-8"))
except FileNotFoundError:
    kuzu_path = Path(os.getenv("KUZU_DB_PATH", "/app/data/kuzu")).expanduser()
    has_legacy_database = (kuzu_path / "bibliotheca.db").exists()
    database_url = (os.getenv("DATABASE_URL") or "").strip().lower()
    postgres_configured = bool(
        (os.getenv("POSTGRES_DSN") or "").strip()
        or database_url.startswith(("postgres://", "postgresql://", "postgresql+psycopg://"))
        or all((os.getenv(name) or "").strip() for name in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER"))
    )
    print("kuzu" if has_legacy_database or not postgres_configured else "postgres")
except Exception as exc:
    print(f"Unable to read persistence state at {state_path}: {exc}", file=sys.stderr)
    raise SystemExit(1)
else:
    backend = state.get("backend") if isinstance(state, dict) else None
    if backend not in {"kuzu", "postgres"}:
        print(f"Invalid persistence backend in {state_path}: {backend!r}", file=sys.stderr)
        raise SystemExit(1)
    if backend == "postgres" and not state.get("migration_run_id"):
        print(f"PostgreSQL persistence state in {state_path} has no migration run id", file=sys.stderr)
        raise SystemExit(1)
    print(backend)
PY
)
PENDING_CUTOVER_PATH="$(dirname "$PERSISTENCE_STATE_PATH")/persistence_cutover_pending.json"
if [ -f "$PENDING_CUTOVER_PATH" ]; then
    ACTIVE_BACKEND="pending_cutover"
fi

if [ "$ACTIVE_BACKEND" = "postgres" ] || [ "$ACTIVE_BACKEND" = "pending_cutover" ]; then
    if [ "$ACTIVE_BACKEND" = "pending_cutover" ]; then
        echo "🔄 A database cutover is pending recovery; leaving Kuzu files untouched"
    else
        echo "🐘 PostgreSQL is authoritative; leaving the preserved Kuzu database untouched"
    fi
else
    echo "🗄️  Existing Kuzu installation detected; preparing KuzuDB..."
    mkdir -p /app/data/kuzu

# Log directory structure and permissions
echo "📂 Data directory structure:"
ls -la /app/data/ || echo "❌ Failed to list /app/data/"
echo "📂 Kuzu directory details:"
ls -la /app/data/kuzu/ || echo "❌ Failed to list /app/data/kuzu/"

# Check if this is a fresh container or restart
if [ -f "/app/data/kuzu/.container_marker" ]; then
    echo "🔄 Container restart detected - existing database should persist"
    echo "📊 Previous container info:"
    cat /app/data/kuzu/.container_marker || echo "❌ Failed to read container marker"
else
    echo "🆕 Fresh container startup - creating container marker"
    echo "Container created: $(date)" > /app/data/kuzu/.container_marker
    echo "Hostname: $HOSTNAME" >> /app/data/kuzu/.container_marker
fi

# Ensure proper permissions on data directory
echo "🔐 Setting permissions on data directory..."
chown -R 1000:1000 /app/data 2>/dev/null || echo "⚠️  Could not change ownership (running as non-root?)"
chmod -R 755 /app/data 2>/dev/null || echo "⚠️  Could not change permissions"

# KuzuDB-specific setup
echo "🗄️  Setting up KuzuDB..."
export KUZU_DB_PATH=${KUZU_DB_PATH:-/app/data/kuzu}
export GRAPH_DATABASE_ENABLED=${GRAPH_DATABASE_ENABLED:-true}

# Show setup details only in debug mode
if [ "${KUZU_DEBUG:-false}" = "true" ]; then
    echo "🔍 KuzuDB setup details:"
    echo "  - KUZU_DB_PATH: $KUZU_DB_PATH"
    echo "  - GRAPH_DATABASE_ENABLED: $GRAPH_DATABASE_ENABLED"
fi

# Check if database files exist (only show details in debug mode)
if [ "${KUZU_DEBUG:-false}" = "true" ]; then
    echo "📊 Checking for existing KuzuDB files..."
    if [ -d "$KUZU_DB_PATH" ]; then
        echo "✅ KuzuDB directory exists"
        KUZU_FILES=$(find "$KUZU_DB_PATH" -type f 2>/dev/null | wc -l)
        echo "📄 Found $KUZU_FILES files in KuzuDB directory"
        if [ $KUZU_FILES -gt 0 ]; then
            echo "📋 KuzuDB files found:"
            find "$KUZU_DB_PATH" -type f -exec ls -lh {} \; 2>/dev/null || echo "❌ Could not list files"
            echo "✅ Database persistence detected - existing data should be available"
        else
            echo "📭 KuzuDB directory is empty - fresh database will be initialized"
        fi
    else
        echo "❌ KuzuDB directory does not exist - will be created"
    fi
fi

# Clean up any stale KuzuDB lock files (critical for Docker restarts)
if [ -f "$KUZU_DB_PATH/.lock" ]; then
    echo "🧹 Removing stale KuzuDB lock file..."
    rm -f "$KUZU_DB_PATH/.lock" 2>/dev/null || echo "❌ Failed to remove lock file"
else
    echo "✅ No stale lock files found"
fi

# Dangerous cleanup is disabled by default; only enabled when explicitly requested
if [ "$ACTIVE_BACKEND" = "kuzu" ] && [ "${KUZU_FORCE_RESET:-false}" = "true" ]; then
    if [ -d "$KUZU_DB_PATH/bibliotheca.db" ]; then
        echo "🚨 KUZU_FORCE_RESET=true - performing safety backup and resetting database directory"
        mkdir -p /app/data/backups
        RESET_BACKUP="/app/data/backups/startup_reset_$(date +%Y%m%d-%H%M%S).tar.gz"
        # Best-effort archive; ignore errors so we don't block reset if tar fails
        tar -czf "$RESET_BACKUP" -C "$KUZU_DB_PATH" bibliotheca.db 2>/dev/null && \
            echo "📦 Safety backup created at $RESET_BACKUP" || \
            echo "⚠️  Failed to create safety backup (continuing reset)"
        echo "🧹 Removing $KUZU_DB_PATH/bibliotheca.db ..."
        rm -rf "$KUZU_DB_PATH/bibliotheca.db" 2>/dev/null || echo "❌ Failed to remove existing directory"
        echo "✅ Database directory reset complete"
    fi
fi

# Additional KuzuDB diagnostic info
echo "🔧 KuzuDB diagnostics:"
echo "  - Directory permissions: $(ls -ld $KUZU_DB_PATH 2>/dev/null || echo 'N/A')"
echo "  - Available disk space: $(df -h $KUZU_DB_PATH 2>/dev/null | tail -1 || echo 'N/A')"

# Warn about single worker requirement
echo "⚠️  NOTE: Running with single worker (WORKERS=1) due to KuzuDB concurrency limitations"
echo "📊 KuzuDB path: $KUZU_DB_PATH"
fi

# Check for SQLite migration if enabled
if [ "$AUTO_MIGRATE" = "true" ]; then
    echo "🔄 Auto-migration enabled, checking for SQLite databases..."
    export DOCKER_ENV=true
    
    # Set default migration user if not specified
    if [ -z "$MIGRATION_DEFAULT_USER" ]; then
        export MIGRATION_DEFAULT_USER="admin"
        echo "📝 Using default migration user: admin"
    fi
    
    echo "🔍 Migration settings:"
    echo "  AUTO_MIGRATE: $AUTO_MIGRATE" 
    echo "  MIGRATION_DEFAULT_USER: $MIGRATION_DEFAULT_USER"
fi

# PostgreSQL can safely serve concurrent Gunicorn workers. Keep Kuzu at one
# worker until the verified cutover; preserve the PostgreSQL target so the
# migration can scale Gunicorn live when it switches backends.
CONFIGURED_WORKERS=${WORKERS:-6}
case "$CONFIGURED_WORKERS" in
    ''|*[!0-9]*)
        echo "⚠️  Invalid WORKERS value '$CONFIGURED_WORKERS'; using 6 for PostgreSQL"
        CONFIGURED_WORKERS=6
        ;;
esac
if [ "$CONFIGURED_WORKERS" -lt 1 ]; then
    echo "⚠️  WORKERS must be at least 1; using 6 for PostgreSQL"
    CONFIGURED_WORKERS=6
fi
if [ "$CONFIGURED_WORKERS" -gt 32 ]; then
    echo "⚠️  WORKERS is capped at 32 for safety"
    CONFIGURED_WORKERS=32
fi
POSTGRES_WORKERS=$CONFIGURED_WORKERS
if [ "$ACTIVE_BACKEND" = "kuzu" ] || [ "$ACTIVE_BACKEND" = "pending_cutover" ]; then
    WORKERS=1
    echo "⚠️  Kuzu is active or awaiting recovery; starting Gunicorn with one worker"
else
    WORKERS=$CONFIGURED_WORKERS
    echo "🐘 PostgreSQL is active; starting Gunicorn with $WORKERS workers"
fi
export WORKERS POSTGRES_WORKERS

echo "✅ Initialization complete, starting application..."
echo "📝 Visit the application to complete setup using the interactive setup page"
echo "🕒 Application startup time: $(date)"

# Persistence diagnostics script removed - no longer needed

# Log final state before starting app
echo "🏁 Final pre-startup state:"
echo "  - Data directory size: $(du -sh /app/data 2>/dev/null || echo 'N/A')"
echo "  - KuzuDB directory size: $(du -sh $KUZU_DB_PATH 2>/dev/null || echo 'N/A')"
echo "  - Process ID: $$"

# Configure logging level for Gunicorn from LOG_LEVEL env (default ERROR)
LOG_LEVEL_NORMALIZED=$(echo "${LOG_LEVEL:-ERROR}" | tr '[:upper:]' '[:lower:]')
export GUNICORN_CMD_ARGS="${GUNICORN_CMD_ARGS} --log-level ${LOG_LEVEL_NORMALIZED}"

# Execute the main command with visibility
echo "🪵 Log level: ${LOG_LEVEL_NORMALIZED} (set via LOG_LEVEL env)"
echo "🔧 Executing: $@"
exec "$@"
