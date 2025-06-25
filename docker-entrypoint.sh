#!/bin/bash
set -e

echo "🚀 Starting MyBibliotheca with KuzuDB setup..."

# Generate a secure secret key if not provided
if [ -z "$SECRET_KEY" ]; then
    echo "⚠️  No SECRET_KEY provided, generating a random one..."
    export SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
    echo "🔑 Generated SECRET_KEY for this session"
fi

# Ensure data directories exist
mkdir -p /app/data
mkdir -p /app/data/kuzu

# Ensure proper permissions on data directory
chown -R 1000:1000 /app/data 2>/dev/null || true

# KuzuDB-specific setup
echo "🗄️  Setting up KuzuDB..."
export KUZU_DB_PATH=${KUZU_DB_PATH:-/app/data/kuzu}
export GRAPH_DATABASE_ENABLED=${GRAPH_DATABASE_ENABLED:-true}

# Clean up any stale KuzuDB lock files (critical for Docker restarts)
if [ -f "$KUZU_DB_PATH/.lock" ]; then
    echo "🧹 Removing stale KuzuDB lock file..."
    rm -f "$KUZU_DB_PATH/.lock" 2>/dev/null || true
fi

# Warn about single worker requirement
echo "⚠️  NOTE: Running with single worker (WORKERS=1) due to KuzuDB concurrency limitations"
echo "📊 KuzuDB path: $KUZU_DB_PATH"

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

echo "✅ Initialization complete, starting application..."
echo "📝 Visit the application to complete setup using the interactive setup page"

# Execute the main command
exec "$@"
