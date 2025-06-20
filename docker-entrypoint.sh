#!/bin/bash
set -e

echo "🚀 Starting MyBibliotheca with setup page..."

# Generate a secure secret key if not provided
if [ -z "$SECRET_KEY" ]; then
    echo "⚠️  No SECRET_KEY provided, generating a random one..."
    export SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
    echo "🔑 Generated SECRET_KEY for this session"
fi

# Ensure data directory exists
mkdir -p /app/data

# Ensure proper permissions on data directory
chown -R 1000:1000 /app/data 2>/dev/null || true

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
