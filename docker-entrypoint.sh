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

echo "✅ Initialization complete, starting application..."
echo "📝 Visit the application to complete setup using the interactive setup page"

# Execute the main command
exec "$@"
