#!/bin/bash
set -e

echo "🚀 Starting Bibliotheca with multi-user authentication..."

# Generate a secure secret key if not provided
if [ -z "$SECRET_KEY" ]; then
    echo "⚠️  No SECRET_KEY provided, generating a random one..."
    export SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
    echo "🔑 Generated SECRET_KEY for this session"
fi

# Set default admin password if not provided
if [ -z "$ADMIN_PASSWORD" ]; then
    export ADMIN_PASSWORD="TempAdmin123!@#"
    echo "⚠️  Using default admin password: $ADMIN_PASSWORD"
    echo "🔒 Admin will be required to change this on first login!"
fi

# Check if database exists and migrate if needed
if [ ! -f "/app/data/books.db" ]; then
    echo "📚 No existing database found, will create fresh database with admin user"
else
    echo "📚 Existing database found, checking for migration needs..."
fi

# Run database migration
echo "🔄 Running database migration to multi-user..."

# First, update the database schema
if [ -f "/app/data/books.db" ]; then
    echo "🔧 Updating database schema..."
    python3 migrate_db_schema.py
fi

# Then run the main migration
python3 migrate_to_multi_user.py

# Ensure proper permissions on data directory
chown -R 1000:1000 /app/data 2>/dev/null || true

echo "✅ Initialization complete, starting application..."

# Execute the main command
exec "$@"
