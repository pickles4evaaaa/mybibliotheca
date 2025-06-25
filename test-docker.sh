#!/bin/bash
# Docker Test Script for MyBibliotheca KuzuDB Setup

set -e

echo "🧪 Testing MyBibliotheca Docker Setup"
echo "======================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check prerequisites
echo "🔍 Checking prerequisites..."

if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed or not in PATH"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose is not installed or not in PATH"
    exit 1
fi

print_status "Docker and Docker Compose are available"

# Check if .env file exists
if [ ! -f .env ]; then
    print_warning ".env file not found. Creating from template..."
    if [ -f .env.docker.example ]; then
        cp .env.docker.example .env
        
        # Generate secure keys
        SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))" 2>/dev/null || echo "change-this-secret-key-$(date +%s)")
        SALT=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))" 2>/dev/null || echo "change-this-salt-$(date +%s)")
        
        # Update .env with generated keys
        sed -i.bak "s/SECRET_KEY=your-secret-key-here/SECRET_KEY=$SECRET_KEY/" .env
        sed -i.bak "s/SECURITY_PASSWORD_SALT=your-salt-here/SECURITY_PASSWORD_SALT=$SALT/" .env
        rm .env.bak 2>/dev/null || true
        
        print_status "Created .env file with generated secrets"
    else
        print_error ".env.docker.example not found. Please create .env manually."
        exit 1
    fi
fi

# Clean up any existing containers
echo "🧹 Cleaning up existing containers..."
docker-compose down 2>/dev/null || true

# Clean up any KuzuDB lock files
if [ -d "./data/kuzu" ]; then
    print_warning "Removing KuzuDB lock files..."
    rm -f ./data/kuzu/.lock 2>/dev/null || true
fi

# Build and start the container
echo "🏗️  Building Docker image..."
if ! docker-compose build --no-cache; then
    print_error "Docker build failed"
    exit 1
fi

print_status "Docker image built successfully"

echo "🚀 Starting container..."
if ! docker-compose up -d; then
    print_error "Failed to start container"
    exit 1
fi

print_status "Container started"

# Wait for the application to be ready
echo "⏳ Waiting for application to be ready..."
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if curl -s -f http://localhost:5054/ > /dev/null 2>&1; then
        print_status "Application is responding!"
        break
    fi
    
    attempt=$((attempt + 1))
    echo "   Attempt $attempt/$max_attempts..."
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    print_error "Application failed to start within expected time"
    echo "📋 Container logs:"
    docker-compose logs bibliotheca --tail=20
    exit 1
fi

# Test basic endpoints
echo "🔍 Testing application endpoints..."

# Test root endpoint
if curl -s -f http://localhost:5054/ > /dev/null; then
    print_status "Root endpoint (/) is accessible"
else
    print_error "Root endpoint (/) is not accessible"
fi

# Test setup endpoint
if curl -s -f http://localhost:5054/auth/setup > /dev/null; then
    print_status "Setup endpoint (/auth/setup) is accessible"
else
    print_warning "Setup endpoint (/auth/setup) returned an error (may be normal if setup is complete)"
fi

# Check container health
echo "🏥 Checking container health..."
container_status=$(docker-compose ps -q bibliotheca | xargs docker inspect --format='{{.State.Status}}' 2>/dev/null || echo "unknown")

if [ "$container_status" = "running" ]; then
    print_status "Container is running"
else
    print_error "Container status: $container_status"
fi

# Check KuzuDB directory
if [ -d "./data/kuzu" ]; then
    print_status "KuzuDB directory exists"
    if [ -f "./data/kuzu/.lock" ]; then
        print_warning "KuzuDB lock file exists (this is normal when app is running)"
    fi
else
    print_warning "KuzuDB directory not found (will be created on first run)"
fi

# Show final status
echo ""
echo "🎉 Docker test completed!"
echo "======================================"
print_status "Application is running at: http://localhost:5054"
print_status "Complete setup by visiting the web interface"
echo ""
echo "📋 Useful commands:"
echo "   View logs:    docker-compose logs -f bibliotheca"
echo "   Stop app:     docker-compose down"
echo "   Restart:      docker-compose restart"
echo "   Rebuild:      docker-compose build --no-cache && docker-compose up -d"
echo ""
echo "🗄️  Data is persisted in: ./data/"
echo "⚠️  Remember: This app uses single worker due to KuzuDB limitations"
