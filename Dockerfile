# Use slim Python base image for smaller footprint
FROM python:3.12-slim

# Avoid writing .pyc files and enable unbuffered logging (good for Docker)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Configure OpenSSL for compatibility with modern Python and enable legacy support
ENV OPENSSL_CONF=/etc/ssl/openssl.cnf
ENV OPENSSL_ENABLE_SHA1_SIGNATURES=1

# Set working directory
WORKDIR /app

# Install system dependencies required for psutil and cryptographic packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    python3-dev \
    openssl \
    ca-certificates \
    libssl-dev \
    libffi-dev \
    build-essential \
    pkg-config \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configure OpenSSL to support legacy algorithms for compatibility
ENV OPENSSL_CONF=/etc/ssl/openssl.cnf
RUN echo "openssl_conf = openssl_init" >> /etc/ssl/openssl.cnf && \
    echo "" >> /etc/ssl/openssl.cnf && \
    echo "[openssl_init]" >> /etc/ssl/openssl.cnf && \
    echo "providers = provider_sect" >> /etc/ssl/openssl.cnf && \
    echo "" >> /etc/ssl/openssl.cnf && \
    echo "[provider_sect]" >> /etc/ssl/openssl.cnf && \
    echo "default = default_sect" >> /etc/ssl/openssl.cnf && \
    echo "legacy = legacy_sect" >> /etc/ssl/openssl.cnf && \
    echo "" >> /etc/ssl/openssl.cnf && \
    echo "[default_sect]" >> /etc/ssl/openssl.cnf && \
    echo "activate = 1" >> /etc/ssl/openssl.cnf && \
    echo "" >> /etc/ssl/openssl.cnf && \
    echo "[legacy_sect]" >> /etc/ssl/openssl.cnf && \
    echo "activate = 1" >> /etc/ssl/openssl.cnf

# Install Python dependencies
COPY requirements.txt .
# Upgrade pip and install cryptographic dependencies first
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir cryptography
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code
COPY . .

# Create directory for SQLite database (used in volume)
RUN mkdir -p /app/data

# Set environment variables for multi-user authentication
ENV DATABASE_URL=sqlite:////app/data/books.db
ENV WTF_CSRF_ENABLED=True

# Default admin credentials (override with docker run -e or docker-compose)
# These defaults should be changed in production via environment variables
ENV ADMIN_EMAIL=admin@bibliotheca.local
ENV ADMIN_USERNAME=admin

# Flask environment (using FLASK_DEBUG instead of deprecated FLASK_ENV)
ENV FLASK_DEBUG=false

# Create entrypoint script for initialization
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Expose internal port used by Gunicorn
EXPOSE 5054

# Use custom entrypoint that handles migration
ENTRYPOINT ["docker-entrypoint.sh"]

# Start the app with Gunicorn in production mode
# Use WORKERS environment variable for Gunicorn workers Default to 6 workers if not specified
ENV WORKERS=6
CMD ["sh", "-c", "gunicorn -w $WORKERS -b 0.0.0.0:5054 run:app"]
