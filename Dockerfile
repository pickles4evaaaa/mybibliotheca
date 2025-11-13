# Use slim Python base image for smaller footprint
FROM python:3.13-slim

# Avoid writing .pyc files and enable unbuffered logging (good for Docker)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Configure OpenSSL for compatibility with modern Python and enable legacy support
ENV OPENSSL_CONF=/etc/ssl/openssl.cnf
ENV OPENSSL_ENABLE_SHA1_SIGNATURES=1

# Set working directory
WORKDIR /app

# Install system dependencies required for psutil, cryptographic packages, OCR, and imaging
RUN set -eux; \
        apt-get update; \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            gcc \
            g++ \
            python3-dev \
            openssl \
            ca-certificates \
            libssl-dev \
            libffi-dev \
            build-essential \
            pkg-config \
            tesseract-ocr \
            tesseract-ocr-eng \
            libtesseract-dev \
            libzbar0 \
            libzbar-dev \
            libgl1 \
            libglib2.0-0 \
            libsm6 \
            libxext6 \
            libxrender1 \
            libgomp1 \
            libgtk-3-0 \
        ; \
        # Clean up apt cache to keep image small (keep runtime libs installed)
        rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

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
RUN pip install --upgrade pip setuptools wheel
RUN pip install cryptography
RUN pip install -r requirements.txt

# Copy all source code
COPY . .

# Create directory for KuzuDB and application data with proper permissions
RUN mkdir -p /app/data /app/data/kuzu /app/data/covers /app/data/uploads /app/static/covers && \
    chmod 755 /app/data /app/data/kuzu /app/data/covers /app/data/uploads /app/static/covers

# Set environment variables for KuzuDB-based multi-user authentication
ENV WTF_CSRF_ENABLED=True
ENV KUZU_DB_PATH=/app/data/kuzu
ENV GRAPH_DATABASE_ENABLED=true

# Create data directory and make it a volume for persistence
RUN mkdir -p /app/data/kuzu
VOLUME ["/app/data"]

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
# CRITICAL: Use single worker and single thread for KuzuDB compatibility (no concurrent access)
ENV WORKERS=1
# Set timeout to 300 seconds (5 minutes) to handle bulk imports with rate limiting
# Disable sendfile to prevent occasional deadlocks on Docker for macOS/overlay FS
# Use sync worker class and force single threaded operation for KuzuDB
# Preload application to avoid multiple KuzuDB initialization attempts
ARG ACCESS_LOGS="false"
# Default: disable access logs to keep container output quiet; errors still go to stderr
CMD ["/bin/sh", "-c", "if [ \"$ACCESS_LOGS\" = \"true\" ]; then exec gunicorn --worker-class sync --no-sendfile -w 1 --threads 1 -b 0.0.0.0:5054 --timeout 300 --graceful-timeout 300 --error-logfile - --access-logfile - --max-requests 1000 --max-requests-jitter 100 run:app; else exec gunicorn --worker-class sync --no-sendfile -w 1 --threads 1 -b 0.0.0.0:5054 --timeout 300 --graceful-timeout 300 --error-logfile - --max-requests 1000 --max-requests-jitter 100 run:app; fi"]
