# 🚀 Production Deployment Guide

This guide covers deploying Bibliotheca in a production environment with security best practices.

## Prerequisites

- Docker and Docker Compose installed
- Server with adequate resources (minimum 1GB RAM, 10GB storage)
- Domain name (optional, for HTTPS setup)
- Reverse proxy knowledge (nginx, Traefik, etc.)

## Production Deployment Steps

### 1. Initial Setup

```bash
# Clone the repository
git clone https://github.com/pickles4evaaaa/mybibliotheca.git
cd mybibliotheca

# Create environment configuration
cp .env.example .env
```

### 2. Security Configuration

#### Generate Secure Keys
```bash
# Generate SECRET_KEY (32+ characters)
python3 -c "import secrets; print('SECRET_KEY=' + secrets.token_urlsafe(32))" >> .env

# Generate SECURITY_PASSWORD_SALT (32+ characters) 
python3 -c "import secrets; print('SECURITY_PASSWORD_SALT=' + secrets.token_urlsafe(32))" >> .env
```

#### Configure Environment Variables
Edit `.env` with your settings:

```bash
# Required Security Keys
SECRET_KEY=your-generated-secret-key-here
SECURITY_PASSWORD_SALT=your-generated-salt-here

# Application Settings
TIMEZONE=America/Chicago
# PostgreSQL defaults to six workers. Kuzu is automatically clamped to one
# until its verified migration to PostgreSQL completes.
WORKERS=6

# Required for the bundled PostgreSQL service
POSTGRES_PASSWORD=use-a-unique-long-random-password

# Optional Performance Tuning
READING_STREAK_OFFSET=0
# Set import concurrency only if you need a fixed fallback; the admin settings UI overrides this at runtime.
IMPORT_METADATA_CONCURRENCY=4

# Optional but recommended for reliable self-hosted ISBN lookups/imports.
# Create a Google Books API key in your own Google Cloud project.
GOOGLE_BOOKS_API_KEY=your-google-books-api-key

# Optional: helps OpenLibrary identify your installation.
OPENLIBRARY_CONTACT_EMAIL=you@example.com
```

> **Tip:** Once the app is running you can adjust metadata fetch concurrency from the Admin → Settings → Server Configuration panel without touching the environment file. Leaving the field blank reverts to the environment fallback shown above.

#### Secure File Permissions
```bash
# Protect environment file
chmod 600 .env

# Ensure data directory has correct permissions
mkdir -p data
chmod 755 data
```

### 3. Deploy Application

```bash
# Pull the pinned 3.0.0 image and start services
docker compose pull
docker compose up -d

# Verify deployment
docker compose ps
docker compose logs bibliotheca
```

The Compose stack starts PostgreSQL and Redis alongside MyBibliotheca. Keep
the PostgreSQL data volume and the mounted `data/` directory in your backup
plan. Existing installations with `data/kuzu/bibliotheca.db` continue using
Kuzu until an administrator completes the migration below. New installations
with PostgreSQL configured start on PostgreSQL.

### Upgrade an existing Kuzu installation

1. Make a separate copy of the complete application `data/` directory and
   PostgreSQL volume before upgrading. Do not delete or move
   `data/kuzu/bibliotheca.db` during this upgrade.
2. Set a strong `POSTGRES_PASSWORD` in `.env`, then update and start the stack:

   ```bash
   # The Compose file pins pickles4evaaaa/mybibliotheca:3.0.0.
   docker compose pull
   docker compose up -d
   docker compose ps
   ```

3. Sign in as an administrator. On startup, the app routes the administrator
   to **Admin → Database Migration** until cutover is complete. First click
   **Create and verify Kuzu backup**. The app pauses Kuzu connections, archives
   the database plus covers/uploads/settings, and checks the ZIP contents and
   CRCs. Download the resulting backup and keep your separate `data/` copy.
4. After step 1 is verified, the page enables step 2. It checks PostgreSQL
   connectivity and inventories the Kuzu graph. The configured PostgreSQL
   target must be empty; point the migration at a fresh database if it already
   contains application rows. Do not truncate data to clear this check.
5. Schedule a maintenance window and start the verified migration. The app
   pauses Kuzu writes while it snapshots and imports the graph, then checks
   preserved source data and normalized PostgreSQL projections. If a check
   fails, Kuzu remains active and the migration report identifies the blocker.
6. When the migration verifies, the app automatically increases Gunicorn to
   the configured PostgreSQL worker count and replaces the Kuzu worker. Keep
   the page open; it reloads when the PostgreSQL workers are ready. Confirm
   **Admin → Database Migration** reports PostgreSQL as the active backend.
   If the app is running outside Gunicorn, restart the app process when prompted.
7. Keep the Kuzu database and both backups until you have verified
   your users, books, reading data, covers, and integrations on PostgreSQL.
   Redis remains enabled as the application cache after cutover.

After PostgreSQL accepts new writes, changing the backend marker back to Kuzu
is deliberately disabled because that would hide those writes. Recover by
restoring a verified PostgreSQL application backup or by following the
documented backup restore procedure. Do not treat the retained Kuzu snapshot
as a current rollback copy after users have resumed writing.

### 4. Initial Admin Setup

1. Navigate to your application URL
2. Complete the setup form to create your admin account
3. Use a strong, unique password following the requirements

### 5. Reverse Proxy Configuration (Recommended)

#### nginx Example
```nginx
server {
    listen 80;
    server_name your-domain.com;
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name your-domain.com;

    ssl_certificate /path/to/certificate.crt;
    ssl_certificate_key /path/to/private.key;

    location / {
        proxy_pass http://localhost:5054;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

#### Traefik Example
```yaml
version: '3.8'
services:
  bibliotheca:
    # ... existing configuration ...
    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.bibliotheca.rule=Host(`your-domain.com`)"
      - "traefik.http.routers.bibliotheca.tls.certresolver=letsencrypt"
      - "traefik.http.services.bibliotheca.loadbalancer.server.port=5054"
```

## Production Security Hardening

### Container Security
```yaml
# Add to docker-compose.yml service definition
security_opt:
  - no-new-privileges:true
read_only: true  # Enable if your app supports it
user: "1000:1000"  # Run as non-root user
```

### Resource Limits
```yaml
# Add to docker-compose.yml service definition
deploy:
  resources:
    limits:
      memory: 512M
      cpus: '1.0'
    reservations:
      memory: 256M
      cpus: '0.5'
```

### Network Security
- Use Docker networks to isolate services
- Implement firewall rules to restrict access
- Consider VPN access for admin functions

## Monitoring and Maintenance

### Health Monitoring
The default docker-compose.yml includes health checks:
- HTTP endpoint health verification
- 30-second intervals with 3 retries
- 40-second startup grace period

### Log Management
```bash
# View application logs
docker compose logs -f bibliotheca

# Rotate logs to prevent disk space issues
docker system prune -f
```

### Backup Strategy

Use **Admin → Backups** to create a full application backup. In PostgreSQL
mode, this includes a validated PostgreSQL dump and the covers, uploads, and
supported application settings. Keep a copy outside the Docker host and
periodically restore it to a disposable staging environment; a successful
backup creation alone does not prove that recovery works.

Also back up the Compose PostgreSQL named volume and the application `data/`
directory as part of host-level disaster recovery. Stop the application and
database services, or use a storage snapshot with database consistency, when
copying the live PostgreSQL volume. Never use the old Kuzu-only archive command
below as a PostgreSQL backup.

```bash
# Legacy Kuzu-only example; do not use after PostgreSQL cutover.
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
docker run --rm -v "$(pwd)/data:/source:ro" \
  -v $(pwd)/backups:/backup alpine \
  tar czf /backup/bibliotheca_backup_$DATE.tar.gz -C /source .

# Make executable and add to cron
chmod +x backup.sh
echo "0 2 * * * /path/to/backup.sh" | crontab -
```

### Updates
```bash
# Update application
git pull
docker compose build --no-cache
docker compose up -d

# Update base images
docker compose pull
docker compose up -d
```

## Troubleshooting

### Common Issues

**Application won't start:**
- Check environment variables in `.env`
- Verify SECRET_KEY and SECURITY_PASSWORD_SALT are set
- Check Docker logs: `docker compose logs bibliotheca`

**Permission denied errors:**
- Ensure data directory has correct permissions
- Check that container user can write to mounted volumes

**Performance issues:**
- PostgreSQL defaults to `WORKERS=6`; reduce this for a memory-constrained host
- Kuzu always uses one worker for database safety
- Monitor resource usage with `docker stats`
- Consider resource limits in docker-compose.yml

**Database corruption:**
- Restore from backup
- Check disk space and file permissions
- Verify SQLite database integrity

### Support

For production deployment issues:
1. Check application logs first
2. Verify environment configuration
3. Test with development setup to isolate issues
4. Review security settings and permissions

## Production Checklist

Before going live:
- [ ] Secure environment variables generated and set
- [ ] File permissions properly configured
- [ ] Reverse proxy with HTTPS configured
- [ ] Firewall rules implemented
- [ ] Backup strategy in place
- [ ] Monitoring setup and tested
- [ ] Admin account created and tested
- [ ] Resource limits configured
- [ ] Security hardening applied
- [ ] Update procedure documented
