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
git clone https://github.com/your-username/bibliotheca.git
cd bibliotheca

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
WORKERS=4  # Adjust based on CPU cores

# Optional Performance Tuning
READING_STREAK_OFFSET=0
```

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
# Start services
docker compose up -d

# Verify deployment
docker compose ps
docker compose logs bibliotheca
```

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
```bash
# Create backup script (backup.sh)
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
docker run --rm -v bibliotheca_bibliotheca_data:/source:ro \
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
- Increase WORKERS count based on CPU cores
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
