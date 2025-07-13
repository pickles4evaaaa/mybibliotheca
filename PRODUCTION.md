# Production Deployment Guide

## Quick Production Setup

### 1. Server Requirements

**Minimum:**
- 2 CPU cores
- 4GB RAM
- 20GB disk space
- Docker & Docker Compose

**Recommended:**
- 4+ CPU cores
- 8GB+ RAM
- SSD storage
- Regular backups

### 2. Initial Setup

```bash
# Clone repository
git clone <your-repo-url> mybibliotheca
cd mybibliotheca

# Create production environment file
cp .env.docker.example .env

# Generate secure keys (IMPORTANT!)
python3 -c "import secrets; print('SECRET_KEY=' + secrets.token_urlsafe(32))"
python3 -c "import secrets; print('SECURITY_PASSWORD_SALT=' + secrets.token_urlsafe(32))"

# Edit .env with your keys
nano .env
```

### 3. Production Environment (.env)

```bash
# REQUIRED: Generate unique values for production
SECRET_KEY=your-generated-secret-key-here
SECURITY_PASSWORD_SALT=your-generated-salt-here

# Production settings
FLASK_DEBUG=false
TIMEZONE=America/New_York

# KuzuDB Configuration (DO NOT CHANGE)
KUZU_DB_PATH=/app/data/kuzu
GRAPH_DATABASE_ENABLED=true
WORKERS=1

# Logging
MYBIBLIOTHECA_VERBOSE_INIT=false
```

### 4. Launch Application

```bash
# Start in production mode
docker-compose up -d

# Check status
docker-compose ps
docker-compose logs -f bibliotheca
```

### 5. Initial Configuration

1. Open `https://your-domain.com` (or `http://server-ip:5054`)
2. Complete the setup wizard
3. Create your admin account
4. Configure any additional settings

## Reverse Proxy Setup (Recommended)

### Nginx Configuration

Create `/etc/nginx/sites-available/mybibliotheca`:

```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    # Redirect HTTP to HTTPS
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name your-domain.com;
    
    # SSL Configuration (use certbot for Let's Encrypt)
    ssl_certificate /etc/letsencrypt/live/your-domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/your-domain.com/privkey.pem;
    
    # Security headers
    add_header X-Frame-Options DENY;
    add_header X-Content-Type-Options nosniff;
    add_header X-XSS-Protection "1; mode=block";
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains";
    
    # Proxy to application
    location / {
        proxy_pass http://127.0.0.1:5054;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # File upload settings
        client_max_body_size 100M;
        proxy_read_timeout 300s;
        proxy_connect_timeout 75s;
    }
    
    # Static files (optional caching)
    location /static/ {
        proxy_pass http://127.0.0.1:5054;
        expires 30d;
        add_header Cache-Control "public, no-transform";
    }
}
```

Enable and restart nginx:
```bash
sudo ln -s /etc/nginx/sites-available/mybibliotheca /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

### Traefik Configuration

Create `docker-compose.prod.yml`:

```yaml
version: '3.8'

services:
  bibliotheca:
    build: .
    volumes:
      - ./data:/app/data
    environment:
      - SECRET_KEY=${SECRET_KEY}
      - SECURITY_PASSWORD_SALT=${SECURITY_PASSWORD_SALT}
      - KUZU_DB_PATH=/app/data/kuzu
      - GRAPH_DATABASE_ENABLED=true
      - WORKERS=1
    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.bibliotheca.rule=Host(`your-domain.com`)"
      - "traefik.http.routers.bibliotheca.tls.certresolver=letsencrypt"
      - "traefik.http.services.bibliotheca.loadbalancer.server.port=5054"
    restart: unless-stopped
    networks:
      - traefik

networks:
  traefik:
    external: true
```

## Backup Strategy

### Automated Backup Script

Create `/opt/mybibliotheca-backup.sh`:

```bash
#!/bin/bash

# Configuration
BACKUP_DIR="/backups/mybibliotheca"
APP_DIR="/path/to/mybibliotheca"
RETENTION_DAYS=30

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Stop application
cd "$APP_DIR"
docker-compose down

# Create backup
BACKUP_FILE="$BACKUP_DIR/mybibliotheca-$(date +%Y%m%d-%H%M%S).tar.gz"
tar -czf "$BACKUP_FILE" -C "$APP_DIR" data/

# Start application
docker-compose up -d

# Clean old backups
find "$BACKUP_DIR" -name "mybibliotheca-*.tar.gz" -mtime +$RETENTION_DAYS -delete

echo "Backup completed: $BACKUP_FILE"
```

Add to crontab:
```bash
# Daily backup at 2 AM
0 2 * * * /opt/mybibliotheca-backup.sh
```

### Manual Backup

```bash
# Stop application
docker-compose down

# Create backup
tar -czf mybibliotheca-backup-$(date +%Y%m%d).tar.gz data/

# Start application
docker-compose up -d
```

### Restore from Backup

```bash
# Stop application
docker-compose down

# Restore data
tar -xzf mybibliotheca-backup-YYYYMMDD.tar.gz

# Start application
docker-compose up -d
```

## Monitoring & Maintenance

### Health Checks

```bash
# Application status
curl -f http://localhost:5054/

# Container status
docker-compose ps

# Logs
docker-compose logs --tail=100 bibliotheca

# Resource usage
docker stats $(docker-compose ps -q)
```

### Log Management

```bash
# Rotate logs (add to crontab)
docker-compose logs --tail=1000 bibliotheca > /var/log/mybibliotheca.log 2>&1
```

### Database Maintenance

```bash
# Check KuzuDB size
du -sh ./data/kuzu/

# Monitor disk space
df -h ./data/
```

## Security Considerations

### Environment Security
- ✅ Use unique `SECRET_KEY` and `SECURITY_PASSWORD_SALT`
- ✅ Set `FLASK_DEBUG=false` in production
- ✅ Use HTTPS (SSL/TLS) with reverse proxy
- ✅ Regular security updates of host system
- ✅ Firewall configuration (only open necessary ports)

### Application Security
- ✅ Regular backups
- ✅ Monitor access logs
- ✅ Use strong admin passwords
- ✅ Regular application updates

### File Permissions
```bash
# Secure data directory
chmod 755 ./data/
chown -R 1000:1000 ./data/
```

## Troubleshooting

### Common Issues

**Lock File Errors:**
```bash
docker-compose down
rm -f ./data/kuzu/.lock
docker-compose up -d
```

**Permission Issues:**
```bash
sudo chown -R 1000:1000 ./data/
```

**Memory Issues:**
```bash
# Check container memory usage
docker stats

# Increase host memory or optimize KuzuDB settings
```

**Performance Issues:**
- Monitor disk I/O (use SSD for better performance)
- Check available memory
- Consider increasing container resources
- Use reverse proxy caching for static assets

### Getting Help

1. Check logs: `docker-compose logs bibliotheca`
2. Verify configuration: Review `.env` file
3. Test connectivity: `curl -I http://localhost:5054/`
4. Check resources: `docker stats`, `df -h`, `free -m`

## Scaling Limitations

⚠️ **Important**: This application uses KuzuDB which has the following limitations:

- **Single Worker Only**: Cannot scale beyond 1 worker process
- **No Load Balancing**: Cannot run multiple instances on same database
- **Vertical Scaling**: Only CPU/RAM can be increased, not horizontal scaling

For high-traffic deployments, consider:
- Using a powerful single server
- Implementing caching layers
- Optimizing database queries
- Using CDN for static assets
