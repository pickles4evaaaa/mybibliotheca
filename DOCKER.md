# Docker Deployment Guide for MyBibliotheca

## Quick Start

1. **Clone and prepare environment:**
```bash
git clone <repository-url>
cd mybibliotheca
cp .env.docker.example .env
# Edit .env with your secret keys
```

2. **Generate secure keys:**
```bash
python3 -c "import secrets; print('SECRET_KEY=' + secrets.token_urlsafe(32))"
python3 -c "import secrets; print('SECURITY_PASSWORD_SALT=' + secrets.token_urlsafe(32))"
```

3. **Start the application:**
```bash
# Production
docker-compose up -d

# Development (with debug enabled)
docker-compose -f docker-compose.dev.yml up -d
```

4. **Access the application:**
   - Open http://localhost:5054
   - Complete the setup using the web interface

## Important Notes

### KuzuDB Limitations
- **Single Worker Only**: This application uses KuzuDB which doesn't support concurrent access
- **No Scaling**: Do not increase WORKERS beyond 1
- **Persistence**: KuzuDB data is stored in `./data/kuzu/` (mounted volume)

### Docker Configuration

#### Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `SECRET_KEY` | **Required** | Flask secret key |
| `SECURITY_PASSWORD_SALT` | **Required** | Password hashing salt |
| `WORKERS` | `1` | **DO NOT CHANGE** - KuzuDB limitation |
| `KUZU_DB_PATH` | `/app/data/kuzu` | KuzuDB storage path |
| `GRAPH_DATABASE_ENABLED` | `true` | Enable KuzuDB |

#### Volume Mounts
- `./data:/app/data` - Application data and KuzuDB storage
- Data persists between container restarts

#### Ports
- `5054` - Web application port

## Troubleshooting

### Lock File Issues
If you see "Could not set lock on file" errors:
```bash
# Stop containers
docker-compose down

# Remove lock files
sudo rm -f ./data/kuzu/.lock

# Restart
docker-compose up -d
```

### Permission Issues
```bash
# Fix data directory permissions
sudo chown -R 1000:1000 ./data/
```

### Reset Database
```bash
# CAUTION: This will delete all data
docker-compose down
sudo rm -rf ./data/kuzu/
docker-compose up -d
```

## Production Deployment

### Security Checklist
- [ ] Set unique `SECRET_KEY` and `SECURITY_PASSWORD_SALT`
- [ ] Use HTTPS with reverse proxy (nginx/traefik)
- [ ] Set `FLASK_DEBUG=false`
- [ ] Regular backups of `./data/` directory
- [ ] Monitor disk space (KuzuDB can grow large)

### Backup Strategy
```bash
# Create backup
docker-compose down
tar -czf mybibliotheca-backup-$(date +%Y%m%d).tar.gz ./data/
docker-compose up -d

# Restore backup
docker-compose down
tar -xzf mybibliotheca-backup-YYYYMMDD.tar.gz
docker-compose up -d
```

### Performance Notes
- Single worker limitation may impact performance under heavy load
- Consider using a reverse proxy with caching for static assets
- Monitor memory usage as KuzuDB loads data into memory
- Use SSD storage for better KuzuDB performance

## Migration from SQLite/Redis

If migrating from a previous version:
1. Stop the application
2. Use the migration tools in the `scripts/` directory
3. Update Docker configuration
4. Start with new Docker setup

See `MIGRATION.md` for detailed migration instructions.
