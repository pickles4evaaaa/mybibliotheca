# MyBibliotheca Configuration Guide

This document provides a comprehensive guide to all configuration options available in MyBibliotheca through environment variables.

## Overview

MyBibliotheca is configured through environment variables that can be set in:
- `.env` file in the project root (for standalone deployment)
- `data/.env` file (persists across Docker container restarts)
- Environment variables passed to Docker container
- System environment variables

## Required Configuration

### Security Settings

These are **required** for production deployments:

| Variable | Description | Example |
|----------|-------------|---------|
| `SECRET_KEY` | Flask secret key for session encryption | Generate with: `python3 -c "import secrets; print(secrets.token_urlsafe(32))"` |
| `SECURITY_PASSWORD_SALT` | Salt for password hashing | Generate with: `python3 -c "import secrets; print(secrets.token_urlsafe(32))"` |

⚠️ **Important**: Never use default or weak values for these settings in production!

## Application Settings

### Basic Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SITE_NAME` | `MyBibliotheca` | Name of your library instance |
| `TIMEZONE` | `UTC` | Timezone for date/time display (e.g., `America/New_York`) |
| `FLASK_DEBUG` | `false` | Enable Flask debug mode (set to `false` in production) |

### Password Policy

| Variable | Default | Description |
|----------|---------|-------------|
| `PASSWORD_MIN_LENGTH` | `12` | Minimum password length (min: 8, max: 128) |

### Reading Features

| Variable | Default | Description |
|----------|---------|-------------|
| `READING_STREAK_OFFSET` | `0` | Offset for reading streak calculations (in days) |

## Database Configuration

### KuzuDB (Graph Database)

| Variable | Default | Description |
|----------|---------|-------------|
| `KUZU_DB_PATH` | `./data/kuzu` | Path to KuzuDB database directory |
| `GRAPH_DATABASE_ENABLED` | `true` | Enable/disable graph database features |

**Note**: For Docker deployments, `KUZU_DB_PATH` should be `/app/data/kuzu`

### Docker Environment Detection

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | (not set) | Used internally to detect Docker environment. When set, indicates the application is running in a Docker container |

⚠️ **Common Mistake**: This is `DATABASE_URL` (not `DATABASE_URI`). It's used for environment detection, not for database connection strings in MyBibliotheca, as the application uses KuzuDB with file-based storage.

## Session Configuration

### Session Type

| Variable | Default | Description |
|----------|---------|-------------|
| `SESSION_TYPE` | `filesystem` | Session storage backend. Options: `filesystem`, `redis`, `cachelib` |

### Redis Configuration

Only needed when `SESSION_TYPE=redis`:

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `localhost` | Redis server hostname |
| `REDIS_PORT` | `6379` | Redis server port |
| `REDIS_PASSWORD` | (not set) | Redis authentication password |
| `REDIS_SESSION_DB` | `0` | Redis database number for sessions |

## Worker Configuration

### Process Workers

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKERS` | `1` | Number of Gunicorn worker processes |

⚠️ **Critical**: Must be set to `1` due to KuzuDB's single-process limitation. Do not change this value!

## Logging and Debugging

### Application Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `ERROR` | Logging level. Options: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG` |
| `MYBIBLIOTHECA_VERBOSE_INIT` | `false` | Enable verbose initialization messages |
| `ACCESS_LOGS` | `false` | Enable Gunicorn access logs |

### Component-Specific Debugging

| Variable | Default | Description |
|----------|---------|-------------|
| `MYBIBLIOTHECA_DEBUG` | `false` | Enable general debug mode |
| `MYBIBLIOTHECA_DEBUG_CSRF` | `false` | Debug CSRF token handling |
| `MYBIBLIOTHECA_DEBUG_SESSION` | `false` | Debug session management |
| `MYBIBLIOTHECA_DEBUG_AUTH` | `false` | Debug authentication |
| `MYBIBLIOTHECA_DEBUG_REQUESTS` | `false` | Debug HTTP requests |
| `MYBIBLIOTHECA_REQUEST_LOG` | `false` | Enable request logging wrapper |

### Database Debugging

| Variable | Default | Description |
|----------|---------|-------------|
| `KUZU_QUERY_LOG` | `false` | Enable verbose Kuzu query logging |
| `KUZU_SLOW_QUERY_MS` | `150` | Log queries slower than this threshold (milliseconds) |
| `KUZU_DEBUG` | `false` | Enable KuzuDB debugging |

## Email Configuration

⚠️ **Future Feature**: Email functionality is planned but not yet fully implemented.

| Variable | Default | Description |
|----------|---------|-------------|
| `MAIL_SERVER` | `localhost` | SMTP server hostname |
| `MAIL_PORT` | `587` | SMTP server port |
| `MAIL_USE_TLS` | `true` | Use TLS for email |
| `MAIL_USERNAME` | (not set) | SMTP authentication username |
| `MAIL_PASSWORD` | (not set) | SMTP authentication password |
| `MAIL_DEFAULT_SENDER` | `noreply@bibliotheca.local` | Default sender email address |

## Admin Configuration

### Admin Account

| Variable | Default | Description |
|----------|---------|-------------|
| `ADMIN_EMAIL` | `admin@bibliotheca.local` | Admin account email |
| `ADMIN_USERNAME` | `admin` | Admin account username |
| `ADMIN_PASSWORD` | (not set) | Admin account password |

### Development-Only Admin

**For development/testing only** - do not use in production:

| Variable | Description |
|----------|-------------|
| `DEV_ADMIN_USERNAME` | Development admin username |
| `DEV_ADMIN_PASSWORD` | Development admin password |

## API Configuration

### API Authentication

| Variable | Description |
|----------|-------------|
| `API_TEST_TOKEN` | Test token for API authentication (development only) |
| `ISBN_API_KEY` | API key for ISBN lookup services |

## Migration Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `AUTO_MIGRATE` | `false` | Enable automatic database migration |
| `MIGRATION_DEFAULT_USER` | `admin` | Default username for migrations |

## Advanced Settings

### Schema Management

| Variable | Default | Description |
|----------|---------|-------------|
| `DISABLE_SCHEMA_PREFLIGHT` | `false` | Disable automatic schema preflight checks |
| `PREFLIGHT_REL_ONLY` | `false` | Only run relationship checks in preflight |
| `PREFLIGHT_NODES_ONLY` | `false` | Only run node checks in preflight |
| `SKIP_PREFLIGHT_BACKUP` | `false` | Skip backup creation before schema changes |
| `KUZU_FORCE_RESET` | `false` | Force complete database reset (⚠️ **destructive**) |

### Recovery Mode

| Variable | Options | Description |
|----------|---------|-------------|
| `KUZU_RECOVERY_MODE` | `FAIL_FAST`, `SOFT_RENAME`, `CLEAR_REBUILD` | Database recovery strategy |

## Configuration Examples

### Minimal Production Configuration

```bash
# Required security
SECRET_KEY=your-super-secret-key-here-32-chars-minimum
SECURITY_PASSWORD_SALT=your-password-salt-here

# Application
TIMEZONE=America/New_York
FLASK_DEBUG=false

# Database (Docker)
KUZU_DB_PATH=/app/data/kuzu
GRAPH_DATABASE_ENABLED=true

# Workers (DO NOT CHANGE)
WORKERS=1

# Logging
LOG_LEVEL=ERROR
MYBIBLIOTHECA_VERBOSE_INIT=false
```

### Development Configuration

```bash
# Security (use strong values even in dev)
SECRET_KEY=dev-secret-key-change-for-production
SECURITY_PASSWORD_SALT=dev-salt-change-for-production

# Application
TIMEZONE=UTC
FLASK_DEBUG=true

# Database
KUZU_DB_PATH=./data/kuzu
GRAPH_DATABASE_ENABLED=true

# Session
SESSION_TYPE=filesystem

# Workers
WORKERS=1

# Logging
LOG_LEVEL=DEBUG
MYBIBLIOTHECA_DEBUG=true
KUZU_QUERY_LOG=true
```

### Docker Production Configuration

```bash
# Required security
SECRET_KEY=your-generated-secret-key
SECURITY_PASSWORD_SALT=your-generated-salt

# Application settings
TIMEZONE=America/New_York
FLASK_DEBUG=false

# KuzuDB Configuration (Docker paths)
KUZU_DB_PATH=/app/data/kuzu
GRAPH_DATABASE_ENABLED=true

# Worker Configuration (DO NOT CHANGE)
WORKERS=1

# Logging
LOG_LEVEL=ERROR
MYBIBLIOTHECA_VERBOSE_INIT=false
ACCESS_LOGS=false
```

## Security Best Practices

1. **Never commit `.env` files** to version control
2. **Use strong, unique values** for `SECRET_KEY` and `SECURITY_PASSWORD_SALT`
3. **Set `FLASK_DEBUG=false`** in production
4. **Use HTTPS** with a reverse proxy in production
5. **Set appropriate file permissions** on `.env` files: `chmod 600 .env`
6. **Rotate secrets regularly** in production environments
7. **Use environment-specific configurations** - don't reuse dev configs in production

## Troubleshooting

### Common Configuration Issues

**"No SECRET_KEY set for Flask application"**
- Solution: Set `SECRET_KEY` in your `.env` file

**"Database locked" errors**
- Solution: Ensure `WORKERS=1` is set (KuzuDB limitation)

**Permission errors in Docker**
- Solution: Ensure `DATABASE_URL` environment variable is set in Docker

**Session issues**
- Solution: Check `SESSION_TYPE` is set correctly and Redis is running if using `SESSION_TYPE=redis`

## See Also

- [Deployment Guide](DEPLOYMENT.md)
- [Docker Guide](../DOCKER.md)
- [Authentication Guide](AUTHENTICATION.md)
- [Debugging Guide](DEBUGGING.md)
