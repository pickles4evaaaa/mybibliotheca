# 📚 MyBibliotheca

# Beta 2.0.0
**⚠️ Warning**: This is a BETA version under active development. Always back up your data before upgrading. The developers do not guarantee data persistence or error-free operation. Please submit issues to the repository, and we will address them as soon as possible.

**MyBibliotheca** is a self-hosted personal library and reading-tracker—your open-source alternative to Goodreads, StoryGraph, and Fable! It lets you log, organize, and visualize your reading journey. Add books by ISBN, track reading progress, log daily reading, and generate monthly wrap-up images of your finished titles.

🆕 **Multi-User Features**: Multi-user authentication, user data isolation, admin management, and secure password handling.

[![Documentation](https://img.shields.io/badge/Documentation-MyBibliotheca-4a90e2?style=for-the-badge&logo=read-the-docs&logoColor=white)](https://mybibliotheca.org)

[![Discord](https://img.shields.io/badge/Discord-7289DA?logo=discord&logoColor=white&labelColor=7289DA&style=for-the-badge)](https://discord.gg/Hc8C5eRm7Q)


---

## ✨ Features

- 📖 **Add Books**: Add books quickly by ISBN with automatic cover and metadata fetching. Now featuring bulk-import from Goodreads and other CSV files!
  - ⚠️ **Camera Scanning**: Currently limited - HTTPS required for device camera access due to browser security. Improvements planned.
- ✅ **Track Progress**: Mark books as *Currently Reading*, *Plan to Read*, *Finished*, or *Library Only*.
- 📅 **Reading Logs**: Log daily reading activity and maintain streaks.
-  **Search**: Find and import books using the Google Books API.
- 📱 **Responsive UI**: Clean, mobile-friendly interface built with Bootstrap.
- 🔐 **Multi-User Support**: Secure authentication with user data isolation
- 👤 **Admin Management**: Administrative tools and user management
- � **Graph Database**: Powered by KuzuDB for advanced relationship modeling and queries

---

## 🚀 Getting Started

### 📦 Run with Docker (Recommended)

MyBibliotheca uses **KuzuDB** as its graph database and is designed to run in Docker for consistent deployment across platforms.

#### ✅ Prerequisites

- [Docker](https://www.docker.com/) installed
- [Docker Compose](https://docs.docker.com/compose/) installed

---

#### 🚀 Quick Start (Build from Source)

⚠️ **Note**: There is currently no Docker Hub image available. You must build the image from source.

```bash
# Clone the repository
git clone https://github.com/pickles4evaaaa/mybibliotheca.git
cd mybibliotheca

# Create environment file
cp .env.example .env

# IMPORTANT: Edit .env with your secure keys (see security section below)
nano .env  # or use your preferred editor

# Build the Docker image
docker compose build

# Start the application
docker compose up -d

# Access at http://localhost:5054
```


#### 🔐 Security Setup (Important!)

Before running, you **must** generate secure keys:

```bash
# Generate SECRET_KEY
python3 -c "import secrets; print('SECRET_KEY=' + secrets.token_urlsafe(32))"

# Generate SECURITY_PASSWORD_SALT
python3 -c "import secrets; print('SECURITY_PASSWORD_SALT=' + secrets.token_urlsafe(32))"

# Add these values to your .env file
```

Update your `.env` file with the generated values:
```bash
SECRET_KEY=<generated-secret-key>
SECURITY_PASSWORD_SALT=<generated-password-salt>
```

#### 🔧 Docker Configuration

Your `.env` file should contain (at minimum):

```bash
# REQUIRED: Generate unique values for production
SECRET_KEY=your-generated-secret-key-here
SECURITY_PASSWORD_SALT=your-generated-salt-here

# Application settings
SITE_NAME=MyBibliotheca
TIMEZONE=America/Chicago

# Database configuration
KUZU_DB_PATH=/app/data/kuzu
GRAPH_DATABASE_ENABLED=true

# CRITICAL: Must be 1 for KuzuDB compatibility
WORKERS=1

# Optional: Logging
LOG_LEVEL=INFO
ACCESS_LOGS=false
```


#### ⚠️ Important Limitations

- **Single Worker Only**: KuzuDB doesn't support concurrent access, so `WORKERS` must remain `1`
- **No Horizontal Scaling**: Cannot run multiple instances accessing the same database
- **Data Persistence**: All data is stored in `./data/` directory (Docker volume)
- **Build Required**: No pre-built Docker Hub image yet - must build from source

#### 🧪 Test Your Setup

```bash
# Check if the container is running
docker compose ps

# View logs
docker compose logs -f

# Test the endpoint
curl -f http://localhost:5054/

# Stop the application
docker compose down
```

#### � Docker Compose Reference

For reference, here's the basic structure if you want to customize:

```yaml
services:
  bibliotheca:
    build: .
    ports:
      - "5054:5054"
    volumes:
      # Data directory for Kuzu database and user uploads
      - ./data:/app/data
    environment:
      - SECRET_KEY=${SECRET_KEY}
      - SECURITY_PASSWORD_SALT=${SECURITY_PASSWORD_SALT}
      - KUZU_DB_PATH=/app/data/kuzu
      - GRAPH_DATABASE_ENABLED=true
      - TIMEZONE=${TIMEZONE:-UTC}
      - WORKERS=1  # MUST be 1 for KuzuDB
      - SITE_NAME=${SITE_NAME:-MyBibliotheca}
      - LOG_LEVEL=${LOG_LEVEL}
    restart: unless-stopped
```

---

### 📖 Alternative Installation Methods

#### 🐍 Run from Source (Development)

For development or if you prefer not to use Docker:

**Prerequisites:**
- Python 3.8+ installed
- pip package manager

**Installation Steps:**

```bash
# 1. Clone the repository
git clone https://github.com/pickles4evaaaa/mybibliotheca.git
cd mybibliotheca

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Setup environment
cp .env.example .env
# Edit .env with your configuration

# 5. Initialize data directory
python3 setup_data_dir.py

# 4. Setup data directory
python3 scripts/setup_data_dir.py

# 5. Run the application
python3 run.py

# Or with Gunicorn (production-like)
gunicorn -w 1 -b 0.0.0.0:5054 run:app

# Access at http://localhost:5054
```

**Note**: Remember to use `WORKERS=1` even when running from source due to KuzuDB limitations.

---

## 🔐 Authentication & User Management

### First Time Setup

When you first run MyBibliotheca, you'll be prompted to complete a one-time setup:

1. **Access the application** at `http://localhost:5054` (or your configured port)
2. **Complete the setup form** to create your administrator account:
   - Choose an admin username (3-20 characters, alphanumeric)
   - Provide a valid admin email address
   - Set a secure password (minimum 8 characters: uppercase, lowercase, number)
   - Confirm your password
3. **Start using MyBibliotheca** - you'll be automatically logged in after setup

✅ **Secure by Design**: No default credentials - you control your admin account from the start!

### Password Security

- **Strong password requirements**: Minimum 8 characters with uppercase, lowercase, and numbers
- **Secure storage**: All passwords are hashed using industry-standard bcrypt
- **Admin controls**: Built-in tools for password management

### Admin Tools

Use the built-in admin tools for user management:

**Docker:**
```bash
# Reset admin password (interactive)
docker exec -it bibliotheca python3 scripts/admin_tools.py reset-admin-password

# Create additional admin user
docker exec -it bibliotheca python3 scripts/admin_tools.py create-admin

# List all users
docker exec -it bibliotheca python3 scripts/admin_tools.py list-users

# System statistics
docker exec -it bibliotheca python3 scripts/admin_tools.py system-stats
```

**From Source:**
```bash
# Activate your virtual environment first
source venv/bin/activate

# Then run admin tools
python3 scripts/admin_tools.py reset-admin-password
python3 scripts/admin_tools.py create-admin
python3 scripts/admin_tools.py list-users
python3 scripts/admin_tools.py system-stats
```

### Setup Troubleshooting

If you encounter issues during setup:

**🔧 Quick Diagnostics:**
```bash
# Check application logs
docker compose logs -f bibliotheca

# Check if container is running
docker compose ps

# Restart the application
docker compose restart

# Check setup status endpoint
curl http://localhost:5054/auth/setup/status
```

**Common Issues & Solutions:**

| Issue | Solution |
|-------|----------|
| "Security token expired" | Refresh the page and try again |
| Button does nothing | Check browser console for errors, ensure JavaScript is enabled |
| "Setup already completed" | Users already exist - go to `/auth/login` |
| Form validation errors | Check that all fields meet requirements (see above) |
| Connection refused | Ensure Docker container is running: `docker compose ps` |
| Database errors | Check data directory permissions and available disk space |

**🐛 Enable Debug Mode:**
```bash
# Add to your .env file
MYBIBLIOTHECA_DEBUG=true
MYBIBLIOTHECA_DEBUG_AUTH=true
MYBIBLIOTHECA_DEBUG_CSRF=true

# Restart the application
docker compose restart

# View detailed logs
docker compose logs -f
```

**🔄 Reset for Fresh Setup:**
```bash
# WARNING: This deletes all data
docker compose down
sudo rm -rf ./data/*
docker compose up -d
```

📖 **Documentation:**
- **[AUTHENTICATION.md](docs/AUTHENTICATION.md)** - Complete authentication guide
- **[ADMIN_TOOLS.md](docs/ADMIN_TOOLS.md)** - Admin tools and user management
- **[MIGRATION.md](docs/MIGRATION.md)** - Database migration guide
- **[TESTING.md](docs/TESTING.md)** - Testing documentation

---

## 🚀 Production Deployment

### Quick Production Setup

1. **Clone and configure**:
```bash
git clone https://github.com/pickles4evaaaa/mybibliotheca.git
cd mybibliotheca
cp .env.example .env
```

2. **Generate secure keys**:
```bash
# Generate SECRET_KEY
python3 -c "import secrets; print('SECRET_KEY=' + secrets.token_urlsafe(32))" >> .env

# Generate SECURITY_PASSWORD_SALT  
python3 -c "import secrets; print('SECURITY_PASSWORD_SALT=' + secrets.token_urlsafe(32))" >> .env
```

3. **Customize configuration** (edit `.env`):
```bash
# Set your timezone
TIMEZONE=America/Chicago

# Set site name
SITE_NAME=MyBibliotheca

# Configure logging
LOG_LEVEL=INFO
ACCESS_LOGS=false

# KuzuDB settings (DO NOT CHANGE)
WORKERS=1
KUZU_DB_PATH=/app/data/kuzu
GRAPH_DATABASE_ENABLED=true
```

4. **Build and deploy**:
```bash
docker compose build
docker compose up -d
```

5. **Complete setup**: Visit your application and create your admin account through the setup page

### Production Security Checklist

- ✅ **Environment Variables**: Use `.env` file with secure random keys (never commit to git)
- ✅ **HTTPS**: Deploy behind reverse proxy with SSL/TLS (nginx, Caddy, Traefik, etc.)
- ✅ **Firewall**: Restrict access to port 5054 from reverse proxy only
- ✅ **Backups**: Implement regular backups of `./data/` directory
- ✅ **Updates**: Keep Docker images and host system updated
- ✅ **Monitoring**: Set up health checks and log monitoring
- ✅ **Secrets**: Never use default/example keys in production

### Reverse Proxy Examples

**Nginx:**
```nginx
server {
    listen 80;
    server_name mybibliotheca.example.com;
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name mybibliotheca.example.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location / {
        proxy_pass http://localhost:5054;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

**Caddy (Automatic HTTPS):**
```
mybibliotheca.example.com {
    reverse_proxy localhost:5054
}
```

### Backup Strategy

```bash
# Create backup script
cat > backup.sh << 'EOF'
#!/bin/bash
BACKUP_DIR="/backups/mybibliotheca"
DATE=$(date +%Y%m%d_%H%M%S)
mkdir -p "$BACKUP_DIR"
tar -czf "$BACKUP_DIR/mybibliotheca_$DATE.tar.gz" ./data/
# Keep last 7 days of backups
find "$BACKUP_DIR" -name "mybibliotheca_*.tar.gz" -mtime +7 -delete
EOF

chmod +x backup.sh

# Add to crontab (daily at 2 AM)
echo "0 2 * * * /path/to/backup.sh" | crontab -
```

---

## 🗂️ Project Structure

```
mybibliotheca/
├── app/
│   ├── __init__.py              # Application factory
│   ├── auth.py                  # Authentication routes
│   ├── domain/                  # Domain models and business logic
│   ├── infrastructure/          # KuzuDB connection and repositories
│   ├── routes/                  # Application routes
│   ├── services/                # Business logic services
│   ├── schema/                  # Database schema definitions
│   ├── templates/               # Jinja2 templates
│   ├── static/                  # Static assets (CSS, JS, images)
│   └── utils/                   # Utility functions
├── data/                        # Data directory (mounted volume)
│   ├── kuzu/                    # KuzuDB database files
│   ├── covers/                  # Book cover images
│   └── uploads/                 # User uploaded files
├── scripts/                     # Admin and utility scripts
├── docs/                        # Documentation
├── docker-compose.yml           # Docker Compose configuration
├── Dockerfile                   # Docker image definition
├── requirements.txt             # Python dependencies
├── run.py                       # Application entry point
└── README.md                    # This file
```

---

## 📄 License

Licensed under the [MIT License](LICENSE).

---

## ❤️ Contribute

**MyBibliotheca** is open source and contributions are welcome!

- 🐛 **Report Bugs**: Open an issue on GitHub
- 💡 **Feature Requests**: Submit ideas for new features
- 🔧 **Pull Requests**: Contribute code improvements
- 📖 **Documentation**: Help improve our docs
- 💬 **Community**: Join our [Discord](https://discord.gg/Hc8C5eRm7Q)

### Development Setup

```bash
# Fork and clone the repository
git clone https://github.com/YOUR_USERNAME/mybibliotheca.git
cd mybibliotheca

# Create a branch for your changes
git checkout -b feature/my-new-feature

# Make your changes and test
docker compose -f docker-compose.dev.yml up -d

# Submit a pull request
```

---

## 🧪 Schema Maintenance & Auto-Migrations

MyBibliotheca uses **KuzuDB** with an additive, startup-time schema augmentation system. A JSON file (`app/schema/master_schema.json`) is the single source of truth for expected node columns and relationship definitions.

### How It Works
On application import (`run.py`), `schema_preflight`:
1. Loads `master_schema.json` (override path with `MASTER_SCHEMA_PATH` env var)
2. Logs schema version & SHA256 hash
3. Detects missing node columns and relationship tables/properties
4. (Optional) Creates a backup before applying changes
5. Executes `ALTER TABLE ... ADD` or `CREATE REL TABLE ...` statements

Only **additive** operations are performed automatically (no drops or renames). Destructive changes must be handled manually.

### Environment Flags
| Variable | Effect |
|----------|--------|
| `DISABLE_SCHEMA_PREFLIGHT` | Skip preflight entirely |
| `SKIP_PREFLIGHT_BACKUP` | Don’t create automatic backup before applying changes |
| `PREFLIGHT_REL_ONLY` | Process only relationship changes |
| `PREFLIGHT_NODES_ONLY` | Process only node columns |
| `MASTER_SCHEMA_PATH` | Alternate location for schema JSON |

### Adding New Columns / Relationships
1. Edit `app/schema/master_schema.json`
2. Increment `version` (optional but recommended)
3. Add new node columns under the appropriate `"columns"` map or new relationship under `"relationships"`
4. Restart the application – preflight will add them automatically

### Backups
Before applying changes, a backup is created via `SimpleBackupService` unless `SKIP_PREFLIGHT_BACKUP` is set. Store backups securely if running in production.

### Safety Tips
* Avoid renaming or deleting columns directly – instead, add new ones and migrate data via a one-off script.
* Review logs on startup to confirm: `Schema preflight upgrade complete`.
* Use `DISABLE_SCHEMA_PREFLIGHT` in emergency scenarios where schema probing must be skipped.

### Future Enhancements (Planned)
* Non-additive migration scripting (manual approval)
* Structured migration history & checksum validation
* Optional dry-run reporting mode

---

## 🔍 Build & Deploy Verification

Use this checklist to ensure your deployment is working correctly:

### ✅ Pre-Deployment Checklist

- [ ] Docker and Docker Compose installed
- [ ] Repository cloned: `git clone https://github.com/pickles4evaaaa/mybibliotheca.git`
- [ ] Environment file created: `cp .env.example .env`
- [ ] Secure keys generated and added to `.env`
- [ ] `WORKERS=1` confirmed in `.env`
- [ ] Data directory will persist: `./data` volume mounted

### ✅ Build Verification

```bash
# 1. Build the image
docker compose build

# Expected: Build completes without errors
# Look for: "Successfully tagged mybibliotheca..."

# 2. Start the container
docker compose up -d

# 3. Check container status
docker compose ps
# Expected: State should be "Up" or "running"

# 4. Check logs for startup
docker compose logs bibliotheca | head -50
# Expected: See "Kuzu connection successful" and no error messages

# 5. Test the endpoint
curl -f http://localhost:5054/
# Expected: HTTP 200 response (or 302 redirect to setup)

# 6. Access the web interface
# Open http://localhost:5054 in your browser
# Expected: Setup page or login page appears
```

### ✅ Post-Deployment Verification

- [ ] Setup page loads successfully
- [ ] Admin account created without errors
- [ ] Can log in with admin credentials
- [ ] Can add a test book (manual or ISBN search)
- [ ] Book appears in library view
- [ ] Data persists after restart: `docker compose restart`
- [ ] Backup created: Check `./backups/` directory

### 🐛 Common Build Issues

| Issue | Solution |
|-------|----------|
| "Permission denied" on `./data` | Run: `sudo chown -R 1000:1000 ./data` |
| Build fails on tesseract | Ensure Docker has enough memory (4GB+) |
| Container exits immediately | Check logs: `docker compose logs` |
| Can't connect to localhost:5054 | Check if port is already in use: `lsof -i :5054` |
| "Database locked" errors | Confirm `WORKERS=1` in environment |
| Static files not loading | Rebuild image: `docker compose build --no-cache` |

### 📞 Getting Help

If you encounter issues:

1. **Check the logs**: `docker compose logs -f`
2. **Enable debug mode**: Add `MYBIBLIOTHECA_DEBUG=true` to `.env` and restart
3. **Search existing issues**: [GitHub Issues](https://github.com/pickles4evaaaa/mybibliotheca/issues)
4. **Ask for help**: [Discord Community](https://discord.gg/Hc8C5eRm7Q)
5. **Create an issue**: Include logs, environment details, and steps to reproduce

---