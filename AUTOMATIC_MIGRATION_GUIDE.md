# SQLite to Redis Migration - Automatic Detection

## 🔄 How Migration Works Now

I've added **automatic migration detection** to Bibliotheca that handles both deployment scenarios:

## 1. 📦 Git Repo Upgrade + Source Installation

### **Automatic Detection**: ✅ YES
When you run the application after upgrading, it will:

1. **Detect SQLite databases** in common locations
2. **Show migration information** at startup
3. **Display instructions** for manual migration

```bash
# After git pull, when you start the app:
gunicorn -w 4 -b 0.0.0.0:5054 run:app

# You'll see:
============================================================
🔄 MIGRATION AVAILABLE
============================================================
Found SQLite database(s) with 128 books
Run: python3 scripts/quick_migrate.py
============================================================
```

### **Manual Migration**: Required
- Users must run `python3 scripts/quick_migrate.py`
- Interactive process guides through migration
- Safe and reversible with automatic backups

## 2. 🐳 Docker Deployment

### **Automatic Detection**: ✅ YES  
### **Automatic Migration**: ✅ OPTIONAL

Docker users have two options:

#### Option A: Manual Migration (Default)
```yaml
# docker-compose.yml - no changes needed
services:
  bibliotheca:
    # ... existing config
```

**Process:**
1. Application detects SQLite databases
2. Shows migration message in logs
3. User runs migration manually if desired

#### Option B: Automatic Migration (Recommended)
```yaml
# docker-compose.yml
services:
  bibliotheca:
    environment:
      # Enable automatic migration
      - AUTO_MIGRATE=true
      - MIGRATION_DEFAULT_USER=admin  # For single-user SQLite databases
      # ... other environment variables
```

**Process:**
1. Application detects SQLite databases
2. **Automatically migrates** them to Redis
3. Application starts normally with migrated data

## 🛡️ Safety Features

### For Both Scenarios:
- **No data loss**: Original SQLite files never modified
- **Automatic backups**: Created before and after migration
- **Validation**: Ensures data integrity post-migration
- **Rollback capability**: Can restore from backups if needed

### Database Location Detection:
The system searches for SQLite databases in:
- Current directory
- `data/` folder
- `test_files/` folder  
- `/app/data` (Docker)
- `~/Documents/Bibliotheca`

## 📋 Migration Process Details

### Schema Support:
- **v1 (Single-user)**: `june16books.db`, `books.db` - requires user ID
- **v2 (Multi-user)**: Databases with user table - auto-detects users

### What Gets Migrated:
- **All books** with metadata
- **Reading history** and logs
- **User accounts** (v2 databases)
- **Reading status** and dates

### Data Preservation:
- **Book titles, authors, ISBNs**
- **Reading dates and status**
- **Custom metadata fields**
- **User preferences** (v2)

## 🚀 Quick Start Examples

### Source Installation After Git Pull:
```bash
# 1. Pull latest code
git pull origin main

# 2. Start application (migration detected automatically)
gunicorn -w 4 -b 0.0.0.0:5054 run:app

# 3. Run migration when prompted
python3 scripts/quick_migrate.py

# 4. Restart application
gunicorn -w 4 -b 0.0.0.0:5054 run:app
```

### Docker with Auto-Migration:
```bash
# 1. Update docker-compose.yml to enable auto-migration
# 2. Start services
docker-compose up -d

# Migration happens automatically on startup!
```

### Docker without Auto-Migration:
```bash
# 1. Start services normally
docker-compose up -d

# 2. Check logs for migration message
docker-compose logs bibliotheca

# 3. Run migration manually if desired
docker-compose exec bibliotheca python3 scripts/quick_migrate.py

# 4. Restart to use migrated data
docker-compose restart bibliotheca
```

## 🔧 Environment Variables

### For Docker Auto-Migration:
```bash
AUTO_MIGRATE=true              # Enable automatic migration
MIGRATION_DEFAULT_USER=admin   # Default user ID for single-user databases
DOCKER_ENV=true               # Automatically set in Docker
```

### For Manual Control:
```bash
# Leave AUTO_MIGRATE unset or false for manual migration
```

## 📊 Your Specific Databases

Based on your current SQLite files:
- `june16books.db`: 59 books, 16 reading logs (v1 schema)  
- `books.db`: 69 books, 0 reading logs (v1 schema)

Both will be detected and can be migrated with user ID "admin" or your choice.

## ✅ Summary

**Git repo upgrade**: ✅ Automatic detection + manual migration  
**Docker deployment**: ✅ Automatic detection + optional auto-migration

Users upgrading from SQLite will have a smooth transition experience with clear guidance and safe migration tools.
