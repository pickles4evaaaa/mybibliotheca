# SQLite to Redis Migration Guide

This guide explains how to migrate your existing SQLite Bibliotheca database to the new Redis-based system.

## 🔄 Migration Overview

The migration process converts your SQLite data to the new Redis-based architecture while preserving all your books, reading history, and user data.

### What Gets Migrated

- **Users** (v2 databases only)
- **Books** with all metadata
- **Reading logs** and history
- **User-book relationships** (reading status, dates, etc.)

### Schema Support

- **v1 Schema**: Single-user databases (no user table)
- **v2 Schema**: Multi-user databases (with user table)

## 🚀 Quick Start

### Option 1: Interactive Helper (Recommended)

```bash
python scripts/quick_migrate.py
```

This script will:
- Find SQLite databases automatically
- Analyze their structure
- Guide you through the migration process
- Handle all the details for you

### Option 2: Manual Migration

```bash
# For single-user databases
python scripts/migrate_sqlite_to_redis.py --db-path test_files/books.db --user-id admin

# For multi-user databases
python scripts/migrate_sqlite_to_redis.py --db-path test_files/library.db
```

## 📋 Prerequisites

1. **Redis Running**: Ensure Redis is running and accessible
2. **Database Backup**: The script creates backups, but manual backup is recommended
3. **Python Environment**: All dependencies installed

## 🛡️ Safety Features

### Automatic Backups

The migration creates backups at multiple points:

1. **Pre-migration backup**:
   - SQLite database copy
   - Current Redis data export

2. **Post-migration backup**:
   - Complete Redis data export after migration

Backups are stored in: `migration_backups/YYYYMMDD_HHMMSS/`

### Validation

After migration, the script validates:
- Record counts match between SQLite and Redis
- Data integrity checks
- Relationship consistency

### Rollback

If migration fails:
1. Check `migration.log` for errors
2. Restore from pre-migration backups if needed
3. Fix issues and retry

## 📊 Database Schema Differences

### v1 Schema (Single-User)
```sql
CREATE TABLE book (
    id INTEGER PRIMARY KEY,
    uid VARCHAR(12) UNIQUE,
    title VARCHAR(255),
    author VARCHAR(255),
    isbn VARCHAR(13),
    start_date DATE,
    finish_date DATE,
    cover_url VARCHAR(512),
    want_to_read BOOLEAN,
    library_only BOOLEAN,
    -- Additional metadata fields
);

CREATE TABLE reading_log (
    id INTEGER PRIMARY KEY,
    book_id INTEGER,
    date DATE
);
```

### v2 Schema (Multi-User)
```sql
CREATE TABLE user (
    id INTEGER PRIMARY KEY,
    username VARCHAR(80) UNIQUE,
    email VARCHAR(120) UNIQUE,
    password_hash VARCHAR(128),
    -- Additional user fields
);

CREATE TABLE book (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,  -- Links to user
    -- Same fields as v1 plus user relationship
);

CREATE TABLE reading_log (
    id INTEGER PRIMARY KEY,
    book_id INTEGER,
    user_id INTEGER,  -- Links to user
    date DATE
);
```

## 🎯 Migration Process Details

### 1. Schema Detection
- Automatically detects v1 vs v2 schema
- Identifies available tables and columns
- Validates database structure

### 2. User Migration (v2 only)
- Migrates all user accounts
- Preserves passwords and settings
- Creates user ID mappings

### 3. Book Migration
- Converts all book records
- Maps SQLite IDs to Redis IDs
- Preserves all metadata

### 4. Relationship Creation
- Links users to their books
- Determines reading status from dates
- Creates Redis graph relationships

### 5. Reading Log Migration
- Preserves reading history
- Links logs to correct users and books
- Maintains chronological order

## 🔧 Advanced Options

### Custom Redis URL
```bash
python scripts/migrate_sqlite_to_redis.py \
  --db-path books.db \
  --redis-url redis://custom-host:6380
```

### Dry Run Mode
```bash
python scripts/migrate_sqlite_to_redis.py \
  --db-path books.db \
  --dry-run
```

## 🐛 Troubleshooting

### Common Issues

**Database Locked**
```
Solution: Ensure no other application is using the SQLite database
```

**Redis Connection Failed**
```
Solution: Check Redis is running and connection URL is correct
```

**Missing User ID for v1 Database**
```
Solution: Provide --user-id parameter for single-user databases
```

**Permission Denied**
```
Solution: Ensure script has read access to database and write access to backup directory
```

### Error Logs

Check `migration.log` for detailed error information:
```bash
tail -f migration.log
```

### Validation Failures

If validation fails:
1. Check the error details in the log
2. Verify Redis connectivity
3. Ensure no data corruption in source database
4. Retry migration with clean Redis instance

## 📈 Performance Notes

### Large Databases

For databases with many records:
- Migration time scales with data size
- Memory usage is minimal (streaming approach)
- Progress is logged regularly

### Network Considerations

- Local Redis: Fast migration
- Remote Redis: Slower but still efficient
- Network issues may require retry

## ✅ Post-Migration Steps

### 1. Verify Data
```bash
# Check user count (if applicable)
redis-cli SCARD users

# Check book count
redis-cli SCARD books

# Verify relationships exist
redis-cli KEYS "rel:user:*"
```

### 2. Test Application
- Start the application
- Verify login works
- Check book library displays correctly
- Test adding/editing books

### 3. Archive Old Database
Once verified, you can archive the SQLite database:
```bash
mkdir archive/
mv old_database.db archive/
```

## 🔄 Re-running Migration

If you need to re-run migration:

1. **Clear Redis data** (optional but recommended):
   ```bash
   redis-cli FLUSHDB
   ```

2. **Run migration again**:
   ```bash
   python scripts/migrate_sqlite_to_redis.py --db-path your_database.db
   ```

The script will create new backups and won't interfere with previous attempts.

## 📞 Support

If you encounter issues:

1. Check this documentation
2. Review `migration.log`
3. Verify your Redis setup
4. Check the backup files were created
5. Open an issue with log details if needed

Remember: The migration process is designed to be safe and reversible, so don't hesitate to try it with your data!
