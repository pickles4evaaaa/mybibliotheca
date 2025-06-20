# Database Migration System

## Overview

Bibliotheca now uses an **automatic migration system** that runs when the application starts. This ensures that your database schema is always up-to-date without requiring manual intervention.

## Features

### ✅ Automatic Backup
- Creates a timestamped backup **only when migrations are needed**
- Backup format: `books.db.backup_YYYYMMDD_HHMMSS`
- Stored in `data/backups/` directory
- No backup created for fresh installations or when schema is up-to-date

### ✅ Incremental Migrations
- Detects missing tables and creates them
- Adds missing columns to existing tables
- Maintains data integrity during schema changes

### ✅ Multi-User Migration
- Redirects to setup page if no users exist 
- Assigns existing books/reading logs to admin users (if they exist)
- Handles transition from single-user to multi-user system

### ✅ Security & Privacy Features
- Adds account lockout fields (`failed_login_attempts`, `locked_until`, `last_login`)
- Adds privacy settings (`share_current_reading`, `share_reading_activity`, `share_library`)

## Migration Process

When the application starts, it will:

1. **Check for existing database**
   - If no database exists, creates fresh schema
   - If database exists, proceeds with migration checks

2. **Create backup** (only if migrations are needed)
   - Backup saved as `data/backups/books.db.backup_YYYYMMDD_HHMMSS`
   - No backup created for fresh installations

3. **Check and add missing tables**
   - User authentication tables
   - Any other new tables

4. **Check and add missing columns**
   - Book table: `user_id`, `description`, `published_date`, etc.
   - User table: security and privacy fields
   - Reading log table: `user_id`, `created_at`

5. **Setup requirement** (if no users exist)
   - User will be redirected to setup page on first visit
   - Admin account created through secure setup form
   - No default credentials used

6. **Assign orphaned data** (only if admin users exist)
   - Books without `user_id` → assigned to existing admin
   - Reading logs without `user_id` → assigned to existing admin

## Interactive Setup

For new installations with no users:

- **Automatic redirect**: Application redirects to `/auth/setup` on first visit
- **Secure setup form**: Create admin account with strong password requirements
- **No default credentials**: Complete control over admin account creation
- **Password requirements**: Strong password automatically enforced
- **Immediate access**: Admin user is logged in after successful setup

## Manual Migration Scripts (Deprecated)

The following manual migration scripts are now **deprecated** as their functionality has been integrated into the automatic system:

- ~~`migrate_db_schema.py`~~ → Integrated
- ~~`migrate_security_features.py`~~ → Integrated  
- ~~`migrate_user_security.py`~~ → Integrated
- ~~`migrate_to_multi_user.py`~~ → Integrated

## Recovery

If something goes wrong during migration:

1. **Stop the application**
2. **Restore from backup**:
   ```bash
   cd data/backups/
   cp books.db.backup_YYYYMMDD_HHMMSS ../books.db
   ```
3. **Check the logs** for specific error messages
4. **Report the issue** with logs and backup information

## Migration Logs

Look for these log messages during startup:

- `✅` = Success
- `🔄` = In progress
- `⚠️` = Warning (non-fatal)
- `❌` = Error

Example successful migration:
```
🔄 Migrations needed: ['user_security_privacy: ['failed_login_attempts', 'locked_until']']
🔄 Creating database backup before migration...
✅ Database backup created: data/backups/books.db.backup_20250617_143022
📁 Backup saved to: data/backups/books.db.backup_20250617_143022
✅ Tables present, checking for migrations...
🔄 Adding security/privacy fields: ['failed_login_attempts', 'locked_until']
✅ Added failed_login_attempts to user table
✅ Added locked_until to user table
✅ Security/privacy migration completed.
🎉 Database migration completed successfully!
```

Example when no migration is needed:
```
✅ Database schema is up-to-date, no migrations needed
✅ Tables present, checking for migrations...
✅ Security/privacy fields already present.
🎉 Database migration completed successfully!
```

## Benefits

- **Zero downtime** migrations
- **Automatic backups** for safety
- **No manual steps** required
- **Docker-friendly** deployment
- **Consistent** across environments

## Validation

To validate that the migration system is properly configured, run:

```bash
python validate_migration.py
```

This script checks:
- ✅ Migration functions are present
- ✅ Configuration is correct
- ✅ Documentation exists
- ✅ Manual scripts are deprecated

## Troubleshooting

### Common Issues

**Migration fails to start:**
- Check database file permissions
- Ensure `data/` directory exists
- Verify SQLite is available

**Backup creation fails:**
- Check disk space
- Verify write permissions to data directory
- Database may be locked by another process

**Setup page issues:**
- Ensure no existing users in database (setup only runs for fresh installs)
- Verify password meets security requirements  
- Check database constraints and permissions
- Ensure application can write to database

### Getting Help

If you encounter issues:

1. Check the application logs for detailed error messages
2. Run the validation script: `python validate_migration.py`
3. Verify your database backup exists before debugging
4. Report issues with log output and system information
