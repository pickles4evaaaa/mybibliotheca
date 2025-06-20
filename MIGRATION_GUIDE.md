# 🔄 Bibliotheca Migration Guide

**Migrating from SQLite to Redis has been simplified!** 

We've streamlined the migration process to use only the **web-based interface**, eliminating complexity and potential user ID mismatches.

## ✨ **Web-Only Migration Process**

### **1. Start Your Application**
```bash
docker-compose up -d
```

### **2. Access the Migration Interface**
- 🌐 Open: `http://localhost:5054`
- 🔐 Create your admin account (if this is a fresh install)
- 📋 The migration wizard will appear automatically if databases are detected

### **3. Follow the Migration Wizard**
- ✅ **Select databases** to migrate
- ✅ **Review migration plan** - all books will be assigned to your admin account
- ✅ **Run migration** with real-time progress
- ✅ **Verify results** - your books should appear immediately

## 🎯 **Key Benefits of Web-Only Migration**

- **✅ No User ID confusion** - uses your logged-in session automatically
- **✅ Visual feedback** with progress bars and status updates  
- **✅ Error handling** with clear, actionable messages
- **✅ Automatic backups** created before migration
- **✅ Immediate verification** - see your books right away

## 🔧 **What Happens During Migration**

1. **Book Import**: All books from SQLite → Redis as global objects
2. **Relationship Creation**: Books linked to your admin user account
3. **Metadata Preservation**: Reading status, dates, categories preserved
4. **Backup Creation**: Original SQLite files backed up safely

## 🆘 **Troubleshooting**

### **Books Not Showing After Migration?**
This was the main issue with the old CLI approach. The web-only migration eliminates this problem by:
- Using your current session user ID directly
- No parameter passing that could cause mismatches
- Immediate verification in the same session

### **Still Having Issues?**
- Check Docker logs: `docker-compose logs bibliotheca`
- Check migration logs: Look for detailed error messages in the web interface
- Verify Redis connection: `docker-compose logs redis-graph`

## 📊 **Migration Architecture**

```
SQLite Database → Web Migration → Redis
     ↓               ↓              ↓
   Books          Current User    Global Books
   Metadata    →  Assignment  →   + User Relations
   Reading Logs                   + Preserved Data
```

Your migration should now work seamlessly! 🎉

## Quick Migration (Docker)

### 1. Check what needs migration
```bash
# Run this to see what databases were found
docker exec bibliotheca-1 python3 scripts/detect_migration.py
```

### 2. Migrate your data
```bash
# For single-user databases (most common)
docker exec bibliotheca-1 python3 scripts/quick_migrate.py --user-id admin

# Or run the helper script
./migrate.sh
```

### 3. Verify migration
```bash
# Check that books appeared in Redis
docker exec redis-graph redis-cli --scan --pattern 'node:book:*' | wc -l
```

## Manual Migration (Local Development)

### 1. Check migration status
```bash
python3 scripts/detect_migration.py
```

### 2. Migrate specific database
```bash
# Single-user database
python3 scripts/migrate_sqlite_to_redis.py --db-path data/books.db --user-id admin

# Multi-user database  
python3 scripts/migrate_sqlite_to_redis.py --db-path data/books.db
```

### 3. Quick migrate all
```bash
python3 scripts/quick_migrate.py --user-id admin
```

## Migration Details

- **Automatic Backups**: All data is backed up before migration
- **Safe Process**: Your original SQLite files are never modified
- **Partial Migration**: The app works even if some databases fail to migrate
- **User Creation**: For single-user databases, an admin user is created automatically
- **Data Preservation**: All books, users, reading logs, and relationships are preserved

## Database Types

- **v1 (Single-user)**: Older databases without user accounts
- **v2 (Multi-user)**: Newer databases with user management

The migration script automatically detects the type and handles accordingly.

## Troubleshooting

### No databases found
- Make sure your `.db` files are in the `data/` directory
- Check file permissions (Docker needs read access)

### Migration fails
- Check Docker logs: `docker logs bibliotheca-1`
- Look for backup files in `migration_backups/`
- Try migrating one database at a time

### Partial migration
- The app will still work with whatever data migrated successfully
- You can re-run migration for failed databases
- Check `migration.log` for specific errors

## After Migration

Once migration is complete:
- Your app runs on Redis (faster and more scalable)
- All your books and data are preserved
- You can safely remove old SQLite files (they're backed up)
- Set up new users through the web interface

The Redis-based system provides better performance, real-time features, and prepares your library for advanced features like collaborative collections and real-time syncing.
