# Advanced SQLite to Redis Migration System

## Overview

The Advanced Migration System provides a comprehensive solution for migrating from SQLite-based Bibliotheca installations to the new Redis-based system. It handles both single-user (V1) and multi-user (V2) databases with full data preservation and backup capabilities.

## Features

### 🔍 **Automatic Database Detection**
- Scans **ONLY** the `data/` directory for production SQLite databases
- **Ignores** `test_files/` and other directories for safety
- Identifies V1 (single-user) vs V2 (multi-user) database formats
- Provides detailed analysis of database contents

### 🛡️ **Complete Backup System**
- Creates timestamped backups before migration
- Backs up both SQLite database and existing Redis data
- Rollback capabilities if migration fails

### 👥 **Multi-User Support**
- V1 databases: Assigns all content to the new admin user
- V2 databases: Allows user mapping and preserves all user accounts
- Maintains user relationships and permissions

### 📚 **Data Preservation**
- Migrates all books with complete metadata
- Preserves reading history and dates
- Maintains user-book relationships
- Transfers reading logs and progress

## Migration Workflows

### First-Time Setup with Migration

1. **Access Setup**: Navigate to `/auth/setup` for new installations
2. **Database Detection**: System automatically detects existing SQLite databases
3. **Admin Creation**: Create your administrator account
4. **Migration Choice**: Select which database to migrate (if multiple found)
5. **User Mapping** (V2 only): Map existing users to new accounts
6. **Migration Execution**: Automated migration with progress tracking
7. **Completion**: Login and access your migrated library

### Admin-Initiated Migration

1. **Access Admin Panel**: Login as administrator
2. **Database Management**: Go to `/migration/admin/databases`
3. **View Available Databases**: See all detected SQLite databases
4. **Initiate Migration**: Choose database and start migration process

## Database Types

### V1 (Single-User) Databases
- **Structure**: Contains `book` and `reading_log` tables without user relationships
- **Migration**: All books assigned to the current admin user
- **Example Files**: `books.db`, basic library exports

### V2 (Multi-User) Databases  
- **Structure**: Contains `user`, `book`, and `reading_log` tables with user relationships
- **Migration**: Preserves all users and their individual libraries
- **User Mapping**: Admin selects which old user corresponds to their new admin account
- **Example Files**: `multi-test.db`, full multi-user installations

## API Endpoints

### Migration Routes
- `GET /migration/detect` - Detect and analyze databases
- `GET/POST /migration/setup` - First-time setup with migration
- `GET/POST /migration/choose` - Choose database to migrate
- `GET/POST /migration/migrate-v1` - V1 database migration
- `GET/POST /migration/setup-v2` - V2 migration user mapping
- `GET/POST /migration/migrate-v2` - V2 database migration
- `GET /migration/complete` - Migration completion status

### Admin Routes
- `GET /migration/admin/databases` - Admin database management
- `GET /migration/admin/migrate/<path>` - Admin-initiated migration

## Technical Implementation

### Database Version Detection
```python
def detect_database_version(self, db_path: Path) -> Tuple[str, Dict]:
    """
    Analyzes SQLite database structure to determine version:
    - V1: No user table or user_id columns
    - V2: Has user table and user_id foreign keys
    """
```

### Migration Process
1. **Backup Creation**: SQLite + Redis data backed up
2. **User Migration** (V2): Create user accounts with preserved credentials
3. **Book Migration**: Transfer books with generated UUIDs
4. **Relationship Creation**: Establish user-book relationships
5. **Reading Log Migration**: Preserve reading history
6. **Verification**: Data integrity checks

### Data Mapping
- **Book IDs**: Old integer IDs mapped to new UUIDs
- **User IDs**: Old user IDs mapped to new UUID-based IDs
- **Relationships**: Preserved through mapping tables
- **Dates**: Converted from SQLite DATE to Python datetime objects

## Configuration

### Environment Variables
- `REDIS_URL`: Redis connection string (default: `redis://localhost:6379`)
- `MIGRATION_BACKUP_DIR`: Custom backup directory (default: `migration_backups/`)

### Search Paths
The system searches for SQLite databases in:
- `./data/` (current working directory)
- `/app/data/` (Docker container path)

## Backup Structure

```
migration_backups/
└── YYYYMMDD_HHMMSS/
    ├── sqlite_backup_books.db
    ├── redis_backup_pre_migration.json
    └── redis_backup_post_migration.json
```

## Error Handling

### Common Issues
- **Database Lock**: Ensure no other processes are accessing SQLite
- **Redis Connection**: Verify Redis is running and accessible
- **Permissions**: Check file system permissions for backup directory
- **Memory**: Large databases may require increased memory limits

### Recovery
- All original data is preserved in backups
- Failed migrations can be retried after fixing issues
- Redis data can be restored from JSON backups if needed

## Security Considerations

### Password Handling
- V2 migrations preserve original password hashes
- Admin passwords are properly hashed with Werkzeug
- No passwords are stored in plain text during migration

### Data Validation
- Input validation on all form submissions
- CSRF protection on all state-changing operations
- Admin-only access to sensitive migration functions

## Testing

### Test Databases
- `test_files/books.db` - V1 single-user database
- `test_files/multi-test.db` - V2 multi-user database
- Various test files for validation

### Migration Testing
```bash
# Test migration detection
python -c "from app.advanced_migration_system import AdvancedMigrationSystem; \
           ms = AdvancedMigrationSystem(); \
           print(ms.find_sqlite_databases())"

# Test database analysis
python -c "from app.advanced_migration_system import AdvancedMigrationSystem; \
           from pathlib import Path; \
           ms = AdvancedMigrationSystem(); \
           print(ms.detect_database_version(Path('test_files/books.db')))"
```

## Troubleshooting

### Common Problems

1. **"No databases found"**
   - Check database file locations
   - Verify file permissions
   - Ensure files have `.db` extension

2. **"Migration failed"**
   - Check Redis connection
   - Verify sufficient disk space
   - Review migration logs

3. **"User mapping error"**
   - Ensure selected user exists in database
   - Check for duplicate usernames/emails
   - Verify database integrity

### Debug Mode
Enable debug logging by setting:
```python
import logging
logging.getLogger('app.advanced_migration_system').setLevel(logging.DEBUG)
```

## Future Enhancements

- Real-time migration progress tracking
- Batch migration for multiple databases
- Advanced user merging options
- Custom field migration support
- Integration with backup scheduling

## Contributing

When contributing to the migration system:

1. **Test thoroughly** with both V1 and V2 databases
2. **Preserve data integrity** - no data loss is acceptable
3. **Maintain backward compatibility** with existing installations
4. **Document changes** in migration logs and user feedback
5. **Add appropriate error handling** for edge cases

## License

This migration system is part of the Bibliotheca project and follows the same license terms.
