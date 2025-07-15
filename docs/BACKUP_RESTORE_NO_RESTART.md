# Backup and Restore Without Container Restart

This document explains the enhanced backup and restore capabilities that eliminate the need for container restarts during restore operations.

## Overview

The backup and restore service has been enhanced with comprehensive connection management and service reinitialization capabilities that allow for seamless database restoration without requiring a Docker container restart.

## Key Improvements

### 1. Comprehensive Connection Management

**Connection Closing (`_close_database_connections`)**:
- Properly closes KuzuDB connections at all levels
- Resets global service instances
- Closes graph storage connections
- Handles SQLAlchemy connections (if present)

**Service Registry Invalidation (`_invalidate_service_registry`)**:
- Clears cached service instances from multiple modules
- Forces recreation of global service objects
- Ensures fresh initialization on next access

### 2. Flask App Context Refresh

**App Context Refresh (`_force_flask_app_context_refresh`)**:
- Creates fresh service instances
- Re-attaches services to Flask app context using `setattr`
- Handles optional services gracefully with try/except blocks
- Clears application-level caches if they exist

### 3. Enhanced Restart Process

**Database Connection Restart (`_restart_database_connections`)**:
- Invalidates all service registries first
- Forces complete KuzuDB reinitialization
- Reestablishes graph storage connections
- Refreshes Flask app context with new service instances
- Includes verification steps

### 4. File Release Management

**File Release Waiting (`_wait_for_file_release`)**:
- Waits for database files to be released by the OS
- Prevents file locking issues during restore
- Uses exclusive file access testing to verify release

**Database File Release (`_ensure_database_files_released`)**:
- Ensures KuzuDB data files are properly released
- Waits for SQLite database files to be released
- Prevents restore failures due to file locking

### 5. Restoration Verification

**Success Verification (`_verify_restoration_success`)**:
- Tests database connectivity after restore
- Runs simple queries to verify data integrity
- Handles different query result formats
- Provides detailed logging for debugging

### 6. Hot Swap Capability

**Atomic Hot Swap (`hot_swap_database`)**:
- Performs atomic database replacement
- Uses staging directories for zero-downtime swaps
- Includes automatic rollback on failure
- Minimal service interruption (typically < 2 seconds)

## Usage

### Standard Restore (Enhanced)

```python
from app.services.backup_restore_service import get_backup_service

backup_service = get_backup_service()
success = backup_service.restore_backup(backup_id)
```

The standard restore process now:
1. Closes all database connections properly
2. Waits for file release
3. Restores files atomically
4. Reinitializes all services comprehensively
5. Verifies restoration success
6. Refreshes Flask app context

### Hot Swap (New)

```python
from app.services.backup_restore_service import get_backup_service

backup_service = get_backup_service()
success = backup_service.hot_swap_database(backup_id)
```

Hot swap provides:
- Atomic database replacement
- Automatic rollback on failure
- Minimal downtime (< 2 seconds typically)
- Full verification and recovery

## Technical Details

### Connection Management Hierarchy

The enhanced system manages connections at multiple levels:

1. **KuzuIntegrationService Level**: Global service instance with repositories
2. **Graph Storage Level**: Direct database connection management
3. **Service Module Level**: Cached service instances in `app.services`
4. **Flask App Level**: Services attached to current_app

### Reinitialization Process

1. **Shutdown Phase**:
   - Close all active database connections
   - Reset service initialization flags
   - Clear cached instances
   - Wait for file release

2. **Restore Phase**:
   - Extract backup files to temporary location
   - Atomically replace database files
   - Ensure proper file permissions and ownership

3. **Restart Phase**:
   - Invalidate all service registries
   - Force service reinitialization
   - Reestablish all database connections
   - Refresh Flask app context
   - Verify successful restoration

### Error Handling and Recovery

- **Graceful Degradation**: Each step includes comprehensive error handling
- **Rollback Capability**: Hot swap includes automatic rollback on verification failure
- **Detailed Logging**: All operations include detailed logging for debugging
- **Pre-restore Backup**: Automatic backup creation before restore operations

## Benefits

1. **No Container Restart Required**: Complete restoration without Docker restart
2. **Minimal Downtime**: Hot swap typically completes in under 2 seconds
3. **Data Safety**: Pre-restore backups and rollback capabilities
4. **Comprehensive Verification**: Multiple verification steps ensure data integrity
5. **Production Ready**: Safe for production environments

## Best Practices

1. **Use Hot Swap for Production**: Minimal downtime for production environments
2. **Monitor Logs**: Check restoration verification logs for any issues
3. **Test Restores**: Regularly test backup restoration in non-production environments
4. **Backup Before Restore**: Always enabled - creates safety backup automatically

## Troubleshooting

### Common Issues

1. **File Locking**: The system now waits for file release automatically
2. **Connection Pooling**: All connection pools are properly cleared and reinitialized
3. **Service Caching**: Global service instances are invalidated and recreated
4. **Flask Context**: App context is refreshed with new service instances

### Verification Failures

If restoration verification fails:
1. Check KuzuDB file integrity
2. Verify database path configuration
3. Check file permissions
4. Review service initialization logs

The system includes automatic rollback for hot swaps if verification fails.

## Configuration

No additional configuration is required. The enhanced system:
- Automatically detects database paths
- Handles both current and legacy database file names
- Works with existing backup configurations
- Maintains backward compatibility

## Monitoring

The enhanced system provides detailed logging at each step:
- Connection closure status
- File release confirmation
- Service reinitialization progress
- Verification results
- Performance timing information

Monitor these logs to ensure optimal restoration performance and quickly identify any issues.
