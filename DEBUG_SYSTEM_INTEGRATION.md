# Debug System Integration Guide

## Summary

I've implemented a comprehensive debug system for the Bibliotheca application that provides:

1. **Admin-controlled debug mode** - Can be toggled on/off by administrators
2. **Structured logging** - Categorized debug messages with extra data
3. **Container log output** - All debug info goes to Docker logs when enabled
4. **Admin UI** - Web interface for viewing and managing debug logs
5. **Template debug panels** - Debug info shown only to admins in the UI

## Files Created/Modified

### New Files:
- `app/debug_system.py` - Core debug management system
- `app/debug_routes.py` - Admin routes for debug management
- `app/template_context.py` - Template context processors
- `app/templates/admin/debug_dashboard.html` - Debug management UI
- `app/templates/admin/debug_logs.html` - Debug logs viewer
- `app/templates/components/debug_panel.html` - Reusable debug component

### Modified Files:
- `app/location_service.py` - Updated to use new debug system
- `app/templates/locations/view.html` - Added debug panel

## Integration Steps

### 1. Add Debug Routes to Main App

In your main `__init__.py` or `routes.py`, register the debug routes:

```python
from app.debug_routes import bp as debug_admin_bp
app.register_blueprint(debug_admin_bp)
```

### 2. Register Template Context Processors

In your app factory function:

```python
from app.template_context import register_context_processors
register_context_processors(app)
```

### 3. Add Admin Navigation Link

Add to your admin navigation menu:

```html
<a href="{{ url_for('debug_admin.debug_dashboard') }}" class="nav-link">
    <i class="bi bi-bug"></i> Debug System
</a>
```

### 4. Add User Admin Check

Ensure your User model has an `is_admin` property or similar:

```python
class User:
    @property
    def is_admin(self):
        return self.role == 'admin' or getattr(self, 'is_admin_flag', False)
```

### 5. Update Other Services

Replace existing print statements with debug_log calls:

```python
from app.debug_system import debug_log

# Replace:
print(f"Some debug info: {data}")

# With:
debug_log(f"Some debug info: {data}", "CATEGORY", {"extra": "data"})
```

## Usage

### For Administrators:

1. **Enable Debug Mode**: Go to `/admin/debug` and toggle debug mode
2. **View Logs**: Debug info appears in Docker logs and admin UI
3. **Monitor Activity**: Real-time log viewing with filters and categories
4. **Template Debug**: Debug panels show on pages when enabled

### For Developers:

```python
from app.debug_system import debug_log, get_debug_manager

# Log debug information
debug_log("User action performed", "USER", {
    "user_id": user.id,
    "action": "create_location",
    "data": {"location_name": "Home"}
})

# Check if debug is enabled
if get_debug_manager().is_debug_enabled():
    # Do expensive debug operations
    pass
```

### Categories:
- `LOCATION` - Location-related operations
- `BOOK` - Book operations
- `USER` - User actions
- `AUTH` - Authentication/authorization
- `ERROR` - Error conditions
- `GENERAL` - General debug info
- `TEST` - Test messages

## Security Features

- Debug info only visible to admin users
- Debug mode can only be toggled by admins
- Non-admin users see no debug information
- Debug logs automatically expire after 7 days
- All debug activity is logged with user attribution

## Docker Integration

Debug logs appear in container output with the format:
```
🐛 [2025-06-24 10:30:15] DEBUG - bibliotheca.debug - [LOCATION] Getting book count for location abc123 | Extra: {"user_id": "user123"}
```

Use `docker logs <container>` to view debug output in production.
