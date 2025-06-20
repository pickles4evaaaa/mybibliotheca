# 🔐 Bibliotheca V2.0 Authentication Guide

This guide covers the multi-user authentication system introduced in Bibliotheca V2.0.

## Overview

Bibliotheca V2.0 introduces a complete multi-user authentication system with:
- User registration and login
- Password security with hashing
- Admin user management
- User data isolation
- Session management
- CSRF protection

## Quick Start

### First Time Setup

When you first run Bibliotheca V2.0, you'll complete a secure setup process:

1. **Navigate to the application** in your web browser
2. **Complete the setup form** to create your administrator account:
   - Choose a unique admin username
   - Provide a valid email address
   - Create a strong password meeting security requirements
3. **Begin using Bibliotheca** immediately after setup

✅ **Secure by Design**: No default passwords or credentials - you're in complete control of your admin account!

### Environment Variables

Configure authentication via environment variables:

```bash
# Required for security
SECRET_KEY=your-super-secret-key-here-32-chars-minimum
SECURITY_PASSWORD_SALT=your-password-salt

# Optional timezone setting
TIMEZONE=America/Chicago
```

# Optional: Email configuration (future feature)
MAIL_SERVER=smtp.gmail.com
MAIL_PORT=587
MAIL_USE_TLS=True
MAIL_USERNAME=your-email@gmail.com
MAIL_PASSWORD=your-app-password
```

## User Management

### User Roles

- **Regular User**: Can manage their own books and reading logs
- **Admin User**: Can access all system features + admin panel (planned for Phase 2)

### Registration

New users can register at `/register` with:
- Username (3-20 characters, alphanumeric + underscore)
- Email address (must be valid and unique)
- Password (minimum 8 characters, at least one letter and one number)

### Login

Users log in at `/login` with their username/email and password.

### Password Management

#### Changing Your Password

Logged-in users can change their password at `/profile`:
1. Enter current password
2. Enter new password (meeting security requirements)
3. Confirm new password

#### Admin Password Reset (Docker)

If you lose admin access, reset the admin password using the Docker container:

```bash
# Reset admin password interactively
docker exec -it bibliotheca python3 admin_tools.py reset-admin-password

# Or reset with a specific password
docker exec -it bibliotheca python3 admin_tools.py reset-admin-password --password newpassword123
```

#### Password Security Requirements

- Minimum 8 characters
- At least one letter (a-z or A-Z)
- At least one number (0-9)
- No common passwords accepted

## User Data Isolation

Each user's data is completely isolated:
- **Books**: Users can only see and manage their own books
- **Reading Logs**: Users can only see their own reading activity
- **Search**: Only searches within the user's library
- **Bulk Import**: Imports books to the current user's library

## Security Features

### Session Management
- Secure session cookies with HttpOnly flag
- Session timeout after inactivity
- Remember me functionality for 30 days (optional)

### CSRF Protection
- All forms protected with CSRF tokens
- Automatic token validation on form submission

### Password Security
- Passwords hashed using Werkzeug's secure password hashing
- No plain text passwords stored
- Password validation on both client and server

### Input Validation
- All user inputs validated and sanitized
- SQL injection protection via SQLAlchemy ORM
- XSS protection via template escaping

## API Access

Currently, authentication is web-based only. API authentication is planned for Phase 3.

## Migration from V1.x

When upgrading from Bibliotheca V1.x:

1. **Automatic Migration**: All existing books and reading logs are assigned to the default admin user
2. **Data Preservation**: No data is lost during migration
3. **Backward Compatibility**: All existing functionality remains available
4. **New Users**: Additional users can be created after migration

### Migration Process

The migration script (`migrate_to_v2.py`) automatically:
- Creates admin user if none exists
- Adds user_id columns to existing tables
- Assigns orphaned data to the admin user
- Updates database constraints

## Docker Configuration

### Docker Compose Example

```yaml
version: '3.8'
services:
  bibliotheca:
    image: pickles4evaaaa/bibliotheca:latest
    container_name: bibliotheca
    ports:
      - "5054:5054"
    volumes:
      - bibliotheca_data:/app/data
    environment:
      # Authentication settings
      - SECRET_KEY=your-super-secret-key-change-this
      - SECURITY_PASSWORD_SALT=your-salt-change-this
      
      # Optional app settings
      - TIMEZONE=America/Chicago
      - WORKERS=6
    restart: unless-stopped

volumes:
  bibliotheca_data:
```

### Security Best Practices

1. **Secure Setup**: Use the interactive setup page to create your admin account with a strong, unique password
2. **Secure Environment Variables**: Use strong, unique values for SECRET_KEY and SECURITY_PASSWORD_SALT
3. **Use HTTPS**: Deploy behind a reverse proxy with SSL/TLS
4. **Regular Backups**: Backup your database regularly
5. **Monitor Access**: Check logs for unusual activity
6. **Password Management**: New users are automatically prompted to change their password on first login

## Troubleshooting

### Common Issues

#### "Invalid username or password"
- Check username/email spelling
- Verify password is correct
- Ensure account is active

#### "CSRF token missing or invalid"
- Refresh the page and try again
- Clear browser cookies if persistent
- Check if JavaScript is enabled

#### "Database locked" errors
- Restart the application
- Check file permissions on database
- Ensure only one instance is running

#### Lost admin access
- Use the admin password reset script (see above)
- Check docker logs for error messages
- Verify environment variables are set correctly

### Debug Mode

For troubleshooting, enable debug logging by setting:
```bash
FLASK_DEBUG=true
```

⚠️ **Never use debug mode in production!**

## Support

For authentication-related issues:
1. Check this documentation first
2. Review Docker logs: `docker logs bibliotheca`
3. Open an issue on GitHub with detailed error information

---

*This authentication system provides secure, scalable user management for your personal library. Report any security concerns immediately via GitHub issues.*
