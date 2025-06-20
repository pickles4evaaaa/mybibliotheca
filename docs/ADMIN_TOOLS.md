# Admin Tools Documentation

## Overview

Bibliotheca includes a comprehensive set of admin tools for managing users and system administration. The `admin_tools.py` script provides command-line utilities for administrative tasks, particularly useful for password management and user administration.

## Available Commands

### 1. Reset Admin Password
Reset the password for the admin user account.

```bash
# Interactive password prompt (recommended for security)
docker exec -it bibliotheca python3 admin_tools.py reset-admin-password

# With specified password (less secure, password visible in history)
docker exec -it bibliotheca python3 admin_tools.py reset-admin-password --password newpassword123
```

**Features:**
- Password validation (minimum 8 chars, letters + numbers)
- Interactive password confirmation
- Secure password input (hidden from terminal)
- Updates existing admin user

### 2. Create Additional Admin
Create new admin users with full administrative privileges.

```bash
# Interactive prompts for username, email, and password
docker exec -it bibliotheca python3 admin_tools.py create-admin

# With specified details
docker exec -it bibliotheca python3 admin_tools.py create-admin --username newadmin --email admin@example.com

# Force creation even if admin exists
docker exec -it bibliotheca python3 admin_tools.py create-admin --force
```

**Features:**
- Validates unique usernames and emails
- Enforces password security requirements
- Prevents duplicate admin creation (unless forced)
- Sets admin privileges automatically

### 3. Promote User to Admin
Grant admin privileges to existing regular users.

```bash
docker exec -it bibliotheca python3 admin_tools.py promote-user --username johndoe
```

**Features:**
- Validates user exists
- Grants admin privileges
- Preserves existing user data

### 4. List Users
Display all users in the system with their details.

```bash
docker exec -it bibliotheca python3 admin_tools.py list-users
```

**Output includes:**
- Username and email
- Admin status (Yes/No)
- Active status (Yes/No)
- Account creation date

### 5. System Statistics
Display comprehensive system information and statistics.

```bash
docker exec -it bibliotheca python3 admin_tools.py system-stats
```

**Displays:**
- Total users, admin users, active users
- Total books and reading logs in system
- Database file size and location
- System health information

## Security Features

### Password Requirements
All passwords must meet these security requirements:
- Minimum 8 characters long
- At least one letter (a-z or A-Z)
- At least one number (0-9)
- No common passwords accepted

### Input Validation
- All user inputs are validated and sanitized
- Username must be 3-20 characters, alphanumeric + underscore
- Email addresses must be valid format
- Duplicate usernames/emails prevented

### Secure Password Handling
- Passwords hashed using Werkzeug's secure methods
- Interactive prompts hide password input
- Password confirmation required for new passwords
- No plain text passwords stored

## Usage Examples

### First Time Setup
```bash
# 1. Start container
docker compose up -d

# 2. Visit the application and complete the setup form
# Navigate to http://localhost:5054 and create your admin account

# 3. Verify admin user was created successfully
docker exec -it bibliotheca python3 admin_tools.py list-users
```

### Emergency Admin Access
```bash
# If you lose admin access, create a new admin user
docker exec -it bibliotheca python3 admin_tools.py create-admin --username emergency --email emergency@bibliotheca.local
```

### User Management Workflow
```bash
# 1. List current users
docker exec -it bibliotheca python3 admin_tools.py list-users

# 2. Promote regular user to admin
docker exec -it bibliotheca python3 admin_tools.py promote-user --username johndoe

# 3. Check system statistics
docker exec -it bibliotheca python3 admin_tools.py system-stats
```

## Integration with Docker

### Docker Compose Usage
The admin tools are automatically available in any running Bibliotheca container:

```yaml
services:
  bibliotheca:
    image: bibliotheca:latest
    # ... other config ...
    environment:
      # Security settings 
      - SECRET_KEY=your-secret-key
      - SECURITY_PASSWORD_SALT=your-salt
```

### Container Access
All admin tools require access to the running container's environment and database:

```bash
# For running container
docker exec -it <container_name> python3 admin_tools.py <command>

# For one-off commands (creates temporary container)
docker run --rm -it -v bibliotheca_data:/app/data bibliotheca:latest python3 admin_tools.py <command>
```

## Error Handling

### Common Issues

**"No admin user found"**
```bash
# Solution: Create a new admin user first
docker exec -it bibliotheca python3 admin_tools.py create-admin
```

**"Username already exists"**
```bash
# Solution: Use a different username or check existing users
docker exec -it bibliotheca python3 admin_tools.py list-users
```

**"Password does not meet requirements"**
- Ensure password has minimum 8 characters
- Include at least one letter and one number
- Avoid common passwords

### Debug Information
For troubleshooting, check:
1. Container logs: `docker logs bibliotheca`
2. Database permissions in mounted volume
3. Environment variables are properly set

## Best Practices

### Security
1. **Complete secure setup** using the interactive setup page on first deployment
2. **Use interactive password prompts** instead of command-line passwords
3. **Regularly rotate admin passwords** for enhanced security
4. **Limit admin accounts** to only necessary users
5. **Leverage automatic password changes** for new users

### Backup and Recovery
1. **Regular database backups** before admin changes
2. **Test admin tools** in staging environment first
3. **Document admin accounts** and their purposes
4. **Have emergency admin access plan** ready

### Monitoring
1. **Regular user audits** using list-users command
2. **Monitor system statistics** for unusual activity
3. **Log admin tool usage** for security auditing
4. **Review admin privileges** periodically

---

*These admin tools provide comprehensive user management for Bibliotheca. Always follow security best practices and maintain proper backups when making administrative changes.*
