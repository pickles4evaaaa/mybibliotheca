# Bibliotheca Testing Documentation

## Overview

This document describes the comprehensive testing suite for Bibliotheca's multi-user authentication system. All tests are designed to run within the Docker container to ensure consistency and reliability.

## Test Scripts

### 1. Core Authentication Tests (`test_ui_auth.py`)

**Purpose**: Tests fundamental authentication functionality
**Coverage**:
- Login page accessibility and form validation
- Route protection (unauthorized access redirects)
- Registration page accessibility
- Admin login functionality
- Authenticated access to protected routes
- User profile page access
- Logout functionality

**Usage**:
```bash
docker exec -it bibliotheca-bibliotheca-1 python test_ui_auth.py
```

### 2. Extended User Tests (`test_ui_extended.py`)

**Purpose**: Tests complete user lifecycle operations
**Coverage**:
- User registration with validation
- New user login
- Password change functionality
- Login with updated credentials

**Usage**:
```bash
docker exec -it bibliotheca-bibliotheca-1 python test_ui_extended.py
```

### 3. Complete Test Suite (`test_complete_auth.py`)

**Purpose**: Runs all authentication tests in sequence
**Coverage**:
- Executes all UI tests
- Tests admin tools functionality
- Provides comprehensive test reporting

**Usage**:
```bash
docker exec -it bibliotheca-bibliotheca-1 python test_complete_auth.py
```

### 4. Admin Tools Tests

**Purpose**: Validates administrative functionality
**Coverage**:
- User listing
- System statistics
- User management capabilities

**Usage**:
```bash
docker exec -it bibliotheca-bibliotheca-1 python admin_tools.py list-users
docker exec -it bibliotheca-bibliotheca-1 python admin_tools.py system-stats
```

## Test Features

### Automated Test Generation
- **Dynamic User Creation**: Tests generate unique usernames and emails for each run
- **CSRF Token Handling**: Automatically extracts and uses CSRF tokens
- **Session Management**: Properly manages authentication sessions

### Error Detection
- **Form Validation**: Detects and reports form validation errors
- **HTTP Status Codes**: Validates proper redirect and response codes
- **Error Messages**: Captures and reports specific error messages

### Comprehensive Coverage
- **Authentication Flow**: Complete login/logout cycle
- **User Registration**: Account creation with validation
- **Password Management**: Password change functionality
- **Route Protection**: Ensures unauthorized access is properly blocked
- **Admin Tools**: Administrative functionality verification

## Running Tests in Docker

### Prerequisites
```bash
# Ensure Docker container is running
docker-compose up -d
```

### Single Test Execution
```bash
# Run specific test
docker exec -it bibliotheca-bibliotheca-1 python test_ui_auth.py
```

### Complete Test Suite
```bash
# Run all tests with comprehensive reporting
docker exec -it bibliotheca-bibliotheca-1 python test_complete_auth.py
```

### Expected Output
- ✅ **All tests passing**: Indicates authentication system is fully functional
- ❌ **Test failures**: Detailed error reporting for debugging
- 📊 **Test summary**: Complete results overview

## Test Data

### Generated Test Users
- **Username Format**: `testuser_[6-char-random]`
- **Email Format**: `test_[6-char-random]@example.com`
- **Default Password**: `TestPassword123`
- **Changed Password**: `NewTestPassword456`

### Admin Credentials
- **Username**: `admin`
- **Default Password**: `changeme123` (should be changed in production)

## Validation Results

The test suite validates:

1. **Authentication Security**
   - Proper route protection
   - CSRF token implementation
   - Password validation
   - Session management

2. **User Management**
   - Account registration
   - Profile management
   - Password changes
   - Admin tools functionality

3. **System Integration**
   - Database operations
   - Flask application responses
   - Template rendering
   - Form submissions

## Troubleshooting

### Common Issues

1. **Container Not Running**
   ```bash
   docker-compose up -d
   ```

2. **Database Migration Needed**
   ```bash
   docker exec -it bibliotheca-bibliotheca-1 python migrate_to_multi_user.py
   ```

3. **Port Conflicts**
   - Ensure port 5054 is available
   - Check docker-compose.yml configuration

### Test Debugging

To debug failing tests:
1. Check container logs: `docker logs bibliotheca-bibliotheca-1`
2. Run individual test components
3. Verify database state with admin tools
4. Check application logs within container

## Security Considerations

- Tests use non-production email domains (`@example.com`)
- Generated test data is ephemeral
- No sensitive data is exposed in test output
- Admin password reset requires interactive confirmation

## Integration with CI/CD

The test suite is designed for automation:
- Non-interactive execution
- Clear pass/fail indicators
- Detailed error reporting
- Timeout handling

Example CI integration:
```yaml
test_auth:
  script:
    - docker-compose up -d
    - docker exec bibliotheca-bibliotheca-1 python test_complete_auth.py
  artifacts:
    reports:
      junit: test_results.xml
```

## Conclusion

This comprehensive testing suite ensures the Bibliotheca multi-user authentication system is robust, secure, and fully functional within the Docker environment. All tests are automated and provide clear feedback on system status.
