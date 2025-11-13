# 📚 MyBibliotheca - Comprehensive Documentation

**[← Back to Documentation Home](../index.md)** | **[View Beta Version →](../beta/index.md)**

---

MyBibliotheca is a self-hosted personal library management system and reading tracker built with Flask and SQLAlchemy. It serves as an open-source alternative to Goodreads, StoryGraph, and similar services, offering complete control over your reading data with multi-user support and advanced privacy controls.

> ⚠️ **Important:** Version 1.1.1 is now officially deprecated. The stable release is **2.0.1**. Please update all deployments and references to use the new version.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Features](#features)
- [Key Components](#key-components)
- [Database Schema](#database-schema)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## Overview

### What is MyBibliotheca?

MyBibliotheca is a comprehensive personal library management system that allows you to:

- Track your book collection and reading progress
- Import books via ISBN lookup or CSV files
- Log daily reading activities and maintain reading streaks
- Generate monthly reading wrap-up images
- Manage multiple users with complete data isolation
- Share reading activity with community features
- Maintain privacy with granular sharing controls

### Key Characteristics

- **Self-hosted:** Complete control over your data
- **Multi-user:** Secure authentication with isolated user data
- **SQLite Database:** Lightweight, file-based database storage
- **Flask Framework:** Modern Python web framework
- **Responsive Design:** Mobile-friendly Bootstrap UI
- **Privacy-focused:** Granular privacy controls for sharing
- **Community Features:** Optional sharing and social features

### Version Information

- **Version:** 2.0.1 (Stable)
- **Deprecated Version:** 1.1.1
- **Database:** SQLite with SQLAlchemy ORM
- **Framework:** Flask with Flask-Login authentication
- **Python:** Requires Python 3.13+
- **Security:** CSRF protection, account lockout, password policies
- **Docker Image:** `pickles4evaaaa/mybibliotheca:2.0.1`

## Quick Start

### Docker Installation (Recommended)

#### Option 1: Docker Run Command

```bash
# Clone the repository for configuration files
git clone https://github.com/pickles4evaaaa/mybibliotheca.git
cd mybibliotheca

# Run using Docker Hub image
docker run -d \
  --name mybibliotheca \
  -p 5054:5054 \
  -v $(pwd)/data:/app/data \
  -e TIMEZONE=America/Chicago \
  -e WORKERS=6 \
  --restart unless-stopped \
  pickles4evaaaa/mybibliotheca:2.0.1
```

#### Option 2: Docker Compose (Recommended)

```bash
# Clone the repository
git clone https://github.com/pickles4evaaaa/mybibliotheca.git
cd mybibliotheca

# Create .env file with required secrets
cp .env.example .env
# Edit .env and set SECRET_KEY and SECURITY_PASSWORD_SALT

# Start with Docker Compose
docker compose up -d
```

**Docker Compose Configuration (docker-compose.yml):**

```yaml
services:
  mybibliotheca:
    image: pickles4evaaaa/mybibliotheca:2.0.1
    ports:
      - "5054:5054"
    volumes:
      - mybibliotheca_data:/app/data
    environment:
      - SECRET_KEY=${SECRET_KEY}
      - SECURITY_PASSWORD_SALT=${SECURITY_PASSWORD_SALT}
      - TIMEZONE=${TIMEZONE:-UTC}
      - WORKERS=${WORKERS:-4}
    restart: unless-stopped

volumes:
  mybibliotheca_data:
    driver: local
```

**Note:** Pre-built Docker images are available on Docker Hub. Ensure all references to version 1.1.1 in documentation, examples, and Docker commands are updated to **2.0.1**.

Access the application at `http://localhost:5054` and complete the first-time setup to create your admin account.

## Features

### Core Library Management

#### 📖 Book Management

* Add books via ISBN lookup with automatic metadata fetching
* Manual book entry with custom fields
* Bulk import via CSV files
* Cover image management (automatic download or manual URL)
* Comprehensive book details (title, authors, publisher, categories, etc.)
* Book status tracking (Want to Read, Currently Reading, Finished, Library Only)

#### 📊 Reading Tracking

* Reading progress with percentage tracking
* Daily reading logs with page counts and notes
* Reading streak calculation and offset management
* Monthly wrap-up image generation
* Reading session timing and duration tracking
* Personal ratings and reviews

#### 🔍 Search & Organization

* Full-text search across titles, authors, and descriptions
* Advanced filtering by category, publisher, language, status
* Library grid and list views
* Sorting by multiple criteria
* Category and publisher management

### Multi-User Features

#### 👥 User Management

* Secure user registration and authentication
* Individual user accounts with isolated data
* Admin user privileges and management tools
* User profile management and settings
* Reading activity and statistics per user

#### 🔐 Security & Privacy

* Strong password requirements (12+ chars with complexity)
* Account lockout protection after failed attempts
* CSRF protection on all forms
* Forced password changes for new users
* Session management and timeout
* Admin tools for user management

#### 🔒 Privacy Controls

Granular sharing settings per user:

* Share current reading status
* Share reading activity and logs
* Share library contents
* Community features with privacy respect
* Public library views with privacy filters

### Advanced Features

#### 📅 Reading Analytics

* Reading streak tracking with offset support
* Monthly and yearly reading statistics
* Books completed tracking
* Reading pace analysis
* Category distribution analytics

#### 🖼️ Visual Features

* Monthly wrap-up image collages
* Responsive book cover displays
* Library grid view with hover effects
* Beautiful book detail pages
* Mobile-optimized interface

#### 🔄 Import & Export

* CSV import with customizable mapping
* Goodreads export compatibility
* Database backup and download
* User data export capabilities

#### 🌐 Community Features

* Community activity feed
* User profile pages
* Shared reading activity (respecting privacy)
* Public library browsing (when enabled)

## Key Components

### Models (app/models.py)

* **User Model:** Authentication, privacy settings, reading streaks
* **Book Model:** Complete bibliographic information and user association
* **ReadingLog Model:** Daily reading activity tracking
* **Database Relationships:** Foreign keys and cascading deletes

### Authentication (app/auth.py)

* **Flask-Login Integration:** Session management and user loading
* **Security Features:** Account lockout, password validation
* **Registration/Login:** Complete user lifecycle management
* **Admin Tools:** User promotion and management

### Routes (app/routes.py)

* **Main Routes:** Library, book management, reading logs
* **Auth Routes:** Login, registration, profile management
* **API Endpoints:** JSON responses for AJAX operations
* **Admin Routes:** Administrative functions and statistics

### Forms (app/forms.py)

* **WTForms Integration:** Form validation and CSRF protection
* **Book Forms:** Add/edit book information
* **Auth Forms:** Login, registration, password change
* **Reading Log Forms:** Daily activity logging

## Database Schema

### Core Tables

**Users Table:**

```sql
CREATE TABLE user (
    id INTEGER PRIMARY KEY,
    username VARCHAR(80) UNIQUE NOT NULL,
    email VARCHAR(120) UNIQUE NOT NULL,
    password_hash VARCHAR(128) NOT NULL,
    is_admin BOOLEAN DEFAULT FALSE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    failed_login_attempts INTEGER DEFAULT 0,
    locked_until DATETIME,
    last_login DATETIME,
    share_current_reading BOOLEAN DEFAULT TRUE,
    share_reading_activity BOOLEAN DEFAULT TRUE,
    share_library BOOLEAN DEFAULT TRUE,
    password_must_change BOOLEAN DEFAULT FALSE,
    password_changed_at DATETIME,
    reading_streak_offset INTEGER DEFAULT 0
);
```

**Books Table:**

```sql
CREATE TABLE book (
    id INTEGER PRIMARY KEY,
    uid VARCHAR(36) UNIQUE NOT NULL,
    user_id INTEGER NOT NULL REFERENCES user(id),
    title VARCHAR(500) NOT NULL,
    author VARCHAR(500),
    isbn VARCHAR(20),
    description TEXT,
    cover_url VARCHAR(1000),
    page_count INTEGER,
    publisher VARCHAR(200),
    published_date DATE,
    categories VARCHAR(500),
    language VARCHAR(10),
    want_to_read BOOLEAN DEFAULT FALSE,
    finish_date DATE,
    rating INTEGER,
    review TEXT,
    library_only BOOLEAN DEFAULT FALSE,
    reading_progress REAL DEFAULT 0.0,
    added_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_read_date DATETIME
);
```

**Reading Logs Table:**

```sql
CREATE TABLE reading_log (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES user(id),
    book_id INTEGER NOT NULL REFERENCES book(id),
    date DATE NOT NULL,
    pages_read INTEGER DEFAULT 0,
    notes TEXT,
    session_start DATETIME,
    session_end DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### Relationships

* **User → Books:** One-to-many (user can have multiple books)
* **User → Reading Logs:** One-to-many (user can have multiple logs)
* **Book → Reading Logs:** One-to-many (book can have multiple logs)
* **Cascade Deletes:** Deleting a user deletes all their books and logs
* **Cascade Deletes:** Deleting a book deletes all its reading logs

## Troubleshooting

### Common Issues

#### Application Won't Start

**Check Logs:**

```bash
docker logs mybibliotheca
tail -f data/logs/app.log
```

**Common Solutions:**

```bash
docker restart mybibliotheca
docker compose down
docker compose up -d --build
```

#### Database Errors

```bash
ls -la data/books.db
sqlite3 data/books.db "PRAGMA integrity_check;"
```

#### Login Issues

```bash
python3 admin_tools.py reset-password --username YOUR_USERNAME
python3 admin_tools.py unlock-user --username YOUR_USERNAME
```

### Debug Mode

```bash
python3 run.py 2>&1 | tee debug.log
grep "Failed login" debug.log
grep "DatabaseError" debug.log
grep "Slow query" debug.log
```

### System Health Check

```python
from app import create_app, db
from app.models import User, Book

def health_check():
    app = create_app()
    with app.app_context():
        try:
            user_count = User.query.count()
            book_count = Book.query.count()
            admin_users = User.query.filter_by(is_admin=True).count()
            print(f"✅ Database connection: OK")
            print(f"✅ Users: {user_count}")
            print(f"✅ Admin users: {admin_users}")
            return True
        except Exception as e:
            print(f"❌ Health check failed: {e}")
            return False

if __name__ == '__main__':
    health_check()
```

## Contributing

* Fork the repository, create a feature branch, implement changes, test, and submit a PR.
* Follow PEP 8 for Python, SQLAlchemy ORM usage, and security best practices.
* Include tests for new features.

## License

MyBibliotheca is licensed under the MIT License. See the [LICENSE file on GitHub](https://github.com/pickles4evaaaa/mybibliotheca/blob/main/LICENSE) for details.

## Acknowledgments

* Flask, SQLAlchemy, Bootstrap, Flask-Login, WTForms, Google Books API, and community contributors.

---

**Version:** 2.0.1
**Last Updated:** November 2025  
**Framework:** Flask + SQLAlchemy  
**Database:** SQLite  
**Docker Image:** `pickles4evaaaa/mybibliotheca:2.0.1`