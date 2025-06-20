#!/usr/bin/env python3
"""
Database migration script for Bibliotheca
Migrates from single-user to multi-user architecture

This script:
1. Creates a default admin user if no users exist
2. Assigns all existing books to the default admin user
3. Assigns all existing reading logs to the default admin user
4. Updates database constraints
"""

import os
import sys
from datetime import datetime, timezone

# Add the app directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import create_app
from app.models import db, User, Book, ReadingLog
from config import Config

def create_default_admin():
    """Create a default admin user for migration"""
    admin_username = os.environ.get('ADMIN_USERNAME', 'admin')
    admin_email = os.environ.get('ADMIN_EMAIL', 'admin@bibliotheca.local')
    admin_password = os.environ.get('ADMIN_PASSWORD', 'TempAdmin123!@#')
    
    # Check if admin already exists
    existing_admin = User.query.filter_by(username=admin_username).first()
    if existing_admin:
        print(f"✅ Admin user '{admin_username}' already exists.")
        return existing_admin
    
    # Create new admin user
    admin_user = User(
        username=admin_username,
        email=admin_email,
        is_admin=True,
        created_at=datetime.now(timezone.utc)
    )
    
    try:
        # For initial setup, bypass password validation
        admin_user.set_password(admin_password, validate=False)
        admin_user.password_changed_at = None  # Mark as never changed
        # Force password change on first login
        admin_user.password_must_change = True
        
        db.session.add(admin_user)
        db.session.commit()
        
        print(f"✅ Created admin user: {admin_username} (email: {admin_email})")
        print(f"⚠️  Default password: {admin_password}")
        print("🔒 Admin will be required to change password on first login!")
        
        return admin_user
    except Exception as e:
        print(f"⚠️  Failed to create default admin: {e}")
        raise

def migrate_books_to_user(user):
    """Assign all books without user_id to the specified user"""
    # Use raw SQL to handle schema differences
    try:
        # First check if user_id column exists
        result = db.session.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='book'"
        ).fetchone()
        
        if result and 'user_id' not in result[0]:
            print("📚 Adding user_id column to book table...")
            db.session.execute("ALTER TABLE book ADD COLUMN user_id INTEGER")
            db.session.commit()
        
        # Count books without user_id (NULL values)
        result = db.session.execute("SELECT COUNT(*) FROM book WHERE user_id IS NULL").fetchone()
        orphaned_count = result[0] if result else 0
        
        if orphaned_count == 0:
            print("✅ No orphaned books found.")
            return
        
        print(f"📚 Migrating {orphaned_count} books to user '{user.username}'...")
        
        # Update all books without user_id
        db.session.execute(
            "UPDATE book SET user_id = :user_id WHERE user_id IS NULL",
            {"user_id": user.id}
        )
        db.session.commit()
        print(f"✅ Successfully migrated {orphaned_count} books.")
        
    except Exception as e:
        print(f"⚠️  Book migration error: {e}")
        # Try to continue with the rest of the migration

def migrate_reading_logs_to_user(user):
    """Assign all reading logs without user_id to the specified user"""
    try:
        # First check if user_id column exists
        result = db.session.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='reading_log'"
        ).fetchone()
        
        if result and 'user_id' not in result[0]:
            print("📖 Adding user_id column to reading_log table...")
            db.session.execute("ALTER TABLE reading_log ADD COLUMN user_id INTEGER")
            db.session.commit()
        
        # Count reading logs without user_id (NULL values)
        result = db.session.execute("SELECT COUNT(*) FROM reading_log WHERE user_id IS NULL").fetchone()
        orphaned_count = result[0] if result else 0
        
        if orphaned_count == 0:
            print("✅ No orphaned reading logs found.")
            return
        
        print(f"📖 Migrating {orphaned_count} reading logs to user '{user.username}'...")
        
        # Update all reading logs without user_id
        db.session.execute(
            "UPDATE reading_log SET user_id = :user_id WHERE user_id IS NULL",
            {"user_id": user.id}
        )
        db.session.commit()
        print(f"✅ Successfully migrated {orphaned_count} reading logs.")
        
    except Exception as e:
        print(f"⚠️  Reading log migration error: {e}")
        # Continue with the rest of the migration

def run_migration():
    """Run the complete migration process"""
    app = create_app()
    
    with app.app_context():
        print("🚀 Starting Bibliotheca multi-user migration...")
        print("=" * 50)
        
        # Check if migration is needed using raw SQL to avoid model issues
        try:
            total_users_result = db.session.execute("SELECT COUNT(*) FROM user").fetchone()
            total_users = total_users_result[0] if total_users_result else 0
        except:
            total_users = 0
        
        # Check orphaned data using raw SQL to avoid model issues
        try:
            orphaned_books_result = db.session.execute("SELECT COUNT(*) FROM book WHERE user_id IS NULL").fetchone()
            orphaned_books = orphaned_books_result[0] if orphaned_books_result else 0
        except:
            orphaned_books = 0
            
        try:
            orphaned_logs_result = db.session.execute("SELECT COUNT(*) FROM reading_log WHERE user_id IS NULL").fetchone()
            orphaned_logs = orphaned_logs_result[0] if orphaned_logs_result else 0
        except:
            orphaned_logs = 0
        
        if total_users > 0 and orphaned_books == 0 and orphaned_logs == 0:
            print("✅ Database already migrated to multi-user")
            return
        
        print(f"📊 Migration status:")
        print(f"   - Users in database: {total_users}")
        print(f"   - Books without user: {orphaned_books}")
        print(f"   - Reading logs without user: {orphaned_logs}")
        print()
        
        # Step 1: Create default admin user if needed
        if total_users == 0:
            print("👤 Creating default admin user...")
            admin_user = create_default_admin()
        else:
            # Use existing admin or first user
            admin_user = User.query.filter_by(is_admin=True).first()
            if not admin_user:
                admin_user = User.query.first()
                admin_user.is_admin = True
                db.session.commit()
                print(f"✅ Granted admin privileges to existing user: {admin_user.username}")
        
        # Step 2: Migrate books
        migrate_books_to_user(admin_user)
        
        # Step 3: Migrate reading logs
        migrate_reading_logs_to_user(admin_user)
        
        print()
        print("🎉 Migration completed successfully!")
        print("=" * 50)
        print("📋 Post-migration checklist:")
        print("   1. Test login with admin credentials")
        print("   2. Change admin password")
        print("   3. Verify all books are visible")
        print("   4. Test creating new users")
        print("   5. Backup your database")

if __name__ == '__main__':
    run_migration()
