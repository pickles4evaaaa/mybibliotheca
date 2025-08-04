#!/usr/bin/env python3
"""
Automatic Migration Detector for Bibliotheca
============================================

This module detects SQLite databases at startup and offers migration options.
It integrates with the application startup process to provide a seamless upgrade experience.
"""

import os
import sqlite3
import logging
from pathlib import Path
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class MigrationDetector:
    """Detects SQLite databases and manages automatic migration."""
    
    def __init__(self):
        # Define potential search paths and deduplicate them
        potential_paths = [
            Path.cwd() / "data",  # Local data directory
            Path("/app/data"),    # Docker data directory  
        ]
        
        # Remove duplicates by resolving paths and using a set
        seen_paths = set()
        self.search_paths = []
        for path in potential_paths:
            resolved_path = path.resolve()
            if resolved_path not in seen_paths:
                seen_paths.add(resolved_path)
                self.search_paths.append(path)
    
    def find_sqlite_databases(self) -> List[Path]:
        """Find SQLite database files in common locations."""
        db_files = []
        
        logger.info("🔍 Searching for SQLite databases...")
        for path in self.search_paths:
            logger.debug(f"   Checking path: {path}")
            if path.exists():
                logger.debug(f"   ✅ Path exists: {path}")
                found_dbs = list(path.glob("*.db"))
                logger.debug(f"   Found *.db files: {[f.name for f in found_dbs]}")
                
                for db_file in found_dbs:
                    if self._is_bibliotheca_database(db_file):
                        logger.info(f"   ✅ Valid Bibliotheca database: {db_file}")
                        db_files.append(db_file)
                    else:
                        logger.debug(f"   ❌ Not a Bibliotheca database: {db_file}")
            else:
                logger.debug(f"   ❌ Path does not exist: {path}")
        
        logger.info(f"📚 Found {len(db_files)} Bibliotheca database(s): {[f.name for f in db_files]}")
        return db_files
    
    def _is_bibliotheca_database(self, db_path: Path) -> bool:
        """Check if this is a Bibliotheca SQLite database."""
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Check for expected tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            conn.close()
            
            # Must have book table, optionally user and reading_log
            has_book_table = 'book' in tables
            logger.debug(f"   Database {db_path.name}: tables={tables}, has_book_table={has_book_table}")
            return has_book_table
            
        except Exception as e:
            logger.debug(f"   Error checking database {db_path}: {e}")
            return False
    
    def analyze_database(self, db_path: Path) -> Dict:
        """Analyze a SQLite database structure and contents."""
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            # Determine schema version
            has_users = 'user' in tables
            schema_version = 2 if has_users else 1
            
            # Count records
            counts = {}
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    counts[table] = cursor.fetchone()[0]
                except Exception:
                    counts[table] = 0
            
            conn.close()
            
            total_books = counts.get('book', 0)
            total_logs = counts.get('reading_log', 0)
            total_users = counts.get('user', 0)
            
            logger.debug(f"   Database {db_path.name}: {total_books} books, {total_users} users, {total_logs} logs")
            
            return {
                'path': db_path,
                'tables': tables,
                'schema_version': schema_version,
                'has_users': has_users,
                'counts': counts,
                'total_books': total_books,
                'total_logs': total_logs,
                'total_users': total_users
            }
            
        except Exception as e:
            logger.error(f"Error analyzing database {db_path}: {e}")
            return {'error': str(e), 'path': db_path, 'total_books': 0}
    
    def check_for_migration_needed(self) -> Optional[Dict]:
        """
        Check if migration is needed at startup.
        
        Returns:
            None if no migration needed
            Dict with migration info if databases found
        """
        databases = self.find_sqlite_databases()
        
        if not databases:
            return None
        
        # Analyze found databases
        db_info = []
        total_books = 0
        total_users = 0
        
        for db in databases:
            info = self.analyze_database(db)
            if 'error' not in info:
                db_info.append(info)
                total_books += info['total_books']
                total_users += info['total_users']
        
        if not db_info:
            return None
        
        return {
            'databases': db_info,
            'total_databases': len(db_info),
            'total_books': total_books,
            'total_users': total_users,
            'has_v1_databases': any(db['schema_version'] == 1 for db in db_info),
            'has_v2_databases': any(db['schema_version'] == 2 for db in db_info)
        }
    
    def create_migration_command(self, db_info: Dict, user_id: Optional[str] = None) -> List[str]:
        """Create the migration command for a specific database."""
        db_path = db_info['path']
        
        # Use absolute path to migration script
        script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts', 'migrate_sqlite_to_redis.py')
        cmd = ["python3", script_path, "--db-path", str(db_path)]
        
        # Add user ID for v1 databases
        if db_info['schema_version'] == 1 and user_id:
            cmd.extend(["--user-id", user_id])
        
        return cmd


def check_migration_at_startup() -> Optional[Dict]:
    """
    Check for migration needs at application startup.
    This function should be called early in the app initialization.
    """
    detector = MigrationDetector()
    migration_info = detector.check_for_migration_needed()
    
    if migration_info:
        logger.info(f"🔍 Found {migration_info['total_databases']} SQLite database(s) with {migration_info['total_books']} books")
        
        # Log details for each database
        for db in migration_info['databases']:
            schema = f"v{db['schema_version']}"
            books = db['total_books']
            users = db['total_users'] if db['has_users'] else "single-user"
            logger.info(f"  📚 {db['path'].name}: {schema} ({users}, {books} books)")
    
    return migration_info


def get_migration_message(migration_info: Dict) -> str:
    """Generate a user-friendly migration message."""
    total_books = migration_info['total_books']
    total_dbs = migration_info['total_databases']
    
    message = f"""
🔄 SQLite Database Migration Available

Found {total_dbs} SQLite database(s) with {total_books} books that can be migrated to the new Redis system.

Databases found:
"""
    
    for db in migration_info['databases']:
        schema = f"v{db['schema_version']}"
        books = db['total_books']
        users_info = f"{db['total_users']} users" if db['has_users'] else "single-user"
        message += f"  • {db['path'].name}: {schema} ({users_info}, {books} books)\n"
    
    message += f"""
To migrate your data:
1. Stop the application
2. Run: python3 scripts/quick_migrate.py
3. Follow the interactive migration process
4. Restart the application

Your SQLite data will be preserved as backups during migration.
"""
    
    return message


# For Docker/automatic environments
def auto_migrate_if_safe(migration_info: Dict, default_user_id: str = "admin") -> bool:
    """
    Automatically migrate if it's safe to do so.
    
    This is designed for Docker environments where user interaction isn't possible.
    Can migrate multiple databases of different types.
    """
    import subprocess
    import sys
    
    try:
        # Auto-migrate if we have a default user ID for v1 databases
        # This handles all scenarios safely:
        # 1. Only v1 databases + default_user_id
        # 2. Only v2 databases (no user ID needed)
        # 3. Mixed v1/v2 databases + default_user_id
        
        # Check if we can handle all v1 databases
        needs_user_id = migration_info['has_v1_databases']
        has_user_id = bool(default_user_id)
        
        if needs_user_id and not has_user_id:
            logger.warning("⚠️  Cannot auto-migrate: v1 databases found but no default user ID provided")
            logger.warning("⚠️  Set MIGRATION_DEFAULT_USER environment variable")
            return False
        
        logger.info("🤖 Starting automatic migration...")
        logger.info(f"📊 Migrating {migration_info['total_databases']} database(s) with {migration_info['total_books']} books")
        
        # Migrate each database
        detector = MigrationDetector()
        migrated_count = 0
        
        for db_info in migration_info['databases']:
            try:
                cmd = detector.create_migration_command(db_info, default_user_id)
                
                logger.info(f"🔄 Migrating {db_info['path'].name} (v{db_info['schema_version']}, {db_info['total_books']} books)...")
                logger.debug(f"Migration command: {' '.join(cmd)}")
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                
                if result.returncode != 0:
                    logger.error(f"❌ Migration failed for {db_info['path'].name}")
                    logger.error(f"Error output: {result.stderr}")
                    logger.error(f"Standard output: {result.stdout}")
                else:
                    logger.info(f"✅ Successfully migrated {db_info['path'].name}")
                    migrated_count += 1
                    
            except subprocess.TimeoutExpired:
                logger.error(f"❌ Migration timed out for {db_info['path'].name}")
            except Exception as e:
                logger.error(f"❌ Migration error for {db_info['path'].name}: {e}")
        
        if migrated_count == migration_info['total_databases']:
            logger.info("🎉 Automatic migration completed successfully!")
            return True
        else:
            logger.warning(f"⚠️  Partial migration: {migrated_count}/{migration_info['total_databases']} databases migrated")
            return False
        
    except Exception as e:
        logger.error(f"❌ Auto-migration failed: {e}")
        return False
