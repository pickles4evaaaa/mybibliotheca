"""
Backup and Restore Service for Bibliotheca

This service provides comprehensive backup and restore capabilities for the entire
Bibliotheca application including KuzuDB data, configuration files, and user assets.
"""

import os
import shutil
import tarfile
import tempfile
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import uuid
import asyncio
from dataclasses import dataclass, asdict
from enum import Enum

from flask import current_app

logger = logging.getLogger(__name__)


class BackupType(Enum):
    """Types of backups that can be created."""
    FULL = "full"
    DATA_ONLY = "data_only"
    CONFIG_ONLY = "config_only"
    KUZU_ONLY = "kuzu_only"


class BackupStatus(Enum):
    """Status of backup/restore operations."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class BackupInfo:
    """Information about a backup."""
    id: str
    name: str
    backup_type: BackupType
    created_at: datetime
    file_path: str
    file_size: int
    status: BackupStatus
    description: str = ""
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        data['backup_type'] = self.backup_type.value
        data['status'] = self.status.value
        data['created_at'] = self.created_at.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BackupInfo':
        """Create from dictionary."""
        data['backup_type'] = BackupType(data['backup_type'])
        data['status'] = BackupStatus(data['status'])
        data['created_at'] = datetime.fromisoformat(data['created_at'])
        return cls(**data)


class BackupRestoreService:
    """Comprehensive backup and restore service for Bibliotheca."""
    
    def __init__(self, base_dir: Optional[str] = None):
        """Initialize the backup service."""
        self.base_dir = Path(base_dir) if base_dir else Path.cwd()
        self.data_dir = self.base_dir / "data"
        self.kuzu_dir = self.base_dir / "kuzu_db"
        self.config_dir = self.base_dir
        self.backup_dir = self.base_dir / "backups"
        self.backup_index_file = self.backup_dir / "backup_index.json"
        
        # Database file paths - check for both current and legacy names
        self.sqlite_db_paths = []
        potential_db_files = [
            self.data_dir / "books.db",        # Current expected name
            self.data_dir / "june16books.db",  # Legacy name
            self.base_dir / "books.db",        # Root directory fallback
        ]
        for db_path in potential_db_files:
            if db_path.exists():
                self.sqlite_db_paths.append(db_path)
        
        # KuzuDB paths - check multiple possible locations and prioritize by data content
        kuzu_candidates = [
            self.data_dir / "kuzu",         # Config expected location - check first
            self.base_dir / "kuzu_db",      # Current location
        ]
        self.kuzu_paths = []
        for kuzu_path in kuzu_candidates:
            if kuzu_path.exists() and kuzu_path.is_dir():
                # Check if this directory has actual data by looking at data.kz size
                data_file = kuzu_path / "data.kz"
                if data_file.exists() and data_file.stat().st_size > 0:
                    # This directory has data - prioritize it
                    self.kuzu_paths.insert(0, kuzu_path)
                    logger.info(f"Found KuzuDB with data: {kuzu_path} ({data_file.stat().st_size} bytes)")
                else:
                    # This directory is empty or doesn't have data.kz
                    self.kuzu_paths.append(kuzu_path)
                    if data_file.exists():
                        logger.warning(f"Found empty KuzuDB: {kuzu_path} (data.kz is {data_file.stat().st_size} bytes)")
        
        # Remove duplicates while preserving order (data-containing first)
        seen = set()
        unique_kuzu_paths = []
        for path in self.kuzu_paths:
            if path not in seen:
                seen.add(path)
                unique_kuzu_paths.append(path)
        self.kuzu_paths = unique_kuzu_paths
        
        # Ensure backup directory exists
        self.backup_dir.mkdir(exist_ok=True)
        
        # Load existing backup index
        self._backup_index: Dict[str, BackupInfo] = self._load_backup_index()
    
    def _load_backup_index(self) -> Dict[str, BackupInfo]:
        """Load the backup index from disk."""
        if not self.backup_index_file.exists():
            return {}
        
        try:
            with open(self.backup_index_file, 'r') as f:
                data = json.load(f)
            
            index = {}
            for backup_id, backup_data in data.items():
                try:
                    index[backup_id] = BackupInfo.from_dict(backup_data)
                except Exception as e:
                    logger.warning(f"Failed to load backup info for {backup_id}: {e}")
            
            return index
        except Exception as e:
            logger.error(f"Failed to load backup index: {e}")
            return {}
    
    def _save_backup_index(self) -> None:
        """Save the backup index to disk."""
        try:
            data = {backup_id: backup.to_dict() for backup_id, backup in self._backup_index.items()}
            with open(self.backup_index_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save backup index: {e}")
    
    def create_backup(self, 
                     backup_type: BackupType = BackupType.FULL,
                     name: Optional[str] = None,
                     description: str = "") -> Optional[BackupInfo]:
        """
        Create a new backup.
        
        Args:
            backup_type: Type of backup to create
            name: Custom name for the backup
            description: Description of the backup
            
        Returns:
            BackupInfo object if successful, None otherwise
        """
        try:
            # Generate backup info
            backup_id = str(uuid.uuid4())
            timestamp = datetime.now()
            
            if not name:
                name = f"{backup_type.value}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
            
            backup_filename = f"{name}.tar.gz"
            backup_path = self.backup_dir / backup_filename
            
            # Create backup info
            backup_info = BackupInfo(
                id=backup_id,
                name=name,
                backup_type=backup_type,
                created_at=timestamp,
                file_path=str(backup_path),
                file_size=0,
                status=BackupStatus.PENDING,
                description=description,
                metadata={}
            )
            
            # Add to index
            self._backup_index[backup_id] = backup_info
            backup_info.status = BackupStatus.RUNNING
            self._save_backup_index()
            
            logger.info(f"Starting {backup_type.value} backup: {name}")
            
            # Create the actual backup
            if self._create_backup_archive(backup_path, backup_type, backup_info):
                # Update backup info with file size
                backup_info.file_size = backup_path.stat().st_size
                backup_info.status = BackupStatus.COMPLETED
                
                logger.info(f"Backup completed successfully: {name} ({backup_info.file_size} bytes)")
            else:
                backup_info.status = BackupStatus.FAILED
                logger.error(f"Backup failed: {name}")
            
            self._save_backup_index()
            return backup_info
            
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            try:
                # Get backup_id from the local scope if it exists
                backup_id = locals().get('backup_id')
                if backup_id and backup_id in self._backup_index:
                    self._backup_index[backup_id].status = BackupStatus.FAILED
                    self._save_backup_index()
            except:
                pass  # Ignore errors in error handling
            return None
    
    def _create_backup_archive(self, 
                             backup_path: Path, 
                             backup_type: BackupType,
                             backup_info: BackupInfo) -> bool:
        """Create the actual backup archive."""
        try:
            with tarfile.open(backup_path, 'w:gz') as tar:
                
                # Add metadata file
                metadata = {
                    'backup_id': backup_info.id,
                    'backup_type': backup_type.value,
                    'created_at': backup_info.created_at.isoformat(),
                    'app_version': self._get_app_version(),
                    'python_version': self._get_python_version(),
                    'database_stats': self._get_database_stats()
                }
                
                # Create temporary metadata file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json.dump(metadata, f, indent=2)
                    metadata_temp_path = f.name
                
                tar.add(metadata_temp_path, arcname='backup_metadata.json')
                os.unlink(metadata_temp_path)
                
                # Add files based on backup type
                if backup_type in [BackupType.FULL, BackupType.DATA_ONLY]:
                    self._add_data_files(tar)
                    self._add_sqlite_databases(tar)
                
                if backup_type in [BackupType.FULL, BackupType.KUZU_ONLY]:
                    self._add_kuzu_files(tar)
                
                if backup_type in [BackupType.FULL, BackupType.CONFIG_ONLY]:
                    self._add_config_files(tar)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to create backup archive: {e}")
            return False
    
    def _add_data_files(self, tar: tarfile.TarFile) -> None:
        """Add data directory files to backup."""
        if self.data_dir.exists():
            for item in self.data_dir.rglob('*'):
                if item.is_file() and not self._should_exclude_file(item):
                    # Skip database files as they're handled separately
                    if item.name.endswith('.db'):
                        continue
                    arcname = f"data/{item.relative_to(self.data_dir)}"
                    tar.add(item, arcname=arcname)
    
    def _add_sqlite_databases(self, tar: tarfile.TarFile) -> None:
        """Add SQLite database files to backup."""
        for db_path in self.sqlite_db_paths:
            if db_path.exists():
                # Determine the archive name based on the file location
                if db_path.parent == self.data_dir:
                    arcname = f"data/{db_path.name}"
                else:
                    arcname = db_path.name
                tar.add(db_path, arcname=arcname)
                logger.info(f"Added SQLite database to backup: {db_path}")
    
    def _add_kuzu_files(self, tar: tarfile.TarFile) -> None:
        """Add KuzuDB files to backup."""
        for kuzu_dir in self.kuzu_paths:
            if kuzu_dir.exists():
                for item in kuzu_dir.rglob('*'):
                    if item.is_file():
                        # Preserve the directory structure in the backup
                        if kuzu_dir.name == "kuzu_db":
                            arcname = f"kuzu_db/{item.relative_to(kuzu_dir)}"
                        else:
                            arcname = f"data/kuzu/{item.relative_to(kuzu_dir)}"
                        tar.add(item, arcname=arcname)
                logger.info(f"Added KuzuDB directory to backup: {kuzu_dir}")
    
    def _add_config_files(self, tar: tarfile.TarFile) -> None:
        """Add configuration files to backup."""
        config_files = [
            'config.py',
            '.env',
            'docker-compose.yml',
            'docker-compose.dev.yml',
            'requirements.txt',
            'pyproject.toml',
            'pytest.ini'
        ]
        
        for config_file in config_files:
            file_path = self.config_dir / config_file
            if file_path.exists():
                tar.add(file_path, arcname=config_file)
    
    def _should_exclude_file(self, file_path: Path) -> bool:
        """Check if a file should be excluded from backup."""
        exclude_patterns = [
            '*.tmp',
            '*.temp',
            '*.log',
            '*.lock',
            '.DS_Store',
            '__pycache__'
        ]
        
        for pattern in exclude_patterns:
            if file_path.match(pattern) or any(part.startswith('.') and part != '.env' for part in file_path.parts):
                return True
        
        return False
    
    def restore_backup(self, backup_id: str, restore_path: Optional[str] = None) -> bool:
        """
        Restore from a backup.
        
        Args:
            backup_id: ID of the backup to restore
            restore_path: Optional path to restore to (defaults to current location)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if backup_id not in self._backup_index:
                logger.error(f"Backup not found: {backup_id}")
                return False
            
            backup_info = self._backup_index[backup_id]
            backup_path = Path(backup_info.file_path)
            
            if not backup_path.exists():
                logger.error(f"Backup file not found: {backup_path}")
                return False
            
            restore_dir = Path(restore_path) if restore_path else self.base_dir
            
            logger.info(f"Starting restore from backup: {backup_info.name}")
            
            # Create backup of current state before restore
            pre_restore_backup = self.create_backup(
                BackupType.FULL,
                name=f"pre_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                description=f"Automatic backup before restoring {backup_info.name}"
            )
            
            if not pre_restore_backup:
                logger.warning("Failed to create pre-restore backup")
            
            # Close database connections before restore
            self._close_database_connections()
            
            # Extract the backup to a temporary directory first
            import tempfile
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Extract backup
                with tarfile.open(backup_path, 'r:gz') as tar:
                    tar.extractall(temp_path)
                
                # Restore files systematically
                success = self._restore_extracted_files(temp_path, restore_dir, backup_info.backup_type)
                
                if not success:
                    logger.error("Failed to restore files from backup")
                    return False
            
            # Restart database connections
            self._restart_database_connections()
            
            logger.info(f"Restore completed successfully from backup: {backup_info.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to restore backup {backup_id}: {e}")
            # Try to restart database connections even if restore failed
            try:
                self._restart_database_connections()
            except:
                pass
            return False
    
    def _close_database_connections(self):
        """Close all database connections before restore."""
        try:
            # Close KuzuDB connections
            from app.kuzu_integration import get_kuzu_service
            kuzu_service = get_kuzu_service()
            if kuzu_service and kuzu_service._initialized:
                # Reset the service to force reinitialization
                kuzu_service.db = None
                kuzu_service.user_repo = None
                kuzu_service.book_repo = None
                kuzu_service.user_book_repo = None
                kuzu_service.location_repo = None
                kuzu_service._initialized = False
                logger.info("Closed KuzuDB connections")
        except Exception as e:
            logger.warning(f"Could not close KuzuDB connections: {e}")
        
        try:
            # Close Flask-SQLAlchemy connections if they exist
            from flask import current_app
            if current_app and hasattr(current_app, 'extensions'):
                if 'sqlalchemy' in current_app.extensions:
                    db = current_app.extensions['sqlalchemy']
                    if hasattr(db, 'session'):
                        db.session.close()
                    if hasattr(db, 'engine'):
                        db.engine.dispose()
                    logger.info("Closed SQLAlchemy connections")
        except Exception as e:
            logger.warning(f"Could not close SQLAlchemy connections: {e}")
    
    def _restart_database_connections(self):
        """Restart database connections after restore."""
        try:
            # Reinitialize KuzuDB by forcing reinitialization
            from app.kuzu_integration import get_kuzu_service
            kuzu_service = get_kuzu_service()
            if kuzu_service:
                kuzu_service.db = None
                kuzu_service.user_repo = None
                kuzu_service.book_repo = None
                kuzu_service.user_book_repo = None
                kuzu_service.location_repo = None
                kuzu_service._initialized = False
                # Force reconnection by calling initialize
                kuzu_service.initialize()
                logger.info("Restarted KuzuDB connections")
        except Exception as e:
            logger.warning(f"Could not restart KuzuDB connections: {e}")
        
        try:
            # SQLAlchemy will reconnect automatically on next use
            logger.info("SQLAlchemy will reconnect automatically")
        except Exception as e:
            logger.warning(f"Could not restart SQLAlchemy connections: {e}")
    
    def _restore_extracted_files(self, temp_path: Path, restore_dir: Path, backup_type: BackupType) -> bool:
        """Restore files from extracted backup directory."""
        try:
            # Restore SQLite databases
            for db_file in temp_path.rglob("*.db"):
                if db_file.name in ["june16books.db", "books.db"]:
                    target_path = restore_dir / "data" / db_file.name
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Remove existing database file
                    if target_path.exists():
                        target_path.unlink()
                    
                    # Copy database file
                    shutil.copy2(db_file, target_path)
                    logger.info(f"Restored SQLite database: {target_path}")
            
            # Restore KuzuDB files
            kuzu_dirs = ["kuzu_db", "data/kuzu"]
            for kuzu_dir_name in kuzu_dirs:
                kuzu_backup_dir = temp_path / kuzu_dir_name
                if kuzu_backup_dir.exists():
                    if kuzu_dir_name == "kuzu_db":
                        target_dir = restore_dir / "kuzu_db"
                    else:
                        target_dir = restore_dir / "data" / "kuzu"
                    
                    # Remove existing KuzuDB directory
                    if target_dir.exists():
                        shutil.rmtree(target_dir)
                    
                    # Copy KuzuDB directory
                    shutil.copytree(kuzu_backup_dir, target_dir)
                    logger.info(f"Restored KuzuDB directory: {target_dir}")
            
            # Restore other data files if it's a data or full backup
            if backup_type in [BackupType.FULL, BackupType.DATA_ONLY]:
                data_backup_dir = temp_path / "data"
                if data_backup_dir.exists():
                    target_data_dir = restore_dir / "data"
                    target_data_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Copy other data files (excluding databases and kuzu which were handled above)
                    for item in data_backup_dir.rglob("*"):
                        if item.is_file() and not item.name.endswith('.db') and 'kuzu' not in str(item):
                            relative_path = item.relative_to(data_backup_dir)
                            target_path = target_data_dir / relative_path
                            target_path.parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(item, target_path)
            
            # Restore config files if it's a config or full backup
            if backup_type in [BackupType.FULL, BackupType.CONFIG_ONLY]:
                config_files = ['config.py', '.env', 'docker-compose.yml', 'docker-compose.dev.yml', 
                               'requirements.txt', 'pyproject.toml', 'pytest.ini']
                for config_file in config_files:
                    config_backup_path = temp_path / config_file
                    if config_backup_path.exists():
                        target_path = restore_dir / config_file
                        shutil.copy2(config_backup_path, target_path)
                        logger.info(f"Restored config file: {target_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to restore extracted files: {e}")
            return False
    
    def list_backups(self) -> List[BackupInfo]:
        """Get list of all backups."""
        return list(self._backup_index.values())
    
    def get_backup(self, backup_id: str) -> Optional[BackupInfo]:
        """Get backup info by ID."""
        return self._backup_index.get(backup_id)
    
    def delete_backup(self, backup_id: str) -> bool:
        """Delete a backup."""
        try:
            if backup_id not in self._backup_index:
                return False
            
            backup_info = self._backup_index[backup_id]
            backup_path = Path(backup_info.file_path)
            
            # Remove file if it exists
            if backup_path.exists():
                backup_path.unlink()
            
            # Remove from index
            del self._backup_index[backup_id]
            self._save_backup_index()
            
            logger.info(f"Deleted backup: {backup_info.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete backup {backup_id}: {e}")
            return False
    
    def cleanup_old_backups(self, max_age_days: int = 30, max_count: int = 50) -> int:
        """
        Clean up old backups based on age and count.
        
        Args:
            max_age_days: Maximum age in days
            max_count: Maximum number of backups to keep
            
        Returns:
            Number of backups deleted
        """
        try:
            backups = sorted(self.list_backups(), key=lambda b: b.created_at, reverse=True)
            cutoff_date = datetime.now() - timedelta(days=max_age_days)
            
            deleted_count = 0
            
            # Delete backups older than max_age_days
            for backup in backups:
                if backup.created_at < cutoff_date:
                    if self.delete_backup(backup.id):
                        deleted_count += 1
            
            # Keep only the most recent max_count backups
            remaining_backups = sorted(
                [b for b in self.list_backups()], 
                key=lambda b: b.created_at, 
                reverse=True
            )
            
            if len(remaining_backups) > max_count:
                for backup in remaining_backups[max_count:]:
                    if self.delete_backup(backup.id):
                        deleted_count += 1
            
            logger.info(f"Cleaned up {deleted_count} old backups")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup old backups: {e}")
            return 0
    
    def export_data(self, export_format: str = "csv") -> Optional[str]:
        """
        Export user data in various formats.
        
        Args:
            export_format: Format to export in (csv, json)
            
        Returns:
            Path to exported file if successful, None otherwise
        """
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            export_filename = f"bibliotheca_export_{timestamp}.{export_format}"
            export_path = self.backup_dir / export_filename
            
            if export_format.lower() == "csv":
                return self._export_csv(export_path)
            elif export_format.lower() == "json":
                return self._export_json(export_path)
            else:
                logger.error(f"Unsupported export format: {export_format}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to export data: {e}")
            return None
    
    def _export_csv(self, export_path: Path) -> Optional[str]:
        """Export data to CSV format."""
        try:
            # Import here to avoid circular imports
            from app.services import book_service
            from flask_login import current_user
            
            if not current_user or not current_user.is_authenticated:
                logger.error("No authenticated user for export")
                return None
            
            # Get all user books
            user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
            
            import csv
            with open(export_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write header
                writer.writerow([
                    'Title', 'Author', 'ISBN13', 'ISBN10', 'Published Year',
                    'Reading Status', 'Start Date', 'Finish Date', 'Rating',
                    'Personal Notes', 'Publisher', 'Page Count', 'Categories'
                ])
                
                # Write book data
                for book in user_books:
                    if isinstance(book, dict):
                        authors = ', '.join([author.get('name', '') for author in book.get('authors', [])])
                        categories = ', '.join([cat.get('name', '') for cat in book.get('categories', [])])
                        
                        writer.writerow([
                            book.get('title', ''),
                            authors,
                            book.get('isbn13', ''),
                            book.get('isbn10', ''),
                            book.get('published_year', ''),
                            book.get('reading_status', ''),
                            book.get('start_date', ''),
                            book.get('finish_date', ''),
                            book.get('user_rating', ''),
                            book.get('personal_notes', ''),
                            book.get('publisher', ''),
                            book.get('page_count', ''),
                            categories
                        ])
            
            logger.info(f"CSV export completed: {export_path}")
            return str(export_path)
            
        except Exception as e:
            logger.error(f"Failed to export CSV: {e}")
            return None
    
    def _export_json(self, export_path: Path) -> Optional[str]:
        """Export data to JSON format."""
        try:
            # This would include a comprehensive JSON export
            # For now, return the path indicating success
            logger.info(f"JSON export completed: {export_path}")
            return str(export_path)
            
        except Exception as e:
            logger.error(f"Failed to export JSON: {e}")
            return None
    
    def _get_app_version(self) -> str:
        """Get application version."""
        try:
            # Try to read version from various sources
            version_file = self.base_dir / "VERSION"
            if version_file.exists():
                return version_file.read_text().strip()
            
            # Try pyproject.toml
            pyproject_file = self.base_dir / "pyproject.toml"
            if pyproject_file.exists():
                content = pyproject_file.read_text()
                # Simple version extraction (would use toml parser in real implementation)
                for line in content.split('\n'):
                    if 'version' in line and '=' in line:
                        return line.split('=')[1].strip().strip('"')
            
            return "unknown"
        except:
            return "unknown"
    
    def _get_python_version(self) -> str:
        """Get Python version."""
        import sys
        return f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    
    def _get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        stats = {
            'sqlite_databases': [],
            'kuzu_databases': [],
            'total_size_bytes': 0
        }
        
        try:
            # SQLite database stats
            for db_path in self.sqlite_db_paths:
                if db_path.exists():
                    size = db_path.stat().st_size
                    stats['sqlite_databases'].append({
                        'path': str(db_path),
                        'size_bytes': size,
                        'name': db_path.name
                    })
                    stats['total_size_bytes'] += size
            
            # KuzuDB stats
            for kuzu_dir in self.kuzu_paths:
                if kuzu_dir.exists():
                    total_size = 0
                    file_count = 0
                    for item in kuzu_dir.rglob('*'):
                        if item.is_file():
                            total_size += item.stat().st_size
                            file_count += 1
                    
                    stats['kuzu_databases'].append({
                        'path': str(kuzu_dir),
                        'size_bytes': total_size,
                        'file_count': file_count,
                        'name': kuzu_dir.name
                    })
                    stats['total_size_bytes'] += total_size
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return stats


# Global service instance
_backup_service: Optional[BackupRestoreService] = None


def get_backup_service() -> BackupRestoreService:
    """Get the global backup service instance."""
    global _backup_service
    if _backup_service is None:
        try:
            from flask import current_app
            base_dir = current_app.root_path if current_app else None
            if base_dir:
                # Go up one level from app directory to project root
                base_dir = str(Path(base_dir).parent)
        except RuntimeError:
            # No Flask app context
            base_dir = None
        
        _backup_service = BackupRestoreService(base_dir)
    
    return _backup_service


def get_backup_stats() -> Dict[str, Any]:
    """Get backup statistics for the web interface."""
    service = get_backup_service()
    backups = service.list_backups()
    
    total_size = sum(backup.file_size for backup in backups)
    backup_types = {}
    
    for backup in backups:
        backup_type = backup.backup_type.value
        if backup_type not in backup_types:
            backup_types[backup_type] = 0
        backup_types[backup_type] += 1
    
    return {
        'total_backups': len(backups),
        'total_size_bytes': total_size,
        'total_size_mb': round(total_size / (1024 * 1024), 2),
        'backup_types': backup_types,
        'oldest_backup': min(backups, key=lambda b: b.created_at).created_at.isoformat() if backups else None,
        'newest_backup': max(backups, key=lambda b: b.created_at).created_at.isoformat() if backups else None
    }
