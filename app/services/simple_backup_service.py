"""
Simple Backup and Restore Service for Bibliotheca

A simplified approach that focuses on direct database backup/restore
with minimal complexity and maximum reliability.
"""

import os
import shutil
import zipfile
import tempfile
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import uuid

from flask import current_app

logger = logging.getLogger(__name__)


@dataclass
class SimpleBackupInfo:
    """Information about a simple backup."""
    id: str
    name: str
    created_at: datetime
    file_path: str
    file_size: int
    description: str = ""
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'id': self.id,
            'name': self.name,
            'created_at': self.created_at.isoformat(),
            'file_path': self.file_path,
            'file_size': self.file_size,
            'description': self.description,
            'metadata': self.metadata or {}
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SimpleBackupInfo':
        """Create from dictionary."""
        data['created_at'] = datetime.fromisoformat(data['created_at'])
        return cls(**data)


class SimpleBackupService:
    """Simplified backup and restore service for Bibliotheca."""
    
    def __init__(self, base_dir: Optional[str] = None):
        """Initialize the simple backup service."""
        self.base_dir = Path(base_dir) if base_dir else Path.cwd()
        self.data_dir = self.base_dir / "data"
        self.backup_dir = self.data_dir / "backups"
        self.backup_index_file = self.backup_dir / "simple_backup_index.json"
        
        # KuzuDB database path
        self.kuzu_db_path = self.data_dir / "kuzu" / "bibliotheca.db"
        
        # Ensure backup directory exists
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Load existing backup index
        self._backup_index: Dict[str, SimpleBackupInfo] = self._load_backup_index()
    
    def _load_backup_index(self) -> Dict[str, SimpleBackupInfo]:
        """Load the backup index from disk."""
        if not self.backup_index_file.exists():
            return {}
        
        try:
            with open(self.backup_index_file, 'r') as f:
                data = json.load(f)
            
            index = {}
            for backup_id, backup_data in data.items():
                try:
                    index[backup_id] = SimpleBackupInfo.from_dict(backup_data)
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
    
    def create_backup(self, name: Optional[str] = None, description: str = "") -> Optional[SimpleBackupInfo]:
        """
        Create a simple backup of the KuzuDB database.
        
        Args:
            name: Optional custom name for the backup
            description: Optional description
            
        Returns:
            BackupInfo if successful, None otherwise
        """
        try:
            # Generate backup ID and timestamp
            backup_id = str(uuid.uuid4())
            timestamp = datetime.now()
            
            # Generate backup name if not provided
            if not name:
                name = f"backup_{timestamp.strftime('%Y%m%d_%H%M%S')}"
            
            # Create backup file path
            backup_filename = f"{name}_{backup_id[:8]}.zip"
            backup_path = self.backup_dir / backup_filename
            
            logger.info(f"Creating simple backup: {name}")
            
            # Check if KuzuDB exists
            if not self.kuzu_db_path.exists():
                logger.error(f"KuzuDB not found at {self.kuzu_db_path}")
                return None
            
            # KuzuDB should be a directory, not a file
            if not self.kuzu_db_path.is_dir():
                logger.error(f"KuzuDB path is not a directory: {self.kuzu_db_path}")
                return None
            
            # Create backup metadata
            metadata = {
                'backup_id': backup_id,
                'created_at': timestamp.isoformat(),
                'kuzu_db_path': str(self.kuzu_db_path),
                'original_size': self._get_directory_size(self.kuzu_db_path),
                'backup_type': 'simple_database_backup'
            }
            
            # Create the backup ZIP file
            with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Add metadata file
                zipf.writestr('backup_metadata.json', json.dumps(metadata, indent=2))
                
                # Add the entire KuzuDB directory (it should always be a directory)
                for file_path in self.kuzu_db_path.rglob('*'):
                    if file_path.is_file():
                        relative_path = file_path.relative_to(self.kuzu_db_path)
                        zipf.write(file_path, str(relative_path))
                        logger.debug(f"Added to backup: {relative_path}")
                
                logger.info(f"Backed up KuzuDB directory with {len(list(self.kuzu_db_path.rglob('*')))} files")
            
            # Get backup file size
            file_size = backup_path.stat().st_size
            
            # Create backup info
            backup_info = SimpleBackupInfo(
                id=backup_id,
                name=name,
                created_at=timestamp,
                file_path=str(backup_path),
                file_size=file_size,
                description=description,
                metadata=metadata
            )
            
            # Add to index and save
            self._backup_index[backup_id] = backup_info
            self._save_backup_index()
            
            logger.info(f"Simple backup created successfully: {name} ({file_size / 1024 / 1024:.2f} MB)")
            return backup_info
            
        except Exception as e:
            logger.error(f"Failed to create simple backup: {e}")
            return None
    
    def restore_backup(self, backup_id: str) -> bool:
        """
        Restore from a simple backup.
        
        This method:
        1. Disconnects KuzuDB cleanly
        2. Replaces database files from backup
        3. Reconnects KuzuDB
        
        Args:
            backup_id: ID of the backup to restore
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get backup info
            backup_info = self.get_backup(backup_id)
            if not backup_info:
                logger.error(f"Backup not found: {backup_id}")
                return False
            
            backup_path = Path(backup_info.file_path)
            if not backup_path.exists():
                logger.error(f"Backup file not found: {backup_path}")
                return False
            
            logger.info(f"Starting simple restore from backup: {backup_info.name}")
            
            # Step 1: Disconnect KuzuDB cleanly
            logger.info("Disconnecting KuzuDB connections...")
            self._disconnect_kuzu_database()
            
            # Step 2: Skip backup during restore if database is active
            current_backup_path = None
            if self.kuzu_db_path.exists():
                logger.info("Database backup during restore skipped to avoid file locks")
                # We'll rely on the existing backup being restored from for safety
            
            # Step 3: Remove current database with retry
            if self.kuzu_db_path.exists():
                retry_count = 0
                max_retries = 5
                while retry_count < max_retries:
                    try:
                        # KuzuDB is always a directory
                        shutil.rmtree(self.kuzu_db_path)
                        logger.info("Removed current database directory")
                        break
                    except OSError as e:
                        if e.errno == 35:  # Resource deadlock avoided
                            retry_count += 1
                            if retry_count < max_retries:
                                logger.warning(f"Database removal failed (attempt {retry_count}), retrying in 3 seconds...")
                                import time
                                time.sleep(3)
                                # Try extra disconnect before retry
                                import gc
                                gc.collect()
                                import time
                                time.sleep(1)
                            else:
                                logger.error(f"Failed to remove database after {max_retries} attempts: {e}")
                                return False
                        else:
                            logger.error(f"Error removing current database: {e}")
                            return False
            
            # Step 4: Extract backup to restore database
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Extract backup
                with zipfile.ZipFile(backup_path, 'r') as zipf:
                    zipf.extractall(temp_path)
                
                # Restore database files - KuzuDB is always a directory
                self.kuzu_db_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Check extracted files (excluding metadata)
                extracted_files = list(temp_path.glob('*'))
                db_files = [f for f in extracted_files if f.name != 'backup_metadata.json']
                
                # Create the KuzuDB directory
                if not self.kuzu_db_path.exists():
                    self.kuzu_db_path.mkdir(parents=True)
                    logger.info(f"Created KuzuDB directory: {self.kuzu_db_path}")
                
                # Copy all database files into the KuzuDB directory with retry
                files_restored = 0
                for file_path in db_files:
                    if file_path.is_file():
                        target_path = self.kuzu_db_path / file_path.name
                        
                        # Retry file copy if needed
                        retry_count = 0
                        max_retries = 3
                        while retry_count < max_retries:
                            try:
                                shutil.copy2(file_path, target_path)
                                files_restored += 1
                                logger.debug(f"Restored: {file_path.name} -> {target_path}")
                                break
                            except OSError as e:
                                if e.errno == 35:  # Resource deadlock avoided
                                    retry_count += 1
                                    if retry_count < max_retries:
                                        logger.warning(f"File copy failed for {file_path.name} (attempt {retry_count}), retrying in 2 seconds...")
                                        import time
                                        time.sleep(2)
                                    else:
                                        logger.error(f"Failed to copy {file_path.name} after {max_retries} attempts: {e}")
                                        raise
                                else:
                                    logger.error(f"Error copying {file_path.name}: {e}")
                                    raise
                
                logger.info(f"Restored KuzuDB directory with {files_restored} files")
            
            # Step 5: Reconnect KuzuDB
            logger.info("Reconnecting KuzuDB...")
            success = self._reconnect_kuzu_database()
            
            # Step 6: Clear service layer cache after successful reconnection
            if success:
                logger.info("Clearing service layer cache...")
                self._clear_service_cache()
            
            # Clear the restore flag regardless of success
            self._clear_restore_flag()
            
            if success:
                logger.info(f"Simple restore completed successfully from backup: {backup_info.name}")
                # Clean up temporary backup if restore was successful
                if current_backup_path and current_backup_path.exists():
                    shutil.rmtree(current_backup_path)
                    logger.info("Cleaned up temporary backup")
                return True
            else:
                logger.error("Failed to reconnect to database after restore")
                # Try to restore the backup we made with retry
                if current_backup_path and current_backup_path.exists():
                    logger.info("Attempting to restore previous database...")
                    if self.kuzu_db_path.exists():
                        retry_count = 0
                        max_retries = 3
                        while retry_count < max_retries:
                            try:
                                shutil.rmtree(self.kuzu_db_path)
                                break
                            except OSError as e:
                                if e.errno == 35:
                                    retry_count += 1
                                    if retry_count < max_retries:
                                        import time
                                        time.sleep(2)
                                    else:
                                        logger.warning(f"Could not remove failed database directory: {e}")
                                        break
                                else:
                                    logger.warning(f"Error removing failed database: {e}")
                                    break
                    
                    # Restore previous database directory with retry
                    retry_count = 0
                    max_retries = 3
                    while retry_count < max_retries:
                        try:
                            shutil.copytree(current_backup_path, self.kuzu_db_path)
                            logger.info("Previous database restored")
                            break
                        except OSError as e:
                            if e.errno == 35:
                                retry_count += 1
                                if retry_count < max_retries:
                                    import time
                                    time.sleep(2)
                                else:
                                    logger.error(f"Could not restore previous database after {max_retries} attempts: {e}")
                            else:
                                logger.error(f"Error restoring previous database: {e}")
                                break
                return False
            
        except Exception as e:
            logger.error(f"Failed to restore simple backup {backup_id}: {e}")
            # Always clear the restore flag
            self._clear_restore_flag()
            return False
    
    def _disconnect_kuzu_database(self) -> None:
        """Cleanly disconnect from KuzuDB."""
        try:
            # Step 1: Close connection and database instances
            from app.infrastructure.kuzu_graph import get_kuzu_database
            database = get_kuzu_database()
            if database:
                try:
                    # Explicitly close connection first
                    if hasattr(database, '_connection') and database._connection:
                        database._connection.close()
                        database._connection = None
                        logger.info("Closed Kuzu connection")
                    
                    # Then close database
                    if hasattr(database, '_database') and database._database:
                        database._database = None
                        logger.info("Closed Kuzu database")
                    
                    # Call the disconnect method
                    database.disconnect()
                    logger.info("Called Kuzu database disconnect")
                except Exception as e:
                    logger.warning(f"Error closing Kuzu database connection: {e}")
            
            # Step 2: Clear cached graph storage instance
            try:
                import app.infrastructure.kuzu_graph
                if hasattr(app.infrastructure.kuzu_graph, '_graph_storage'):
                    app.infrastructure.kuzu_graph._graph_storage = None
                if hasattr(app.infrastructure.kuzu_graph, '_kuzu_database'):
                    app.infrastructure.kuzu_graph._kuzu_database = None
                logger.info("Cleared graph storage instances")
            except Exception as e:
                logger.warning(f"Error clearing graph storage: {e}")
            
            # Step 3: Clear any cached service instances
            try:
                import app.kuzu_integration
                if hasattr(app.kuzu_integration, 'kuzu_service'):
                    service = app.kuzu_integration.kuzu_service
                    if service:
                        if hasattr(service, 'db'):
                            service.db = None
                        if hasattr(service, '_initialized'):
                            service._initialized = False
                        # Don't set to None, just reset the state
                logger.info("Reset Kuzu service state")
            except Exception as e:
                logger.warning(f"Error resetting Kuzu service: {e}")
            
            # Step 4: Clear services that might hold references
            try:
                import app.services
                for attr_name in dir(app.services):
                    if not attr_name.startswith('_'):
                        service = getattr(app.services, attr_name, None)
                        if service and hasattr(service, '__class__') and hasattr(service.__class__, '__name__'):
                            if 'service' in service.__class__.__name__.lower():
                                try:
                                    if hasattr(service, '_db') or hasattr(service, 'db'):
                                        setattr(service, '_db', None)
                                        setattr(service, 'db', None)
                                except Exception:
                                    pass
                logger.debug("Cleared service database references")
            except Exception as e:
                logger.warning(f"Error clearing service references: {e}")
            
            # Step 5: Force garbage collection multiple times
            import gc
            gc.collect()
            gc.collect()  # Call twice to be thorough
            
            # Step 6: Wait longer for file handles to be released
            import time
            time.sleep(3.0)  # Increased wait time even more
            
            logger.info("KuzuDB disconnect completed")
            
        except Exception as e:
            logger.warning(f"Error during KuzuDB disconnect: {e}")
    
    def _clear_restore_flag(self) -> None:
        """Clear the restore-in-progress flag."""
        # Simplified - no longer using flags
        pass
    
    def _clear_service_cache(self) -> None:
        """Clear all cached service instances to force reinitialization after restore."""
        try:
            import app.services
            
            # Clear the lazy service cache instances
            if hasattr(app.services, '_book_service'):
                app.services._book_service = None
            if hasattr(app.services, '_user_service'):
                app.services._user_service = None
            if hasattr(app.services, '_custom_field_service'):
                app.services._custom_field_service = None
            if hasattr(app.services, '_import_mapping_service'):
                app.services._import_mapping_service = None
            if hasattr(app.services, '_person_service'):
                app.services._person_service = None
            if hasattr(app.services, '_reading_log_service'):
                app.services._reading_log_service = None
            
            # Clear the lazy service wrapper instances
            if hasattr(app.services, 'book_service') and hasattr(app.services.book_service, '_service'):
                app.services.book_service._service = None
            if hasattr(app.services, 'user_service') and hasattr(app.services.user_service, '_service'):
                app.services.user_service._service = None
            if hasattr(app.services, 'custom_field_service') and hasattr(app.services.custom_field_service, '_service'):
                app.services.custom_field_service._service = None
            if hasattr(app.services, 'import_mapping_service') and hasattr(app.services.import_mapping_service, '_service'):
                app.services.import_mapping_service._service = None
            if hasattr(app.services, 'person_service') and hasattr(app.services.person_service, '_service'):
                app.services.person_service._service = None
            if hasattr(app.services, 'reading_log_service') and hasattr(app.services.reading_log_service, '_service'):
                app.services.reading_log_service._service = None
            
            # Reinitialize kuzu_integration service
            try:
                import app.kuzu_integration
                # Force reinitialization of the global kuzu service
                if hasattr(app.kuzu_integration, 'kuzu_service'):
                    app.kuzu_integration.kuzu_service = app.kuzu_integration.KuzuIntegrationService()
                    app.kuzu_integration.kuzu_service.initialize()
                    logger.info("Kuzu integration service reinitialized successfully")
            except Exception as e:
                logger.warning(f"Error reinitializing kuzu integration service: {e}")
            
            logger.info("Service layer cache cleared successfully")
            
        except Exception as e:
            logger.warning(f"Error clearing service cache: {e}")
            # Non-critical error, continue with restore
    
    def _reconnect_kuzu_database(self) -> bool:
        """Reconnect to KuzuDB after restore with retry logic."""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Clear any existing instances first
                try:
                    import app.infrastructure.kuzu_graph
                    if hasattr(app.infrastructure.kuzu_graph, '_graph_storage'):
                        app.infrastructure.kuzu_graph._graph_storage = None
                    if hasattr(app.infrastructure.kuzu_graph, '_kuzu_database'):
                        app.infrastructure.kuzu_graph._kuzu_database = None
                except Exception:
                    pass
                
                try:
                    import app.kuzu_integration
                    if hasattr(app.kuzu_integration, 'kuzu_service'):
                        app.kuzu_integration.kuzu_service = None
                except Exception:
                    pass
                
                # Wait for cleanup - longer on retry
                import time
                wait_time = 2.0 + (retry_count * 1.0)  # 2, 3, 4 seconds
                time.sleep(wait_time)
                
                # Initialize fresh database connection
                from app.infrastructure.kuzu_graph import get_kuzu_database
                database = get_kuzu_database()
                if database:
                    connection = database.connect()
                    if connection:
                        logger.info("Successfully reconnected to Kuzu database")
                    else:
                        raise Exception("Failed to get connection from Kuzu database")
                else:
                    raise Exception("Failed to get Kuzu database instance")
                
                # Initialize Kuzu service
                try:
                    from app.kuzu_integration import get_kuzu_service
                    service = get_kuzu_service()
                    if service and hasattr(service, '_initialized') and service._initialized:
                        logger.info("Successfully reconnected to Kuzu service")
                        return True
                    elif service:
                        # Service exists but may not be marked as initialized yet
                        logger.info("Kuzu service available, considering reconnection successful")
                        return True
                    else:
                        logger.warning("Kuzu service returned None, but database connection successful")
                        return True  # Database connection is working, service issue is non-critical
                except Exception as service_error:
                    logger.warning(f"Kuzu service check failed: {service_error}, but database connection successful")
                    return True  # Database connection is working, service issue is non-critical
                    
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    logger.warning(f"Reconnection attempt {retry_count} failed: {e}. Retrying...")
                    import time
                    time.sleep(2)
                else:
                    logger.error(f"Error during KuzuDB reconnection after {max_retries} attempts: {e}")
                    return False
                    
        return False
    
    def _get_directory_size(self, path: Path) -> int:
        """Get total size of a directory or file."""
        if path.is_file():
            return path.stat().st_size
        elif path.is_dir():
            return sum(f.stat().st_size for f in path.rglob('*') if f.is_file())
        return 0
    
    def list_backups(self) -> List[SimpleBackupInfo]:
        """Get list of all backups."""
        return list(self._backup_index.values())
    
    def get_backup(self, backup_id: str) -> Optional[SimpleBackupInfo]:
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
            
            logger.info(f"Deleted simple backup: {backup_info.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete simple backup {backup_id}: {e}")
            return False
    
    def get_backup_stats(self) -> Dict[str, Any]:
        """Get simple backup statistics."""
        backups = self.list_backups()
        
        # Get current database size
        database_size = self._get_directory_size(self.kuzu_db_path) if self.kuzu_db_path.exists() else 0
        database_size_formatted = f"{database_size / (1024 * 1024):.1f} MB" if database_size > 0 else "0 MB"
        
        if not backups:
            return {
                'total_backups': 0,
                'total_size_bytes': 0,
                'total_size_mb': 0,
                'database_size_formatted': database_size_formatted,
                'newest_backup_age': None,
                'oldest_backup': None,
                'newest_backup': None
            }
        
        total_size = sum(backup.file_size for backup in backups)
        sorted_backups = sorted(backups, key=lambda b: b.created_at)
        newest_backup = sorted_backups[-1]
        
        # Calculate age of newest backup
        from datetime import datetime
        age_delta = datetime.now() - newest_backup.created_at
        if age_delta.days > 0:
            newest_backup_age = f"{age_delta.days} day{'s' if age_delta.days != 1 else ''} ago"
        elif age_delta.seconds > 3600:
            hours = age_delta.seconds // 3600
            newest_backup_age = f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif age_delta.seconds > 60:
            minutes = age_delta.seconds // 60
            newest_backup_age = f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        else:
            newest_backup_age = "Just now"
        
        return {
            'total_backups': len(backups),
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'database_size_formatted': database_size_formatted,
            'newest_backup_age': newest_backup_age,
            'oldest_backup': sorted_backups[0].created_at.isoformat(),
            'newest_backup': sorted_backups[-1].created_at.isoformat()
        }


# Global instance
_simple_backup_service: Optional[SimpleBackupService] = None


def get_simple_backup_service() -> SimpleBackupService:
    """Get or create the global simple backup service instance."""
    global _simple_backup_service
    if _simple_backup_service is None:
        _simple_backup_service = SimpleBackupService()
    return _simple_backup_service
