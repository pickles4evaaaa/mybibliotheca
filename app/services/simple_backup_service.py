"""
Simple Backup and Restore Service for Bibliotheca

A simplified approach that focuses on direct database backup/restore
with minimal complexity and maximum reliability.
"""

import os
import contextlib
import shutil
import zipfile
import tempfile
import logging
import json
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import uuid

from flask import current_app
from app.utils.safe_kuzu_manager import SafeKuzuManager, get_safe_kuzu_manager

# Helper function for query result conversion
def _convert_query_result_to_list(result) -> list:
    """Convert KuzuDB query result to list of dictionaries."""
    if not result:
        return []
    
    data = []
    while result.has_next():
        row = result.get_next()
        record = {}
        for i in range(len(row)):
            column_name = result.get_column_names()[i]
            record[column_name] = row[i]
        data.append(record)
    
    return data

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
        self.backup_settings_file = self.data_dir / "backup_settings.json"
        
        # KuzuDB database path - points to the entire kuzu directory
        self.kuzu_db_path = self.data_dir / "kuzu"
        
        # Static files paths - now stored in data directory
        self.app_static_dir = self.base_dir / "app" / "static"
        self.covers_dir = self.data_dir / "covers"
        self.uploads_dir = self.data_dir / "uploads"
        self.env_file = self.base_dir / ".env"
        self.ai_config_file = self.data_dir / "ai_config.json"
        
        # Ensure backup directory exists
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Load existing backup index
        self._backup_index: Dict[str, SimpleBackupInfo] = self._load_backup_index()

        # Scheduler internals
        self._scheduler_thread: Optional[threading.Thread] = None
        self._scheduler_stop = threading.Event()
        self._last_scheduled_backup: Optional[datetime] = None
        # Initialize settings (creates defaults if missing)
        self._settings = self._load_or_create_settings()
        # Auto-start scheduler if enabled
        if self._settings.get('enabled', True):
            self.ensure_scheduler()
        # Prevent concurrent create_backup overlap
        self._create_lock = threading.Lock()

    # -------------------------- Settings & Scheduling -----------------------
    def _load_or_create_settings(self) -> Dict[str, Any]:
        defaults = {
            'enabled': True,
            'frequency': 'daily',  # future: weekly
            'retention_days': 14,
            'last_run': None,
            'scheduled_hour': 2,   # 02:30 local
            'scheduled_minute': 30
        }
        try:
            if self.backup_settings_file.exists():
                with open(self.backup_settings_file, 'r') as f:
                    data = json.load(f)
                # Merge defaults
                merged = {**defaults, **data}
            else:
                merged = defaults
                with open(self.backup_settings_file, 'w') as f:
                    json.dump(merged, f, indent=2)
            return merged
        except Exception as e:
            logger.error(f"Failed loading backup settings, using defaults: {e}")
            return defaults

    def _save_settings(self) -> bool:
        """Save settings to disk. Returns True if successful, False otherwise."""
        try:
            # Ensure parent directory exists
            self.backup_settings_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to a temporary file first
            temp_file = self.backup_settings_file.with_suffix('.json.tmp')
            with open(temp_file, 'w') as f:
                json.dump(self._settings, f, indent=2)
            
            # Verify the file was written correctly by reading it back and comparing
            with open(temp_file, 'r') as f:
                verified = json.load(f)
            
            # Validate that all critical settings were saved correctly
            for key in ['enabled', 'frequency', 'retention_days', 'scheduled_hour', 'scheduled_minute']:
                if key in self._settings and verified.get(key) != self._settings[key]:
                    raise ValueError(f"Setting '{key}' verification failed: expected {self._settings[key]}, got {verified.get(key)}")
            
            # If verification succeeds, rename temp file to actual file
            temp_file.replace(self.backup_settings_file)
            logger.info(f"Backup settings saved and verified successfully: {self._settings}")
            return True
        except Exception as e:
            logger.error(f"Failed saving backup settings: {e}", exc_info=True)
            # Clean up temp file if it exists (using the same path calculation as above)
            try:
                cleanup_temp = self.backup_settings_file.with_suffix('.json.tmp')
                if cleanup_temp.exists():
                    cleanup_temp.unlink()
            except Exception:
                pass
            return False

    def ensure_scheduler(self):
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            return
        def _loop():
            logger.info("Backup scheduler thread started")
            while not self._scheduler_stop.is_set():
                try:
                    self._maybe_run_scheduled_backup()
                except Exception as e:
                    logger.warning(f"Scheduled backup check failed: {e}")
                # Sleep 5 minutes between checks to keep lightweight
                self._scheduler_stop.wait(300)
            logger.info("Backup scheduler thread exiting")
        self._scheduler_thread = threading.Thread(target=_loop, daemon=True)
        self._scheduler_thread.start()

    def _maybe_run_scheduled_backup(self):
        if not self._settings.get('enabled', True):
            return
        freq = self._settings.get('frequency', 'daily')
        now = datetime.now()
        last_run_iso = self._settings.get('last_run')
        last_run = datetime.fromisoformat(last_run_iso) if last_run_iso else None
        # Determine if due
        due = False
        if freq == 'daily':
            if not last_run or (now.date() > last_run.date() and now.hour >= self._settings.get('scheduled_hour', 2) and now.minute >= self._settings.get('scheduled_minute', 30)):
                due = True
        # Future expansion: weekly
        if due:
            logger.info("Running scheduled daily backup")
            self.create_backup(description='Scheduled daily backup', reason='scheduled_daily')
            self._settings['last_run'] = datetime.now().isoformat()
            if not self._save_settings():
                logger.error("Failed to save last_run timestamp after scheduled backup")

    def stop_scheduler(self):
        if self._scheduler_thread:
            self._scheduler_stop.set()

    # -----------------------------------------------------------------------
    
    def _load_backup_index(self) -> Dict[str, SimpleBackupInfo]:
        """Load the backup index from disk."""
        if not self.backup_index_file.exists():
            return {}
        
        try:
            with open(self.backup_index_file, 'r') as f:
                content = f.read().strip()
                
            # Try to handle corrupted JSON by finding the first complete JSON object
            try:
                data = json.loads(content)
            except json.JSONDecodeError as parse_error:
                logger.warning(f"Backup index JSON corrupted at position {parse_error.pos}, attempting recovery...")
                
                # Try to extract valid JSON by finding the last complete closing brace
                if content.startswith('{'):
                    brace_count = 0
                    last_valid_pos = 0
                    for i, char in enumerate(content):
                        if char == '{':
                            brace_count += 1
                        elif char == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                last_valid_pos = i + 1
                                break
                    
                    if last_valid_pos > 0:
                        try:
                            data = json.loads(content[:last_valid_pos])
                            logger.info(f"Successfully recovered backup index by truncating at position {last_valid_pos}")
                            # Save the corrected version
                            self._backup_index = {}
                            for backup_id, backup_data in data.items():
                                try:
                                    self._backup_index[backup_id] = SimpleBackupInfo.from_dict(backup_data)
                                except Exception as e:
                                    logger.warning(f"Failed to load backup info for {backup_id}: {e}")
                            self._save_backup_index()
                            return self._backup_index
                        except json.JSONDecodeError:
                            pass
                
                # If recovery fails, backup the corrupted file and start fresh
                corrupted_backup = self.backup_index_file.with_suffix('.json.corrupted')
                self.backup_index_file.rename(corrupted_backup)
                logger.warning(f"Backup index was corrupted, moved to {corrupted_backup} and starting fresh")
                return {}
            
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
    
    def create_backup(self, name: Optional[str] = None, description: str = "", reason: str = 'manual') -> Optional[SimpleBackupInfo]:
        """
        Create a simple backup of the KuzuDB database.
        
        Args:
            name: Optional custom name for the backup
            description: Optional description
            
        Returns:
            BackupInfo if successful, None otherwise
        """
        # Fast-fail if another thread/process in same runtime is already executing
        if not getattr(self, '_create_lock', None):  # safety init
            self._create_lock = threading.Lock()
        if not self._create_lock.acquire(blocking=False):
            logger.warning("Backup already in progress; skipping concurrent create_backup request")
            return None
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
                'covers_path': str(self.covers_dir),
                'uploads_path': str(self.uploads_dir),
                'covers_size': self._get_directory_size(self.covers_dir) if self.covers_dir.exists() else 0,
                'uploads_size': self._get_directory_size(self.uploads_dir) if self.uploads_dir.exists() else 0,
                'backup_type': 'simple_database_backup_with_images_and_settings',
                'reason': reason
            }
            
            # Optionally quiesce writes for consistent snapshot
            quiesce_enabled = os.getenv('KUZU_BACKUP_QUIESCE', 'false').lower() in ('1','true','yes')
            manager = None
            if quiesce_enabled:
                try:
                    from app.utils.safe_kuzu_manager import get_safe_kuzu_manager as _gskm
                    manager = _gskm()
                except Exception:
                    manager = None

            quiesce_ctx = manager.quiesce_for_backup(reason='simple_backup') if manager and quiesce_enabled else contextlib.nullcontext()
            with quiesce_ctx:
                with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    # Add metadata file
                    zipf.writestr('backup_metadata.json', json.dumps(metadata, indent=2))
                    
                    # Add the entire KuzuDB directory (it should always be a directory)
                    db_files_count = 0
                    for file_path in self.kuzu_db_path.rglob('*'):
                        if file_path.is_file():
                            relative_path = file_path.relative_to(self.kuzu_db_path)
                            zipf.write(file_path, f"kuzu/{relative_path}")
                            db_files_count += 1
                            logger.debug(f"Added to backup: kuzu/{relative_path}")
                    
                    logger.info(f"Backed up KuzuDB directory with {db_files_count} files")
                    
                    # Add cover images
                    covers_count = 0
                    if self.covers_dir.exists():
                        for file_path in self.covers_dir.rglob('*'):
                            if file_path.is_file():
                                relative_path = file_path.relative_to(self.covers_dir)
                                zipf.write(file_path, f"data/covers/{relative_path}")
                                covers_count += 1
                                logger.debug(f"Added cover to backup: data/covers/{relative_path}")
                        
                        logger.info(f"Backed up {covers_count} cover images")
                    
                    # Add upload files if they exist
                    uploads_count = 0
                    if self.uploads_dir.exists():
                        for file_path in self.uploads_dir.rglob('*'):
                            if file_path.is_file():
                                relp = file_path.relative_to(self.uploads_dir)
                                zipf.write(file_path, f"data/uploads/{relp}")
                                uploads_count += 1
                                logger.debug(f"Added upload to backup: data/uploads/{relp}")
                    
                    logger.info(f"Backed up {uploads_count} uploaded files")

                    # Add settings/config files (non-secret) while zip is still open
                    settings_added = 0
                    try:
                        if self.env_file.exists():
                            zipf.write(self.env_file, "config/.env")
                            settings_added += 1
                        if self.ai_config_file.exists():
                            zipf.write(self.ai_config_file, "config/ai_config.json")
                            settings_added += 1
                        if self.backup_settings_file.exists():
                            zipf.write(self.backup_settings_file, "config/backup_settings.json")
                            settings_added += 1
                        logger.info(f"Included {settings_added} config/settings files in backup")
                    except Exception as se:
                        logger.warning(f"Failed adding settings files to backup: {se}")
            
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
            # Apply retention pruning
            try:
                self._apply_retention_policy()
            except Exception as rp_err:
                logger.warning(f"Retention pruning failed: {rp_err}")

            return backup_info
            
        except Exception as e:
            logger.error(f"Failed to create simple backup: {e}")
            return None
        finally:
            try:
                self._create_lock.release()
            except Exception:
                pass
    
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
            # Mark restore in progress
            self._set_restore_flag(backup_info)
            
            # Step 1: Disconnect KuzuDB cleanly
            logger.info("Disconnecting KuzuDB connections...")
            self._disconnect_kuzu_database()
            
            # Step 2: Make a pre-restore safety backup by moving aside and zipping (expanded to include covers/uploads/settings)
            current_backup_path = None
            moved_old_dir = None
            if self.kuzu_db_path.exists():
                try:
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    short_id = str(uuid.uuid4())[:8]
                    moved_old_dir = self.kuzu_db_path.parent / f"kuzu.pre_restore_{timestamp}_{short_id}"
                    logger.info(f"Moving current Kuzu directory to {moved_old_dir} for safety snapshot...")
                    shutil.move(str(self.kuzu_db_path), str(moved_old_dir))
                    logger.info("Current Kuzu directory moved aside successfully")

                    # Create a zip snapshot in backups including covers/uploads/settings
                    pre_name = f"pre_restore_{timestamp}"
                    pre_backup_filename = f"{pre_name}_{short_id}.zip"
                    pre_backup_path = self.backup_dir / pre_backup_filename
                    metadata = {
                        'backup_id': str(uuid.uuid4()),
                        'created_at': datetime.now().isoformat(),
                        'kuzu_db_path': str(moved_old_dir),
                        'original_size': self._get_directory_size(moved_old_dir),
                        'backup_type': 'pre_restore_snapshot_full',
                        'source_backup_restored': backup_info.name,
                    }
                    logger.info(f"Creating pre-restore backup zip at {pre_backup_path} ...")
                    with zipfile.ZipFile(pre_backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                        zipf.writestr('backup_metadata.json', json.dumps(metadata, indent=2))
                        files_count = 0
                        for file_path in moved_old_dir.rglob('*'):
                            if file_path.is_file():
                                relative_path = file_path.relative_to(moved_old_dir)
                                zipf.write(file_path, f"kuzu/{relative_path}")
                                files_count += 1
                        # Add covers
                        if self.covers_dir.exists():
                            for file_path in self.covers_dir.rglob('*'):
                                if file_path.is_file():
                                    rel = file_path.relative_to(self.covers_dir)
                                    zipf.write(file_path, f"data/covers/{rel}")
                        # Add uploads
                        if self.uploads_dir.exists():
                            for file_path in self.uploads_dir.rglob('*'):
                                if file_path.is_file():
                                    rel = file_path.relative_to(self.uploads_dir)
                                    zipf.write(file_path, f"data/uploads/{rel}")
                        # Add settings
                        try:
                            if self.env_file.exists():
                                zipf.write(self.env_file, f"config/.env")
                            if self.ai_config_file.exists():
                                zipf.write(self.ai_config_file, f"config/ai_config.json")
                            if self.backup_settings_file.exists():
                                zipf.write(self.backup_settings_file, f"config/backup_settings.json")
                        except Exception as se:
                            logger.warning(f"Failed adding settings to pre-restore snapshot: {se}")
                        logger.info(f"Pre-restore snapshot archived: {files_count} files")

                    # Record in index for visibility
                    backup_info_obj = SimpleBackupInfo(
                        id=metadata['backup_id'],
                        name=pre_name,
                        created_at=datetime.now(),
                        file_path=str(pre_backup_path),
                        file_size=pre_backup_path.stat().st_size,
                        description=f"Auto-created before restoring '{backup_info.name}'",
                        metadata=metadata,
                    )
                    self._backup_index[backup_info_obj.id] = backup_info_obj
                    self._save_backup_index()
                except Exception as e:
                    logger.warning(f"Failed to create pre-restore snapshot: {e}. Proceeding cautiously.")
            
            # Step 3: Ensure fresh target directory exists (it was moved away above)
            self.kuzu_db_path.mkdir(parents=True, exist_ok=True)
            
            # Step 4: Extract backup to restore database and static files
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Extract backup
                with zipfile.ZipFile(backup_path, 'r') as zipf:
                    zipf.extractall(temp_path)
                
                # Load backup metadata to determine backup structure
                metadata_path = temp_path / 'backup_metadata.json'
                backup_metadata = {}
                if metadata_path.exists():
                    with open(metadata_path, 'r') as f:
                        backup_metadata = json.load(f)
                
                backup_type = backup_metadata.get('backup_type', 'simple_database_backup')
                logger.info(f"Restoring backup type: {backup_type}")
                
                # Restore KuzuDB directory
                self.kuzu_db_path.parent.mkdir(parents=True, exist_ok=True)
                if not self.kuzu_db_path.exists():
                    self.kuzu_db_path.mkdir(parents=True)
                    logger.info(f"Created KuzuDB directory: {self.kuzu_db_path}")
                
                # Handle different backup structures
                kuzu_source_path = temp_path / "kuzu"
                files_restored = 0
                
                if kuzu_source_path.exists():
                    # New backup format with kuzu/ subdirectory
                    for file_path in kuzu_source_path.rglob('*'):
                        if file_path.is_file():
                            relative_path = file_path.relative_to(kuzu_source_path)
                            target_path = self.kuzu_db_path / relative_path
                            target_path.parent.mkdir(parents=True, exist_ok=True)
                            
                            self._copy_file_with_retry(file_path, target_path)
                            files_restored += 1
                            logger.debug(f"Restored: {relative_path}")
                else:
                    # Legacy backup format - files in root (excluding metadata)
                    for file_path in temp_path.glob('*'):
                        if file_path.is_file() and file_path.name != 'backup_metadata.json':
                            target_path = self.kuzu_db_path / file_path.name
                            self._copy_file_with_retry(file_path, target_path)
                            files_restored += 1
                            logger.debug(f"Restored: {file_path.name}")
                
                logger.info(f"Restored KuzuDB directory with {files_restored} files")
                
                # Restore static files if present (legacy backups)
                static_source_path = temp_path / "static"
                if static_source_path.exists():
                    # Restore cover images from static/covers
                    covers_source = static_source_path / "covers"
                    if covers_source.exists():
                        covers_restored = 0
                        self.covers_dir.mkdir(parents=True, exist_ok=True)
                        
                        for file_path in covers_source.rglob('*'):
                            if file_path.is_file():
                                relative_path = file_path.relative_to(covers_source)
                                target_path = self.covers_dir / relative_path
                                target_path.parent.mkdir(parents=True, exist_ok=True)
                                
                                self._copy_file_with_retry(file_path, target_path)
                                covers_restored += 1
                                logger.debug(f"Restored cover: {relative_path}")
                        
                        logger.info(f"Restored {covers_restored} cover images from legacy backup")
                    
                    # Restore upload files from static/uploads
                    uploads_source = static_source_path / "uploads"
                    if uploads_source.exists():
                        uploads_restored = 0
                        self.uploads_dir.mkdir(parents=True, exist_ok=True)
                        
                        for file_path in uploads_source.rglob('*'):
                            if file_path.is_file():
                                relative_path = file_path.relative_to(uploads_source)
                                target_path = self.uploads_dir / relative_path
                                target_path.parent.mkdir(parents=True, exist_ok=True)
                                
                                self._copy_file_with_retry(file_path, target_path)
                                uploads_restored += 1
                                logger.debug(f"Restored upload: {relative_path}")
                        
                        logger.info(f"Restored {uploads_restored} uploaded files from legacy backup")
                
                # Restore data files if present (new format)
                data_source_path = temp_path / "data"
                if data_source_path.exists():
                    # Restore cover images from data/covers
                    covers_source = data_source_path / "covers"
                    if covers_source.exists():
                        covers_restored = 0
                        self.covers_dir.mkdir(parents=True, exist_ok=True)
                        
                        for file_path in covers_source.rglob('*'):
                            if file_path.is_file():
                                relative_path = file_path.relative_to(covers_source)
                                target_path = self.covers_dir / relative_path
                                target_path.parent.mkdir(parents=True, exist_ok=True)
                                
                                self._copy_file_with_retry(file_path, target_path)
                                covers_restored += 1
                                logger.debug(f"Restored cover: {relative_path}")
                        
                        logger.info(f"Restored {covers_restored} cover images")
                    
                    # Restore upload files from data/uploads
                    uploads_source = data_source_path / "uploads"
                    if uploads_source.exists():
                        uploads_restored = 0
                        self.uploads_dir.mkdir(parents=True, exist_ok=True)
                        
                        for file_path in uploads_source.rglob('*'):
                            if file_path.is_file():
                                relative_path = file_path.relative_to(uploads_source)
                                target_path = self.uploads_dir / relative_path
                                target_path.parent.mkdir(parents=True, exist_ok=True)
                                
                                self._copy_file_with_retry(file_path, target_path)
                                uploads_restored += 1
                                logger.debug(f"Restored upload: {relative_path}")
                        
                        logger.info(f"Restored {uploads_restored} uploaded files")
            
            # Step 5: Cleanup moved old dir after successful restore
            try:
                if moved_old_dir and moved_old_dir.exists():
                    shutil.rmtree(moved_old_dir)
                    logger.info(f"Removed pre-restore directory: {moved_old_dir}")
            except Exception as e:
                logger.warning(f"Failed to remove pre-restore directory {moved_old_dir}: {e}")

            # Step 6: Set restart flag instead of immediate reconnection
            logger.info("Setting restart flag for clean reconnection...")
            self._set_restart_required_flag()
            
            # Clear the restore flag
            self._clear_restore_flag()
            
            logger.info(f"Simple restore completed successfully from backup: {backup_info.name}")
            logger.info("Application restart required for clean database reconnection")
            
            # Clean up temporary backup if restore was successful
            if current_backup_path and current_backup_path.exists():
                shutil.rmtree(current_backup_path)
                logger.info("Cleaned up temporary backup")
            
            return True
        except Exception as e:
            logger.error(f"Failed to restore simple backup {backup_id}: {e}")
            # Attempt rollback if possible
            try:
                # Find any moved pre-restore directory to roll back
                parent = self.kuzu_db_path.parent
                candidates = sorted(parent.glob('kuzu.pre_restore_*'), key=lambda p: p.stat().st_mtime, reverse=True)
                moved_old_dir = candidates[0] if candidates else None
                if moved_old_dir and moved_old_dir.exists():
                    logger.warning(f"Attempting rollback from {moved_old_dir} ...")
                    # Remove partially restored target dir
                    if self.kuzu_db_path.exists():
                        try:
                            shutil.rmtree(self.kuzu_db_path)
                        except Exception:
                            pass
                    shutil.move(str(moved_old_dir), str(self.kuzu_db_path))
                    logger.info("Rollback completed; original database restored")
            except Exception as rb_err:
                logger.error(f"Rollback failed: {rb_err}")
            finally:
                # Always clear the restore flag
                self._clear_restore_flag()
            return False
    
    def _disconnect_kuzu_database(self) -> None:
        """Cleanly disconnect from KuzuDB."""
        try:
            # Step 1: Close connection and database instances
            # For backup operations, we need to ensure complete disconnection
            # Use the safe manager's health status to check connections
            safe_manager = get_safe_kuzu_manager()
            health_status = safe_manager.get_health_status()
            
            if health_status.get('database_initialized', False):
                try:
                    # Force cleanup of any active connections
                    cleaned_connections = safe_manager.cleanup_stale_connections(max_age_minutes=0)
                    logger.info(f"Cleaned up {cleaned_connections} stale connections")
                    
                    # Force reset if needed for backup operations
                    safe_manager.force_reset()
                    logger.info("Forced SafeKuzuManager reset for backup operation")
                except Exception as e:
                    logger.warning(f"Error during SafeKuzuManager cleanup: {e}")
            
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
        try:
            flag_path = self.backup_dir / '.restore_in_progress'
            if flag_path.exists():
                flag_path.unlink()
                logger.info("Restore-in-progress flag cleared")
        except Exception as e:
            logger.warning(f"Failed to clear restore flag: {e}")

    def _set_restore_flag(self, backup_info: SimpleBackupInfo) -> None:
        """Set a flag indicating a restore is in progress."""
        try:
            flag_path = self.backup_dir / '.restore_in_progress'
            details = {
                'started_at': datetime.now().isoformat(),
                'backup_id': backup_info.id,
                'backup_name': backup_info.name,
            }
            flag_path.write_text(json.dumps(details))
            logger.info(f"Restore-in-progress flag set at: {flag_path}")
        except Exception as e:
            logger.warning(f"Failed to set restore flag: {e}")
    
    def _set_restart_required_flag(self) -> None:
        """Set a flag indicating that application restart is required."""
        try:
            restart_flag_path = self.backup_dir / '.restart_required'
            restart_flag_path.write_text(str(datetime.now().isoformat()))
            logger.info(f"Restart flag set at: {restart_flag_path}")
        except Exception as e:
            logger.warning(f"Failed to set restart flag: {e}")
    
    def check_restart_required(self) -> bool:
        """Check if application restart is required."""
        restart_flag_path = self.backup_dir / '.restart_required'
        return restart_flag_path.exists()
    
    def clear_restart_flag(self) -> None:
        """Clear the restart required flag."""
        try:
            restart_flag_path = self.backup_dir / '.restart_required'
            if restart_flag_path.exists():
                restart_flag_path.unlink()
                logger.info("Restart flag cleared")
        except Exception as e:
            logger.warning(f"Failed to clear restart flag: {e}")
    
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
                
                # Initialize fresh database connection using SafeKuzuManager
                safe_manager = get_safe_kuzu_manager()
                
                # Test the connection by executing a simple query
                try:
                    test_query = "MATCH (n) RETURN count(n) as node_count LIMIT 1"
                    test_result = safe_manager.execute_query(test_query)
                    logger.info("Successfully reconnected to Kuzu database via SafeKuzuManager")
                except Exception as e:
                    raise Exception(f"Failed to test Kuzu database connection: {e}")
                
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
    
    def _copy_file_with_retry(self, source: Path, target: Path, max_retries: int = 3) -> None:
        """Copy a file with retry logic for handling resource locks."""
        retry_count = 0
        while retry_count < max_retries:
            try:
                shutil.copy2(source, target)
                return
            except OSError as e:
                if e.errno == 35:  # Resource deadlock avoided
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"File copy failed for {source.name} (attempt {retry_count}), retrying in 2 seconds...")
                        import time
                        time.sleep(2)
                    else:
                        logger.error(f"Failed to copy {source.name} after {max_retries} attempts: {e}")
                        raise
                else:
                    logger.error(f"Error copying {source.name}: {e}")
                    raise
    
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
        
        # Get current static files size
        covers_size = self._get_directory_size(self.covers_dir) if self.covers_dir.exists() else 0
        uploads_size = self._get_directory_size(self.uploads_dir) if self.uploads_dir.exists() else 0
        covers_count = len(list(self.covers_dir.rglob('*'))) if self.covers_dir.exists() else 0
        uploads_count = len(list(self.uploads_dir.rglob('*'))) if self.uploads_dir.exists() else 0
        
        covers_size_formatted = f"{covers_size / (1024 * 1024):.1f} MB" if covers_size > 0 else "0 MB"
        uploads_size_formatted = f"{uploads_size / (1024 * 1024):.1f} MB" if uploads_size > 0 else "0 MB"
        
        if not backups:
            return {
                'total_backups': 0,
                'total_size_bytes': 0,
                'total_size_mb': 0,
                'database_size_formatted': database_size_formatted,
                'covers_size_formatted': covers_size_formatted,
                'uploads_size_formatted': uploads_size_formatted,
                'covers_count': covers_count,
                'uploads_count': uploads_count,
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
            'covers_size_formatted': covers_size_formatted,
            'uploads_size_formatted': uploads_size_formatted,
            'covers_count': covers_count,
            'uploads_count': uploads_count,
            'newest_backup_age': newest_backup_age,
            'oldest_backup': sorted_backups[0].created_at.isoformat(),
            'newest_backup': sorted_backups[-1].created_at.isoformat()
        }

    # -------------------------- Retention Policy ---------------------------
    def _apply_retention_policy(self):
        """Delete backups older than retention_days (except protected types)."""
        retention_days = int(self._settings.get('retention_days', 14))
        if retention_days <= 0:
            return
        cutoff = datetime.now() - timedelta(days=retention_days)
        to_delete = []
        for b in self._backup_index.values():
            # Keep pre-restore snapshots for minimum retention as well; allow deletion like others
            if b.created_at < cutoff:
                # Allow manual protection via metadata flag protected=True
                if b.metadata and b.metadata.get('protected'):
                    continue
                to_delete.append(b.id)
        if not to_delete:
            return
        logger.info(f"Retention policy: deleting {len(to_delete)} old backups (>{retention_days} days)")
        for bid in to_delete:
            try:
                self.delete_backup(bid)
            except Exception as e:
                logger.warning(f"Failed deleting old backup {bid}: {e}")


# Global instance
_simple_backup_service: Optional[SimpleBackupService] = None


def get_simple_backup_service() -> SimpleBackupService:
    """Get or create the global simple backup service instance."""
    global _simple_backup_service
    if _simple_backup_service is None:
        # Determine the base directory from Flask app config or fall back to file-relative path
        try:
            # current_app is a Flask proxy that requires an active application context
            data_dir = current_app.config.get('DATA_DIR')
            if data_dir:
                # DATA_DIR points to <project_root>/data, so we need .parent to get <project_root>
                base_dir = Path(data_dir).parent
            else:
                # DATA_DIR not configured, use file-relative fallback
                # This file is at <project_root>/app/services/simple_backup_service.py
                # parents[2] gives us: parents[0]=services, parents[1]=app, parents[2]=project_root
                base_dir = Path(__file__).resolve().parents[2]
        except RuntimeError:
            # Flask context not available, use same file-relative path as above
            base_dir = Path(__file__).resolve().parents[2]
        # Convert Path to str to match the type signature of SimpleBackupService.__init__
        _simple_backup_service = SimpleBackupService(base_dir=str(base_dir))
    return _simple_backup_service
