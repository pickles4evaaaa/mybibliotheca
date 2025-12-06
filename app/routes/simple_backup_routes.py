"""
Simple Backup routes for Bibliotheca admin interface.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file, current_app
from flask_login import login_required, current_user
from datetime import datetime
import os
from typing import Optional

from app import csrf
from app.services.simple_backup_service import get_simple_backup_service
from app.admin import admin_required

# Create simple backup blueprint
simple_backup_bp = Blueprint('simple_backup', __name__, url_prefix='/auth/simple-backup')

# Constants for backup settings validation
VALID_BACKUP_FREQUENCIES = ['daily', 'weekly']


@simple_backup_bp.route('/')
@login_required
@admin_required
def index():
    """Main simple backup management page."""
    try:
        backup_service = get_simple_backup_service()
        backups = backup_service.list_backups()
        stats = backup_service.get_backup_stats()
        
        # Sort backups by creation date (newest first)
        backups.sort(key=lambda b: b.created_at, reverse=True)
        
        # Enhance backup objects with display properties
        from datetime import datetime
        from pathlib import Path
        
        enhanced_backups = []
        for backup in backups:
            # Calculate age
            age_delta = datetime.now() - backup.created_at
            if age_delta.days > 0:
                age = f"{age_delta.days} day{'s' if age_delta.days != 1 else ''} ago"
            elif age_delta.seconds > 3600:
                hours = age_delta.seconds // 3600
                age = f"{hours} hour{'s' if hours != 1 else ''} ago"
            elif age_delta.seconds > 60:
                minutes = age_delta.seconds // 60
                age = f"{minutes} minute{'s' if minutes != 1 else ''} ago"
            else:
                age = "Just now"
            
            # Format file size
            size_mb = backup.file_size / (1024 * 1024)
            size_formatted = f"{size_mb:.1f} MB"
            
            # Get database size from metadata
            if backup.metadata and 'original_size' in backup.metadata:
                db_size = backup.metadata['original_size']
                database_size_formatted = f"{db_size / (1024 * 1024):.1f} MB"
            else:
                database_size_formatted = "Unknown"
            
            # Check if backup file still exists
            valid = Path(backup.file_path).exists()
            
            # Create enhanced backup object
            enhanced_backup = {
                'id': backup.id,
                'name': backup.name,
                'description': backup.description,
                'created_at': backup.created_at,
                'file_path': backup.file_path,
                'file_size': backup.file_size,
                'age': age,
                'size_formatted': size_formatted,
                'database_size_formatted': database_size_formatted,
                'valid': valid
            }
            enhanced_backups.append(enhanced_backup)
        
        # Load current backup settings if available
        backup_settings = {}
        try:
            svc = backup_service
            if hasattr(svc, '_settings'):
                # expose a safe copy
                backup_settings = {
                    'enabled': svc._settings.get('enabled', True),
                    'frequency': svc._settings.get('frequency', 'daily'),
                    'retention_days': svc._settings.get('retention_days', 14),
                    'last_run': svc._settings.get('last_run'),
                    'scheduled_hour': svc._settings.get('scheduled_hour', 2),
                    'scheduled_minute': svc._settings.get('scheduled_minute', 30)
                }
        except Exception as se:
            current_app.logger.warning(f"Failed loading backup settings for UI: {se}")

        return render_template(
            'admin/simple_backup.html',
            backups=enhanced_backups,
            backup_stats=stats,
            backup_settings=backup_settings
        )
    except Exception as e:
        current_app.logger.error(f"Error loading simple backup page: {e}")
    flash('Error loading backup page.', 'danger')
    return redirect(url_for('auth.settings', section='data', panel='backup'))


@simple_backup_bp.route('/create', methods=['POST'])
@login_required
@admin_required
def create_backup():
    """Create a new simple backup."""
    try:
        backup_service = get_simple_backup_service()
        
        # Get form data
        custom_name = request.form.get('custom_name', '').strip()
        description = request.form.get('description', '').strip()
        
        # Create the backup
        backup_info = backup_service.create_backup(
            name=custom_name if custom_name else None,
            description=description
        )
        
        if backup_info:
            flash(f'✅ Backup "{backup_info.name}" created successfully! ({backup_info.file_size / 1024 / 1024:.2f} MB)', 'success')
        else:
            flash('❌ Failed to create backup.', 'danger')
            
    except Exception as e:
        current_app.logger.error(f"Error creating backup: {e}")
        flash('❌ Error creating backup.', 'danger')
    
    return redirect(url_for('simple_backup.index'))


@simple_backup_bp.route('/restore/<backup_id>', methods=['POST'])
@login_required
@admin_required
def restore_backup(backup_id: str):
    """Restore from a simple backup."""
    try:
        backup_service = get_simple_backup_service()
        
        # Get backup info
        backup_info = backup_service.get_backup(backup_id)
        if not backup_info:
            flash('❌ Backup not found.', 'danger')
            return redirect(url_for('simple_backup.index'))
        
        # Confirm restoration
        confirm = request.form.get('confirm', '').lower()
        if confirm != 'yes':
            flash('⚠️ Restoration cancelled. Please confirm by typing "yes".', 'warning')
            return redirect(url_for('simple_backup.index'))
        
        # Perform restoration
        current_app.logger.info(f"Starting restore from backup: {backup_info.name}")
        success = backup_service.restore_backup(backup_id)
        
        if success:
            # Check if restart is required
            if backup_service.check_restart_required():
                flash(f'✅ Successfully restored from backup "{backup_info.name}"! You will be logged out while the application restarts to complete the restore process.', 'success')
                # Trigger container restart and redirect to a completion page
                return redirect(url_for('simple_backup.restore_complete'))
            else:
                flash(f'✅ Successfully restored from backup "{backup_info.name}"! Your data has been restored.', 'success')
                return redirect(url_for('main.library'))
        else:
            flash('❌ Failed to restore from backup. Check logs for details.', 'danger')
            
    except Exception as e:
        current_app.logger.error(f"Error restoring backup {backup_id}: {e}")
        flash('❌ Error restoring backup.', 'danger')
    
    return redirect(url_for('simple_backup.index'))


@simple_backup_bp.route('/restore-complete')
def restore_complete():
    """Show restore completion page and trigger logout/restart."""
    import os
    import threading
    import time
    
    def delayed_shutdown():
        time.sleep(3)  # Give time for response to be sent
        # Use logging instead of current_app.logger to avoid context issues
        import logging
        logger = logging.getLogger(__name__)
        logger.info("Initiating graceful shutdown for container restart after backup restore")
        os._exit(0)  # Force exit to trigger container restart
    
    # Start shutdown in background thread
    thread = threading.Thread(target=delayed_shutdown)
    thread.daemon = True
    thread.start()
    
    return render_template('simple_backup/restore_complete.html')


@simple_backup_bp.route('/check-restart-status')
def check_restart_status():
    """Check if restart is complete and redirect to library."""
    try:
        backup_service = get_simple_backup_service()
        if not backup_service.check_restart_required():
            # Restart flag cleared, redirect to library
            return jsonify({'status': 'complete', 'redirect': url_for('main.library')})
        else:
            # Still waiting for restart
            return jsonify({'status': 'waiting'})
    except Exception as e:
        # If we can check the service, restart is probably complete
        return jsonify({'status': 'complete', 'redirect': url_for('main.library')})


@simple_backup_bp.route('/delete/<backup_id>', methods=['POST'])
@login_required
@admin_required
def delete_backup(backup_id: str):
    """Delete a simple backup."""
    try:
        backup_service = get_simple_backup_service()
        
        backup_info = backup_service.get_backup(backup_id)
        if not backup_info:
            flash('❌ Backup not found.', 'danger')
            return redirect(url_for('simple_backup.index'))
        
        success = backup_service.delete_backup(backup_id)
        
        if success:
            flash(f'✅ Backup "{backup_info.name}" deleted successfully.', 'success')
        else:
            flash('❌ Failed to delete backup.', 'danger')
            
    except Exception as e:
        current_app.logger.error(f"Error deleting backup {backup_id}: {e}")
        flash('❌ Error deleting backup.', 'danger')
    
    return redirect(url_for('simple_backup.index'))


@simple_backup_bp.route('/download/<backup_id>')
@login_required
@admin_required
def download_backup(backup_id: str):
    """Download a simple backup file."""
    try:
        backup_service = get_simple_backup_service()
        
        backup_info = backup_service.get_backup(backup_id)
        if not backup_info:
            flash('❌ Backup not found.', 'danger')
            return redirect(url_for('simple_backup.index'))
        
        backup_path = backup_info.file_path
        if not os.path.exists(backup_path):
            flash('❌ Backup file not found.', 'danger')
            return redirect(url_for('simple_backup.index'))
        
        return send_file(
            backup_path,
            as_attachment=True,
            download_name=f"{backup_info.name}.zip",
            mimetype='application/zip'
        )
        
    except Exception as e:
        current_app.logger.error(f"Error downloading backup {backup_id}: {e}")
        flash('❌ Error downloading backup.', 'danger')
        return redirect(url_for('simple_backup.index'))


@simple_backup_bp.route('/api/stats')
@login_required
@admin_required
def api_backup_stats():
    """API endpoint to get simple backup statistics."""
    try:
        backup_service = get_simple_backup_service()
        stats = backup_service.get_backup_stats()
        # Attach settings
        try:
            if hasattr(backup_service, '_settings'):
                stats['settings'] = backup_service._settings
        except Exception:
            pass
        return jsonify(stats)
        
    except Exception as e:
        current_app.logger.error(f"Error getting backup stats: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@simple_backup_bp.route('/api/settings', methods=['GET', 'POST'])
@login_required
@admin_required
@csrf.exempt  # JSON API endpoint; protecting via auth+admin
def api_backup_settings():
    """Get or update backup scheduler settings (admin only)."""
    try:
        backup_service = get_simple_backup_service()
        if request.method == 'GET':
            return jsonify(backup_service._settings if hasattr(backup_service, '_settings') else {})
        # POST update
        data = request.get_json(silent=True) or {}
        if not data:
            return jsonify({'error': 'No data provided'}), 400
            
        changed = {}
        if not hasattr(backup_service, '_settings'):
            return jsonify({'error': 'Settings not available'}), 500
            
        settings = backup_service._settings
        
        # Validate and update settings
        for key in ['enabled', 'frequency', 'retention_days', 'scheduled_hour', 'scheduled_minute']:
            if key in data:
                # Type validation
                if key == 'enabled':
                    settings[key] = bool(data[key])
                elif key == 'frequency':
                    # Validate and sanitize frequency value
                    freq_value = str(data[key]) if data[key] else ''
                    if freq_value not in VALID_BACKUP_FREQUENCIES:
                        # Don't include user input in error message to prevent injection
                        return jsonify({'error': f'Invalid frequency value. Must be one of: {", ".join(VALID_BACKUP_FREQUENCIES)}'}), 400
                    settings[key] = freq_value
                elif key == 'retention_days':
                    try:
                        val = int(data[key])
                        if val < 1:
                            return jsonify({'error': 'Retention days must be at least 1'}), 400
                        settings[key] = val
                    except (ValueError, TypeError):
                        return jsonify({'error': 'Invalid retention_days value'}), 400
                elif key in ['scheduled_hour', 'scheduled_minute']:
                    try:
                        val = int(data[key])
                        if key == 'scheduled_hour' and not (0 <= val <= 23):
                            return jsonify({'error': 'Hour must be between 0 and 23'}), 400
                        if key == 'scheduled_minute' and not (0 <= val <= 59):
                            return jsonify({'error': 'Minute must be between 0 and 59'}), 400
                        settings[key] = val
                    except (ValueError, TypeError):
                        return jsonify({'error': f'Invalid {key} value'}), 400
                changed[key] = settings[key]
        
        # Persist changes
        if not backup_service._save_settings():
            current_app.logger.error(f"Failed to save backup settings. Attempted changes: {changed}")
            return jsonify({
                'error': 'Failed to save settings to disk. Check file permissions and disk space.',
                'changed': changed
            }), 500
        
        # Restart scheduler if needed
        if settings.get('enabled'):
            try:
                backup_service.ensure_scheduler()
                current_app.logger.info("Backup scheduler started/ensured after settings update")
            except Exception as e:
                current_app.logger.warning(f"Failed to start backup scheduler: {e}")
        else:
            try:
                backup_service.stop_scheduler()
                current_app.logger.info("Backup scheduler stopped after settings update")
            except Exception as e:
                current_app.logger.warning(f"Failed to stop backup scheduler: {e}")
        
        current_app.logger.info(f"Backup settings updated successfully: {changed}")
        return jsonify({'status': 'ok', 'changed': changed, 'settings': settings})
    except Exception as e:
        current_app.logger.error(f"Error handling backup settings: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@simple_backup_bp.route('/api/status/<backup_id>')
@login_required
@admin_required
def api_backup_status(backup_id: str):
    """API endpoint to get backup status."""
    try:
        backup_service = get_simple_backup_service()
        backup_info = backup_service.get_backup(backup_id)
        if not backup_info:
            return jsonify({'error': 'Backup not found'}), 404
        return jsonify(backup_info.to_dict())
    except Exception as e:
        current_app.logger.error(f"Error getting backup status {backup_id}: {e}")
        return jsonify({'error': 'Internal server error'}), 500
