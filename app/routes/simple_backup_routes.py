"""
Simple Backup routes for Bibliotheca admin interface.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file, current_app
from flask_login import login_required, current_user
from datetime import datetime
import os
from typing import Optional

from app.services.simple_backup_service import get_simple_backup_service
from app.admin import admin_required

# Create simple backup blueprint
simple_backup_bp = Blueprint('simple_backup', __name__, url_prefix='/admin/simple-backup')


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
        
        return render_template(
            'admin/simple_backup.html',
            backups=enhanced_backups,
            backup_stats=stats
        )
    except Exception as e:
        current_app.logger.error(f"Error loading simple backup page: {e}")
        flash('Error loading backup page.', 'danger')
        return redirect(url_for('admin.dashboard'))


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
            flash(f'✅ Successfully restored from backup "{backup_info.name}"! Your data has been restored and database connections refreshed.', 'success')
            # Redirect to library page to immediately show restored data
            return redirect(url_for('main.library'))
        else:
            flash('❌ Failed to restore from backup. Check logs for details.', 'danger')
            
    except Exception as e:
        current_app.logger.error(f"Error restoring backup {backup_id}: {e}")
        flash('❌ Error restoring backup.', 'danger')
    
    return redirect(url_for('simple_backup.index'))


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
        return jsonify(stats)
        
    except Exception as e:
        current_app.logger.error(f"Error getting backup stats: {e}")
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
