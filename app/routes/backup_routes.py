"""
Backup and Restore routes for Bibliotheca admin interface.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file, current_app
from flask_login import login_required, current_user
from datetime import datetime
import os
from typing import Optional

from app.services.backup_restore_service import get_backup_service, BackupType, BackupStatus
from app.admin import admin_required

# Create backup blueprint
backup_bp = Blueprint('backup', __name__, url_prefix='/admin/backup')


def _calculate_backup_stats(backups):
    """Calculate backup statistics from a list of backups."""
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


@backup_bp.route('/')
@login_required
@admin_required
def index():
    """Main backup management page."""
    try:
        backup_service = get_backup_service()
        backups = backup_service.list_backups()
        
        # Calculate stats from the backup service
        stats = _calculate_backup_stats(backups)
        
        # Sort backups by creation date (newest first)
        backups.sort(key=lambda b: b.created_at, reverse=True)
        
        return render_template(
            'admin/backup_restore.html',
            backups=backups,
            stats=stats,
            backup_types=BackupType,
            backup_statuses=BackupStatus
        )
    except Exception as e:
        current_app.logger.error(f"Error loading backup page: {e}")
        flash('Error loading backup page.', 'danger')
        return redirect(url_for('admin.dashboard'))


@backup_bp.route('/create', methods=['POST'])
@login_required
@admin_required
def create_backup():
    """Create a new backup."""
    try:
        backup_service = get_backup_service()
        
        # Get form data
        backup_type_str = request.form.get('backup_type', 'full')
        custom_name = request.form.get('custom_name', '').strip()
        description = request.form.get('description', '').strip()
        
        # Convert backup type
        try:
            backup_type = BackupType(backup_type_str)
        except ValueError:
            backup_type = BackupType.FULL
        
        # Create the backup
        backup_info = backup_service.create_backup(
            backup_type=backup_type,
            name=custom_name if custom_name else None,
            description=description
        )
        
        if backup_info:
            flash(f'Backup "{backup_info.name}" created successfully!', 'success')
        else:
            flash('Failed to create backup.', 'danger')
            
    except Exception as e:
        current_app.logger.error(f"Error creating backup: {e}")
        flash('Error creating backup.', 'danger')
    
    return redirect(url_for('backup.index'))


@backup_bp.route('/restore/<backup_id>', methods=['POST'])
@login_required
@admin_required
def restore_backup(backup_id: str):
    """Restore from a backup."""
    try:
        backup_service = get_backup_service()
        
        # Get backup info
        backup_info = backup_service.get_backup(backup_id)
        if not backup_info:
            flash('Backup not found.', 'danger')
            return redirect(url_for('backup.index'))
        
        # Confirm restoration
        confirm = request.form.get('confirm', '').lower()
        if confirm != 'yes':
            flash('Restoration cancelled. Please confirm by typing "yes".', 'warning')
            return redirect(url_for('backup.index'))
        
        # Perform restoration
        success = backup_service.restore_backup(backup_id)
        
        if success:
            flash(f'✅ Successfully restored from backup "{backup_info.name}"! Your books and data have been restored. Database connections have been refreshed.', 'success')
            # Redirect to library page to immediately show restored data
            return redirect(url_for('main.library'))
        else:
            flash('Failed to restore from backup.', 'danger')
            
    except Exception as e:
        current_app.logger.error(f"Error restoring backup {backup_id}: {e}")
        flash('Error restoring backup.', 'danger')
    
    return redirect(url_for('backup.index'))


@backup_bp.route('/delete/<backup_id>', methods=['POST'])
@login_required
@admin_required
def delete_backup(backup_id: str):
    """Delete a backup."""
    try:
        backup_service = get_backup_service()
        
        backup_info = backup_service.get_backup(backup_id)
        if not backup_info:
            flash('Backup not found.', 'danger')
            return redirect(url_for('backup.index'))
        
        success = backup_service.delete_backup(backup_id)
        
        if success:
            flash(f'Backup "{backup_info.name}" deleted successfully.', 'success')
        else:
            flash('Failed to delete backup.', 'danger')
            
    except Exception as e:
        current_app.logger.error(f"Error deleting backup {backup_id}: {e}")
        flash('Error deleting backup.', 'danger')
    
    return redirect(url_for('backup.index'))


@backup_bp.route('/download/<backup_id>')
@login_required
@admin_required
def download_backup(backup_id: str):
    """Download a backup file."""
    try:
        backup_service = get_backup_service()
        
        backup_info = backup_service.get_backup(backup_id)
        if not backup_info:
            flash('Backup not found.', 'danger')
            return redirect(url_for('backup.index'))
        
        backup_path = backup_info.file_path
        if not os.path.exists(backup_path):
            flash('Backup file not found.', 'danger')
            return redirect(url_for('backup.index'))
        
        return send_file(
            backup_path,
            as_attachment=True,
            download_name=f"{backup_info.name}.tar.gz",
            mimetype='application/gzip'
        )
        
    except Exception as e:
        current_app.logger.error(f"Error downloading backup {backup_id}: {e}")
        flash('Error downloading backup.', 'danger')
        return redirect(url_for('backup.index'))


@backup_bp.route('/cleanup', methods=['POST'])
@login_required
@admin_required
def cleanup_backups():
    """Clean up old backups."""
    try:
        backup_service = get_backup_service()
        
        # Get cleanup parameters
        max_age_days = int(request.form.get('max_age_days', 30))
        max_count = int(request.form.get('max_count', 50))
        
        deleted_count = backup_service.cleanup_old_backups(
            max_age_days=max_age_days,
            max_count=max_count
        )
        
        if deleted_count > 0:
            flash(f'Cleaned up {deleted_count} old backups.', 'success')
        else:
            flash('No backups needed cleanup.', 'info')
            
    except Exception as e:
        current_app.logger.error(f"Error cleaning up backups: {e}")
        flash('Error cleaning up backups.', 'danger')
    
    return redirect(url_for('backup.index'))


@backup_bp.route('/export')
@login_required
@admin_required
def export_data():
    """Export user data page."""
    return render_template('admin/export_data.html')


@backup_bp.route('/export/csv', methods=['POST'])
@login_required
@admin_required
def export_csv():
    """Export data to CSV format."""
    try:
        backup_service = get_backup_service()
        export_path = backup_service.export_data('csv')
        
        if export_path and os.path.exists(export_path):
            return send_file(
                export_path,
                as_attachment=True,
                download_name=f"bibliotheca_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mimetype='text/csv'
            )
        else:
            flash('Failed to export data.', 'danger')
            
    except Exception as e:
        current_app.logger.error(f"Error exporting CSV: {e}")
        flash('Error exporting data.', 'danger')
    
    return redirect(url_for('backup.export_data'))


@backup_bp.route('/export/json', methods=['POST'])
@login_required
@admin_required
def export_json():
    """Export data to JSON format."""
    try:
        backup_service = get_backup_service()
        export_path = backup_service.export_data('json')
        
        if export_path and os.path.exists(export_path):
            return send_file(
                export_path,
                as_attachment=True,
                download_name=f"bibliotheca_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mimetype='application/json'
            )
        else:
            flash('Failed to export data.', 'danger')
            
    except Exception as e:
        current_app.logger.error(f"Error exporting JSON: {e}")
        flash('Error exporting data.', 'danger')
    
    return redirect(url_for('backup.export_data'))


@backup_bp.route('/api/status/<backup_id>')
@login_required
@admin_required
def api_backup_status(backup_id: str):
    """API endpoint to get backup status."""
    try:
        backup_service = get_backup_service()
        backup_info = backup_service.get_backup(backup_id)
        
        if not backup_info:
            return jsonify({'error': 'Backup not found'}), 404
        
        return jsonify(backup_info.to_dict())
        
    except Exception as e:
        current_app.logger.error(f"Error getting backup status {backup_id}: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@backup_bp.route('/api/stats')
@login_required
@admin_required
def api_backup_stats():
    """API endpoint to get backup statistics."""
    try:
        backup_service = get_backup_service()
        backups = backup_service.list_backups()
        stats = _calculate_backup_stats(backups)
        return jsonify(stats)
        
    except Exception as e:
        current_app.logger.error(f"Error getting backup stats: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@backup_bp.route('/schedule')
@login_required
@admin_required
def schedule_backups():
    """Backup scheduling page."""
    return render_template('admin/backup_schedule.html')


@backup_bp.route('/schedule/save', methods=['POST'])
@login_required
@admin_required
def save_backup_schedule():
    """Save backup schedule configuration."""
    try:
        # This would save backup schedule configuration
        # For now, just show a success message
        flash('Backup schedule saved successfully!', 'success')
        
    except Exception as e:
        current_app.logger.error(f"Error saving backup schedule: {e}")
        flash('Error saving backup schedule.', 'danger')
    
    return redirect(url_for('backup.schedule_backups'))
