"""
Admin functionality for Bibliotheca multi-user platform
Provides admin-only decorators, middleware, and management functions
"""

import os
from functools import wraps
from flask import Blueprint, render_template, redirect, url_for, flash, request, jsonify, abort, current_app
from flask_login import login_required, current_user
from .services import user_service, book_service, reading_log_service
from .forms import UserProfileForm, AdminPasswordResetForm
from datetime import datetime, timedelta, timezone
import pytz
import os

admin = Blueprint('admin', __name__, url_prefix='/admin')

def load_ai_config():
    """Load AI configuration from .env file in data directory"""
    env_path = os.path.join(current_app.config.get('DATA_DIR', 'data'), '.env')
    config = {}
    
    if os.path.exists(env_path):
        try:
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        config[key.strip()] = value.strip()
        except Exception as e:
            current_app.logger.error(f"Error loading AI config: {e}")
    
    # Set defaults for missing values
    defaults = {
        'OPENAI_API_KEY': '',
        'OPENAI_BASE_URL': 'https://api.openai.com/v1',
        'OPENAI_MODEL': 'gpt-4o',
        'OLLAMA_BASE_URL': 'http://localhost:11434',
        'OLLAMA_MODEL': 'llama3.2-vision:11b',
        'AI_PROVIDER': 'openai',
        'AI_TIMEOUT': '30',
        'AI_MAX_TOKENS': '1000',
        'AI_TEMPERATURE': '0.1',
        'AI_BOOK_EXTRACTION_ENABLED': 'false',
        'AI_BOOK_EXTRACTION_AUTO_SEARCH': 'true'
    }
    
    for key, default_value in defaults.items():
        if key not in config:
            config[key] = default_value
    
    return config

def save_ai_config(config):
    """Save AI configuration to .env file in data directory"""
    env_path = os.path.join(current_app.config.get('DATA_DIR', 'data'), '.env')
    
    try:
        # Ensure data directory exists
        os.makedirs(os.path.dirname(env_path), exist_ok=True)
        
        # Read existing content to preserve comments
        existing_lines = []
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                existing_lines = f.readlines()
        
        # Build new content
        lines = ['# AI Configuration for MyBibliotheca\n',
                '# This file stores API keys and configuration for AI book extraction features\n',
                '# Keep this file secure and do not commit to version control\n\n']
        
        # Add configuration sections
        lines.append('# OpenAI Configuration\n')
        lines.append(f"OPENAI_API_KEY={config.get('OPENAI_API_KEY', '')}\n")
        lines.append(f"OPENAI_BASE_URL={config.get('OPENAI_BASE_URL', 'https://api.openai.com/v1')}\n")
        lines.append(f"OPENAI_MODEL={config.get('OPENAI_MODEL', 'gpt-4o')}\n\n")
        
        lines.append('# Ollama Configuration (Local AI)\n')
        lines.append(f"OLLAMA_BASE_URL={config.get('OLLAMA_BASE_URL', 'http://localhost:11434')}\n")
        lines.append(f"OLLAMA_MODEL={config.get('OLLAMA_MODEL', 'llama3.2-vision:11b')}\n\n")
        
        lines.append('# AI Processing Settings\n')
        lines.append(f"AI_PROVIDER={config.get('AI_PROVIDER', 'openai')}\n")
        lines.append(f"AI_TIMEOUT={config.get('AI_TIMEOUT', '30')}\n")
        lines.append(f"AI_MAX_TOKENS={config.get('AI_MAX_TOKENS', '1000')}\n")
        lines.append(f"AI_TEMPERATURE={config.get('AI_TEMPERATURE', '0.1')}\n\n")
        
        lines.append('# Feature Flags\n')
        lines.append(f"AI_BOOK_EXTRACTION_ENABLED={config.get('AI_BOOK_EXTRACTION_ENABLED', 'false')}\n")
        lines.append(f"AI_BOOK_EXTRACTION_AUTO_SEARCH={config.get('AI_BOOK_EXTRACTION_AUTO_SEARCH', 'true')}\n")
        
        with open(env_path, 'w') as f:
            f.writelines(lines)
        
        return True
    except Exception as e:
        current_app.logger.error(f"Error saving AI config: {e}")
        return False

def admin_required(f):
    """
    Decorator to require admin privileges for route access
    Usage: @admin_required
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('auth.login', next=request.url))
        
        if not current_user.is_admin:
            flash('Access denied. Admin privileges required.', 'error')
            abort(403)
        
        return f(*args, **kwargs)
    return decorated_function

def admin_or_self_required(user_id_param='user_id'):
    """
    Decorator to require admin privileges OR access to own user data
    Usage: @admin_or_self_required() or @admin_or_self_required('id')
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                flash('Please log in to access this page.', 'error')
                return redirect(url_for('auth.login', next=request.url))
            
            # Get the user_id from the route parameters
            target_user_id = kwargs.get(user_id_param)
            if target_user_id is None and request.view_args:
                target_user_id = request.view_args.get(user_id_param)
            
            # Allow if admin or accessing own data
            if current_user.is_admin or str(current_user.id) == str(target_user_id):
                return f(*args, **kwargs)
            
            flash('Access denied. Insufficient privileges.', 'error')
            abort(403)
        
        return decorated_function
    return decorator

@admin.route('/dashboard')
@login_required
@admin_required
def dashboard():
    """Admin dashboard with system overview"""
    try:
        # Get system statistics
        stats = get_system_stats()
        
        # Get recent user registrations (last 30 days) - placeholder for now
        # This would need to be implemented in the user service
        recent_users = []
        
        # Get recent book additions (last 30 days) - placeholder for now
        # This would need to be implemented in the book service  
        recent_books = []
        
        return render_template('admin/dashboard.html', 
                             title='Admin Dashboard',
                             stats=stats,
                             recent_users=recent_users,
                             recent_books=recent_books)
    except Exception as e:
        current_app.logger.error(f"Error loading admin dashboard: {e}")
        flash('Error loading dashboard data.', 'danger')
        # Provide default stats structure to prevent template errors
        default_stats = {
            'total_books': 0,
            'total_users': 0,
            'active_users': 0,
            'admin_users': 0,
            'new_users_30d': 0,
            'new_books_30d': 0,
            'total_categories': 0,
            'total_contributors': 0,
            'recent_activity': 0,
            'database_size': 'Unknown',
            'system': {
                'disk_free_gb': 'N/A',
                'disk_total_gb': 'N/A', 
                'disk_percent': 'N/A',
                'memory_percent': 'N/A',
                'memory_available_gb': 'N/A'
            },
            'top_users': []
        }
        return render_template('admin/dashboard.html', 
                             title='Admin Dashboard',
                             stats=default_stats,
                             recent_users=[],
                             recent_books=[])

@admin.route('/users')
@login_required
@admin_required
def users():
    """User management interface"""
    search = request.args.get('search', '', type=str)
    
    try:
        page = request.args.get('page', 1, type=int)
        
        # For now, return basic info - search functionality would need to be implemented
        # in the user service for full functionality
        all_users = user_service.get_all_users_sync() if hasattr(user_service, 'get_all_users_sync') else []
        
        # Simple client-side search filtering
        if search:
            filtered_users = [user for user in all_users 
                            if search.lower() in user.username.lower() or search.lower() in user.email.lower()]
        else:
            filtered_users = all_users
        
        # Simple pagination
        per_page = 20
        start = (page - 1) * per_page
        end = start + per_page
        page_users = filtered_users[start:end]
        
        # Create pagination object simulation
        class PaginationResult:
            def __init__(self, items, page, per_page, total):
                self.items = items
                self.page = page
                self.per_page = per_page
                self.total = total
                self.pages = (total + per_page - 1) // per_page  # Calculate total pages
                self.has_prev = page > 1
                self.has_next = end < total
                self.prev_num = page - 1 if self.has_prev else None
                self.next_num = page + 1 if self.has_next else None
        
        users_paginated = PaginationResult(page_users, page, per_page, len(filtered_users))
        
        return render_template('admin/users.html',
                             title='User Management',
                             users=users_paginated,
                             search=search)
    except Exception as e:
        current_app.logger.error(f"Error loading users: {e}")
        flash('Error loading users.', 'danger')
        return render_template('admin/users.html',
                             title='User Management',
                             users=None,
                             search=search)

@admin.route('/users/<string:user_id>')
@login_required
@admin_required
def user_detail(user_id):
    """Individual user management"""
    try:
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            abort(404)
        
        # Get user statistics from services
        user_books = book_service.get_all_books_with_user_overlay_sync(str(user_id))
        book_count = len(user_books)
        
        reading_count = reading_log_service.get_user_logs_count_sync(user_id) if hasattr(reading_log_service, 'get_user_logs_count_sync') else 0  # type: ignore
        
        # Get more detailed stats
        current_year = datetime.now().year
        books_this_year = len([book for book in user_books 
                              if book.get('created_at') and 
                              isinstance(book.get('created_at'), datetime) and 
                              book['created_at'].year == current_year])
        
        # Reading logs this month would need service implementation
        logs_this_month = 0  # Placeholder
        
        # Get recent activity - limit to recent books
        recent_books = sorted(user_books, 
                            key=lambda x: x.get('created_at') or datetime.min, 
                            reverse=True)[:5]
        recent_logs = []  # Would need service implementation
        
        return render_template('admin/user_detail.html',
                             title=f'User: {user.username}',
                             user=user,
                             book_count=book_count,
                             reading_count=reading_count,
                             books_this_year=books_this_year,
                             logs_this_month=logs_this_month,
                             recent_books=recent_books,
                             recent_logs=recent_logs)
    except Exception as e:
        current_app.logger.error(f"Error loading user detail {user_id}: {e}")
        flash('Error loading user details.', 'danger')
        return redirect(url_for('admin.users'))

@admin.route('/users/<string:user_id>/toggle_admin', methods=['POST'])
@login_required
@admin_required
def toggle_admin(user_id):
    """Toggle admin status for a user"""
    try:
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            abort(404)
        
        # Prevent removing admin from the last admin
        if user.is_admin:
            admin_count = user_service.get_admin_count_sync() if hasattr(user_service, 'get_admin_count_sync') else 1  # type: ignore
            if admin_count <= 1:
                flash('Cannot remove admin privileges from the last admin user.', 'error')
                return redirect(url_for('admin.user_detail', user_id=user_id))
        
        # Toggle admin status
        user.is_admin = not user.is_admin
        user_service.update_user_sync(user)
        
        action = 'granted' if user.is_admin else 'removed'
        flash(f'Admin privileges {action} for user {user.username}.', 'success')
        
        return redirect(url_for('admin.user_detail', user_id=user_id))
    except Exception as e:
        current_app.logger.error(f"Error toggling admin status for user {user_id}: {e}")
        flash('Error updating user privileges.', 'danger')
        return redirect(url_for('admin.user_detail', user_id=user_id))

@admin.route('/users/<string:user_id>/toggle_active', methods=['POST'])
@login_required
@admin_required
def toggle_active(user_id):
    """Toggle active status for a user"""
    try:
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            abort(404)
        
        # Prevent deactivating the current admin
        if user.id == current_user.id:
            flash('Cannot deactivate your own account.', 'error')
            return redirect(url_for('admin.user_detail', user_id=user_id))
        
        # Toggle active status
        user.is_active = not user.is_active
        user_service.update_user_sync(user)
        
        status = 'activated' if user.is_active else 'deactivated'
        flash(f'User {user.username} has been {status}.', 'success')
        
        return redirect(url_for('admin.user_detail', user_id=user_id))
    except Exception as e:
        current_app.logger.error(f"Error toggling active status for user {user_id}: {e}")
        flash('Error updating user status.', 'danger')
        return redirect(url_for('admin.user_detail', user_id=user_id))

@admin.route('/users/<string:user_id>/delete', methods=['POST'])
@login_required
@admin_required
def delete_user(user_id):
    """Delete a user and handle their data"""
    try:
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            abort(404)
        
        # Prevent deleting own account
        if user.id == current_user.id:
            flash('Cannot delete your own account.', 'error')
            return redirect(url_for('admin.user_detail', user_id=user_id))
        
        # Prevent deleting the last admin
        if user.is_admin:
            admin_count = user_service.get_admin_count_sync() if hasattr(user_service, 'get_admin_count_sync') else 1  # type: ignore
            if admin_count <= 1:
                flash('Cannot delete the last admin user.', 'error')
                return redirect(url_for('admin.user_detail', user_id=user_id))
        
        username = user.username
        
        # Delete user (would need service implementation for cascading deletes)
        # For now, just mark as placeholder
        flash(f'User deletion not fully implemented for Kuzu backend. User {username} would be deleted.', 'warning')
        return redirect(url_for('admin.users'))
    except Exception as e:
        current_app.logger.error(f"Error deleting user {user_id}: {e}")
        flash('Error deleting user.', 'danger')
        return redirect(url_for('admin.user_detail', user_id=user_id))

@admin.route('/settings', methods=['GET', 'POST'])
@login_required 
@admin_required
def settings():
    """Admin settings page"""
    from .infrastructure.kuzu_graph import get_graph_storage
    storage = get_graph_storage()
    
    if request.method == 'POST':
        site_name = request.form.get('site_name', 'MyBibliotheca')
        server_timezone = request.form.get('server_timezone', 'UTC')
        
        # For Kuzu version, settings are managed via environment variables
        # Store settings notification (in production, these would be environment variables)
        flash(f'Settings noted! Site name: {site_name}, Server timezone: {server_timezone}. Configure via environment variables for persistence.', 'info')
        return redirect(url_for('admin.settings'))
    
    # Get the current settings from environment variables
    current_site_name = os.getenv('SITE_NAME', 'MyBibliotheca')
    current_timezone = os.getenv('SERVER_TIMEZONE', 'UTC')
    
    # Get available timezones
    available_timezones = pytz.all_timezones
    common_timezones = [
        'UTC',
        'US/Eastern', 'US/Central', 'US/Mountain', 'US/Pacific',
        'Europe/London', 'Europe/Paris', 'Europe/Berlin', 'Europe/Rome',
        'Asia/Tokyo', 'Asia/Shanghai', 'Asia/Kolkata',
        'Australia/Sydney', 'Australia/Melbourne',
        'America/Toronto', 'America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles'
    ]
    
    # Get debug manager
    try:
        from .debug_system import get_debug_manager
        debug_manager = get_debug_manager()
    except Exception as e:
        current_app.logger.warning(f"Could not load debug manager: {e}")
        # Create a simple mock debug manager to prevent template errors
        class MockDebugManager:
            def is_debug_enabled(self):
                return False
        debug_manager = MockDebugManager()
    
    return render_template('admin/settings.html', 
                         title='Admin Settings', 
                         site_name=current_site_name,
                         server_timezone=current_timezone,
                         common_timezones=common_timezones,
                         available_timezones=sorted(available_timezones),
                         ai_config=load_ai_config(),
                         debug_manager=debug_manager)

@admin.route('/api/stats')
@login_required
@admin_required
def api_stats():
    """API endpoint for dashboard statistics (for auto-refresh)"""
    stats = get_system_stats()
    return jsonify(stats)

@admin.route('/update-ai-settings', methods=['POST'])
@login_required
@admin_required
def update_ai_settings():
    """Update AI configuration settings"""
    try:
        config = {}
        
        # Get form data
        config['AI_PROVIDER'] = request.form.get('ai_provider', 'openai')
        config['OPENAI_API_KEY'] = request.form.get('openai_api_key', '')
        config['OPENAI_BASE_URL'] = request.form.get('openai_base_url', 'https://api.openai.com/v1')
        config['OPENAI_MODEL'] = request.form.get('openai_model', 'gpt-4o')
        config['OLLAMA_BASE_URL'] = request.form.get('ollama_base_url', 'http://localhost:11434/v1')
        
        # Handle Ollama model selection (dropdown vs manual input)
        ollama_model_manual = request.form.get('ollama_model_manual', '').strip()
        ollama_model_select = request.form.get('ollama_model', '').strip()
        if ollama_model_manual:
            config['OLLAMA_MODEL'] = ollama_model_manual
        elif ollama_model_select:
            config['OLLAMA_MODEL'] = ollama_model_select
        else:
            config['OLLAMA_MODEL'] = 'llama3.2-vision:11b'  # fallback
            
        config['AI_TIMEOUT'] = request.form.get('ai_timeout', '30')
        config['AI_MAX_TOKENS'] = request.form.get('ai_max_tokens', '1000')
        config['AI_TEMPERATURE'] = request.form.get('ai_temperature', '0.1')
        config['AI_BOOK_EXTRACTION_ENABLED'] = 'true' if request.form.get('ai_book_extraction_enabled') else 'false'
        config['AI_BOOK_EXTRACTION_AUTO_SEARCH'] = 'true' if request.form.get('ai_book_extraction_auto_search') else 'false'
        
        # Save configuration
        if save_ai_config(config):
            flash('AI settings saved successfully!', 'success')
        else:
            flash('Error saving AI settings. Please try again.', 'danger')
        
        return redirect(url_for('admin.settings'))
        
    except Exception as e:
        current_app.logger.error(f"Error updating AI settings: {e}")
        flash('Error updating AI settings. Please try again.', 'danger')
        return redirect(url_for('admin.settings'))

@admin.route('/test-ai-connection', methods=['POST'])
@login_required
@admin_required
def test_ai_connection():
    """Test AI connection with provided settings"""
    try:
        # Build config from form data
        config = {
            'AI_PROVIDER': request.form.get('ai_provider', 'openai'),
            'OPENAI_API_KEY': request.form.get('openai_api_key', ''),
            'OPENAI_BASE_URL': request.form.get('openai_base_url', 'https://api.openai.com/v1'),
            'OPENAI_MODEL': request.form.get('openai_model', 'gpt-4o'),
            'OLLAMA_BASE_URL': request.form.get('ollama_base_url', 'http://localhost:11434/v1'),
            'OLLAMA_MODEL': request.form.get('ollama_model_manual') or request.form.get('ollama_model', 'llama3.2-vision:11b'),
            'AI_TIMEOUT': request.form.get('ai_timeout', '30'),
            'AI_MAX_TOKENS': request.form.get('ai_max_tokens', '1000'),
            'AI_TEMPERATURE': request.form.get('ai_temperature', '0.1'),
        }
        
        # Test connection using AI service
        from app.services.ai_service import AIService
        ai_service = AIService(config)
        result = ai_service.test_connection()
        
        return jsonify(result)
            
    except Exception as e:
        current_app.logger.error(f"Error testing AI connection: {e}")
        return jsonify({'success': False, 'message': 'Connection test failed. Please check your settings.'})

@admin.route('/test-ollama-connection', methods=['POST'])
@login_required
@admin_required
def test_ollama_connection():
    """Test Ollama connection and return available models"""
    try:
        # Create minimal config for testing
        config = {
            'AI_PROVIDER': 'ollama',
            'OLLAMA_BASE_URL': request.form.get('ollama_base_url', 'http://localhost:11434/v1'),
            'AI_TIMEOUT': '10'  # Short timeout for testing
        }
        
        # Test connection using AI service
        from app.services.ai_service import AIService
        ai_service = AIService(config)
        result = ai_service._test_ollama_connection()
        
        return jsonify(result)
            
    except Exception as e:
        current_app.logger.error(f"Error testing Ollama connection: {e}")
        return jsonify({'success': False, 'message': 'Ollama connection test failed. Please check your settings.'})

@admin.route('/users/<string:user_id>/reset_password', methods=['GET', 'POST'])
@login_required
@admin_required
def reset_user_password(user_id):
    """Admin function to reset a user's password"""
    try:
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            abort(404)
        form = AdminPasswordResetForm()
        
        if form.validate_on_submit():
            try:
                # Update password using service - would need implementation
                # user.set_password(form.new_password.data)
                # Set force password change if requested
                if form.force_change.data:
                    user.password_must_change = True
                # Also unlock the account if it was locked
                if hasattr(user, 'unlock_account'):
                    user.unlock_account()
                user_service.update_user_sync(user)
                
                force_msg = " User will be required to change password on next login." if form.force_change.data else ""
                flash(f'Password reset functionality not fully implemented for Kuzu backend. User {user.username} would be updated.{force_msg}', 'warning')
                return redirect(url_for('admin.user_detail', user_id=user.id))
            except ValueError as e:
                flash(str(e), 'error')
        
        return render_template('admin/reset_password.html', 
                             title=f'Reset Password - {user.username}',
                             form=form, 
                             user=user)
    except Exception as e:
        current_app.logger.error(f"Error resetting password for user {user_id}: {e}")
        flash('Error resetting password.', 'danger')
        return redirect(url_for('admin.user_detail', user_id=user_id))

@admin.route('/users/<string:user_id>/unlock_account', methods=['POST'])
@login_required
@admin_required
def unlock_user_account(user_id):
    """Admin function to unlock a locked user account"""
    try:
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            abort(404)
        
        if hasattr(user, 'is_locked') and user.is_locked():
            if hasattr(user, 'unlock_account'):
                user.unlock_account()
                user_service.update_user_sync(user)
            flash(f'Account unlocked for user {user.username}.', 'success')
        else:
            flash(f'User {user.username} account is not locked.', 'info')
        
        return redirect(url_for('admin.user_detail', user_id=user.id))
    except Exception as e:
        current_app.logger.error(f"Error unlocking user account {user_id}: {e}")
        flash('Error unlocking user account.', 'danger')
        return redirect(url_for('admin.user_detail', user_id=user_id))

@admin.route('/migration')
@login_required
@admin_required
def migration_management():
    """Migration management page for admins"""
    try:
        from .migration_detector import MigrationDetector
        detector = MigrationDetector()
        databases = detector.find_sqlite_databases()
        
        migration_info = {
            'databases_found': len(databases),
            'migration_available': len(databases) > 0,
            'databases': []
        }
        
        for db_path in databases:
            info = detector.analyze_database(db_path)
            migration_info['databases'].append({
                'path': db_path,
                'filename': os.path.basename(db_path),
                'version': info.get('version', 'unknown'),
                'books': info.get('total_books', 0),
                'users': info.get('total_users', 0),
                'reading_logs': info.get('total_reading_logs', 0),
                'size_mb': round(os.path.getsize(db_path) / (1024 * 1024), 2) if os.path.exists(db_path) else 0
            })
        
        return render_template('admin/migration.html',
                             title='Database Migration',
                             migration_info=migration_info)
    
    except Exception as e:
        current_app.logger.error(f"Error loading migration info: {e}")
        flash('Error loading migration information.', 'danger')
        return render_template('admin/migration.html',
                             title='Database Migration',
                             migration_info={
                                 'databases_found': 0,
                                 'migration_available': False,
                                 'databases': []
                             })

def get_system_stats():
    """Get system statistics for admin dashboard"""
    try:
        # Get basic stats from services
        total_users = user_service.get_user_count_sync() if hasattr(user_service, 'get_user_count_sync') else 0
        active_users = user_service.get_active_user_count_sync() if hasattr(user_service, 'get_active_user_count_sync') else 0  # type: ignore
        admin_users = user_service.get_admin_count_sync() if hasattr(user_service, 'get_admin_count_sync') else 0  # type: ignore
        
        # Total books across all users - would need service implementation
        total_books = 0  # Placeholder
        
        # Users registered in last 30 days - would need service implementation
        new_users_30d = 0  # Placeholder
        
        # Books added in last 30 days - would need service implementation
        new_books_30d = 0  # Placeholder
        
        # Most active users (by book count) - would need service implementation
        top_users = []  # Placeholder
        
        # System health info (with fallback if psutil not available)
        system_info = {}
        try:
            import psutil
            disk_usage = psutil.disk_usage('/')
            memory = psutil.virtual_memory()
            
            system_info = {
                'disk_free_gb': round(disk_usage.free / (1024**3), 2),
                'disk_total_gb': round(disk_usage.total / (1024**3), 2),
                'disk_percent': round((disk_usage.used / disk_usage.total) * 100, 1),
                'memory_percent': memory.percent,
                'memory_available_gb': round(memory.available / (1024**3), 2)
            }
        except ImportError:
            # Fallback if psutil is not available
            system_info = {
                'disk_free_gb': 'N/A',
                'disk_total_gb': 'N/A', 
                'disk_percent': 'N/A',
                'memory_percent': 'N/A',
                'memory_available_gb': 'N/A'
            }
        
        return {
            'total_users': total_users,
            'active_users': active_users,
            'admin_users': admin_users,
            'total_books': total_books,
            'new_users_30d': new_users_30d,
            'new_books_30d': new_books_30d,
            'top_users': [{'username': user[0], 'book_count': user[1]} for user in top_users],
            'system': system_info
        }
    except Exception as e:
        current_app.logger.error(f"Error getting system stats: {e}")
        return {
            'total_users': 0,
            'active_users': 0,
            'admin_users': 0,
            'total_books': 0,
            'new_users_30d': 0,
            'new_books_30d': 0,
            'top_users': [],
            'system': {}
        }

def is_admin(user):
    """Helper function to check if user is admin"""
    return user.is_authenticated and user.is_admin

def promote_user_to_admin(user_id):
    """Promote a user to admin status"""
    try:
        user = user_service.get_user_by_id_sync(user_id)
        if user:
            user.is_admin = True
            user_service.update_user_sync(user)
            return True
        return False
    except Exception as e:
        current_app.logger.error(f"Error promoting user {user_id} to admin: {e}")
        return False

def demote_admin_user(user_id):
    """Demote an admin user (with safety checks)"""
    try:
        user = user_service.get_user_by_id_sync(user_id)
        if user and user.is_admin:
            # Check if this is the last admin
            admin_count = user_service.get_admin_count_sync() if hasattr(user_service, 'get_admin_count_sync') else 1  # type: ignore
            if admin_count > 1:
                user.is_admin = False
                user_service.update_user_sync(user)
                return True
        return False
    except Exception as e:
        current_app.logger.error(f"Error demoting admin user {user_id}: {e}")
        return False

def unlock_user_account_by_id(user_id):
    """Helper function to unlock a user account"""
    try:
        user = user_service.get_user_by_id_sync(user_id)
        if user and hasattr(user, 'unlock_account'):
            user.unlock_account()
            user_service.update_user_sync(user)
            return True
        return False
    except Exception as e:
        current_app.logger.error(f"Error unlocking user account {user_id}: {e}")
        return False
