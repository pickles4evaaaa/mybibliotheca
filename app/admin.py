"""
Admin functionality for Bibliotheca multi-user platform
Provides admin-only decorators, middleware, and management functions
"""

import os
from typing import Dict
from functools import wraps
from pathlib import Path
from flask import Blueprint, render_template, redirect, url_for, flash, request, jsonify, abort, current_app
from flask_login import login_required, current_user
from .services import user_service, book_service, reading_log_service
from .forms import UserProfileForm, AdminPasswordResetForm
from datetime import datetime, timedelta, timezone
import pytz
import os
from app.utils.safe_kuzu_manager import SafeKuzuManager, get_safe_kuzu_manager

admin = Blueprint('admin', __name__, url_prefix='/admin')

def _convert_query_result_to_list(result):
    """Convert SafeKuzuManager query result to legacy list format"""
    try:
        # Try to iterate over the result directly
        if hasattr(result, 'get_next'):
            rows = []
            while result.has_next():
                rows.append(result.get_next())
            return rows
        else:
            return list(result) if result else []
    except Exception:
        # Fallback to empty list if conversion fails
        return []

def _get_root_env_path() -> str:
    """Resolve the project root .env path regardless of CWD or Docker paths."""
    try:
        # current_app.root_path points to .../app; parent is project root
        root_dir = Path(current_app.root_path).parent
    except Exception:
        # Fallback to this file's parent directory's parent
        root_dir = Path(__file__).resolve().parents[1]
    return str(root_dir / '.env')


def load_ai_config():
    """Load AI configuration combining persisted JSON override and .env, with caching."""
    import json, time
    cache_key = '_cached_ai_config'
    cache_ts_key = '_cached_ai_config_ts'
    try:
        # Quick in-process cache (5s) to avoid re-reading files on rapid requests
        if cache_key in current_app.config and cache_ts_key in current_app.config:
            if (time.time() - current_app.config[cache_ts_key]) < 5:
                return current_app.config[cache_key]
    except Exception:
        pass

    env_path = _get_root_env_path()
    config: Dict[str,str] = {}

    # 1. Load .env base values
    if os.path.exists(env_path):
        try:
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        config[key.strip()] = value.strip()
        except Exception as e:
            try:
                current_app.logger.error(f"Error loading AI config from .env: {e}")
            except Exception:
                pass

    # 2. Overlay runtime-persisted JSON (data/ai_config.json) if present
    try:
        data_dir = current_app.config.get('DATA_DIR', 'data')
    except Exception:
        data_dir = 'data'
    ai_json_path = os.path.join(data_dir, 'ai_config.json')
    if os.path.exists(ai_json_path):
        try:
            with open(ai_json_path, 'r') as jf:
                json_data = json.load(jf)
            # Overlay keys explicitly stored
            for k,v in json_data.items():
                if isinstance(v, (str,int,float)):
                    config[k] = str(v)
        except Exception as e:
            try:
                current_app.logger.warning(f"Failed reading ai_config.json: {e}")
            except Exception:
                pass

    # 2b. Backward-compatibility: also overlay legacy data/ai_settings.json if present
    #     This allows older setups to keep using ai_settings.json without losing values.
    try:
        ai_legacy_path = os.path.join(data_dir, 'ai_settings.json')
        if os.path.exists(ai_legacy_path):
            with open(ai_legacy_path, 'r') as jf2:
                legacy_data = json.load(jf2)
            if isinstance(legacy_data, dict):
                for k, v in legacy_data.items():
                    if isinstance(v, (str, int, float)):
                        config[k] = str(v)
    except Exception as le:
        try:
            current_app.logger.warning(f"Failed reading ai_settings.json: {le}")
        except Exception:
            pass

    # 3. Apply defaults for any unset keys
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
        if key not in config or config[key] == '':
            config[key] = default_value

    # Cache result
    try:
        current_app.config[cache_key] = config
        current_app.config[cache_ts_key] = time.time()
    except Exception:
        pass
    return config

def save_system_config(config):
    """Save system configuration to config file in data directory"""
    import json
    
    try:
        # Get data directory from current app config or default
        try:
            data_dir = current_app.config.get('DATA_DIR', 'data')
        except RuntimeError:
            # Working outside of application context, use default
            data_dir = 'data'
        
        config_path = os.path.join(data_dir, 'system_config.json')
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        
        # Load existing config
        existing_config = {}
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    existing_config = json.load(f)
            except (json.JSONDecodeError, Exception):
                # If file is corrupted, start fresh
                existing_config = {}
        
        # Update with new system settings
        existing_config['site_name'] = config.get('site_name', 'MyBibliotheca')
        existing_config['server_timezone'] = config.get('server_timezone', 'UTC')
        existing_config['terminology_preference'] = config.get('terminology_preference', 'genre')
        
    # Update background configuration if provided
        if 'background_config' in config:
            existing_config['background_config'] = config['background_config']
        # Optional: reading log defaults
        if 'reading_log_defaults' in config:
            # Normalize to simple ints or None
            rld = config.get('reading_log_defaults') or {}
            try:
                dp = rld.get('default_pages_per_log')
                dm = rld.get('default_minutes_per_log')
                dp_i = int(dp) if dp not in (None, '',) else None
                dm_i = int(dm) if dm not in (None, '',) else None
            except Exception:
                dp_i = rld.get('default_pages_per_log') if isinstance(rld.get('default_pages_per_log'), int) else None
                dm_i = rld.get('default_minutes_per_log') if isinstance(rld.get('default_minutes_per_log'), int) else None
            existing_config['reading_log_defaults'] = {
                'default_pages_per_log': dp_i,
                'default_minutes_per_log': dm_i
            }
        # Optional: library defaults
        if 'library_defaults' in config:
            lib = config.get('library_defaults') or {}
            try:
                dr = lib.get('default_rows_per_page')
                dr_i = int(dr) if dr not in (None, '',) else None
            except Exception:
                dr_i = lib.get('default_rows_per_page') if isinstance(lib.get('default_rows_per_page'), int) else None
            existing_config['library_defaults'] = {
                'default_rows_per_page': dr_i
            }
        if 'import_settings' in config:
            existing_import_settings = existing_config.get('import_settings', {}).copy()
            import_settings = config.get('import_settings') or {}
            metadata_concurrency_val = import_settings.get('metadata_concurrency')
            try:
                if metadata_concurrency_val in (None, '',):
                    metadata_concurrency_int = None
                else:
                    metadata_concurrency_int = int(metadata_concurrency_val)
                    if metadata_concurrency_int < 1:
                        metadata_concurrency_int = 1
            except Exception:
                metadata_concurrency_int = None
            if metadata_concurrency_int is None:
                existing_import_settings.pop('metadata_concurrency', None)
            else:
                existing_import_settings['metadata_concurrency'] = metadata_concurrency_int
            if existing_import_settings:
                existing_config['import_settings'] = existing_import_settings
            elif 'import_settings' in existing_config:
                existing_config.pop('import_settings', None)
        existing_config['last_updated'] = datetime.now().isoformat()
        
        # Save updated config
        with open(config_path, 'w') as f:
            json.dump(existing_config, f, indent=2)
        
        return True
    except Exception as e:
        # Log error if we have current_app context, otherwise print
        try:
            current_app.logger.error(f"Error saving system config: {e}")
        except RuntimeError:
            print(f"Error saving system config: {e}")
        return False

def load_system_config():
    """Load system configuration from config file in data directory"""
    import json
    
    try:
        # Get data directory from current app config or default
        try:
            data_dir = current_app.config.get('DATA_DIR', 'data')
        except RuntimeError:
            # Working outside of application context, use default
            data_dir = 'data'
        
        config_path = os.path.join(data_dir, 'system_config.json')
        
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
    except (json.JSONDecodeError, Exception) as e:
        # Log warning if we have current_app context, otherwise print
        try:
            current_app.logger.warning(f"Error loading system config: {e}")
        except RuntimeError:
            print(f"Error loading system config: {e}")
    
    # Return defaults if file doesn't exist or is corrupted
    return {
        'site_name': 'MyBibliotheca',
        'server_timezone': 'UTC',
        'terminology_preference': 'genre',
        'background_config': {
            'type': 'default',
            'solid_color': '#667eea',
            'gradient_start': '#667eea',
            'gradient_end': '#764ba2',
            'gradient_direction': '135deg',
            'image_url': '',
            'image_position': 'cover'
        },
        'reading_log_defaults': {
            'default_pages_per_log': None,
            'default_minutes_per_log': None
        },
        'library_defaults': {
            'default_rows_per_page': None
        },
        'import_settings': {
            'metadata_concurrency': None
        }
    }

def save_ai_config(config):
    """Safely update AI configuration keys: write to .env and persist overrides JSON."""
    env_path = _get_root_env_path()
    ai_keys = [
        'OPENAI_API_KEY', 'OPENAI_BASE_URL', 'OPENAI_MODEL',
        'OLLAMA_BASE_URL', 'OLLAMA_MODEL',
        'AI_PROVIDER', 'AI_TIMEOUT', 'AI_MAX_TOKENS', 'AI_TEMPERATURE',
        'AI_BOOK_EXTRACTION_ENABLED', 'AI_BOOK_EXTRACTION_AUTO_SEARCH'
    ]
    # If user provided any provider/model key and did not explicitly set extraction flag, auto-enable extraction
    try:
        if 'AI_BOOK_EXTRACTION_ENABLED' not in config or str(config.get('AI_BOOK_EXTRACTION_ENABLED')).strip() == '':
            provider_keys = ['OPENAI_API_KEY','OLLAMA_MODEL','OPENAI_MODEL']
            if any(str(config.get(k,'')).strip() for k in provider_keys):
                # Auto-enable cover lookup / book extraction after initial configuration
                config['AI_BOOK_EXTRACTION_ENABLED'] = 'true'
    except Exception:
        pass
    try:
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(env_path), exist_ok=True)

        existing_lines = []
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                existing_lines = f.readlines()

        # Track which keys we've updated
        updated = {k: False for k in ai_keys}
        new_lines = []

        # Replace existing AI key lines in-place, preserve others verbatim
        for line in existing_lines:
            stripped = line.strip()
            if not stripped or stripped.startswith('#') or '=' not in stripped:
                new_lines.append(line)
                continue
            key, _sep, _val = stripped.partition('=')
            key = key.strip()
            if key in updated:
                value = str(config.get(key, '')).strip()
                new_lines.append(f"{key}={value}\n")
                updated[key] = True
            else:
                new_lines.append(line)

        # Append any missing AI keys at the end with a tiny header if needed
        missing = [k for k, done in updated.items() if not done]
        if missing:
            new_lines.append('\n# AI Configuration (managed by Admin UI)\n')
            for key in missing:
                value = str(config.get(key, '')).strip()
                new_lines.append(f"{key}={value}\n")

        with open(env_path, 'w') as f:
            f.writelines(new_lines)

        # Persist a JSON overlay for runtime (so containerized env without committed .env still remembers UI changes)
        try:
            try:
                data_dir = current_app.config.get('DATA_DIR', 'data')
            except Exception:
                data_dir = 'data'
            os.makedirs(data_dir, exist_ok=True)
            ai_json_path = os.path.join(data_dir, 'ai_config.json')
            # Only store AI keys to keep file minimal
            subset = {k: config.get(k, '') for k in ai_keys}
            import json
            with open(ai_json_path, 'w') as jf:
                json.dump(subset, jf, indent=2)
        except Exception as je:
            try:
                current_app.logger.warning(f"Failed writing ai_config.json: {je}")
            except Exception:
                pass

        # Invalidate cache
        try:
            current_app.config.pop('_cached_ai_config', None)
        except Exception:
            pass
        return True
    except Exception as e:
        try:
            current_app.logger.error(f"Error saving AI config: {e}")
        except Exception:
            print(f"Error saving AI config: {e}")
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
        # Perform actual delete now that repository supports it
        deleted = False
        try:
            if hasattr(user_service, 'delete_user_sync'):
                deleted = user_service.delete_user_sync(user_id)  # type: ignore
        except Exception as de:
            current_app.logger.error(f"Admin delete exception for user {user_id}: {de}")
            deleted = False
        if deleted:
            flash(f'User {username} deleted.', 'success')
        else:
            flash(f'Failed to delete user {username}.', 'error')
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
    safe_manager = get_safe_kuzu_manager()
    
    if request.method == 'POST':
        site_name = request.form.get('site_name', 'MyBibliotheca')
        server_timezone = request.form.get('server_timezone', 'UTC')
        terminology_preference = request.form.get('terminology_preference', 'genre')
        
        # Handle background configuration
        background_config = {
            'type': request.form.get('background_type', 'default'),
            'solid_color': request.form.get('solid_color', '#667eea'),
            'gradient_start': request.form.get('gradient_start', '#667eea'),
            'gradient_end': request.form.get('gradient_end', '#764ba2'),
            'gradient_direction': request.form.get('gradient_direction', '135deg'),
            'image_url': request.form.get('background_image_url', ''),
            'image_position': request.form.get('image_position', 'cover')
        }
        
        # Handle background image upload
        if 'background_image_file' in request.files:
            file = request.files['background_image_file']
            if file and file.filename:
                # Validate file type
                allowed_extensions = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
                if '.' in file.filename and file.filename.rsplit('.', 1)[1].lower() in allowed_extensions:
                    try:
                        import uuid
                        # Generate unique filename
                        file_extension = file.filename.rsplit('.', 1)[1].lower()
                        unique_filename = f"bg_{uuid.uuid4().hex}.{file_extension}"
                        
                        # Save to uploads/backgrounds directory in data folder
                        data_dir = getattr(current_app.config, 'DATA_DIR', None)
                        if data_dir:
                            upload_dir = os.path.join(data_dir, 'uploads', 'backgrounds')
                        else:
                            # Fallback to data directory relative to app root
                            base_dir = Path(current_app.root_path).parent
                            upload_dir = os.path.join(base_dir, 'data', 'uploads', 'backgrounds')
                        
                        # Ensure directory exists
                        os.makedirs(upload_dir, exist_ok=True)
                        upload_path = os.path.join(upload_dir, unique_filename)
                        file.save(upload_path)
                        
                        # Update background config with uploaded image URL
                        background_config['image_url'] = f"/uploads/backgrounds/{unique_filename}"
                        background_config['type'] = 'image'
                        
                        flash(f'Background image uploaded successfully: {file.filename}', 'success')
                    except Exception as e:
                        current_app.logger.error(f"Error uploading background image: {e}")
                        flash('Error uploading background image. Please try again.', 'error')
                else:
                    flash('Invalid file type. Please upload a PNG, JPG, JPEG, GIF, or WebP image.', 'error')
        
        # Reading defaults (optional)
        try:
            dp_raw = (request.form.get('default_pages_per_log') or '').strip()
            dm_raw = (request.form.get('default_minutes_per_log') or '').strip()
        except Exception:
            dp_raw = ''
            dm_raw = ''
        def _to_int_or_none(v: str):
            try:
                return int(v) if v not in (None, '',) else None
            except Exception:
                return None
        reading_log_defaults = {
            'default_pages_per_log': _to_int_or_none(dp_raw),
            'default_minutes_per_log': _to_int_or_none(dm_raw)
        }

        # Metadata/import configuration
        metadata_concurrency_raw = (request.form.get('metadata_concurrency') or '').strip()
        try:
            metadata_concurrency = int(metadata_concurrency_raw)
            if metadata_concurrency < 1:
                metadata_concurrency = 1
        except Exception:
            metadata_concurrency = None if metadata_concurrency_raw == '' else None

        # Save system configuration to .env file
        config = {
            'site_name': site_name,
            'server_timezone': server_timezone,
            'terminology_preference': terminology_preference,
            'background_config': background_config,
            'reading_log_defaults': reading_log_defaults,
            'library_defaults': {
                'default_rows_per_page': (request.form.get('default_rows_per_page') or '').strip() or None
            },
            'import_settings': {
                'metadata_concurrency': metadata_concurrency
            }
        }
        
        if save_system_config(config):
            if 'background_image_file' not in request.files or not request.files['background_image_file'].filename:
                flash('System settings saved successfully! Changes are now active.', 'success')
        else:
            flash('Error saving system settings. Please check permissions and try again.', 'error')
        
        if request.form.get('inline') == '1' or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            # Return updated partial for unified settings
            ctx = get_admin_settings_context()
            return render_template('admin/partials/server_config.html', **ctx)
    # Always bounce to unified settings page (auth.settings) for non-inline posts
    return redirect(url_for('auth.settings'))

    # NOTE: api_delete_user route moved to module scope for proper registration.
    
    # Reuse helper to build context for template
    ctx = get_admin_settings_context()
    
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
    
    # ctx already contains debug_manager; avoid passing duplicate keyword
    return render_template('admin/settings.html', title='Admin Settings', **ctx)

# ---------------- API User Deletion (diagnostic) -----------------
@admin.route('/api/users/<string:user_id>/delete', methods=['POST'])
@login_required
@admin_required
def api_delete_user(user_id):
    """Diagnostic JSON deletion endpoint used by unified settings UI to debug failures.
    Now serves as the primary deletion API (normal paths log at INFO)."""
    try:
        current_app.logger.info(f"[USER_DELETE_DEBUG_API] start user_id={user_id}")
        if not user_id:
            return jsonify({'ok': False, 'error': 'missing id'}), 400
        if getattr(current_user, 'id', None) == user_id:
            return jsonify({'ok': False, 'error': 'cannot delete self'}), 400
        # Optional admin password verification (if provided)
        admin_pwd = request.form.get('admin_password') or request.json.get('admin_password') if request.is_json else None  # type: ignore
        if admin_pwd and not current_user.check_password(admin_pwd):  # type: ignore
            return jsonify({'ok': False, 'error': 'admin password invalid'}), 400
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            current_app.logger.info(f"[USER_DELETE_DEBUG_API] user not found user_id={user_id}")
            return jsonify({'ok': False, 'error': 'not found'}), 404
        if getattr(user, 'is_admin', False):
            admin_count = user_service.get_admin_count_sync() if hasattr(user_service,'get_admin_count_sync') else 1
            if admin_count <= 1:
                return jsonify({'ok': False, 'error': 'last admin'}), 400
        deleted = False
        try:
            deleted = user_service.delete_user_sync(user_id) if hasattr(user_service,'delete_user_sync') else False
            current_app.logger.info(f"[USER_DELETE_DEBUG_API] service.delete returned {deleted}")
        except Exception as de:
            current_app.logger.error(f"[USER_DELETE_DEBUG_API] service.delete exception {de}")
        exists_flag = True
        try:
            repo = getattr(user_service, 'user_repo', None)
            if repo and hasattr(repo, 'safe_manager'):
                res = repo.safe_manager.execute_query("MATCH (u:User {id: $uid}) RETURN COUNT(u) as c", {"uid": user_id})
                from app.services.kuzu_service_facade import _convert_query_result_to_list as _cvt
                data = _cvt(res)
                exists_flag = bool(data and int(data[0].get('c',0))>0)
            current_app.logger.info(f"[USER_DELETE_DEBUG_API] exists_after={exists_flag}")
        except Exception as ce:
            current_app.logger.error(f"[USER_DELETE_DEBUG_API] existence check exception {ce}")
        return jsonify({'ok': deleted and not exists_flag, 'deleted': deleted, 'exists_after': exists_flag})
    except Exception as e:
        current_app.logger.error(f"[USER_DELETE_DEBUG_API] fatal error {e}")
        return jsonify({'ok': False, 'error': 'server error'}), 500

def get_admin_settings_context():
    """Helper to assemble context variables for admin settings forms (reused in partial)."""
    system_config = load_system_config()
    current_site_name = system_config.get('site_name', os.getenv('SITE_NAME', 'MyBibliotheca'))
    current_timezone = system_config.get('server_timezone', os.getenv('TIMEZONE', 'UTC'))
    current_terminology = system_config.get('terminology_preference', 'genre')
    available_timezones = pytz.all_timezones
    common_timezones = [
        'UTC',
        'US/Eastern', 'US/Central', 'US/Mountain', 'US/Pacific',
        'Europe/London', 'Europe/Paris', 'Europe/Berlin', 'Europe/Rome',
        'Asia/Tokyo', 'Asia/Shanghai', 'Asia/Kolkata',
        'Australia/Sydney', 'Australia/Melbourne',
        'America/Toronto', 'America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles'
    ]
    try:
        from .debug_system import get_debug_manager
        debug_manager = get_debug_manager()
    except Exception:
        class MockDebugManager:
            def is_debug_enabled(self):
                return False
        debug_manager = MockDebugManager()
    return {
        'site_name': current_site_name,
        'server_timezone': current_timezone,
        'terminology_preference': current_terminology,
        'common_timezones': common_timezones,
        'available_timezones': sorted(available_timezones),
        'ai_config': load_ai_config(),
        'debug_manager': debug_manager,
        'background_config': system_config.get('background_config', {
            'type': 'default',
            'solid_color': '#667eea',
            'gradient_start': '#667eea',
            'gradient_end': '#764ba2',
            'gradient_direction': '135deg',
            'image_url': '',
            'image_position': 'cover'
        }),
        'reading_log_defaults': system_config.get('reading_log_defaults', {
            'default_pages_per_log': None,
            'default_minutes_per_log': None
        }),
        'library_defaults': system_config.get('library_defaults', {
            'default_rows_per_page': None
        }),
        'import_settings': system_config.get('import_settings', {
            'metadata_concurrency': None
        })
    }

@admin.route('/settings/config_partial')
@login_required
@admin_required
def settings_config_partial():
    """Return just the server configuration form for embedding in unified settings page."""
    ctx = get_admin_settings_context()
    return render_template('admin/partials/server_config.html', **ctx)

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
        
        # Inline (AJAX/unified settings) support
        if request.form.get('inline') == '1' or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            from app.admin import get_admin_settings_context, load_ai_config
            ctx = get_admin_settings_context()
            ctx['ai_config'] = load_ai_config()
            # Return just the AI panel partial so caller can replace content
            return render_template('settings/partials/server_ai.html', **ctx)
        ref = request.referrer or ''
        if '/settings' in ref and '/admin/settings' not in ref:
            return redirect(url_for('auth.settings', section='server', panel='ai'))
        return redirect(url_for('admin.settings', section='ai'))
        
    except Exception as e:
        current_app.logger.error(f"Error updating AI settings: {e}")
    flash('Error updating AI settings. Please try again.', 'danger')
    return redirect(url_for('admin.settings', section='ai'))

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
