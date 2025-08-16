from flask import Blueprint, render_template, redirect, url_for, flash, request, session, current_app, jsonify
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash, generate_password_hash
from app.domain.models import User
from app.services import user_service, book_service, reading_log_service
from app.infrastructure.kuzu_graph import safe_execute_kuzu_query
from app.services.kuzu_service_facade import _convert_query_result_to_list  # Reuse helper for query results
from wtforms import IntegerField, SubmitField
from wtforms.validators import Optional, NumberRange
from flask_wtf import FlaskForm
from .forms import (LoginForm, RegistrationForm, UserProfileForm, ChangePasswordForm,
                   PrivacySettingsForm, ForcedPasswordChangeForm, SetupForm, ReadingStreakForm)
from .debug_utils import debug_route, debug_auth, debug_csrf, debug_session
from datetime import datetime, timezone
from typing import cast, Any
from pathlib import Path

auth = Blueprint('auth', __name__)

def _safe_get_row_value(row: Any, index: int) -> Any:
    """Safely extract a value from a KuzuDB row at the given index."""
    if isinstance(row, list):
        return row[index] if index < len(row) else None
    elif isinstance(row, dict):
        keys = list(row.keys())
        return row[keys[index]] if index < len(keys) else None
    else:
        try:
            return row[index]  # type: ignore
        except (IndexError, KeyError, TypeError):
            return None

@auth.route('/setup', methods=['GET', 'POST'])
@debug_route('SETUP')
def setup():
    """Initial setup route - redirects to new onboarding system"""
    debug_auth("="*60)
    debug_auth(f"Setup route accessed - Method: {request.method}")
    debug_auth("Redirecting to new onboarding system")
    
    # Check if any users already exist using Kuzu service
    try:
        user_count = cast(int, user_service.get_user_count_sync())
        debug_auth(f"Current user count in database: {user_count}")
        if user_count and user_count > 0:
            debug_auth("Users already exist, redirecting to login")
            flash('Setup has already been completed.', 'info')
            return redirect(url_for('auth.login'))
    except Exception as e:
        debug_auth(f"Error checking user count: {e}")
        # If we can't check, assume no users exist and continue with setup
    
    # Handle POST request for simple setup form
    if request.method == 'POST':
        debug_auth("Processing simple setup form submission")
        username = request.form.get('username')
        email = request.form.get('email')
        password = request.form.get('password')
        confirm_password = request.form.get('confirm_password')
        
        # Basic validation
        if not all([username, email, password, confirm_password]):
            flash('All fields are required.', 'error')
            return render_template('auth/simple_setup.html')
        
        if password != confirm_password:
            flash('Passwords do not match.', 'error')
            return render_template('auth/simple_setup.html')
        
        # Ensure we have non-None values after validation
        assert username is not None
        assert email is not None
        assert password is not None
        
        try:
            # Create the admin user
            from werkzeug.security import generate_password_hash
            import uuid
            from datetime import datetime, timezone
            
            admin_user = User(
                id=str(uuid.uuid4()),
                username=username,
                email=email,
                password_hash=generate_password_hash(password),
                is_admin=True,
                is_active=True,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc)
            )
            
            # Save the user
            created_user = user_service.create_user_sync(
                username=admin_user.username,
                email=admin_user.email,
                password_hash=admin_user.password_hash,
                is_admin=admin_user.is_admin
            )
            if created_user:
                flash('Admin account created successfully! You can now log in.', 'success')
                return redirect(url_for('auth.login'))
            else:
                flash('Failed to create admin account. Please try again.', 'error')
                return render_template('auth/simple_setup.html')
                
        except Exception as e:
            debug_auth(f"Error creating admin user: {e}")
            flash('An error occurred while creating the admin account. Please try again.', 'error')
            return render_template('auth/simple_setup.html')
    
    # Handle GET request - check if setup is actually needed
    try:
        user_count = cast(int, user_service.get_user_count_sync())
        debug_auth(f"Setup route GET: User count is {user_count}")
        
        if user_count and user_count > 0:
            debug_auth("Setup already completed, redirecting to login")
            flash('Setup has already been completed.', 'info')
            return redirect(url_for('auth.login'))
        
        # No users exist, proceed with onboarding
        debug_auth("No users found, starting onboarding")
        flash('Welcome to Bibliotheca! Let\'s set up your library.', 'info')
        return redirect(url_for('onboarding.start'))
        
    except Exception as e:
        debug_auth(f"Error checking user count in setup route: {e}")
        # If we can't check user count, try onboarding, fall back to simple setup
        flash('Welcome to Bibliotheca! Let\'s set up your library.', 'info')
        try:
            return redirect(url_for('onboarding.start'))
        except Exception:
            # Fallback: show simple setup form if onboarding system is not available
            debug_auth("Onboarding system not available, showing simple setup form")
            return render_template('auth/simple_setup.html')

@auth.route('/setup/status')
def setup_status():
    """API endpoint to check setup status - useful for troubleshooting"""
    try:
        user_count = cast(int, user_service.get_user_count_sync())
        return {
            'setup_completed': user_count > 0,
            'user_count': user_count,
            'csrf_enabled': current_app.config.get('WTF_CSRF_ENABLED', False),
            'debug_mode': current_app.config.get('DEBUG_MODE', False),
            'kuzu_connected': True  # If we got here, Kuzu is working
        }
    except Exception as e:
        return {
            'setup_completed': False,
            'user_count': 0,
            'error': str(e),
            'kuzu_connected': False
        }, 500

@auth.route('/login', methods=['GET', 'POST'])
@debug_route('AUTH')
def login():
    debug_auth("Login route accessed")
    
    if current_user.is_authenticated:
        debug_auth("User already authenticated, redirecting to index")
        return redirect(url_for('main.index'))
    
    form = LoginForm()
    debug_auth(f"Form created, CSRF token should be generated")
    
    # Debug CSRF token generation
    from flask_wtf.csrf import generate_csrf
    try:
        csrf_token = generate_csrf()
        debug_csrf(f"Generated CSRF token: {csrf_token[:10]}...")
    except Exception as e:
        debug_csrf(f"Error generating CSRF token: {e}")
    
    if form.validate_on_submit():
        debug_auth(f"Login form submitted for user: {form.username.data}")
        debug_csrf("Form validation passed, checking CSRF")
        
        # Ensure form data is not None
        username_or_email = form.username.data
        password = form.password.data
        
        if not username_or_email or not password:
            flash('Username/email and password are required.', 'error')
            return render_template('auth/login.html', title='Sign In', form=form)
        
        # Try to find user by username or email using Kuzu service
        user = user_service.get_user_by_username_or_email_sync(username_or_email)
        
        if user:
            debug_auth(f"User found: {user.username} (ID: {user.id})")
            # Check if account is locked
            if user.is_locked():
                debug_auth("Account is locked")
                flash('Account is temporarily locked due to too many failed login attempts. Please try again later.', 'error')
                return redirect(url_for('auth.login'))
            
            # Check if account is active
            if not user.is_active:
                debug_auth("Account is inactive")
                flash('Your account has been deactivated. Please contact an administrator.', 'error')
                return redirect(url_for('auth.login'))
            
            # Check password
            if user.check_password(password):
                debug_auth("Password check passed")
                # Successful login - reset failed login attempts if any
                if user.failed_login_attempts > 0 or user.locked_until:
                    user.reset_failed_login()
                    user_service.update_user_sync(user)
                
                login_user(user, remember=form.remember_me.data)
                debug_auth(f"User logged in successfully: {user.username}")
                
                # Check if user must change password
                if user.password_must_change:
                    debug_auth("User must change password - redirecting to forced password change")
                    flash('You must change your password before continuing.', 'warning')
                    return redirect(url_for('auth.forced_password_change'))
                
                next_page = request.args.get('next')
                if not next_page or not next_page.startswith('/'):
                    next_page = url_for('main.index')
                debug_auth(f"Redirecting to: {next_page}")
                flash(f'Welcome back, {user.username}!', 'success')
                return redirect(next_page)
            else:
                debug_auth("Password check failed")
                # Failed password - increment failed attempts and save to Kuzu
                user.increment_failed_login()
                user_service.update_user_sync(user)
                attempts_left = max(0, 5 - user.failed_login_attempts)
                if attempts_left > 0:
                    flash(f'Invalid password. You have {attempts_left} attempts remaining.', 'error')
                else:
                    flash('Account locked due to too many failed attempts. Please try again in 30 minutes.', 'error')
        else:
            debug_auth("User not found")
            # User not found
            flash('Invalid username/email or password', 'error')
    
    return render_template('auth/login.html', title='Sign In', form=form)

@auth.route('/logout')
@login_required
def logout():
    username = current_user.username
    
    # Clear all user session data first
    session.clear()
    
    # Then call logout_user
    logout_user()
    
    # Force session regeneration by creating a new session
    session.permanent = False
    
    flash(f'Goodbye, {username}!', 'info')
    return redirect(url_for('main.index'))

@auth.route('/register', methods=['GET', 'POST'])
@login_required
def register():
    # Only admin users can create new users
    if not current_user.is_admin:
        flash('Access denied. Only administrators can create new users.', 'error')
        return redirect(url_for('main.index'))
    
    form = RegistrationForm()
    if form.validate_on_submit():
        try:
            # Check if this is the very first user in the system
            user_count = user_service.get_user_count_sync()
            is_first_user = user_count == 0
            
            # Ensure form data is not None
            username = form.username.data
            email = form.email.data
            password = form.password.data
            
            if not username or not email or not password:
                flash('All fields are required.', 'error')
                return render_template('auth/register.html', title='Create New User', form=form)
            
            # Create user through Kuzu service
            password_hash = generate_password_hash(password)
            domain_user = user_service.create_user_sync(
                username=username,
                email=email,
                password_hash=password_hash,
                is_admin=is_first_user,
                password_must_change=True  # All new users must change password on first login
            )
            
            if is_first_user:
                flash('Congratulations! As the first user, you have been granted admin privileges. You must change your password on first login.', 'info')
            else:
                if domain_user:
                    flash(f'User {domain_user.username} has been created successfully! They will be required to change their password on first login.', 'success')
                else:
                    flash('User has been created successfully! They will be required to change their password on first login.', 'success')
            
            return redirect(url_for('admin.users'))
        except ValueError as e:
            flash(str(e), 'error')
    
    return render_template('auth/register.html', title='Create New User', form=form)

@auth.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    form = UserProfileForm(current_user.username, current_user.email)
    
    if form.validate_on_submit():
        try:
            # Validate form data
            username = form.username.data
            email = form.email.data
            
            if not username or not email:
                flash('Username and email are required.', 'error')
                return render_template('auth/profile.html', title='Profile', form=form)
            
            # Get current user from Kuzu to ensure we have the latest data
            user_from_kuzu = user_service.get_user_by_id_sync(current_user.id)
            if user_from_kuzu:
                # Update profile fields
                user_from_kuzu.username = username
                user_from_kuzu.email = email
                
                # Save through Kuzu service
                updated_user = user_service.update_user_sync(user_from_kuzu)
                if updated_user:
                    # Update current_user object for immediate UI reflection
                    current_user.username = updated_user.username
                    current_user.email = updated_user.email
                    flash('Your profile has been updated.', 'success')
                    return redirect(url_for('auth.profile'))
                else:
                    flash('Failed to update profile.', 'error')
            else:
                flash('User not found.', 'error')
        except Exception as e:
            flash(f'Failed to update profile: {str(e)}', 'error')
    elif request.method == 'GET':
        form.username.data = current_user.username
        form.email.data = current_user.email
    
    return render_template('auth/profile.html', title='Profile', form=form)

@auth.route('/change_password', methods=['GET', 'POST'])
@login_required
def change_password():
    form = ChangePasswordForm()
    
    if form.validate_on_submit():
        current_password = form.current_password.data
        new_password = form.new_password.data
        
        if not current_password or not new_password:
            flash('Both current and new passwords are required.', 'error')
            return render_template('auth/change_password.html', title='Change Password', form=form)
            
        if current_user.check_password(current_password):
            try:
                # Generate new password hash
                from werkzeug.security import generate_password_hash
                new_password_hash = generate_password_hash(new_password)
                
                # Get current user from Kuzu to update password
                user_from_kuzu = user_service.get_user_by_id_sync(current_user.id)
                if user_from_kuzu:
                    # Update password fields
                    user_from_kuzu.password_hash = new_password_hash
                    user_from_kuzu.password_must_change = False
                    
                    # Save through Kuzu service
                    updated_user = user_service.update_user_sync(user_from_kuzu)
                    if updated_user:
                        # Update current_user object for immediate reflection
                        current_user.password_hash = updated_user.password_hash
                        current_user.password_must_change = updated_user.password_must_change
                        flash('Your password has been changed.', 'success')
                        return redirect(url_for('auth.profile'))
                    else:
                        flash('Failed to update password.', 'error')
                else:
                    flash('User not found.', 'error')
            except Exception as e:
                flash(f'Failed to update password: {str(e)}', 'error')
        else:
            flash('Current password is incorrect.', 'error')
    
    return render_template('auth/change_password.html', title='Change Password', form=form)

@auth.route('/forced_password_change', methods=['GET', 'POST'])
@login_required
@debug_route('AUTH')
def forced_password_change():
    debug_auth("Forced password change route accessed")
    
    # If user doesn't need to change password, redirect to main page
    if not current_user.password_must_change:
        debug_auth("User doesn't need to change password, redirecting to index")
        return redirect(url_for('main.index'))
    
    form = ForcedPasswordChangeForm()
    
    if form.validate_on_submit():
        debug_auth("Forced password change form submitted")
        debug_csrf("Form validation passed for forced password change")
        
        new_password = form.new_password.data
        if not new_password:
            flash('New password is required.', 'error')
            return render_template('auth/forced_password_change.html', title='Change Required Password', form=form)
        
        try:
            # Generate new password hash
            from werkzeug.security import generate_password_hash
            new_password_hash = generate_password_hash(new_password)
            
            # Get current user from Kuzu to update password
            user_from_kuzu = user_service.get_user_by_id_sync(current_user.id)
            if user_from_kuzu:
                # Update password fields
                user_from_kuzu.password_hash = new_password_hash
                user_from_kuzu.password_must_change = False
                
                # Save through Kuzu service
                updated_user = user_service.update_user_sync(user_from_kuzu)
                if updated_user:
                    # Update current_user object for immediate reflection
                    current_user.password_hash = updated_user.password_hash
                    current_user.password_must_change = updated_user.password_must_change
                    debug_auth("Password changed successfully")
                    flash('Your password has been changed successfully. You can now continue using the application.', 'success')
                    return redirect(url_for('main.index'))
                else:
                    flash('Failed to update password.', 'error')
            else:
                flash('User not found.', 'error')
        except Exception as e:
            debug_auth(f"Password update failed: {e}")
            flash(f'Failed to update password: {str(e)}', 'error')
    else:
        if request.method == 'POST':
            debug_csrf("Form validation failed for forced password change")
            debug_csrf(f"Form errors: {form.errors}")
    
    debug_auth("Rendering forced password change template")
    return render_template('auth/forced_password_change.html', title='Change Required Password', form=form)

@auth.route('/debug_info')
@login_required
def debug_info():
    """Debug route to display comprehensive debug information (only if debug mode enabled)"""
    from .debug_utils import get_debug_info
    from flask import current_app, jsonify
    
    if not current_app.config.get('DEBUG_MODE', False):
        flash('Debug mode is not enabled.', 'error')
        return redirect(url_for('main.index'))
    
    if not current_user.is_admin:
        flash('Access denied. Admin privileges required.', 'error')
        return redirect(url_for('main.index'))
    
    debug_data = get_debug_info()
    return jsonify(debug_data)

@auth.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    """Main user settings page."""
    # Resolve site name from persisted system config (fallback to env/default)
    try:
        from app.admin import load_system_config
        cfg = load_system_config() or {}
        site_name = cfg.get('site_name') or 'MyBibliotheca'
    except Exception:
        import os
        site_name = os.getenv('SITE_NAME', 'MyBibliotheca')
    # Collect lightweight aggregate stats for overview tiles
    stats = {
        'books': 0,
        'people': 0,
        'reading_logs': 0,
        'users': 0,
        'active_users': 0,
        'admins': 0,
        'avg_books_per_user': 0.0,
        'app_version': 'unknown',
        'utc_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
    }
    try:
        # Users counts (service methods already exist)
        stats['users'] = user_service.get_user_count_sync() if hasattr(user_service, 'get_user_count_sync') else 0  # type: ignore
        stats['active_users'] = user_service.get_active_user_count_sync() if hasattr(user_service, 'get_active_user_count_sync') else 0  # type: ignore
        stats['admins'] = user_service.get_admin_count_sync() if hasattr(user_service, 'get_admin_count_sync') else 0  # type: ignore
    except Exception as e:
        current_app.logger.warning(f"Settings stats user counts failed: {e}")
    # Generic helper to run count queries
    def _simple_count(cypher: str, key: str):
        try:
            result = safe_execute_kuzu_query(cypher)
            rows = _convert_query_result_to_list(result)
            if rows:
                # Row structure may be {'col_0': value} or {'result': value}
                first = rows[0]
                val = first.get('col_0') or first.get('result') or 0
                stats[key] = int(val) if isinstance(val, (int, float)) else 0
        except Exception as e:
            current_app.logger.debug(f"Count query for {key} failed: {e}")
    _simple_count("MATCH (b:Book) RETURN COUNT(b)", 'books')
    _simple_count("MATCH (p:Person) RETURN COUNT(p)", 'people')
    _simple_count("MATCH (rl:ReadingLog) RETURN COUNT(rl)", 'reading_logs')
    # Fallbacks if user-related counts are zero (sync helpers missing or returned 0)
    if stats['users'] == 0:
        _simple_count("MATCH (u:User) RETURN COUNT(u)", 'users')
    if stats['admins'] == 0:
        _simple_count("MATCH (u:User) WHERE u.is_admin = true RETURN COUNT(u)", 'admins')
    if stats['active_users'] == 0:
        # Assume is_active flag; default to treating missing flag as active
        _simple_count("MATCH (u:User) WHERE coalesce(u.is_active, true) = true RETURN COUNT(u)", 'active_users')
    try:
        if stats['users']:
            stats['avg_books_per_user'] = round(stats['books'] / stats['users'], 2)
    except Exception:
        pass
    # Try to read version from pyproject once (could cache later)
    try:
        import tomllib, os
        pyproject_path = os.path.join(current_app.root_path, '..', 'pyproject.toml')
        if os.path.exists(pyproject_path):
            with open(pyproject_path, 'rb') as f:
                data = tomllib.load(f)
                stats['app_version'] = data.get('project', {}).get('version', stats['app_version'])
    except Exception as e:
        current_app.logger.debug(f"Version load failed: {e}")
    return render_template('settings.html', title='Settings', site_name=site_name, stats=stats)

# ---------------- Inline Settings Partials (AJAX) -----------------
@auth.route('/settings/partial/profile', methods=['GET', 'POST'])
@login_required
def settings_profile_partial():
    form = UserProfileForm(current_user.username, current_user.email)
    if form.validate_on_submit():
        try:
            current_user.username = form.username.data  # type: ignore
            current_user.email = form.email.data  # type: ignore
            user_service.update_user_sync(current_user)
            flash('Profile updated successfully!', 'success')
            # Stay on panel if HTMX / AJAX request
            if request.headers.get('HX-Request') or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return render_template('settings/partials/profile_form.html', form=form)
            return redirect(url_for('auth.settings'))
        except Exception as e:
            current_app.logger.error(f"Inline profile update failed: {e}")
            flash('Error updating profile.', 'error')
    elif request.method == 'GET':
        # Ensure fields are pre-populated with current values when first loaded
        try:
            form.username.data = getattr(current_user, 'username', '')  # type: ignore
            form.email.data = getattr(current_user, 'email', '')  # type: ignore
        except Exception:
            pass
    return render_template('settings/partials/profile_form.html', form=form)

@auth.route('/settings/partial/password', methods=['GET'])
@login_required
def settings_password_partial():
    form = ChangePasswordForm()
    return render_template('settings/partials/password_form.html', form=form)

@auth.route('/settings/partial/password', methods=['POST'])
@login_required
def settings_password_submit():
    form = ChangePasswordForm()
    if form.validate_on_submit():
        try:
            # Validate current password
            if not current_user.check_password(form.current_password.data):  # type: ignore
                flash('Current password is incorrect.', 'error')
            else:
                current_user.set_password(form.new_password.data)  # type: ignore
                user_service.update_user_sync(current_user)
                flash('Password updated successfully!', 'success')
                if request.headers.get('HX-Request') or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                    return render_template('settings/partials/password_form.html', form=form)
                return redirect(url_for('auth.settings'))
        except Exception as e:
            current_app.logger.error(f"Error updating password inline: {e}")
            flash('Error updating password.', 'error')
    return render_template('settings/partials/password_form.html', form=form)

@auth.route('/settings/partial/privacy', methods=['GET', 'POST'])
@login_required
def settings_privacy_partial():
    from app.forms import PrivacySettingsForm, ReadingStreakForm
    p_form = PrivacySettingsForm()
    streak_form = ReadingStreakForm()
    # Populate timezone choices as in privacy_settings route
    try:
        import pytz
        common_timezones = [
            'UTC','US/Eastern','US/Central','US/Mountain','US/Pacific',
            'Europe/London','Europe/Paris','Europe/Berlin','Europe/Rome',
            'Asia/Tokyo','Asia/Shanghai','Asia/Kolkata',
            'Australia/Sydney','Australia/Melbourne'
        ]
        p_form.timezone.choices = [(tz, tz) for tz in common_timezones]
    except Exception:
        p_form.timezone.choices = [('UTC','UTC')]
    if p_form.validate_on_submit():
        try:
            # Always fetch a fresh User domain object from service to avoid dict-like proxy issues
            user_obj = None
            try:
                user_obj = user_service.get_user_by_id_sync(getattr(current_user, 'id', None))
            except Exception as fe:
                current_app.logger.warning(f"Failed to refetch user for privacy update, falling back to current_user: {fe}")
            if user_obj is None:
                # Fallback: mutate current_user if it quacks like domain object
                user_obj = current_user  # type: ignore
            # Apply changes
            setattr(user_obj, 'share_current_reading', p_form.share_current_reading.data)
            setattr(user_obj, 'share_reading_activity', p_form.share_reading_activity.data)
            updated = user_service.update_user_sync(user_obj)  # type: ignore
            if updated:
                # Mirror onto session's current_user for immediate reflection
                try:
                    current_user.share_current_reading = updated.share_current_reading  # type: ignore
                    current_user.share_reading_activity = updated.share_reading_activity  # type: ignore
                except Exception:
                    pass
                flash('Privacy settings updated.', 'success')
            else:
                flash('Failed to persist privacy settings.', 'error')
            if request.headers.get('HX-Request') or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return render_template('settings/partials/privacy_form.html', form=p_form, streak_form=streak_form)
            return redirect(url_for('auth.settings'))
        except Exception as e:
            current_app.logger.error(f"Privacy inline update failed: {e}")
            flash('Error updating privacy settings.', 'error')
    else:
        try:
            p_form.share_current_reading.data = getattr(current_user, 'share_current_reading', False)
            p_form.share_reading_activity.data = getattr(current_user, 'share_reading_activity', False)
        except Exception:
            pass
    return render_template('settings/partials/privacy_form.html', form=p_form, streak_form=streak_form)

@auth.route('/settings/partial/data/<string:panel>')
@login_required
def settings_data_partial(panel: str):
    if panel not in {'import_books','import_reading','backup','export_logs'}:
        return '<div class="text-danger small">Unknown panel.</div>'
    if panel == 'import_books':
        return render_template('settings/partials/data_import_books.html')
    if panel == 'import_reading':
        return render_template('settings/partials/data_import_reading.html')
    if panel == 'backup':
        if not current_user.is_admin:
            return '<div class="text-danger small">Not authorized.</div>'
        # Inline backup manager: replicate logic from simple_backup.index
        try:
            from app.services.simple_backup_service import get_simple_backup_service
            svc = get_simple_backup_service()
            backups = svc.list_backups()
            backups.sort(key=lambda b: b.created_at, reverse=True)
            enhanced = []
            from datetime import datetime as _dt
            from pathlib import Path as _Path
            for b in backups:
                age_delta = _dt.now() - b.created_at
                if age_delta.days > 0:
                    age = f"{age_delta.days} day{'s' if age_delta.days != 1 else ''} ago"
                elif age_delta.seconds > 3600:
                    hrs = age_delta.seconds // 3600
                    age = f"{hrs} hour{'s' if hrs != 1 else ''} ago"
                elif age_delta.seconds > 60:
                    mins = age_delta.seconds // 60
                    age = f"{mins} minute{'s' if mins != 1 else ''} ago"
                else:
                    age = 'Just now'
                size_mb = b.file_size / (1024 * 1024)
                size_formatted = f"{size_mb:.1f} MB"
                if b.metadata and 'original_size' in b.metadata:
                    db_sz = b.metadata['original_size']
                    db_fmt = f"{db_sz / (1024 * 1024):.1f} MB"
                else:
                    db_fmt = 'Unknown'
                enhanced.append({
                    'id': b.id,
                    'name': b.name,
                    'description': b.description,
                    'created_at': b.created_at,
                    'file_path': b.file_path,
                    'file_size': b.file_size,
                    'age': age,
                    'size_formatted': size_formatted,
                    'database_size_formatted': db_fmt,
                    'valid': _Path(b.file_path).exists()
                })
            stats = svc.get_backup_stats()
            try:
                backup_settings = {
                    'enabled': svc._settings.get('enabled', True),
                    'frequency': svc._settings.get('frequency', 'daily'),
                    'retention_days': svc._settings.get('retention_days', 14),
                    'last_run': svc._settings.get('last_run'),
                    'scheduled_hour': svc._settings.get('scheduled_hour', 2),
                    'scheduled_minute': svc._settings.get('scheduled_minute', 30)
                }
            except Exception:
                backup_settings = {
                    'enabled': True,
                    'frequency': 'daily',
                    'retention_days': 14,
                    'last_run': None,
                    'scheduled_hour': 2,
                    'scheduled_minute': 30
                }
            return render_template('settings/partials/data_backup_manager.html', backups=enhanced, backup_stats=stats, backup_settings=backup_settings)
        except Exception as e:
            current_app.logger.error(f"Inline backup manager load failed: {e}")
            return '<div class="text-danger small">Failed to load backup manager.</div>'
    if panel == 'export_logs':
        return '<div class="card p-3"><h5 class="mb-2">Export Reading Logs</h5><p class="text-muted small mb-2">Download your reading activity as CSV.</p><a class="btn btn-sm btn-outline-secondary" href="' + url_for('reading_logs.export_my_logs') + '">Export</a></div>'
    return render_template('settings/partials/data_backup.html')

@auth.route('/settings/ai/ollama/models', methods=['POST'])
@login_required
def settings_ai_ollama_models():
    """Inline unified settings endpoint to fetch available Ollama models."""
    if not current_user.is_admin:
        return jsonify({'ok': False, 'error': 'Not authorized'}), 403
    base_url = (request.form.get('base_url') or '').strip()
    if not base_url:
        return jsonify({'ok': False, 'error': 'Missing base_url'}), 400
    if base_url.endswith('/v1'):
        base_url = base_url[:-3]
    base_url = base_url.rstrip('/')
    tags_url = base_url + '/api/tags'
    try:
        import requests  # type: ignore
        r = requests.get(tags_url, timeout=5)
        r.raise_for_status()
        data = r.json() if r.content else {}
        models = []
        for m in data.get('models', []):
            name = m.get('name') or m.get('model')
            if name and name not in models:
                models.append(name)
        return jsonify({'ok': True, 'models': models})
    except Exception as e:
        current_app.logger.error(f"Ollama models fetch failed: {e}")
        return jsonify({'ok': False, 'error': 'Failed to fetch models'}), 400

@auth.route('/settings/partial/server/<string:panel>', methods=['GET','POST'])
@login_required
def settings_server_partial(panel: str):
    if not current_user.is_admin:
        return '<div class="text-danger small">Not authorized.</div>'
    if panel == 'users':
        # Recreate admin.users view inline (no pagination controls yet)
        search = request.args.get('search', '', type=str)
        from app.services.kuzu_async_helper import run_async
        from app.domain.models import User as DomainUser
        from datetime import datetime, timezone as _tz
        def _now():
            try:
                from app.domain.models import now_utc
                return now_utc()
            except Exception:
                return datetime.now(_tz.utc)
        users_render = []
        try:
            repo = getattr(user_service, 'user_repo', None)
            if repo and hasattr(repo, 'get_all'):
                raw_list = run_async(repo.get_all(limit=2000))  # type: ignore
                for row in raw_list:
                    try:
                        # row may be dict-like
                        uid = row.get('id') if isinstance(row, dict) else getattr(row, 'id', None)
                        if not uid:
                            continue
                        username_val = (row.get('username') if isinstance(row, dict) else getattr(row,'username','')) or ''
                        email_val = (row.get('email') if isinstance(row, dict) else getattr(row,'email','')) or ''
                        user_obj = DomainUser(
                            id=uid,
                            username=str(username_val),
                            email=str(email_val),
                            is_admin=bool(row.get('is_admin', False) if isinstance(row, dict) else getattr(row,'is_admin', False)),
                            is_active=bool(row.get('is_active', True) if isinstance(row, dict) else getattr(row,'is_active', True)),
                            password_hash=(row.get('password_hash','') if isinstance(row, dict) else getattr(row,'password_hash','')) or '',
                        )
                        # created_at may be timestamp or string
                        if isinstance(row, dict) and 'created_at' in row and row['created_at']:
                            try:
                                # If numeric timestamp, convert; else parse iso
                                ca = row['created_at']
                                from datetime import datetime
                                if isinstance(ca, (int,float)):
                                    user_obj.created_at = datetime.fromtimestamp(ca/1000, _tz.utc)
                                elif isinstance(ca, str):
                                    user_obj.created_at = datetime.fromisoformat(ca.replace('Z',''))
                            except Exception:
                                pass
                        users_render.append(user_obj)
                    except Exception as ie:
                        current_app.logger.warning(f"User row build failed: {ie}")
        except Exception as e:
            current_app.logger.error(f"Inline users load error (repo path): {e}")
        if search:
            s = search.lower()
            users_render = [u for u in users_render if s in (u.username or '').lower() or s in (u.email or '').lower()]
        # Sort by created_at desc for consistency
        try:
            users_render.sort(key=lambda u: getattr(u,'created_at', _now()), reverse=True)
        except Exception:
            pass
        return render_template('settings/partials/server_users_full.html', users=users_render, search=search)
    if panel == 'user_edit':
        user_id = request.args.get('user_id') or request.form.get('user_id')
        if not user_id:
            return '<div class="text-danger small">User ID missing.</div>'
        target_user = None
        try:
            target_user = user_service.get_user_by_id_sync(user_id)
        except Exception as e:
            current_app.logger.error(f"User edit load error: {e}")
            return '<div class="text-danger small">Failed to load user.</div>'
        if not target_user:
            return '<div class="text-danger small">User not found.</div>'
        # Handle POST actions
        if request.method == 'POST':
            action = request.form.get('action')
            # --- TEMP DEBUG LOGGING START (use error level so it appears with LOG_LEVEL=error) ---
            try:
                current_app.logger.error(f"[USER_DELETE_DEBUG] POST entry action={action} user_id={user_id} form_keys={list(request.form.keys())} headers={{'HX':request.headers.get('HX-Request'), 'XHR': request.headers.get('X-Requested-With')}}")
            except Exception:
                pass
            from app.services.simple_backup_service import get_simple_backup_service
            backup_service = get_simple_backup_service()
            def _trigger_backup(reason: str):
                try:
                    backup_service.create_backup(reason=reason)
                except Exception as be:
                    current_app.logger.warning(f"Auto-backup failed after user change: {be}")
            if action == 'update_profile':
                current_app.logger.error(f"[USER_DELETE_DEBUG] update_profile path entered for {user_id}")
                new_username = request.form.get('username','').strip()
                new_email = request.form.get('email','').strip()
                if new_username and new_email:
                    target_user.username = new_username  # type: ignore
                    target_user.email = new_email  # type: ignore
                    user_service.update_user_sync(target_user)
                    _trigger_backup('user_profile_update')
                    flash('User profile updated & backup created.', 'success')
                else:
                    flash('Username and email required.', 'error')
            elif action == 'reset_password':
                current_app.logger.error(f"[USER_DELETE_DEBUG] reset_password path entered for {user_id}")
                pwd1 = request.form.get('password','')
                pwd2 = request.form.get('confirm_password','')
                if pwd1 and pwd1 == pwd2:
                    target_user.set_password(pwd1)  # type: ignore
                    user_service.update_user_sync(target_user)
                    _trigger_backup('user_password_reset')
                    flash('Password reset & backup created.', 'success')
                else:
                    flash('Passwords must match.', 'error')
            elif action == 'toggle_active':
                current_app.logger.error(f"[USER_DELETE_DEBUG] toggle_active path entered for {user_id}")
                target_user.is_active = not getattr(target_user,'is_active',True)  # type: ignore
                user_service.update_user_sync(target_user)
                _trigger_backup('user_status_change')
                flash('User status toggled & backup created.', 'success')
            elif action == 'delete_user':
                current_app.logger.error(f"[USER_DELETE_DEBUG] delete_user path entered for {user_id}")
                admin_pwd = request.form.get('admin_password','')
                if current_user.check_password(admin_pwd):  # type: ignore
                    try:
                        # Protect against deleting yourself
                        if target_user.id == current_user.id:  # type: ignore
                            flash('Cannot delete your own account.', 'error')
                            current_app.logger.error(f"[USER_DELETE_DEBUG] Attempt to delete self blocked: current_user={current_user.id} target={target_user.id}")
                        else:
                            # Prevent deleting last admin
                            if getattr(target_user, 'is_admin', False):  # type: ignore
                                admin_count = user_service.get_admin_count_sync() if hasattr(user_service, 'get_admin_count_sync') else 1  # type: ignore
                                if admin_count <= 1:
                                    flash('Cannot delete the last admin user.', 'error')
                                    current_app.logger.error(f"[USER_DELETE_DEBUG] Last admin delete blocked admin_count={admin_count}")
                                else:
                                    deleted = user_service.delete_user_sync(target_user.id)  # type: ignore
                                    current_app.logger.error(f"[USER_DELETE_DEBUG] delete attempt admin user deleted={deleted}")
                                    if deleted:
                                        _trigger_backup('user_deleted')
                                        flash('User deleted & backup created.', 'success')
                                    else:
                                        # Diagnostic: check existence directly
                                        try:
                                            repo = getattr(user_service, 'user_repo', None)
                                            exists_flag = False
                                            if repo and hasattr(repo, 'safe_manager'):
                                                check_q = "MATCH (u:User {id: $uid}) RETURN COUNT(u) as c"
                                                res = repo.safe_manager.execute_query(check_q, {"uid": target_user.id})
                                                from app.services.kuzu_service_facade import _convert_query_result_to_list as _cvt
                                                data = _cvt(res)
                                                if data and int(data[0].get('c',0))>0:
                                                    exists_flag = True
                                            current_app.logger.error(f"[USER_DELETE_DEBUG] delete admin failed exists_flag={exists_flag}")
                                            flash(f'Delete failed (diagnostic: exists={exists_flag}).', 'error')
                                        except Exception as de_diag:
                                            current_app.logger.error(f"[USER_DELETE_DEBUG] diagnostic error admin delete: {de_diag}")
                                            flash(f'Delete failed (diag error: {de_diag}).', 'error')
                                        flash('Delete failed.', 'error')
                            else:
                                deleted = user_service.delete_user_sync(target_user.id)  # type: ignore
                                current_app.logger.error(f"[USER_DELETE_DEBUG] delete attempt non-admin deleted={deleted}")
                                if deleted:
                                    _trigger_backup('user_deleted')
                                    flash('User deleted & backup created.', 'success')
                                else:
                                    try:
                                        repo = getattr(user_service, 'user_repo', None)
                                        exists_flag = False
                                        if repo and hasattr(repo, 'safe_manager'):
                                            check_q = "MATCH (u:User {id: $uid}) RETURN COUNT(u) as c"
                                            res = repo.safe_manager.execute_query(check_q, {"uid": target_user.id})
                                            from app.services.kuzu_service_facade import _convert_query_result_to_list as _cvt
                                            data = _cvt(res)
                                            if data and int(data[0].get('c',0))>0:
                                                exists_flag = True
                                        current_app.logger.error(f"[USER_DELETE_DEBUG] delete non-admin failed exists_flag={exists_flag}")
                                        flash(f'Delete failed (diagnostic: exists={exists_flag}).', 'error')
                                    except Exception as de_diag:
                                        current_app.logger.error(f"[USER_DELETE_DEBUG] diagnostic error non-admin delete: {de_diag}")
                                        flash(f'Delete failed (diag error: {de_diag}).', 'error')
                                    flash('Delete failed.', 'error')
                        # Always refresh users list inline (message shown once)
                        try:
                            current_app.logger.error("[USER_DELETE_DEBUG] Fetching updated user list after delete attempt")
                        except Exception:
                            pass
                        updated_users = [u for u in user_service.get_all_users_sync() if u.id != target_user.id]
                        # Retrieve flashed messages (if any) to embed
                        from flask import get_flashed_messages
                        msgs = get_flashed_messages(with_categories=True)
                        try:
                            current_app.logger.error(f"[USER_DELETE_DEBUG] Messages after delete attempt: {msgs}")
                        except Exception:
                            pass
                        return render_template('settings/partials/server_users_full.html', users=updated_users, search='', inline_messages=msgs)
                    except Exception as de:
                        current_app.logger.error(f"[USER_DELETE_DEBUG] Exception during delete flow: {de}")
                        flash('Delete failed.', 'error')
                else:
                    current_app.logger.error("[USER_DELETE_DEBUG] Admin password incorrect for delete")
                    flash('Admin password incorrect.', 'error')
            # reload updated user
            try:
                current_app.logger.error(f"[USER_DELETE_DEBUG] Reloading user editor for user_id={user_id}")
            except Exception:
                pass
            target_user = user_service.get_user_by_id_sync(user_id)
        return render_template('settings/partials/server_user_edit.html', user=target_user)
    if panel == 'debug':
        try:
            return render_template('settings/partials/server_debug.html')
        except Exception as e:
            current_app.logger.error(f"Debug panel render error: {e}")
            return '<div class="text-danger small">Error loading debug tools.</div>'
    if panel == 'config':
        # Inline server configuration management (mirrors admin.settings POST logic) without redirect
        from app.admin import get_admin_settings_context, save_system_config
        import os, uuid
        ctx = {}
        if request.method == 'POST':
            # Gather form fields
            site_name = request.form.get('site_name', os.getenv('SITE_NAME', 'MyBibliotheca'))
            server_timezone = request.form.get('server_timezone', 'UTC')
            terminology_preference = request.form.get('terminology_preference', 'genre')
            background_config = {
                'type': request.form.get('background_type', 'default'),
                'solid_color': request.form.get('solid_color', '#667eea'),
                'gradient_start': request.form.get('gradient_start', '#667eea'),
                'gradient_end': request.form.get('gradient_end', '#764ba2'),
                'gradient_direction': request.form.get('gradient_direction', '135deg'),
                'image_url': request.form.get('background_image_url', ''),
                'image_position': request.form.get('image_position', 'cover')
            }
            # Handle optional image upload
            if 'background_image_file' in request.files:
                file = request.files['background_image_file']
                if file and file.filename:
                    allowed_extensions = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
                    if '.' in file.filename and file.filename.rsplit('.', 1)[1].lower() in allowed_extensions:
                        try:
                            file_extension = file.filename.rsplit('.', 1)[1].lower()
                            unique_filename = f"bg_{uuid.uuid4().hex}.{file_extension}"
                            data_dir = getattr(current_app.config, 'DATA_DIR', None)
                            if data_dir:
                                upload_dir = os.path.join(data_dir, 'uploads', 'backgrounds')
                            else:
                                base_dir = Path(current_app.root_path).parent
                                upload_dir = os.path.join(base_dir, 'data', 'uploads', 'backgrounds')
                            os.makedirs(upload_dir, exist_ok=True)
                            upload_path = os.path.join(upload_dir, unique_filename)
                            file.save(upload_path)
                            background_config['image_url'] = f"/uploads/backgrounds/{unique_filename}"
                            background_config['type'] = 'image'
                            flash(f'Background image uploaded successfully: {file.filename}', 'success')
                        except Exception as e:
                            current_app.logger.error(f"Inline background upload error: {e}")
                            flash('Error uploading background image.', 'error')
                    else:
                        flash('Invalid background image type.', 'error')
            config = {
                'site_name': site_name,
                'server_timezone': server_timezone,
                'terminology_preference': terminology_preference,
                'background_config': background_config
            }
            if save_system_config(config):
                flash('System settings saved.', 'success')
            else:
                flash('Failed to save system settings.', 'error')
        # Always refresh context after (or for GET)
        ctx = get_admin_settings_context()
        return render_template('admin/partials/server_config.html', **ctx)
    if panel == 'ai':
        from app.admin import get_admin_settings_context, load_ai_config
        ctx = get_admin_settings_context()
        ctx['ai_config'] = load_ai_config()
        return render_template('settings/partials/server_ai.html', **ctx)
    if panel == 'metadata':
        return '<div class="card p-3"><h5 class="mb-2">Metadata Providers</h5><p class="small text-muted mb-3">Configuration UI coming soon. This will manage external book/cover/search provider API keys and priorities.</p><div class="alert alert-info small mb-0">Placeholder panel.</div></div>'
    # 'system' panel removed; info moved to overview section
    return '<div class="text-danger small">Unknown panel.</div>'

@auth.route('/privacy_settings', methods=['GET', 'POST'])
@login_required
def privacy_settings():
    from app.forms import PrivacySettingsForm, ReadingStreakForm
    import pytz
    
    form = PrivacySettingsForm()
    streak_form = ReadingStreakForm()
    
    # Populate timezone choices with common timezones
    common_timezones = [
        ('UTC', 'UTC'),
        ('America/New_York', 'Eastern Time (US & Canada)'),
        ('America/Chicago', 'Central Time (US & Canada)'),
        ('America/Denver', 'Mountain Time (US & Canada)'),
        ('America/Los_Angeles', 'Pacific Time (US & Canada)'),
        ('America/Phoenix', 'Arizona'),
        ('America/Anchorage', 'Alaska'),
        ('Pacific/Honolulu', 'Hawaii'),
        ('Europe/London', 'London'),
        ('Europe/Paris', 'Paris'),
        ('Europe/Berlin', 'Berlin'),
        ('Europe/Rome', 'Rome'),
        ('Europe/Madrid', 'Madrid'),
        ('Europe/Amsterdam', 'Amsterdam'),
        ('Asia/Tokyo', 'Tokyo'),
        ('Asia/Shanghai', 'Shanghai'),
        ('Asia/Dubai', 'Dubai'),
        ('Asia/Kolkata', 'Mumbai/Kolkata'),
        ('Australia/Sydney', 'Sydney'),
        ('Australia/Melbourne', 'Melbourne'),
    ]
    # Use type: ignore to suppress the type checker warning for this assignment
    form.timezone.choices = common_timezones  # type: ignore
    
    # Populate forms with current values
    if request.method == 'GET':
        form.share_current_reading.data = current_user.share_current_reading
        form.share_reading_activity.data = current_user.share_reading_activity
        form.share_library.data = current_user.share_library
        form.timezone.data = getattr(current_user, 'timezone', 'UTC')
        streak_form.reading_streak_offset.data = current_user.reading_streak_offset
    
    if form.validate_on_submit():
        try:
            # Get current user from Kuzu to ensure we have the latest data
            user_from_kuzu = user_service.get_user_by_id_sync(current_user.id)
            if user_from_kuzu:
                # Update privacy settings (excluding timezone)
                user_from_kuzu.share_current_reading = form.share_current_reading.data
                user_from_kuzu.share_reading_activity = form.share_reading_activity.data
                user_from_kuzu.share_library = form.share_library.data
                
                # Save through Kuzu service
                updated_user = user_service.update_user_sync(user_from_kuzu)
                if updated_user:
                    # Update current_user object for immediate reflection
                    current_user.share_current_reading = updated_user.share_current_reading
                    current_user.share_reading_activity = updated_user.share_reading_activity
                    current_user.share_library = updated_user.share_library
                    flash('Privacy settings updated successfully!', 'success')
                    return redirect(url_for('auth.privacy_settings'))
                else:
                    flash('Failed to update privacy settings.', 'error')
            else:
                flash('User not found.', 'error')
        except Exception as e:
            flash(f'Failed to update privacy settings: {str(e)}', 'error')
    
    # Get current timezone info for display
    try:
        user_tz = pytz.timezone(getattr(current_user, 'timezone', 'UTC'))
        current_time = datetime.now(user_tz)
        timezone_info = {
            'name': getattr(current_user, 'timezone', 'UTC'),
            'current_time': current_time.strftime('%Y-%m-%d %H:%M:%S %Z'),
            'offset': current_time.strftime('%z')
        }
    except:
        timezone_info = {
            'name': 'UTC',
            'current_time': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
            'offset': '+0000'
        }
    
    return render_template('auth/privacy_settings.html', 
                         title='Privacy Settings', 
                         form=form, 
                         streak_form=streak_form,
                         timezone_info=timezone_info)

@auth.route('/my_activity')
@login_required
def my_activity():
    try:
        # Get user's books from Kuzu
        user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
        total_books = len(user_books)
        
        # Get books added this year
        current_year = datetime.now(timezone.utc).year
        books_this_year = sum(1 for book in user_books 
                             if book.get('created_at') and isinstance(book.get('created_at'), datetime) and book['created_at'].year == current_year)
        
        # Get recent books (last 10) - sort by created_at descending
        # Filter out books without created_at and sort safely
        books_with_dates = [book for book in user_books if book.get('created_at')]
        recent_books = sorted(books_with_dates, 
                             key=lambda x: x.get('created_at') or datetime.min, 
                             reverse=True)[:10]
        
        # For reading logs, we'll need to implement a method or use a placeholder for now
        # TODO: Implement reading log functionality when needed
        reading_logs = 0  # Placeholder
        recent_logs = []  # Placeholder
        
    except Exception as e:
        # Fallback if services fail
        total_books = 0
        books_this_year = 0
        recent_books = []
        reading_logs = 0
        recent_logs = []
    
    return render_template('auth/my_activity.html', 
                         title='My Activity',
                         total_books=total_books,
                         reading_logs=reading_logs,
                         books_this_year=books_this_year,
                         recent_books=recent_books,
                         recent_logs=recent_logs)

@auth.route('/update_streak_settings', methods=['POST'])
@login_required
def update_streak_settings():
    form = ReadingStreakForm()
    
    if form.validate_on_submit():
        try:
            # Get current user from Kuzu to ensure we have the latest data
            user_from_kuzu = user_service.get_user_by_id_sync(current_user.id)
            if user_from_kuzu:
                # Update reading streak offset
                user_from_kuzu.reading_streak_offset = form.reading_streak_offset.data or 0
                
                # Save through Kuzu service
                updated_user = user_service.update_user_sync(user_from_kuzu)
                if updated_user:
                    # Update current_user object for immediate reflection
                    current_user.reading_streak_offset = updated_user.reading_streak_offset
                    flash('Reading streak settings updated successfully!', 'success')
                else:
                    flash('Failed to update streak settings.', 'error')
            else:
                flash('User not found.', 'error')
        except Exception as e:
            flash(f'Error updating streak settings: {str(e)}', 'error')
    else:
        flash('Error updating streak settings. Please try again.', 'danger')
    
    return redirect(url_for('auth.privacy_settings'))

@auth.route('/update_timezone', methods=['POST'])
@login_required
def update_timezone():
    from app.forms import PrivacySettingsForm
    import pytz
    
    form = PrivacySettingsForm()
    
    # Populate timezone choices (same as in privacy_settings)
    common_timezones = [
        ('UTC', 'UTC'),
        ('America/New_York', 'Eastern Time (US & Canada)'),
        ('America/Chicago', 'Central Time (US & Canada)'),
        ('America/Denver', 'Mountain Time (US & Canada)'),
        ('America/Los_Angeles', 'Pacific Time (US & Canada)'),
        ('America/Phoenix', 'Arizona'),
        ('America/Anchorage', 'Alaska'),
        ('Pacific/Honolulu', 'Hawaii'),
        ('Europe/London', 'London'),
        ('Europe/Paris', 'Paris'),
        ('Europe/Berlin', 'Berlin'),
        ('Europe/Rome', 'Rome'),
        ('Europe/Madrid', 'Madrid'),
        ('Europe/Amsterdam', 'Amsterdam'),
        ('Asia/Tokyo', 'Tokyo'),
        ('Asia/Shanghai', 'Shanghai'),
        ('Asia/Dubai', 'Dubai'),
        ('Asia/Kolkata', 'Mumbai/Kolkata'),
        ('Australia/Sydney', 'Sydney'),
        ('Australia/Melbourne', 'Melbourne'),
    ]
    # Use type: ignore to suppress the type checker warning for this assignment
    form.timezone.choices = common_timezones  # type: ignore
    
    if form.validate_on_submit():
        try:
            # Get current user from Kuzu to ensure we have the latest data
            user_from_kuzu = user_service.get_user_by_id_sync(current_user.id)
            if user_from_kuzu:
                # Update timezone
                user_from_kuzu.timezone = form.timezone.data
                
                # Save through Kuzu service
                updated_user = user_service.update_user_sync(user_from_kuzu)
                if updated_user:
                    # Update current_user object for immediate reflection
                    current_user.timezone = updated_user.timezone
                    flash('Timezone updated successfully!', 'success')
                else:
                    flash('Failed to update timezone.', 'error')
            else:
                flash('User not found.', 'error')
        except Exception as e:
            flash(f'Error updating timezone: {str(e)}', 'error')
    else:
        flash('Error updating timezone. Please try again.', 'danger')
    
    return redirect(url_for('auth.privacy_settings'))


@auth.route('/debug/user-count')
def debug_user_count():
    """Debug route to check user count - TEMPORARY"""
    try:
        
        # Test multiple methods
        results = {}
        
        # Method 1: Direct service call
        try:
            count1 = user_service.get_user_count_sync()
            results['service_count'] = count1
        except Exception as e:
            results['service_error'] = str(e)
        
        # Method 2: Direct repository call
        try:
            from .infrastructure.kuzu_repositories import KuzuUserRepository
            user_repo = KuzuUserRepository()
            from .services.kuzu_async_helper import run_async
            all_users = run_async(user_repo.get_all(limit=10000))
            results['repo_count'] = len(all_users)
        except Exception as e:
            results['repo_error'] = str(e)
        
        # Method 3: Direct SafeKuzuManager call
        try:
            from .utils.safe_kuzu_manager import get_safe_kuzu_manager
            safe_manager = get_safe_kuzu_manager()
            query_result = safe_manager.execute_query("MATCH (u:User) RETURN COUNT(u) as count")
            
            if query_result and hasattr(query_result, 'get_next') and query_result.has_next():
                count3 = _safe_get_row_value(query_result.get_next(), 0)
                results['direct_count'] = count3
            elif query_result and hasattr(query_result, 'get_as_df'):
                df = query_result.get_as_df()
                if not df.empty:
                    count3 = df.iloc[0]['count']
                    results['direct_count'] = count3
            else:
                results['direct_error'] = f"Could not parse result: {query_result}"
        except Exception as e:
            results['direct_error'] = str(e)
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
