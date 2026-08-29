from flask import Blueprint, render_template, redirect, url_for, flash, request, session, current_app, jsonify, abort, get_flashed_messages
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash, generate_password_hash
from app.domain.models import User, MediaType
from app.services import user_service, book_service, reading_log_service
from app.infrastructure.kuzu_graph import safe_execute_kuzu_query
from app.services.kuzu_service_facade import _convert_query_result_to_list  # Reuse helper for query results
from app.admin import (
    admin_required,
    save_ai_config,
    load_ai_config,
    save_system_config,
    load_system_config,
    get_admin_settings_context,
    save_smtp_config,
    load_smtp_config,
    save_backup_config,
    load_backup_config,
    _log,
    _log_force,
)
from app.utils.user_settings import get_default_book_format, get_effective_reading_defaults
from wtforms import IntegerField, SubmitField
from wtforms.validators import Optional, NumberRange
from flask_wtf import FlaskForm
from .forms import (LoginForm, RegistrationForm, UserProfileForm, ChangePasswordForm,
                   PrivacySettingsForm, ForcedPasswordChangeForm, SetupForm, ReadingStreakForm)
from .debug_utils import debug_route, debug_auth, debug_csrf, debug_session
from datetime import datetime, timezone
from typing import cast, Any, Optional
import json
from pathlib import Path
import os

auth = Blueprint('auth', __name__)

_MEDIA_TYPE_VALUES = {mt.value for mt in MediaType}

def _safe_get_row_value(row: Any, index: int) -> Any:
    """Safely extract a value from a Kuzu row that may be dict-like or sequence-like."""
    try:
        if hasattr(row, 'keys'):
            keys = list(row.keys())  # type: ignore[attr-defined]
            return row[keys[index]] if index < len(keys) else None  # type: ignore[index]
        else:
            return row[index]  # type: ignore[index]
    except Exception:
        return None

@auth.route('/setup', methods=['GET', 'POST'])
@debug_route('SETUP')
def setup():
    """Initial setup route - redirects to new onboarding system."""
    debug_auth("=" * 60)
    debug_auth("Redirecting to new onboarding system")
    try:
        user_count = cast(int, user_service.get_user_count_sync())
        if user_count and user_count > 0:
            flash('Setup has already been completed.', 'info')
            return redirect(url_for('auth.login'))
        # No users: begin onboarding
        flash('Welcome to Bibliotheca! Let\'s set up your library.', 'info')
        return redirect(url_for('onboarding.start'))
    except Exception as e:
        debug_auth(f"Setup redirect error: {e}")
        flash('Welcome to Bibliotheca! Let\'s set up your library.', 'info')
        # Fallback: simple setup form
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
                
                # Set session as permanent if remember_me is checked
                # This allows Flask-Login's remember cookie to work properly
                if form.remember_me.data:
                    session.permanent = True
                    debug_auth("Remember me enabled - session marked as permanent")
                else:
                    session.permanent = False
                    debug_auth("Remember me not enabled - session non-permanent")
                
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
    try:
        user_count = user_service.get_user_count_sync()
    except Exception:
        user_count = 0
    is_first_user = user_count == 0

    if request.method == 'GET':
        # Pre-select administrator for the very first user and lock role choice
        if is_first_user:
            form.role.data = 'admin'
        elif not form.role.data:
            form.role.data = 'user'

    if form.validate_on_submit():
        try:
            # Ensure form data is not None
            username = form.username.data
            email = form.email.data
            password = form.password.data
            selected_role = (form.role.data or 'user').lower()
            
            if not username or not email or not password:
                flash('All fields are required.', 'error')
                return render_template('auth/register.html', title='Create New User', form=form, is_first_user=is_first_user)
            
            # Create user through Kuzu service
            password_hash = generate_password_hash(password)
            should_be_admin = is_first_user or selected_role == 'admin'
            domain_user = user_service.create_user_sync(
                username=username,
                email=email,
                password_hash=password_hash,
                is_admin=should_be_admin,
                password_must_change=True  # All new users must change password on first login
            )
            
            if is_first_user:
                flash('Congratulations! As the first user, you have been granted admin privileges. You must change your password on first login.', 'info')
            else:
                if domain_user:
                    role_label = 'administrator' if domain_user.is_admin else 'standard user'
                    article = 'an' if role_label[0].lower() in ('a', 'e', 'i', 'o', 'u') else 'a'
                    flash(f'User {domain_user.username} has been created successfully as {article} {role_label}. They will be required to change their password on first login.', 'success')
                else:
                    fallback_role = 'administrator' if should_be_admin else 'standard user'
                    fallback_article = 'an' if fallback_role[0].lower() in ('a', 'e', 'i', 'o', 'u') else 'a'
                    flash(f'User has been created successfully as {fallback_article} {fallback_role}. They will be required to change their password on first login.', 'success')
            
            return redirect(url_for('admin.users'))
        except ValueError as e:
            flash(str(e), 'error')

    return render_template('auth/register.html', title='Create New User', form=form, is_first_user=is_first_user)

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
            setattr(user_obj, 'share_library', p_form.share_library.data)
            updated = user_service.update_user_sync(user_obj)  # type: ignore
            if updated:
                # Mirror onto session's current_user for immediate reflection
                try:
                    current_user.share_current_reading = updated.share_current_reading  # type: ignore
                    current_user.share_reading_activity = updated.share_reading_activity  # type: ignore
                    current_user.share_library = updated.share_library  # type: ignore
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
            p_form.share_library.data = getattr(current_user, 'share_library', False)
        except Exception:
            pass
    return render_template('settings/partials/privacy_form.html', form=p_form, streak_form=streak_form)

@auth.route('/settings/partial/reading_prefs', methods=['GET', 'POST'])
@login_required
def settings_reading_prefs_partial():
    # Simple manual form parsing; no WTForms needed
    from app.utils.user_settings import (
        get_library_sort_choices,
        get_library_status_choices,
        get_default_reading_status_choices,
        load_user_settings,
        save_user_settings,
    )
    if request.method == 'POST':
        rows_raw = (request.form.get('library_rows_per_page') or '').strip()
        dp_raw = (request.form.get('default_pages_per_log') or '').strip()
        dm_raw = (request.form.get('default_minutes_per_log') or '').strip()
        status_raw = (request.form.get('library_default_status') or 'all').strip()
        sort_raw = (request.form.get('library_default_sort') or 'title_asc').strip()
        reading_status_raw = (request.form.get('default_reading_status') or '').strip()
        def _to_int_or_none(v: str):
            try:
                return int(v) if v not in (None, '',) else None
            except Exception:
                return None
        payload = {
            'library_rows_per_page': _to_int_or_none(rows_raw),
            'default_pages_per_log': _to_int_or_none(dp_raw),
            'default_minutes_per_log': _to_int_or_none(dm_raw),
            'library_default_status': status_raw or 'all',
            'library_default_sort': sort_raw or 'title_asc',
            'default_reading_status': reading_status_raw
        }
        ok = save_user_settings(getattr(current_user, 'id', None), payload)
        if ok:
            flash('Library preferences saved.', 'success')
        else:
            flash('Failed to save preferences.', 'error')
    # Load current settings for display
    settings = load_user_settings(getattr(current_user, 'id', None))
    return render_template(
        'settings/partials/reading_prefs.html',
        settings=settings,
        library_status_choices=get_library_status_choices(),
        library_sort_choices=get_library_sort_choices(),
        default_reading_status_choices=get_default_reading_status_choices()
    )

# New: Personal Audiobookshelf partial (per-user ABS settings)
@auth.route('/settings/partial/personal_abs', methods=['GET', 'POST'])
@login_required
def settings_personal_abs_partial():
    from app.utils.user_settings import load_user_settings, save_user_settings
    from app.utils.audiobookshelf_settings import load_abs_settings
    abs_settings = load_abs_settings()
    if request.method == 'POST':
        abs_username = (request.form.get('abs_username') or '').strip()
        abs_api_key = (request.form.get('abs_api_key') or '').strip()
        abs_sync_books = True if request.form.get('abs_sync_books') in ('on','true','1') else False
        abs_sync_listening = True if request.form.get('abs_sync_listening') in ('on','true','1') else False
        payload = {
            'abs_username': abs_username,
            'abs_api_key': abs_api_key,
            'abs_sync_books': abs_sync_books,
            'abs_sync_listening': abs_sync_listening
        }
        ok = save_user_settings(getattr(current_user, 'id', None), payload)
        if ok:
            flash('ABS settings saved.', 'success')
        else:
            flash('Failed to save ABS settings.', 'error')
    settings = load_user_settings(getattr(current_user, 'id', None))
    return render_template('settings/partials/personal_abs.html', settings=settings, abs_settings=abs_settings)

# Note: User-triggered ABS sync is disabled; only admins can trigger ABS sync from Server settings.

@auth.route('/settings/partial/data/<string:panel>')
@login_required
def settings_data_partial(panel: str):
    if panel not in {'import_books','import_reading','backup','export_logs'}:
        return '<div class="text-danger small">Unknown panel.</div>'
    if panel == 'import_books':
        return render_template('settings/partials/data_import_books.html')
    if panel == 'import_reading':
        defaults = get_effective_reading_defaults(getattr(current_user, 'id', None))
        default_pages = defaults[0] if defaults and (defaults[0] or 0) > 0 else 1
        default_minutes = defaults[1] if defaults and (defaults[1] or 0) > 0 else 1
        try:
            from app.routes.import_routes import UNASSIGNED_READING_LOG_TITLE
            unassigned_title = UNASSIGNED_READING_LOG_TITLE
        except Exception:
            unassigned_title = 'Unassigned Reading Logs'
        return render_template(
            'settings/partials/data_import_reading.html',
            default_quick_add_days=7,
            default_quick_add_pages=default_pages,
            default_quick_add_minutes=default_minutes,
            unassigned_title=unassigned_title
        )
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

from .auth_settings import settings_server_partial as _settings_server_partial

@auth.route('/settings/partial/server/<string:panel>', methods=['GET','POST'])
def settings_server_partial(panel: str):
    return _settings_server_partial(panel)


@auth.route('/settings/server/ai', methods=['POST'])
@login_required
@admin_required
def save_ai_settings():
    """Persist AI configuration updates from the unified settings page."""
    try:
        config = {
            'AI_PROVIDER': request.form.get('ai_provider', 'openai'),
            'OPENAI_API_KEY': request.form.get('openai_api_key', ''),
            'OPENAI_BASE_URL': request.form.get('openai_base_url', 'https://api.openai.com/v1'),
            'OPENAI_MODEL': request.form.get('openai_model', 'gpt-4o'),
            'OLLAMA_BASE_URL': request.form.get('ollama_base_url', 'http://localhost:11434/v1'),
            'AI_TIMEOUT': request.form.get('ai_timeout', '30'),
            'AI_MAX_TOKENS': request.form.get('ai_max_tokens', '1000'),
            'AI_TEMPERATURE': request.form.get('ai_temperature', '0.1'),
            'AI_BOOK_EXTRACTION_ENABLED': 'true' if request.form.get('ai_book_extraction_enabled') else 'false',
            'AI_BOOK_EXTRACTION_AUTO_SEARCH': 'true' if request.form.get('ai_book_extraction_auto_search') else 'false',
        }
        ollama_manual = (request.form.get('ollama_model_manual') or '').strip()
        ollama_selected = (request.form.get('ollama_model') or '').strip()
        config['OLLAMA_MODEL'] = ollama_manual or ollama_selected or 'llama3.2-vision:11b'

        if save_ai_config(config):
            flash('AI settings saved successfully!', 'success')
        else:
            flash('Error saving AI settings. Please try again.', 'danger')
    except Exception as exc:
        _log('error', f"Error updating AI settings: {exc}")
        flash('Error saving AI settings. Please try again.', 'danger')

    expects_partial = (
        request.form.get('inline') == '1'
        or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        or request.headers.get('HX-Request')
    )
    if expects_partial:
        ctx = get_admin_settings_context()
        ctx['ai_config'] = load_ai_config()
        return render_template('settings/partials/server_ai.html', **ctx)
    return redirect(url_for('auth.settings', section='server', panel='ai'))


@auth.route('/settings/server/ai/test', methods=['POST'])
@login_required
@admin_required
def test_ai_connection():
    """Test connectivity for the configured AI provider."""
    try:
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
        from app.services.ai_service import AIService

        ai_service = AIService(config)
        result = ai_service.test_connection()
        if 'success' not in result:
            result['success'] = bool(result.get('ok', result.get('status') == 'ok'))
        return jsonify(result)
    except Exception as exc:
        _log('error', f"Error testing AI connection: {exc}")
        return jsonify({'success': False, 'message': 'Connection test failed. Please check your settings.'}), 500


@auth.route('/settings/server/ai/ollama', methods=['POST'])
@login_required
@admin_required
def test_ollama_connection():
    """Probe an Ollama instance and return available models."""
    try:
        config = {
            'AI_PROVIDER': 'ollama',
            'OLLAMA_BASE_URL': request.form.get('ollama_base_url', 'http://localhost:11434/v1'),
            'AI_TIMEOUT': '10',
        }
        from app.services.ai_service import AIService

        ai_service = AIService(config)
        result = ai_service._test_ollama_connection()
        if 'success' not in result:
            result['success'] = bool(result.get('ok'))
        return jsonify(result)
    except Exception as exc:
        _log('error', f"Error testing Ollama connection: {exc}")
        return jsonify({'success': False, 'message': 'Ollama connection test failed. Please check your settings.'}), 500


@auth.route('/settings/server/smtp', methods=['POST'])
@login_required
@admin_required
def save_smtp_settings():
    """Persist SMTP configuration from unified settings."""
    try:
        config = {
            'smtp_server': (request.form.get('smtp_server') or '').strip(),
            'smtp_username': (request.form.get('smtp_username') or '').strip(),
            'smtp_password': (request.form.get('smtp_password') or '').strip(),
            'smtp_from_email': (request.form.get('smtp_from_email') or '').strip(),
            'smtp_from_name': (request.form.get('smtp_from_name') or 'MyBibliotheca').strip(),
        }
        raw_port = (request.form.get('smtp_port') or '').strip()
        try:
            smtp_port_numeric = int(raw_port or 587)
        except (TypeError, ValueError):
            smtp_port_numeric = 587
        allowed_security_values = {'starttls', 'ssl', 'none'}
        raw_security = (request.form.get('smtp_security') or '').strip().lower()
        if raw_security not in allowed_security_values:
            legacy_tls = (request.form.get('smtp_use_tls') or '').strip().lower()
            raw_security = 'starttls' if legacy_tls in {'on', 'true', '1'} else 'none'
        config['smtp_port'] = str(smtp_port_numeric)
        config['smtp_security'] = raw_security
        config['smtp_use_tls'] = 'true' if raw_security == 'starttls' else 'false'

        if save_smtp_config(config):
            flash('SMTP settings saved successfully!', 'success')
        else:
            flash('Error saving SMTP settings. Please try again.', 'danger')
    except Exception as exc:
        _log('error', f"Error updating SMTP settings: {exc}", extra_secrets=[request.form.get('smtp_password', '')])
        flash('Error saving SMTP settings. Please try again.', 'danger')

    expects_partial = (
        request.form.get('inline') == '1'
        or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        or request.headers.get('HX-Request')
    )
    if expects_partial:
        ctx = get_admin_settings_context()
        ctx['smtp_config'] = load_smtp_config()
        ctx['site_name'] = (load_system_config() or {}).get('site_name', 'MyBibliotheca')
        return render_template('settings/partials/server_smtp.html', **ctx)
    return redirect(url_for('auth.settings', section='server', panel='smtp'))


@auth.route('/settings/server/smtp/test', methods=['POST'])
@login_required
@admin_required
def test_smtp_connection():
    """Test SMTP connectivity with the supplied form data."""
    import smtplib
    import socket
    import ssl

    smtp_server = (request.form.get('smtp_server') or '').strip()
    raw_port = (request.form.get('smtp_port') or '').strip()
    smtp_username = (request.form.get('smtp_username') or '').strip()
    smtp_password = (request.form.get('smtp_password') or '').strip()
    allowed_security_values = {'starttls', 'ssl', 'none'}
    raw_security = (request.form.get('smtp_security') or '').strip().lower()
    if raw_security not in allowed_security_values:
        legacy_tls = (request.form.get('smtp_use_tls') or '').strip().lower()
        raw_security = 'starttls' if legacy_tls in {'true', '1', 'on'} else 'none'
    try:
        smtp_port = int(raw_port or 587)
    except (TypeError, ValueError):
        return jsonify({'success': False, 'message': 'SMTP port must be a number.'}), 400

    secret_values = [smtp_password] if smtp_password else []
    _log_force('info', f"[SMTP] Testing connection to {smtp_server}:{smtp_port}", extra_secrets=secret_values)
    _log_force('info', f"[SMTP] Configuration - Security: {raw_security.upper()}, Username: {smtp_username or '(none)'}", extra_secrets=secret_values)

    if not smtp_server:
        _log_force('warning', '[SMTP] Test aborted - no server specified', extra_secrets=secret_values)
        return jsonify({'success': False, 'message': 'SMTP server is required'}), 400

    server = None
    try:
        _log_force('info', f"[SMTP] Step 1/4: Resolving DNS for {smtp_server}...", extra_secrets=secret_values)
        try:
            resolved_ip = socket.gethostbyname(smtp_server)
            _log_force('info', f"[SMTP] DNS resolved: {smtp_server} -> {resolved_ip}", extra_secrets=secret_values)
        except socket.gaierror as dns_err:
            _log_force('error', f"[SMTP] DNS resolution failed for {smtp_server}: {dns_err}", extra_secrets=secret_values)
            return jsonify({'success': False, 'message': f'DNS resolution failed for {smtp_server}. Please check the server address.'}), 500

        if raw_security == 'ssl':
            _log_force('info', f"[SMTP] Step 2/4: Connecting with implicit SSL to {smtp_server}:{smtp_port} (timeout: 30s)...", extra_secrets=secret_values)
            try:
                context = ssl.create_default_context()
                server = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=30, context=context)
                server.set_debuglevel(0)
                server.ehlo()
                _log_force('info', '[SMTP] Connection established over SSL', extra_secrets=secret_values)
            except socket.timeout:
                _log_force('error', f"[SMTP] Connection timeout after 30s to {smtp_server}:{smtp_port}", extra_secrets=secret_values)
                return jsonify({'success': False, 'message': f'Connection timeout to {smtp_server}:{smtp_port}. Check firewall settings or try a different port.'}), 500
            except ConnectionRefusedError:
                _log_force('error', f"[SMTP] Connection refused by {smtp_server}:{smtp_port}", extra_secrets=secret_values)
                return jsonify({'success': False, 'message': f'Connection refused by {smtp_server}:{smtp_port}. Server may not be accepting connections.'}), 500
            except socket.error as sock_err:
                _log_force('error', f"[SMTP] Socket error connecting to {smtp_server}:{smtp_port}: {sock_err}", extra_secrets=secret_values)
                return jsonify({'success': False, 'message': f'Network error: {sock_err}. Check server address and port.'}), 500
        else:
            _log_force('info', f"[SMTP] Step 2/4: Connecting to {smtp_server}:{smtp_port} (timeout: 30s)...", extra_secrets=secret_values)
            try:
                server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
                server.set_debuglevel(0)
                server.ehlo()
                _log_force('info', f"[SMTP] Connection established to {smtp_server}:{smtp_port}", extra_secrets=secret_values)
            except socket.timeout:
                _log_force('error', f"[SMTP] Connection timeout after 30s to {smtp_server}:{smtp_port}", extra_secrets=secret_values)
                return jsonify({'success': False, 'message': f'Connection timeout to {smtp_server}:{smtp_port}. Check firewall settings or try a different port.'}), 500
            except ConnectionRefusedError:
                _log_force('error', f"[SMTP] Connection refused by {smtp_server}:{smtp_port}", extra_secrets=secret_values)
                return jsonify({'success': False, 'message': f'Connection refused by {smtp_server}:{smtp_port}. Server may not be accepting connections.'}), 500
            except socket.error as sock_err:
                _log_force('error', f"[SMTP] Socket error connecting to {smtp_server}:{smtp_port}: {sock_err}", extra_secrets=secret_values)
                return jsonify({'success': False, 'message': f'Network error: {sock_err}. Check server address and port.'}), 500

        if raw_security == 'starttls':
            _log_force('info', '[SMTP] Step 3/4: Initiating STARTTLS...', extra_secrets=secret_values)
            try:
                context = ssl.create_default_context()
                server.starttls(context=context)
                server.ehlo()
                _log_force('info', '[SMTP] STARTTLS successful', extra_secrets=secret_values)
            except smtplib.SMTPException as tls_err:
                _log_force('error', f"[SMTP] STARTTLS failed: {tls_err}", extra_secrets=secret_values)
                return jsonify({'success': False, 'message': f'TLS negotiation failed: {tls_err}'}), 500
        elif raw_security == 'ssl':
            _log_force('info', '[SMTP] Step 3/4: SSL negotiation completed during connection', extra_secrets=secret_values)
        else:
            _log_force('info', '[SMTP] Step 3/4: No TLS requested', extra_secrets=secret_values)

        if smtp_username and smtp_password:
            _log_force('info', f"[SMTP] Step 4/4: Authenticating as {smtp_username}...", extra_secrets=secret_values)
            try:
                server.login(smtp_username, smtp_password)
                _log_force('info', f"[SMTP] Authentication successful for {smtp_username}", extra_secrets=secret_values)
            except smtplib.SMTPAuthenticationError as auth_err:
                _log_force('error', f"[SMTP] Authentication failed for {smtp_username}: {auth_err}", extra_secrets=secret_values)
                return jsonify({'success': False, 'message': 'Authentication failed. Please check your username and password.'}), 401
            except smtplib.SMTPException as smtp_err:
                _log_force('error', f"[SMTP] SMTP error during authentication: {smtp_err}", extra_secrets=secret_values)
                return jsonify({'success': False, 'message': f'SMTP authentication error: {smtp_err}'}), 500
        else:
            _log_force('info', '[SMTP] Step 4/4: No authentication (username/password not provided)', extra_secrets=secret_values)

        _log_force('info', f"[SMTP] All steps completed successfully. Closing connection...", extra_secrets=secret_values)
        server.quit()
        _log_force('info', f"[SMTP] Test completed successfully for {smtp_server}:{smtp_port}", extra_secrets=secret_values)
        return jsonify({'success': True, 'message': f'Successfully connected to {smtp_server}:{smtp_port} using {raw_security.upper()} security'})
    except smtplib.SMTPException as exc:
        _log_force('error', f"[SMTP] SMTP error: {type(exc).__name__}: {exc}", extra_secrets=secret_values)
        return jsonify({'success': False, 'message': f'SMTP error: {exc}'}), 500
    except socket.timeout:
        _log_force('error', f"[SMTP] Operation timeout for {smtp_server}:{smtp_port}", extra_secrets=secret_values)
        return jsonify({'success': False, 'message': 'Operation timeout. The server may be slow or unreachable.'}), 500
    except Exception as exc:
        _log_force('error', f"[SMTP] Unexpected error: {type(exc).__name__}: {exc}", extra_secrets=secret_values, exc_info=True)
        return jsonify({'success': False, 'message': f'Connection failed: {exc}'}), 500
    finally:
        if server:
            try:
                server.quit()
            except Exception:
                pass


@auth.route('/settings/server/backup', methods=['POST'])
@login_required
@admin_required
def save_backup_settings():
    """Persist backup configuration updates."""
    try:
        backup_directory = (request.form.get('backup_directory') or 'data/backups').strip()
        if not backup_directory:
            flash('Backup directory cannot be empty.', 'danger')
        else:
            if save_backup_config({'backup_directory': backup_directory}):
                flash('Backup settings saved successfully!', 'success')
            else:
                flash('Error saving backup settings. Please try again.', 'danger')
    except Exception as exc:
        _log('error', f"Error updating backup settings: {exc}")
        flash('Error saving backup settings. Please try again.', 'danger')

    expects_partial = (
        request.form.get('inline') == '1'
        or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        or request.headers.get('HX-Request')
    )
    if expects_partial:
        return render_template('settings/partials/server_backup.html', backup_config=load_backup_config())
    return redirect(url_for('auth.settings', section='server', panel='backup'))


@auth.route('/settings/opds/preview/<int:row_index>')
@login_required
def opds_preview_detail(row_index: int):
    if not current_user.is_admin:
        abort(403)
    from app.utils.opds_settings import load_opds_settings
    from app.utils.opds_mapping import MB_FIELD_LABELS

    settings = load_opds_settings()
    preview_list = settings.get('last_test_preview') or []
    if not isinstance(preview_list, list) or row_index < 0 or row_index >= len(preview_list):
        abort(404)

    entry_obj = preview_list[row_index]
    entry_payload = entry_obj if isinstance(entry_obj, dict) else {}
    mapped_payload = entry_payload.get('entry') if isinstance(entry_payload, dict) else {}
    if not isinstance(mapped_payload, dict):
        mapped_payload = entry_payload if isinstance(entry_payload, dict) else {}

    opds_id = None
    if isinstance(entry_payload, dict):
        opds_id = entry_payload.get('opds_source_id') or mapped_payload.get('opds_source_id')
    raw_links = mapped_payload.get('raw_links') if isinstance(mapped_payload, dict) else None
    inspect_link = None
    if isinstance(raw_links, list):
        for link in raw_links:
            if not isinstance(link, dict):
                continue
            href = link.get('href')
            if not isinstance(href, str) or not href:
                continue
            rel = str(link.get('rel') or '').lower()
            if rel in {'self', 'alternate'} or rel.endswith('/self'):
                inspect_link = href
                break
        if not inspect_link:
            for link in raw_links:
                if not isinstance(link, dict):
                    continue
                href = link.get('href')
                if isinstance(href, str) and href:
                    inspect_link = href
                    break

    summary = settings.get('last_test_summary') or {}
    payload_json = json.dumps(mapped_payload, indent=2, ensure_ascii=False)

    column_map_obj = entry_payload.get('columns') if isinstance(entry_payload, dict) else None
    column_map = column_map_obj if isinstance(column_map_obj, dict) else {}

    mapping_raw = settings.get('mapping')
    mapping_config = mapping_raw if isinstance(mapping_raw, dict) else {}
    mapped_field_order: list[str] = []
    for key in mapping_config.keys():
        field_name = str(key)
        if field_name and field_name not in mapped_field_order:
            mapped_field_order.append(field_name)
    for field_name in column_map.keys():
        if field_name not in mapped_field_order:
            mapped_field_order.append(field_name)

    def _stringify_detail_value(value: Any) -> str:
        if value is None or value == "":
            return ""
        if isinstance(value, list):
            return ", ".join(str(item) for item in value if item not in (None, ""))
        if isinstance(value, dict):
            try:
                return json.dumps(value, ensure_ascii=False)
            except Exception:
                return str(value)
        return str(value)

    mapped_fields: list[dict[str, Any]] = []
    for field_name in mapped_field_order:
        cell = column_map.get(field_name) if isinstance(column_map, dict) else None
        raw_value = mapped_payload.get(field_name) if isinstance(mapped_payload, dict) else None
        text_value: str
        link_url: Optional[str] = None
        if isinstance(cell, dict):
            text_value = cell.get('text') or ''
            link_url = cell.get('url') if isinstance(cell.get('url'), str) else None
        else:
            if field_name.startswith('contributors.') and isinstance(mapped_payload.get('contributors'), list):
                role = field_name.split('.', 1)[1].upper() if '.' in field_name else ''
                contributor_names = []
                for contributor in mapped_payload.get('contributors', []):
                    if not isinstance(contributor, dict):
                        continue
                    c_role = str(contributor.get('role') or '').upper()
                    if c_role == role and contributor.get('name'):
                        contributor_names.append(str(contributor['name']))
                text_value = ", ".join(contributor_names)
            else:
                text_value = _stringify_detail_value(raw_value)
            if isinstance(raw_value, str) and raw_value.lower().startswith(('http://', 'https://', '/')):
                link_url = raw_value
        if not text_value:
            text_value = '—'
        elif link_url and text_value == link_url:
            # Avoid duplicating long URLs as both text and link
            text_value = link_url
        friendly_label = MB_FIELD_LABELS.get(field_name)
        if not friendly_label:
            friendly_label = field_name.replace('contributors.', 'Contributor · ').replace('_', ' ').title()
        mapped_fields.append({
            'name': field_name,
            'label': friendly_label,
            'text': text_value,
            'url': link_url,
        })

    if request.args.get('format') == 'json':
        return jsonify(mapped_payload)

    return render_template(
        'settings/opds_preview_detail.html',
        row_index=row_index,
        preview_entry=entry_payload,
        mapped_payload=mapped_payload,
        payload_json=payload_json,
        opds_id=opds_id,
        inspect_link=inspect_link,
        summary=summary,
        mapped_fields=mapped_fields,
    )

# Lightweight endpoint to test ABS connection via AJAX
@auth.route('/settings/audiobookshelf/test', methods=['POST'])
@login_required
def test_audiobookshelf_connection():
    if not current_user.is_admin:
        return jsonify({'ok': False, 'error': 'not_authorized'}), 403
    try:
        from app.utils.audiobookshelf_settings import load_abs_settings
        from app.services.audiobookshelf_service import get_client_from_settings
        settings = load_abs_settings()
        client = get_client_from_settings(settings)
        result = client.test_connection() if client else { 'ok': False, 'message': 'Missing base_url or api_key' }
        return jsonify(result)
    except Exception as e:
        current_app.logger.error(f"ABS test error: {e}")
        return jsonify({'ok': False, 'message': 'error'}), 500

# Start a background ABS Test Sync (limited import)
@auth.route('/settings/audiobookshelf/test-sync', methods=['POST'])
@login_required
def audiobookshelf_test_sync():
    # Only admins can trigger a library-level sync test
    if not current_user.is_admin:
        return jsonify({'ok': False, 'error': 'not_authorized'}), 403
    try:
        # Load settings and enqueue job into ABS runner
        from app.utils.audiobookshelf_settings import load_abs_settings
        from app.services.audiobookshelf_sync_runner import get_abs_sync_runner
        settings = load_abs_settings()
        library_ids = settings.get('library_ids') or []
        if isinstance(library_ids, str):
            library_ids = [s.strip() for s in library_ids.split(',') if s.strip()]
        # Limit from request JSON (optional)
        limit = 5
        try:
            payload = request.get_json(silent=True) or {}
            limit = int(payload.get('limit') or 5)
        except Exception:
            limit = 5
        runner = get_abs_sync_runner()
        task_id = runner.enqueue_test_sync(str(current_user.id), library_ids, limit=limit)
        # Reuse legacy import progress UI endpoints
        from app.routes.import_routes import import_bp
        progress_url = url_for('import.import_books_progress', task_id=task_id)
        api_progress_url = url_for('import.api_import_progress', task_id=task_id)
        return jsonify({'ok': True, 'task_id': task_id, 'progress_url': progress_url, 'api_progress_url': api_progress_url})
    except Exception as e:
        current_app.logger.error(f"ABS test sync error: {e}")
        return jsonify({'ok': False, 'message': 'error'}), 500

# Start a background ABS Full Sync (all items)
@auth.route('/settings/audiobookshelf/full-sync', methods=['POST'])
@login_required
def audiobookshelf_full_sync():
    if not current_user.is_admin:
        return jsonify({'ok': False, 'error': 'not_authorized'}), 403
    try:
        # Trigger a composite sync for ALL users to respect per-user credentials and prefs
        from app.utils.audiobookshelf_settings import load_abs_settings
        from app.services.audiobookshelf_sync_runner import get_abs_sync_runner
        from app.services import user_service, run_async

        settings = load_abs_settings()
        if not settings.get('enabled'):
            return jsonify({'ok': False, 'message': 'Audiobookshelf is disabled in settings'}), 400

        # Get users (async service wrapped to sync)
        try:
            users = run_async(user_service.get_all_users(limit=1000))  # type: ignore[attr-defined]
        except Exception as e:
            current_app.logger.error(f"ABS full sync: failed to load users: {e}")
            users = []
        if not users:
            return jsonify({'ok': False, 'message': 'No users found to sync'}), 400

        runner = get_abs_sync_runner()
        task_ids = []
        for u in users:
            try:
                if not getattr(u, 'is_active', True):
                    continue
                tid = runner.enqueue_user_composite_sync(
                    str(getattr(u, 'id')),
                    page_size=50,
                    force_books=True,
                    force_listening=True,
                )
                task_ids.append(tid)
            except Exception as e:
                current_app.logger.error(
                    f"ABS full sync: failed to enqueue for user {getattr(u, 'id', 'unknown')}: {e}"
                )
        current_app.logger.info(f"ABS full sync queued {len(task_ids)} user jobs")
        # Provide progress URLs for the first task so UI can poll status
        progress_url = url_for('import.import_books_progress', task_id=task_ids[0]) if task_ids else None
        api_progress_url = url_for('import.api_import_progress', task_id=task_ids[0]) if task_ids else None
        return jsonify({'ok': True, 'queued': len(task_ids), 'task_ids': task_ids, 'progress_url': progress_url, 'api_progress_url': api_progress_url})
    except Exception as e:
        current_app.logger.error(f"ABS full sync error: {e}")
        return jsonify({'ok': False, 'message': 'error'}), 500

# Start a background ABS Listening-only Test (no book import, just sessions/progress)
@auth.route('/settings/audiobookshelf/listen-test', methods=['POST'])
@login_required
def audiobookshelf_listen_test():
    if not current_user.is_admin:
        return jsonify({'ok': False, 'error': 'not_authorized'}), 403
    try:
        # Optional page_size from body
        page_size = 200
        try:
            payload = request.get_json(silent=True) or {}
            page_size = int(payload.get('page_size') or 200)
        except Exception:
            page_size = 200
        from app.services.audiobookshelf_sync_runner import get_abs_sync_runner
        runner = get_abs_sync_runner()
        task_id = runner.enqueue_listening_sync(str(current_user.id), page_size=page_size)
        try:
            current_app.logger.info(f"[ABS Listen] Enqueued listening-only test task={task_id} user={current_user.id} page_size={page_size}")
        except Exception:
            pass
        from app.routes.import_routes import import_bp  # noqa: F401
        progress_url = url_for('import.import_books_progress', task_id=task_id)
        api_progress_url = url_for('import.api_import_progress', task_id=task_id)
        return jsonify({'ok': True, 'task_id': task_id, 'progress_url': progress_url, 'api_progress_url': api_progress_url})
    except Exception as e:
        current_app.logger.error(f"ABS listen test error: {e}")
        return jsonify({'ok': False, 'message': 'error'}), 500

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
