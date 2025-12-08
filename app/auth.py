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

@auth.route('/settings/partial/server/<string:panel>', methods=['GET','POST'])
@login_required
def settings_server_partial(panel: str):
    # Normalize panel to avoid subtle whitespace/case mismatches
    try:
        panel = (panel or '').strip().lower()
    except Exception:
        panel = str(panel)
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
            elif action == 'update_role':
                current_app.logger.error(f"[USER_DELETE_DEBUG] update_role path entered for {user_id}")
                requested_role = (request.form.get('role') or '').strip().lower()
                if requested_role not in ('admin', 'user'):
                    flash('Invalid role selection.', 'error')
                else:
                    desired_admin_state = requested_role == 'admin'
                    current_admin_state = bool(getattr(target_user, 'is_admin', False))  # type: ignore
                    if desired_admin_state == current_admin_state:
                        flash('User role already set to the selected value.', 'info')
                    else:
                        if not desired_admin_state:
                            admin_count = user_service.get_admin_count_sync() if hasattr(user_service, 'get_admin_count_sync') else 1  # type: ignore
                            if current_admin_state and admin_count <= 1:
                                flash('Cannot remove admin privileges from the last administrator.', 'error')
                            else:
                                target_user.is_admin = False  # type: ignore
                                user_service.update_user_sync(target_user)
                                _trigger_backup('user_role_change')
                                flash('User role updated & backup created.', 'success')
                        else:
                            target_user.is_admin = True  # type: ignore
                            user_service.update_user_sync(target_user)
                            _trigger_backup('user_role_change')
                            flash('User role updated & backup created.', 'success')
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
                        from flask import get_flashed_messages as _flashed_msgs
                        msgs = _flashed_msgs(with_categories=True)
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
            import os  # ensure availability in this scope for env and fs ops
            # Support POST to update .env debug flags
            if request.method == 'POST':
                action = request.form.get('action')
                if action == 'update_debug_env':
                    try:
                        # Persist settings to data/.env (volume-backed) for reliability
                        data_dir = current_app.config.get('DATA_DIR', None)
                        if not data_dir:
                            try:
                                # Fall back to project root /data
                                import pathlib
                                data_dir = str(pathlib.Path(current_app.root_path).parent / 'data')
                            except Exception:
                                data_dir = 'data'
                        env_path = os.path.join(data_dir, '.env')
                        # Keys we manage here
                        manage_keys = [
                            'MYBIBLIOTHECA_DEBUG', 'MYBIBLIOTHECA_DEBUG_AUTH', 'MYBIBLIOTHECA_DEBUG_CSRF',
                            'MYBIBLIOTHECA_DEBUG_SESSION', 'MYBIBLIOTHECA_DEBUG_REQUESTS', 'MYBIBLIOTHECA_VERBOSE_INIT',
                            'ABS_LISTENING_DEBUG', 'KUZU_DEBUG', 'LOG_LEVEL'
                        ]
                        # Build desired values from form (checkbox true/false, plus LOG_LEVEL text)
                        def _to_str_bool(name: str) -> str:
                            v = request.form.get(name)
                            return 'true' if (v in ('on','true','1','yes')) else 'false'
                        updates = {
                            'MYBIBLIOTHECA_DEBUG': _to_str_bool('MYBIBLIOTHECA_DEBUG'),
                            'MYBIBLIOTHECA_DEBUG_AUTH': _to_str_bool('MYBIBLIOTHECA_DEBUG_AUTH'),
                            'MYBIBLIOTHECA_DEBUG_CSRF': _to_str_bool('MYBIBLIOTHECA_DEBUG_CSRF'),
                            'MYBIBLIOTHECA_DEBUG_SESSION': _to_str_bool('MYBIBLIOTHECA_DEBUG_SESSION'),
                            'MYBIBLIOTHECA_DEBUG_REQUESTS': _to_str_bool('MYBIBLIOTHECA_DEBUG_REQUESTS'),
                            'MYBIBLIOTHECA_VERBOSE_INIT': _to_str_bool('MYBIBLIOTHECA_VERBOSE_INIT'),
                            'ABS_LISTENING_DEBUG': _to_str_bool('ABS_LISTENING_DEBUG'),
                            'KUZU_DEBUG': _to_str_bool('KUZU_DEBUG'),
                            'LOG_LEVEL': (request.form.get('LOG_LEVEL') or os.getenv('LOG_LEVEL') or 'INFO').strip().upper()
                        }
                        # Ensure directory exists
                        os.makedirs(os.path.dirname(env_path), exist_ok=True)
                        # Use python-dotenv to safely upsert each key
                        try:
                            from dotenv import set_key, load_dotenv as _load
                            for k in manage_keys:
                                if k in updates:
                                    # Avoid quote_mode arg for compatibility with older python-dotenv
                                    set_key(env_path, k, str(updates[k]))
                            # Reload to reflect file changes; also override current env
                            _load(dotenv_path=env_path, override=True)
                        except Exception:
                            # Fallback simple writer if python-dotenv set_key unavailable
                            existing = {}
                            if os.path.exists(env_path):
                                try:
                                    with open(env_path, 'r') as rf:
                                        for line in rf:
                                            s = line.strip()
                                            if s and not s.startswith('#') and '=' in s:
                                                k, v = s.split('=', 1)
                                                existing[k.strip()] = v.strip()
                                except Exception:
                                    existing = {}
                            existing.update(updates)
                            tmp_path = env_path + '.tmp'
                            with open(tmp_path, 'w') as wf:
                                wf.write('# Debug settings (managed by Admin UI)\n')
                                for k in manage_keys:
                                    wf.write(f"{k}={existing.get(k, updates.get(k, ''))}\n")
                            os.replace(tmp_path, env_path)
                        # Also apply updates to current process so changes take effect without full restart
                        try:
                            import logging as _logging
                            for k, v in updates.items():
                                os.environ[k] = str(v)
                            # Adjust Python logging level dynamically when LOG_LEVEL changes
                            lvl_name = str(updates.get('LOG_LEVEL', os.getenv('LOG_LEVEL', 'ERROR'))).upper()
                            lvl = getattr(_logging, lvl_name, _logging.ERROR)
                            _logging.getLogger().setLevel(lvl)
                            try:
                                current_app.logger.setLevel(lvl)
                            except Exception:
                                pass
                        except Exception:
                            # Best effort; if anything fails, at least .env was updated
                            pass
                        flash('Debug environment settings updated. Restart may be required.', 'success')
                    except Exception as we:
                        current_app.logger.error(f"Failed updating debug env: {we}")
                        flash('Failed to update debug environment settings.', 'error')
                elif action == 'toggle_abs_debug':
                    try:
                        from app.utils.audiobookshelf_settings import load_abs_settings, save_abs_settings
                        toggle_to = (request.form.get('toggle_to') or '').strip().lower()
                        enabled = True if toggle_to == 'enable' else False
                        save_abs_settings({'debug_listening_sync': enabled})
                        flash(f"ABS listening debug {'enabled' if enabled else 'disabled'}.", 'success')
                    except Exception as te:
                        current_app.logger.error(f"ABS debug toggle error: {te}")
                        flash('Failed to toggle ABS listening debug.', 'error')
                elif action == 'run_abs_listening_sync':
                    try:
                        from app.services.audiobookshelf_sync_runner import get_abs_sync_runner
                        page_size = request.form.get('page_size')
                        ps = int(page_size) if page_size else 200
                        runner = get_abs_sync_runner()
                        runner.enqueue_listening_sync(str(current_user.id), page_size=ps)
                        flash('Listening sync started. Check Import Progress for updates.', 'info')
                    except Exception as re:
                        current_app.logger.error(f"ABS run sync now error: {re}")
                        flash('Failed to start listening sync.', 'error')
            # Build context values
            def _env_bool(name: str) -> bool:
                return str(os.getenv(name, 'false')).strip().lower() in ('1','true','yes','on')
            env_flags = {
                'MYBIBLIOTHECA_DEBUG': _env_bool('MYBIBLIOTHECA_DEBUG'),
                'MYBIBLIOTHECA_DEBUG_AUTH': _env_bool('MYBIBLIOTHECA_DEBUG_AUTH'),
                'MYBIBLIOTHECA_DEBUG_CSRF': _env_bool('MYBIBLIOTHECA_DEBUG_CSRF'),
                'MYBIBLIOTHECA_DEBUG_SESSION': _env_bool('MYBIBLIOTHECA_DEBUG_SESSION'),
                'MYBIBLIOTHECA_DEBUG_REQUESTS': _env_bool('MYBIBLIOTHECA_DEBUG_REQUESTS'),
                'MYBIBLIOTHECA_VERBOSE_INIT': _env_bool('MYBIBLIOTHECA_VERBOSE_INIT'),
                'ABS_LISTENING_DEBUG': _env_bool('ABS_LISTENING_DEBUG'),
                'KUZU_DEBUG': _env_bool('KUZU_DEBUG')
            }
            log_level = (os.getenv('LOG_LEVEL') or 'INFO').upper()
            from app.utils.audiobookshelf_settings import load_abs_settings
            abs_debug = False
            try:
                abs_debug = bool(load_abs_settings().get('debug_listening_sync'))
            except Exception:
                pass
            # If this was an HTMX/fetch request, just return the partial; otherwise keep users on full settings page
            tpl = render_template('settings/partials/server_debug.html', env_flags=env_flags, log_level=log_level, abs_debug_listening=abs_debug)
            if request.headers.get('HX-Request') or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return tpl
            # Non-ajax POST/GET: render the partial (settings.html will normally fetch it). Returning partial avoids redirect loop
            return tpl
        except Exception as e:
            current_app.logger.error(f"Debug panel render error: {e}")
            return '<div class="text-danger small">Error loading debug tools.</div>'
    if panel == 'config':
        # Inline server configuration management (mirrors admin.settings POST logic) without redirect
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
            # Reading defaults (optional numbers)
            try:
                dp_raw = request.form.get('default_pages_per_log', '').strip()
                dm_raw = request.form.get('default_minutes_per_log', '').strip()
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
            metadata_concurrency_raw = (request.form.get('metadata_concurrency') or '').strip()
            try:
                metadata_concurrency = int(metadata_concurrency_raw)
                if metadata_concurrency < 1:
                    metadata_concurrency = 1
            except Exception:
                metadata_concurrency = None if metadata_concurrency_raw == '' else None
            default_rows_value = (request.form.get('default_rows_per_page') or '').strip()
            raw_default_book_format = (request.form.get('default_book_format') or '').strip().lower()
            if raw_default_book_format not in _MEDIA_TYPE_VALUES:
                raw_default_book_format = MediaType.PHYSICAL.value
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
                'background_config': background_config,
                'reading_log_defaults': reading_log_defaults,
                'library_defaults': {
                    'default_rows_per_page': default_rows_value or None,
                    'default_book_format': raw_default_book_format
                },
                'import_settings': {
                    'metadata_concurrency': metadata_concurrency
                }
            }
            if save_system_config(config):
                flash('System settings saved.', 'success')
            else:
                flash('Failed to save system settings.', 'error')
        # Always refresh context after (or for GET)
        ctx = get_admin_settings_context()
        return render_template('settings/partials/server_config.html', **ctx)
    if panel == 'smtp':
        if not current_user.is_admin:
            return '<div class="text-danger small">Not authorized.</div>'
        ctx = {
            'smtp_config': load_smtp_config(),
            'site_name': (load_system_config() or {}).get('site_name', 'MyBibliotheca'),
        }
        return render_template('settings/partials/server_smtp.html', **ctx)
    if panel == 'backup':
        if not current_user.is_admin:
            return '<div class="text-danger small">Not authorized.</div>'
        ctx = {
            'backup_config': load_backup_config(),
        }
        return render_template('settings/partials/server_backup.html', **ctx)
    if panel == 'ai':
        ctx = get_admin_settings_context()
        ctx['ai_config'] = load_ai_config()
        return render_template('settings/partials/server_ai.html', **ctx)
    if panel == 'opds':
        from app.utils.opds_settings import load_opds_settings, save_opds_settings
        from app.utils.opds_mapping import clean_mapping, build_source_options, MB_FIELD_WHITELIST, MB_FIELD_LABELS
        from app.services import opds_probe_service as _opds_probe_service
        from app.services import ensure_opds_sync_runner, get_opds_sync_runner
        import json as _json
        from datetime import datetime as _dt, timezone as _tz
        from markupsafe import Markup, escape

        try:
            ensure_opds_sync_runner()
        except Exception as runner_err:
            try:
                current_app.logger.debug(f"OPDS runner init skipped: {runner_err}")
            except Exception:
                pass

        settings = load_opds_settings()
        stored_password = settings.get('password') or ''
        has_password = bool(settings.get('password_present')) or bool(stored_password)
        field_inventory = session.get('opds_field_inventory') or settings.get('last_field_inventory') or {}
        mapping = settings.get('mapping') or {}
        probe_result = None
        sync_result = None
        suggestions = None
        status_message = None
        error_message = None

        last_test_summary = settings.get('last_test_summary')
        last_test_preview = settings.get('last_test_preview') or []
        pending_jobs: list[dict[str, Any]] = []
        pending_job_ids: set[Any] = set()

        def _build_pending_job(kind: str, status_value: Any, task_id: Any, api_url: Any, progress_url: Any) -> dict[str, Any] | None:
            if not task_id or not api_url:
                return None
            status_norm = str(status_value or '').strip().lower() or 'queued'
            allowed_prefixes = ('queued', 'running', 'in-progress', 'in progress')
            if not any(status_norm.startswith(prefix) for prefix in allowed_prefixes):
                return None
            return {
                'task_id': task_id,
                'api_progress_url': api_url,
                'progress_url': progress_url,
                'kind': kind,
                'status': status_norm,
            }

        def _append_pending_job(job: dict[str, Any]) -> None:
            task_id = job.get('task_id')
            if not task_id or task_id in pending_job_ids:
                return
            pending_job_ids.add(task_id)
            pending_jobs.append(job)

        if request.method == 'POST':
            form = request.form
            action = (form.get('action') or '').strip().lower()
            base_url = (form.get('base_url') or settings.get('base_url') or '').strip()
            username = (form.get('username') or settings.get('username') or '').strip()
            user_agent = (form.get('user_agent') or settings.get('user_agent') or '').strip()
            password_input = form.get('password')
            clear_password = form.get('clear_password')
            mapping_json = form.get('mapping_json') or '{}'
            try:
                incoming_mapping = _json.loads(mapping_json) if mapping_json else {}
            except Exception as parse_err:
                current_app.logger.warning(f"OPDS mapping parse failed: {parse_err}")
                incoming_mapping = {}
                error_message = 'Mapping JSON malformed – ignoring submitted mapping.'

            inventory_for_clean = field_inventory or settings.get('last_field_inventory') or {}
            cleaned_mapping = clean_mapping(incoming_mapping, inventory_for_clean)

            update_payload: dict[str, Any] = {}
            if base_url:
                update_payload['base_url'] = base_url
            if username or settings.get('username'):
                update_payload['username'] = username
            if user_agent or settings.get('user_agent'):
                update_payload['user_agent'] = user_agent
            if cleaned_mapping is not None:
                update_payload['mapping'] = cleaned_mapping

            auto_sync_flag = form.get('auto_sync_enabled')
            if auto_sync_flag is not None:
                update_payload['auto_sync_enabled'] = str(auto_sync_flag).strip().lower() in ('1', 'true', 'yes', 'on')
            interval_raw = form.get('auto_sync_every_hours')
            if interval_raw is not None:
                update_payload['auto_sync_every_hours'] = interval_raw
            auto_user_id = form.get('auto_sync_user_id')
            if auto_user_id is not None:
                update_payload['auto_sync_user_id'] = auto_user_id.strip()

            password_for_request = stored_password
            if clear_password:
                update_payload['password'] = ''
                password_for_request = ''
                has_password = False
            elif password_input:
                update_payload['password'] = password_input
                password_for_request = password_input
                has_password = True

            def _refresh_settings() -> None:
                nonlocal settings, stored_password, mapping, last_test_summary, last_test_preview
                settings = load_opds_settings()
                stored_password = settings.get('password') or ''
                mapping = settings.get('mapping') or {}
                last_test_summary = settings.get('last_test_summary')
                last_test_preview = settings.get('last_test_preview') or []

            if action == 'save-settings':
                try:
                    if (
                        update_payload
                        and 'password' not in update_payload
                        and not base_url
                        and not username
                        and not user_agent
                        and incoming_mapping == {}
                        and auto_sync_flag is None
                        and interval_raw is None
                        and auto_user_id is None
                    ):
                        status_message = 'No changes detected.'
                    else:
                        save_ok = save_opds_settings(update_payload)
                        if save_ok:
                            status_message = 'OPDS settings saved.'
                            _refresh_settings()
                            field_inventory = settings.get('last_field_inventory') or field_inventory
                            session['opds_field_inventory'] = field_inventory
                            has_password = bool(settings.get('password_present')) or bool(settings.get('password'))
                        else:
                            error_message = 'Failed to save OPDS settings.'
                except Exception as err:
                    current_app.logger.error(f"OPDS settings save error: {err}")
                    error_message = 'Unexpected error saving settings.'
            elif action == 'probe':
                if not base_url:
                    error_message = 'Base URL is required for probe.'
                else:
                    try:
                        if update_payload:
                            save_opds_settings(update_payload)
                            _refresh_settings()
                            has_password = bool(settings.get('password_present')) or bool(settings.get('password'))
                        probe_result = _opds_probe_service.probe_sync(
                            base_url,
                            username=username or None,
                            password=password_for_request or None,
                            user_agent=user_agent or None,
                        )
                        field_inventory = probe_result.get('field_inventory') or {}
                        session['opds_field_inventory'] = field_inventory
                        suggestions = probe_result.get('mapping_suggestions') or {}
                        status_message = f"Probe complete: {len(probe_result.get('samples', []))} sample entries detected."
                        save_opds_settings({
                            'last_field_inventory': field_inventory,
                            'mapping': cleaned_mapping,
                            'last_probe_summary': probe_result,
                        })
                        _refresh_settings()
                        has_password = bool(settings.get('password_present')) or bool(settings.get('password'))
                    except Exception as err:
                        current_app.logger.error(f"OPDS probe failed: {err}")
                        error_message = f"Probe failed: {err}"
            elif action == 'sync-now':
                if not base_url:
                    error_message = 'Base URL is required before syncing.'
                elif not cleaned_mapping:
                    error_message = 'At least one field mapping is required before syncing.'
                else:
                    try:
                        if update_payload:
                            save_opds_settings(update_payload)
                            _refresh_settings()
                            has_password = bool(settings.get('password_present')) or bool(settings.get('password'))
                        ensure_opds_sync_runner()
                        runner = get_opds_sync_runner()
                        limit_raw = form.get('sync_limit')
                        limit_value = None
                        if limit_raw:
                            try:
                                limit_value = max(1, int(limit_raw))
                            except Exception:
                                limit_value = None
                        job_info = runner.enqueue_sync(str(current_user.id), limit=limit_value)
                        now_iso = _dt.now(_tz.utc).isoformat()
                        message_text = 'Sync job queued.'
                        sync_task_id = None
                        sync_progress_url = None
                        sync_api_url = None
                        if isinstance(job_info, dict):
                            sync_task_id = job_info.get('task_id')
                            sync_progress_url = job_info.get('progress_url')
                            sync_api_url = job_info.get('api_progress_url')
                            task_id_text = escape(str(sync_task_id or ''))
                            message_text = f"Sync job queued as task <code>{task_id_text}</code>."
                            if sync_progress_url:
                                message_text += f' <a href="{escape(sync_progress_url)}" class="link-primary">View progress</a>'
                        status_message = Markup(message_text)
                        save_payload = {
                            'last_sync_status': 'queued',
                            'last_sync_at': now_iso,
                            'last_sync_task_id': sync_task_id,
                            'last_sync_task_progress_url': sync_progress_url,
                            'last_sync_task_api_url': sync_api_url,
                        }
                        if limit_value:
                            save_payload['last_sync_summary'] = {'status': 'queued', 'limit': limit_value, 'timestamp': now_iso}
                        save_opds_settings(save_payload)
                        _refresh_settings()
                        has_password = bool(settings.get('password_present')) or bool(settings.get('password'))
                        queued_job = _build_pending_job('sync', 'queued', sync_task_id, sync_api_url, sync_progress_url)
                        if queued_job:
                            _append_pending_job(queued_job)
                    except Exception as err:
                        current_app.logger.error(f"OPDS sync enqueue failed: {err}")
                        error_message = f"Sync failed: {err}"
            elif action == 'test-sync':
                if not base_url:
                    error_message = 'Base URL is required before running a test sync.'
                elif not cleaned_mapping:
                    error_message = 'At least one field mapping is required before testing the sync.'
                else:
                    try:
                        if update_payload:
                            save_opds_settings(update_payload)
                            _refresh_settings()
                            has_password = bool(settings.get('password_present')) or bool(settings.get('password'))
                        ensure_opds_sync_runner()
                        runner = get_opds_sync_runner()
                        limit_raw = form.get('test_limit') or '10'
                        try:
                            limit_value = max(1, min(50, int(limit_raw)))
                        except Exception:
                            limit_value = 10
                        job_info = runner.enqueue_test_sync(str(current_user.id), limit=limit_value)
                        message_text = f"Test sync queued (limit {limit_value})."
                        test_task_id = None
                        test_progress_url = None
                        test_api_url = None
                        if isinstance(job_info, dict):
                            test_task_id = job_info.get('task_id')
                            test_progress_url = job_info.get('progress_url')
                            test_api_url = job_info.get('api_progress_url')
                            task_id_text = escape(str(test_task_id or ''))
                            if task_id_text:
                                message_text += f" Task <code>{task_id_text}</code>."
                            if test_progress_url:
                                message_text += f' <a href="{escape(test_progress_url)}" class="link-primary">Track progress</a>'
                        status_message = Markup(message_text)
                        now_iso = _dt.now(_tz.utc).isoformat()
                        save_opds_settings({
                            'last_test_summary': {'status': 'queued', 'limit': limit_value, 'timestamp': now_iso},
                            'last_test_preview': [],
                            'last_test_task_id': test_task_id,
                            'last_test_task_progress_url': test_progress_url,
                            'last_test_task_api_url': test_api_url,
                        })
                        _refresh_settings()
                        has_password = bool(settings.get('password_present')) or bool(settings.get('password'))
                        queued_job = _build_pending_job('test', 'queued', test_task_id, test_api_url, test_progress_url)
                        if queued_job:
                            _append_pending_job(queued_job)
                    except Exception as err:
                        current_app.logger.error(f"OPDS test sync enqueue failed: {err}")
                        error_message = f"Test sync failed: {err}"
            else:
                error_message = 'Unknown action.'

        if probe_result is None:
            probe_result = settings.get('last_probe_summary')
        if sync_result is None:
            sync_result = settings.get('last_sync_summary')

        # Prepare view context (avoid leaking password)
        settings_view = dict(settings)
        password_value = stored_password if has_password else ''
        settings_view.pop('password', None)
        settings_view['password_present'] = has_password
        settings_view['password_value'] = password_value

        source_options = build_source_options(field_inventory)
        if suggestions is None:
            suggestions = settings.get('mapping_suggestions') or {}

        def _register_pending_job(kind: str, status_value: Any, task_id: Any, api_url: Any, progress_url: Any) -> None:
            job = _build_pending_job(kind, status_value, task_id, api_url, progress_url)
            if job:
                _append_pending_job(job)

        _register_pending_job(
            'test',
            (last_test_summary or {}).get('status'),
            settings.get('last_test_task_id'),
            settings.get('last_test_task_api_url'),
            settings.get('last_test_task_progress_url'),
        )
        _register_pending_job(
            'sync',
            settings.get('last_sync_status'),
            settings.get('last_sync_task_id'),
            settings.get('last_sync_task_api_url'),
            settings.get('last_sync_task_progress_url'),
        )

        preview_columns = list(mapping.keys()) if mapping else []
        preview_rows: list[dict[str, Any]] = []

        if not preview_columns and last_test_preview:
            fallback_priority = [
                'title',
                'subtitle',
                'authors',
                'opds_source_id',
                'entry_id',
                'publisher',
                'average_rating',
                'language',
                'categories',
                'tags',
                'series',
                'series_order',
                'page_count',
                'published_date',
                'cover_url',
                'media_type',
            ]
            discovered_fields: list[str] = []
            for entry in last_test_preview:
                if not isinstance(entry, dict):
                    continue
                for key in entry.keys():
                    if key in ('action', 'reason', 'recent_activity', 'summary', 'raw_links'):
                        continue
                    if key not in discovered_fields:
                        discovered_fields.append(key)
            prioritized = [field for field in fallback_priority if field in discovered_fields]
            remaining = [field for field in discovered_fields if field not in prioritized]
            preview_columns = (prioritized + remaining)[:10]

        def _stringify_preview_value(value: Any) -> str:
            if value is None or value == '':
                return ''
            if isinstance(value, list):
                return ', '.join(str(v) for v in value if v not in (None, ''))
            if isinstance(value, dict):
                try:
                    return _json.dumps(value, ensure_ascii=False)
                except Exception:
                    return str(value)
            return str(value)

        def _build_preview_cell(entry: dict[str, Any], field_name: str) -> dict[str, Any]:
            entry_payload = entry.get('entry') if isinstance(entry.get('entry'), dict) else {}
            raw_value: Any
            if field_name.startswith('contributors.'):
                role = field_name.split('.', 1)[1].upper() if '.' in field_name else ''
                contributors = entry.get('contributors') or []
                if not contributors and isinstance(entry_payload, dict):
                    contributors = entry_payload.get('contributors') or []
                names = []
                for contributor in contributors:
                    if not isinstance(contributor, dict):
                        continue
                    c_role = str(contributor.get('role') or '').upper()
                    if c_role == role:
                        name_val = contributor.get('name')
                        if name_val:
                            names.append(str(name_val))
                raw_value = names
            elif field_name == 'opds_source_id':
                raw_value = entry.get('opds_source_id') or entry.get('entry_id') or entry.get('id')
                if raw_value is None and isinstance(entry_payload, dict):
                    raw_value = entry_payload.get('opds_source_id') or entry_payload.get('id')
            else:
                raw_value = entry.get(field_name)
                if raw_value is None and isinstance(entry_payload, dict):
                    raw_value = entry_payload.get(field_name)
            display_text = _stringify_preview_value(raw_value)
            cell_url = None
            if isinstance(raw_value, str):
                lower_value = raw_value.lower()
                if lower_value.startswith(('http://', 'https://')):
                    cell_url = raw_value
                elif field_name == 'cover_url' and raw_value.startswith('/'):
                    cell_url = raw_value
            return {
                'text': display_text or '—',
                'url': cell_url,
            }

        def _resolve_entry_link(payload: Optional[dict[str, Any]]) -> Optional[str]:
            if not isinstance(payload, dict):
                return None
            links = payload.get('raw_links')
            if isinstance(links, list):
                for link in links:
                    if not isinstance(link, dict):
                        continue
                    rel = str(link.get('rel') or '').lower()
                    href = link.get('href')
                    if not isinstance(href, str) or not href:
                        continue
                    if rel in {'self', 'alternate'} or rel.endswith('/self'):
                        return href
                for link in links:
                    if not isinstance(link, dict):
                        continue
                    href = link.get('href')
                    if isinstance(href, str) and href:
                        return href
            return None

        if preview_columns and last_test_preview:
            for entry in last_test_preview:
                if not isinstance(entry, dict):
                    continue
                row_values = {field: _build_preview_cell(entry, field) for field in preview_columns}
                raw_inspect_payload = entry.get('entry')
                inspect_payload: dict[str, Any] = raw_inspect_payload if isinstance(raw_inspect_payload, dict) else {}
                entry_link = _resolve_entry_link(inspect_payload)
                opds_identifier = inspect_payload.get('opds_source_id') if inspect_payload else None
                if not opds_identifier:
                    opds_identifier = entry.get('opds_source_id')
                preview_rows.append({
                    'action': entry.get('action'),
                    'reason': entry.get('reason'),
                    'columns': row_values,
                    'inspect_payload': inspect_payload,
                    'inspect_entry_link': entry_link,
                    'opds_source_id': opds_identifier,
                })

        return render_template(
            'settings/partials/server_opds.html',
            settings=settings_view,
            mapping=mapping,
            mapping_fields=MB_FIELD_WHITELIST,
            mapping_labels=MB_FIELD_LABELS,
            source_options=source_options,
            field_inventory=field_inventory,
            probe_result=probe_result,
            sync_result=sync_result,
            status_message=status_message,
            error_message=error_message,
            suggestions=suggestions,
            last_test_summary=last_test_summary,
            last_test_preview=last_test_preview,
            preview_columns=preview_columns,
            preview_rows=preview_rows,
            pending_jobs=pending_jobs,
        )
    if panel == 'audiobookshelf':
        # Admin-only ABS settings management
        if not current_user.is_admin:
            return '<div class="text-danger small">Not authorized.</div>'
        from app.utils.audiobookshelf_settings import load_abs_settings, save_abs_settings
        from app.services.audiobookshelf_service import get_client_from_settings
        import json as _json
        settings = load_abs_settings()
        connection_test = None
        expects_partial = bool(
            request.headers.get('HX-Request')
            or (request.headers.get('X-Requested-With') or '').lower() in {'xmlhttprequest', 'fetch'}
        )
        # Handle POST to save settings
        if request.method == 'POST':
            payload: dict[str, Any] = {}
            # Allow form or JSON
            if request.content_type and 'application/json' in request.content_type.lower():
                try:
                    payload = (request.get_json(silent=True) or {})
                except Exception:
                    payload = {}
            else:
                payload = {}
                if 'base_url' in request.form:
                    payload['base_url'] = (request.form.get('base_url') or '').strip()
                # Global toggles (treat absence as False)
                payload['enabled'] = True if request.form.get('enabled') else False
                libs_raw = (request.form.get('library_ids') or '').strip()
                if libs_raw:
                    try:
                        # support comma-separated or JSON array -> store as comma string; utils will normalize
                        if libs_raw.startswith('['):
                            arr = _json.loads(libs_raw)
                            if isinstance(arr, list):
                                payload['library_ids'] = ','.join([str(s).strip() for s in arr if str(s).strip()])
                            else:
                                payload['library_ids'] = libs_raw
                        else:
                            payload['library_ids'] = ','.join([s.strip() for s in libs_raw.split(',') if s.strip()])
                    except Exception:
                        payload['library_ids'] = libs_raw
                # Scheduler fields (from form)
                if 'auto_sync_enabled' in request.form or 'library_sync_every_hours' in request.form or 'listening_sync_every_hours' in request.form:
                    payload['auto_sync_enabled'] = True if request.form.get('auto_sync_enabled') else False
                    try:
                        payload['library_sync_every_hours'] = int(request.form.get('library_sync_every_hours') or 24)
                    except Exception:
                        payload['library_sync_every_hours'] = 24
                    try:
                        payload['listening_sync_every_hours'] = int(request.form.get('listening_sync_every_hours') or 12)
                    except Exception:
                        payload['listening_sync_every_hours'] = 12
                # Enforce order field (treat absence as False)
                payload['enforce_book_first'] = True if request.form.get('enforce_book_first') else False
            ok = save_abs_settings(payload)
            if ok:
                settings = load_abs_settings()
            if not expects_partial and not request.is_json:
                flash(
                    'Audiobookshelf settings saved.' if ok else 'Failed to save Audiobookshelf settings.',
                    'success' if ok else 'error',
                )
                return redirect(url_for('auth.settings', section='server', panel='audiobookshelf'))
        # Optionally test connection if query flag present
        try:
            if request.args.get('test') == '1':
                client = get_client_from_settings(settings)
                # Prefer current user's ABS API key if set
                try:
                    from app.utils.user_settings import load_user_settings
                    us = load_user_settings(getattr(current_user, 'id', None))
                    base_url = settings.get('base_url') or ''
                    user_api_key = (us.get('abs_api_key') or '').strip() if isinstance(us, dict) else ''
                    if base_url and user_api_key:
                        from app.services.audiobookshelf_service import AudiobookShelfClient
                        client = AudiobookShelfClient(base_url, user_api_key)
                except Exception:
                    pass
                connection_test = client.test_connection() if client else { 'ok': False, 'message': 'Missing base_url or api_key' }
        except Exception:
            connection_test = { 'ok': False, 'message': 'Connection test failed' }
        return render_template('settings/partials/server_audiobookshelf.html', settings=settings, connection_test=connection_test)
    if panel == 'metadata':
        from app.utils.metadata_settings import get_metadata_settings, save_metadata_settings
        if request.method == 'POST':
            data = {}
            try:
                current_app.logger.error('[METADATA_SAVE][BEGIN] ct=%s is_json=%s content_length=%s', request.content_type, request.is_json, request.content_length)
                # IMPORTANT: use cache=True so we can read body and still parse
                raw_body = request.get_data(cache=True, as_text=True)
                current_app.logger.error('[METADATA_SAVE][RAW_BODY]%s', (raw_body or '')[:2000])
                import json as _json
                if request.content_type and 'application/json' in request.content_type.lower():
                    try:
                        data = _json.loads(raw_body or '{}')
                    except Exception as je:
                        current_app.logger.error(f'[METADATA_SAVE][JSON_DECODE_ERR] {je}')
                        data = {}
                else:
                    raw = request.form.get('data')
                    if raw:
                        try:
                            data = _json.loads(raw)
                        except Exception as fe:
                            current_app.logger.error(f'[METADATA_SAVE][FORM_JSON_ERR] {fe}')
                    # fallback attempt if still empty and raw body looks like JSON
                    if not data and raw_body and raw_body.strip().startswith('{'):
                        try:
                            data = _json.loads(raw_body)
                            current_app.logger.error('[METADATA_SAVE][FALLBACK_PARSE_OK]')
                        except Exception as fe2:
                            current_app.logger.error(f'[METADATA_SAVE][FALLBACK_PARSE_ERR] {fe2}')
                has_books = isinstance(data.get('books'), dict)
                has_people = isinstance(data.get('people'), dict)
                current_app.logger.error('[METADATA_SAVE][PARSED_KEYS] keys=%s books=%s people=%s', list(data.keys())[:8], has_books, has_people)
                if not (has_books or has_people):
                    current_app.logger.error('[METADATA_SAVE][WARN] No valid books/people objects found in payload; aborting save.')
                    return jsonify({'ok': False, 'error': 'no_valid_payload'}), 400
                # sanity: ensure payload not unexpectedly huge
                if len(str(data)) > 20000:
                    current_app.logger.error('[METADATA_SAVE][WARN] Payload unusually large size=%s', len(str(data)))
                ok = save_metadata_settings(data)
                current_app.logger.error('[METADATA_SAVE][RESULT] ok=%s', ok)
                if not ok:
                    return jsonify({'ok': False, 'error': 'save_returned_false', 'received_keys': list(data.keys())}), 400
                return jsonify({'ok': True, 'metadata_settings': get_metadata_settings()})
            except Exception as e:
                current_app.logger.error(f"Metadata settings save failed: {e}")
                import traceback, sys
                traceback.print_exc(file=sys.stderr)
                return jsonify({'ok': False, 'error': 'save_failed'}), 400
        metadata_settings = get_metadata_settings()
        return render_template('settings/partials/server_metadata.html', metadata_settings=metadata_settings)
    if panel == 'repairs':
        if not current_user.is_admin:
            return '<div class="text-danger small">Not authorized.</div>'

        def _run_query(rows_query: str, params: dict[str, Any] | None = None, op: str = "repairs") -> list[dict[str, Any]]:
            try:
                result = safe_execute_kuzu_query(rows_query, params or {}, operation=op)
                rows = _convert_query_result_to_list(result)
                # Normalize to plain dicts with string keys
                normalized: list[dict[str, Any]] = []
                for row in rows or []:
                    if isinstance(row, dict):
                        norm_row = {str(k): row[k] for k in row.keys()}
                        # Provide col_0 fallback for single-column 'result' rows
                        if 'result' in norm_row and 'col_0' not in norm_row:
                            norm_row['col_0'] = norm_row['result']
                        normalized.append(norm_row)
                return normalized
            except Exception as err:
                try:
                    current_app.logger.warning(f"Repairs query failed ({op}): {err}")
                except Exception:
                    pass
                return []

        action_taken = (request.form.get('action') or '').strip().lower() if request.method == 'POST' else ''
        default_media = get_default_book_format()

        if request.method == 'POST' and action_taken:
            if action_taken == 'backfill_media_type':
                try:
                    updated_rows = _run_query(
                        """
                        MATCH (b:Book)
                        WHERE b.media_type IS NULL OR b.media_type = ''
                        SET b.media_type = $media_type
                        RETURN COUNT(b) AS updated
                        """,
                        {'media_type': default_media},
                        op='repairs_backfill_media_type'
                    )
                    updated = 0
                    if updated_rows:
                        row = updated_rows[0]
                        updated = int(row.get('updated') or row.get('col_0') or 0)
                    if updated:
                        flash(f'Updated media type for {updated} book(s).', 'success')
                    else:
                        flash('No books were missing a media type.', 'info')
                except Exception as err:
                    current_app.logger.error(f"Repair action backfill_media_type failed: {err}")
                    flash('Failed to backfill media types. Check logs for details.', 'error')
            elif action_taken == 'assign_default_location':
                try:
                    from app.location_service import LocationService
                    location_service = LocationService()
                    default_location = location_service.get_default_location()
                    if not default_location:
                        defaults = location_service.setup_default_locations()
                        default_location = defaults[0] if defaults else None
                    if not default_location or not getattr(default_location, 'id', None):
                        flash('No default location is available. Create a location first.', 'warning')
                    else:
                        updated_rows = _run_query(
                            """
                            MATCH (loc:Location {id: $loc_id})
                            WITH loc
                            MATCH (b:Book)
                            WHERE NOT (b)-[:STORED_AT]->(:Location)
                            MERGE (b)-[:STORED_AT]->(loc)
                            RETURN COUNT(b) AS updated
                            """,
                            {'loc_id': default_location.id},
                            op='repairs_assign_default_location'
                        )
                        updated = 0
                        if updated_rows:
                            row = updated_rows[0]
                            updated = int(row.get('updated') or row.get('col_0') or 0)
                        if updated:
                            flash(f'Default location assigned to {updated} book(s).', 'success')
                        else:
                            flash('All books already have a location assigned.', 'info')
                except Exception as err:
                    current_app.logger.error(f"Repair action assign_default_location failed: {err}")
                    flash('Failed to assign default locations. Check logs for details.', 'error')
            else:
                flash('Unknown repair action.', 'warning')

        issue_stats: dict[str, int | None] = {}
        issue_samples: dict[str, list[dict[str, Any]]] = {}

        total_books_rows = _run_query(
            "MATCH (b:Book) RETURN COUNT(b) AS total",
            op='repairs_total_books'
        )
        total_books = 0
        if total_books_rows:
            total_books = int(
                total_books_rows[0].get('total')
                or total_books_rows[0].get('count')
                or total_books_rows[0].get('result')
                or total_books_rows[0].get('col_0')
                or 0
            )

        stats_config = [
            (
                'missing_media_type',
                "MATCH (b:Book) WHERE b.media_type IS NULL OR b.media_type = '' RETURN COUNT(b) AS total",
                "MATCH (b:Book) WHERE b.media_type IS NULL OR b.media_type = '' RETURN b.id AS id, b.title AS title, b.updated_at AS updated_at ORDER BY b.updated_at DESC LIMIT 6"
            ),
            (
                'missing_authors',
                "MATCH (b:Book) WHERE NOT (b)-[:AUTHORED]->(:Person) AND NOT (b)-[:WRITTEN_BY]->(:Person) RETURN COUNT(b) AS total",
                "MATCH (b:Book) WHERE NOT (b)-[:AUTHORED]->(:Person) AND NOT (b)-[:WRITTEN_BY]->(:Person) RETURN b.id AS id, b.title AS title, b.updated_at AS updated_at ORDER BY b.updated_at DESC LIMIT 6"
            ),
            (
                'missing_locations',
                "MATCH (b:Book) WHERE NOT (b)-[:STORED_AT]->(:Location) RETURN COUNT(b) AS total",
                "MATCH (b:Book) WHERE NOT (b)-[:STORED_AT]->(:Location) RETURN b.id AS id, b.title AS title, b.updated_at AS updated_at ORDER BY b.updated_at DESC LIMIT 6"
            ),
        ]

        for key, count_query, sample_query in stats_config:
            count_rows = _run_query(count_query, op=f'repairs_{key}_count')
            count_val = 0
            if count_rows:
                row = count_rows[0]
                try:
                    count_val = int(
                        row.get('total')
                        or row.get('count')
                        or row.get('result')
                        or row.get('col_0')
                        or 0
                    )
                except Exception:
                    count_val = 0
            issue_stats[key] = count_val
            sample_rows = _run_query(sample_query, op=f'repairs_{key}_sample')
            normalized_samples: list[dict[str, Any]] = []
            for item in sample_rows:
                normalized_samples.append({
                    'id': item.get('id') or item.get('book_id') or item.get('col_0'),
                    'title': item.get('title') or item.get('name') or item.get('col_1'),
                    'updated_at': item.get('updated_at') or item.get('col_2') or item.get('result')
                })
            issue_samples[key] = normalized_samples

        try:
            total_issues = sum(int(issue_stats.get(k) or 0) for k in issue_stats.keys())
        except Exception:
            total_issues = 0

        inline_messages = get_flashed_messages(with_categories=True)

        return render_template(
            'settings/partials/server_repairs.html',
            issue_stats=issue_stats,
            issue_samples=issue_samples,
            total_books=total_books,
            total_issues=total_issues,
            default_media=default_media,
            inline_messages=inline_messages
        )
    if panel == 'jobs':
        # Admin view of all import/sync jobs across users
        if not current_user.is_admin:
            return '<div class="text-danger small">Not authorized.</div>'
        try:
            from app.utils.safe_import_manager import safe_import_manager, safe_get_import_job
            # ABS runner health (best effort)
            runner_alive = False
            try:
                from app.services.audiobookshelf_sync_runner import get_abs_sync_runner
                _runner = get_abs_sync_runner()
                try:
                    _runner.ensure_started()
                except Exception:
                    pass
                th = getattr(_runner, '_thread', None)
                runner_alive = bool(th and hasattr(th, 'is_alive') and th.is_alive())
            except Exception:
                runner_alive = False
            # High-level debug map to discover all user_ids and task_ids
            debug_map = safe_import_manager.get_jobs_for_admin_debug(current_user.id, include_user_data=True)
            # Collect detailed records for rendering
            jobs = []
            jobs_by_user = debug_map.get('jobs_by_user') or {}
            for uid, tasks in jobs_by_user.items():
                try:
                    for tid in tasks.keys():
                        try:
                            job = safe_get_import_job(uid, tid) or {}
                        except Exception:
                            job = {'task_id': tid, 'user_id': uid, 'status': 'unknown'}
                        # Normalize fields
                        entry = {
                            'task_id': job.get('task_id', tid),
                            'user_id': job.get('user_id', uid),
                            'type': job.get('type', job.get('import_type', 'unknown')),
                            'status': job.get('status', 'unknown'),
                            'created_at': job.get('created_at', ''),
                            'updated_at': job.get('updated_at', ''),
                            'processed': job.get('processed', 0),
                            'total': job.get('total', job.get('total_books', 0)),
                            'message': job.get('message') or (job.get('error_messages', [''])[0] if job.get('error_messages') else ''),
                        }
                        jobs.append(entry)
                except Exception:
                    continue
            # Sort newest first (fallback to unsorted on parse error)
            def _ts(j):
                ts = j.get('updated_at') or j.get('created_at') or ''
                return str(ts)
            try:
                jobs.sort(key=_ts, reverse=True)
            except Exception:
                pass
            # Manager stats for header
            stats = safe_import_manager.get_statistics()
            stats['abs_runner_alive'] = runner_alive
        except Exception as e:
            current_app.logger.error(f"Jobs panel error: {e}")
            jobs = []
            stats = {'total_active_jobs': 0, 'total_users_with_jobs': 0, 'operation_stats': {}, 'abs_runner_alive': False}
        return render_template('settings/partials/server_jobs.html', jobs=jobs, manager_stats=stats)
    # 'system' panel removed; info moved to overview section
    return '<div class="text-danger small">Unknown panel.</div>'


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
