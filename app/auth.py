from flask import Blueprint, render_template, redirect, url_for, flash, request, session, current_app, jsonify
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash, generate_password_hash
from app.domain.models import User
from app.services import user_service, book_service, reading_log_service
from wtforms import IntegerField, SubmitField
from wtforms.validators import Optional, NumberRange
from flask_wtf import FlaskForm
from .forms import (LoginForm, RegistrationForm, UserProfileForm, ChangePasswordForm,
                   PrivacySettingsForm, ForcedPasswordChangeForm, SetupForm, ReadingStreakForm)
from .debug_utils import debug_route, debug_auth, debug_csrf, debug_session
from datetime import datetime, timezone
from typing import cast, Any

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
    # Get site name for admin users
    site_name = None
    if current_user.is_admin:
        # For Kuzu version, site name is managed via environment variables
        import os
        site_name = os.getenv('SITE_NAME', 'MyBibliotheca')
    else:
        site_name = 'MyBibliotheca'
    
    return render_template('settings.html', 
                         title='Settings', 
                         site_name=site_name)

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
