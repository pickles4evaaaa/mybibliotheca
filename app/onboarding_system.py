"""
Multi-Step Onboarding System for Bibliotheca
============================================

This module provides a comprehensive onboarding wizard that guides new users through:
1. Admin account setup
2. Site configuration (location, timezone, site name)
3. Data import/migration options
4. Migration/import configuration
5. Execution and completion

The system maintains state through Flask sessions and provides a seamless setup experience.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, session, current_app, jsonify
from flask_login import login_user, current_user
from werkzeug.security import generate_password_hash
import json
import logging
import csv
import uuid
import threading
import time
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Any
import pytz
from datetime import datetime

from .advanced_migration_system import AdvancedMigrationSystem, DatabaseVersion
from .routes.import_routes import store_job_in_kuzu, start_import_job, auto_create_custom_fields, update_job_in_kuzu
from .utils.safe_import_manager import (
    safe_import_manager,
    safe_create_import_job,
    safe_update_import_job,
    safe_get_import_job,
    safe_get_user_import_jobs,
    safe_delete_import_job
)
from .services import user_service
from .location_service import LocationService
from .forms import SetupForm
from .debug_utils import debug_route
from .utils.safe_kuzu_manager import SafeKuzuManager

# Create blueprint
onboarding_bp = Blueprint('onboarding', __name__, url_prefix='/onboarding')

logger = logging.getLogger(__name__)

# Onboarding steps
ONBOARDING_STEPS = {
    1: 'admin_setup',
    2: 'site_config', 
    3: 'data_options',
    4: 'data_config',
    5: 'confirmation'
}

def get_onboarding_data() -> Dict:
    """Get all onboarding data from session."""
    # Force session to be permanent for better persistence
    session.permanent = True
    
    data = session.get('onboarding_data', {})
    backup = session.get('onboarding_backup', {})
    
    # If main data is missing but backup exists, use backup and restore
    if not data and backup:
        session['onboarding_data'] = backup
        session.modified = True
        data = backup
    
    logger.info(f"🔍 SESSION DEBUG: get_onboarding_data returning: {data}")
    return data


def update_onboarding_data(data: Dict):
    """Update onboarding data in session."""
    logger.info(f"🔍 SESSION DEBUG: update_onboarding_data called with: {data}")
    
    # Force session to be permanent for better persistence
    session.permanent = True
    
    # Ensure onboarding_data exists in session
    if 'onboarding_data' not in session:
        session['onboarding_data'] = {}
    
    current_data = session['onboarding_data']
    logger.info(f"🔍 SESSION DEBUG: current_data before update: {current_data}")
    
    # Update the data
    current_data.update(data)
    session['onboarding_data'] = current_data
    session.modified = True
    
    # Also store in a backup location to help with debugging
    session['onboarding_backup'] = current_data.copy()
    
    logger.info(f"🔍 SESSION DEBUG: session after update: {dict(session)}")
    logger.info(f"🔍 SESSION DEBUG: onboarding_data in session: {session.get('onboarding_data', 'NOT_FOUND')}")


def set_onboarding_step(step: int):
    """Set current onboarding step in session."""
    logger.info(f"🔍 SESSION DEBUG: set_onboarding_step called with step: {step}")
    
    # Force session to be permanent for better persistence
    session.permanent = True
    session['onboarding_step'] = step
    session.modified = True
    
    logger.info(f"🔍 SESSION DEBUG: onboarding_step set to: {session.get('onboarding_step', 'NOT_FOUND')}")


def get_onboarding_step() -> int:
    """Get current onboarding step from session."""
    # Force session to be permanent for better persistence
    session.permanent = True
    return session.get('onboarding_step', 1)

def clear_onboarding_session():
    """Clear all onboarding data from session.
    
    Note: After onboarding completion, JavaScript timers in import progress templates
    may continue to make requests to onboarding routes. The authentication checks
    added to routes help prevent confusion and redirect users appropriately.
    """
    session.pop('onboarding_step', None)
    session.pop('onboarding_data', None)
    session.pop('onboarding_backup', None)
    session.pop('onboarding_import_task_id', None)
    session.modified = True
    logger.info(f"🔍 SESSION DEBUG: Onboarding session cleared")


@onboarding_bp.route('/start')
@debug_route('ONBOARDING_START')  
def start():
    """Start the onboarding process."""
    
    # Check if user is already authenticated (onboarding completed)
    from flask_login import current_user
    if current_user.is_authenticated:
        logger.info(f"🔍 ONBOARDING DEBUG: User is already authenticated - redirecting to library")
        flash('You have already completed the setup process.', 'info')
        return redirect(url_for('main.library'))
    
    try:
        # Only check user count if we're not in the middle of onboarding
        # Skip this check if onboarding is in progress
        current_step = session.get('onboarding_step', 0)
        onboarding_data = session.get('onboarding_data', {})
        
        
        if current_step == 0 and not onboarding_data:  # Only check on fresh start
            user_count = user_service.get_user_count_sync()
            if user_count is not None and user_count > 0:
                flash('Setup has already been completed.', 'info')
                return redirect(url_for('auth.login'))
        else:
            logger.info("Onboarding already in progress, skipping user count check")
    except Exception as e:
        logger.error(f"Error checking user count: {e}")
    
    
    # Only clear session if we're not in the middle of active onboarding
    current_step = session.get('onboarding_step', 0)
    onboarding_data = session.get('onboarding_data', {})
    
    if current_step > 0 and onboarding_data:
        return redirect(url_for('onboarding.step', step_num=current_step))
    else:
        # Initialize onboarding session with explicit session management
        clear_onboarding_session()
    
    # Force session to be permanent and ensure it's set up properly
    session.permanent = True
    session['onboarding_step'] = 1
    session['onboarding_data'] = {}
    session['onboarding_backup'] = {}
    session.modified = True
    
    # Generate CSRF token to ensure forms will work
    try:
        from flask_wtf.csrf import generate_csrf
        csrf_token = generate_csrf()
        session.modified = True
    except Exception as e:
        logger.warning(f"Failed to generate CSRF token: {e}")
    
    return redirect(url_for('onboarding.step', step_num=1))


@onboarding_bp.route('/step/<int:step_num>', methods=['GET', 'POST'])
@debug_route('ONBOARDING_STEP')
def step(step_num: int):
    """Handle individual onboarding steps."""
    current_step = get_onboarding_step()
    
    # DEBUG: Log step access attempt
    logger.info(f"🔍 ONBOARDING DEBUG: User accessing step {step_num}, current_step={current_step}")
    logger.info(f"🔍 ONBOARDING DEBUG: Session data: {dict(session)}")
    logger.info(f"🔍 ONBOARDING DEBUG: Request method: {request.method}")
    
    # Check if user is already authenticated (onboarding completed)
    from flask_login import current_user
    if current_user.is_authenticated:
        # If user is logged in and there's no onboarding session, they've completed onboarding
        current_step = get_onboarding_step()
        onboarding_data = get_onboarding_data()
        
        if current_step == 1 and not onboarding_data:  # Default empty state after session clear
            logger.info(f"🔍 ONBOARDING DEBUG: User is authenticated but accessing onboarding routes - redirecting to library")
            flash('You have already completed the setup process.', 'info')
            return redirect(url_for('main.library'))
    
    # Allow backward navigation always
    # For forward navigation, check if we have completed previous steps
    if step_num > current_step:
        # Check if we have the required data for this step
        onboarding_data = get_onboarding_data()
        
        logger.info(f"🔍 ONBOARDING DEBUG: Forward navigation check - onboarding_data keys: {list(onboarding_data.keys())}")
        
        # Step 2 requires admin data from step 1
        if step_num >= 2 and 'admin' not in onboarding_data:
            
            # Try to recover from backup
            backup = session.get('onboarding_backup', {})
            if 'admin' in backup:
                session['onboarding_data'] = backup
                session.modified = True
                onboarding_data = backup
            else:
                logger.warning(f"🔍 ONBOARDING DEBUG: Missing admin data for step {step_num}, redirecting to step 1")
                flash('Please complete the admin setup first.', 'warning')
                return redirect(url_for('onboarding.step', step_num=1))
        
        # Step 3 requires site config from step 2  
        if step_num >= 3 and 'site_config' not in onboarding_data:
            logger.warning(f"🔍 ONBOARDING DEBUG: Missing site_config data for step {step_num}, redirecting to step 2")
            return redirect(url_for('onboarding.step', step_num=2))
        
        # Step 4+ requires data options from step 3
        if step_num >= 4 and 'data_options' not in onboarding_data:
            logger.warning(f"🔍 ONBOARDING DEBUG: Missing data_options for step {step_num}, redirecting to step 3")
            return redirect(url_for('onboarding.step', step_num=3))
        
        # Step 5 requires additional validation for import option
        if step_num >= 5:
            data_options = onboarding_data.get('data_options', {})
            if data_options.get('option') == 'import':
                # Check if import file was selected
                if 'import_file' not in data_options or not data_options.get('import_file'):
                    logger.warning(f"🔍 ONBOARDING DEBUG: Import option selected but no file chosen for step {step_num}")
                    flash('Please select a file to import before proceeding.', 'error')
                    return redirect(url_for('onboarding.step', step_num=3))
            
        # Allow access if we have the required data
        logger.info(f"🔍 ONBOARDING DEBUG: Access granted, setting step to {step_num}")
        set_onboarding_step(step_num)
    elif step_num == current_step:
        # User is accessing their current step - this is always allowed
        logger.info(f"🔍 ONBOARDING DEBUG: User accessing current step {step_num}")
    else:
        # Backward navigation - always allowed, but don't change the current step
        logger.info(f"🔍 ONBOARDING DEBUG: Backward navigation to step {step_num} from {current_step}")
        # Don't call set_onboarding_step() for backward navigation
        # This preserves the user's actual progress
    
    logger.info(f"🔍 ONBOARDING DEBUG: Proceeding to handle step {step_num}")
    
    # Handle each step
    if step_num == 1:
        return admin_setup_step()
    elif step_num == 2:
        return site_config_step()
    elif step_num == 3:
        return data_options_step()
    elif step_num == 4:
        return data_config_step()
    elif step_num == 5:
        return confirmation_step()
    else:
        logger.warning(f"🔍 ONBOARDING DEBUG: Invalid step {step_num}, redirecting to start")
        return redirect(url_for('onboarding.start'))


def admin_setup_step():
    """Step 1: Admin account setup."""
    logger.info(f"🔍 ONBOARDING DEBUG: admin_setup_step called")
    
    # Force session to be permanent to ensure persistence
    session.permanent = True
    
    form = SetupForm()
    
    logger.info(f"🔍 ONBOARDING DEBUG: Form created, is_submitted={form.is_submitted()}")
    
    if request.method == 'POST':
        
        # Manual form validation for better debugging
        form_valid = form.validate()
        
        # Check if basic required fields are present
        username = request.form.get('username', '').strip()
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '').strip()
        password2 = request.form.get('password2', '').strip()
        
        
        # If form validation fails, try to process anyway if we have required data
        if form_valid or (username and email and password and password == password2):
            try:
                logger.info(f"🔍 ONBOARDING DEBUG: Form processing, using form data directly")
                
                # Use form data directly if form validation failed but data is present
                if form_valid:
                    admin_data = {
                        'username': form.username.data,
                        'email': form.email.data,
                        'password': form.password.data
                    }
                else:
                    # Use raw form data
                    admin_data = {
                        'username': username,
                        'email': email,
                        'password': password
                    }
                
                logger.info(f"🔍 ONBOARDING DEBUG: Saving admin data: {admin_data['username']}, {admin_data['email']}")
                
                
                # Use direct session assignment instead of helper function
                if 'onboarding_data' not in session:
                    session['onboarding_data'] = {}
                session['onboarding_data']['admin'] = admin_data
                session.permanent = True
                session.modified = True
                
                # Also create a backup
                session['onboarding_backup'] = session['onboarding_data'].copy()
                
                
                # Move to next step
                logger.info(f"🔍 ONBOARDING DEBUG: Moving to step 2")
                session['onboarding_step'] = 2
                session.modified = True
                
                logger.info(f"🔍 ONBOARDING DEBUG: Session after update: {dict(session)}")
                
                flash('Admin account configured successfully!', 'success')
                return redirect(url_for('onboarding.step', step_num=2))
                
            except Exception as e:
                import traceback
                traceback.print_exc()
                logger.error(f"🔍 ONBOARDING DEBUG: Error in admin setup: {e}")
                flash(f'Error configuring admin account: {e}', 'error')
        else:
            if not form_valid:
                for field, errors in form.errors.items():
                    for error in errors:
                        flash(f'{field}: {error}', 'error')
    
    logger.info(f"🔍 ONBOARDING DEBUG: Rendering step 1 template")
    
    return render_template('onboarding/step1_admin_setup.html', 
                         form=form, 
                         step=1, 
                         total_steps=5)


def site_config_step():
    """Step 2: Site configuration (location, timezone, site name)."""
    logger.info(f"🔍 ONBOARDING DEBUG: site_config_step called, method={request.method}")
    
    # Force session to be permanent for better persistence
    session.permanent = True
    
    if request.method == 'POST':
        try:
            logger.info(f"🔍 ONBOARDING DEBUG: Processing POST data: {dict(request.form)}")
            
            site_config = {
                'site_name': request.form.get('site_name', 'MyBibliotheca'),
                'timezone': request.form.get('timezone', 'UTC'),
                'location': request.form.get('location', ''),
                'location_set_as_default': 'location_set_as_default' in request.form
            }
            
            logger.info(f"🔍 ONBOARDING DEBUG: Saving site_config: {site_config}")
            
            # Use direct session assignment for better reliability
            if 'onboarding_data' not in session:
                session['onboarding_data'] = {}
            session['onboarding_data']['site_config'] = site_config
            session.permanent = True
            session.modified = True
            
            # Also create a backup
            session['onboarding_backup'] = session['onboarding_data'].copy()
            
            # Move to next step
            logger.info(f"🔍 ONBOARDING DEBUG: Moving to step 3")
            session['onboarding_step'] = 3
            session.modified = True
            
            logger.info(f"🔍 ONBOARDING DEBUG: Session after update: {dict(session)}")
            
            flash('Site configuration saved!', 'success')
            return redirect(url_for('onboarding.step', step_num=3))
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"🔍 ONBOARDING DEBUG: Error in site config: {e}")
            flash(f'Error saving site configuration: {e}', 'error')
    
    # Get timezone list
    timezones = pytz.common_timezones
    current_config = get_onboarding_data().get('site_config', {})
    
    logger.info(f"🔍 ONBOARDING DEBUG: Rendering step 2 template with current_config: {current_config}")
    
    return render_template('onboarding/step2_site_config.html',
                         timezones=timezones,
                         current_config=current_config,
                         step=2,
                         total_steps=5)


def data_options_step():
    """Step 3: Data import/migration options."""
    logger.info(f"🔍 ONBOARDING DEBUG: data_options_step called, method={request.method}")
    
    # Detect existing databases
    migration_system = AdvancedMigrationSystem()
    databases = migration_system.find_sqlite_databases()
    
    logger.info(f"🔍 ONBOARDING DEBUG: Found {len(databases)} databases: {[str(db) for db in databases]}")
    
    db_analysis = []
    for db_path in databases:
        version, analysis = migration_system.detect_database_version(db_path)
        db_analysis.append({
            'path': str(db_path),
            'name': db_path.name,
            'version': version,
            'analysis': analysis
        })
    
    logger.info(f"🔍 ONBOARDING DEBUG: Database analysis: {db_analysis}")
    
    if request.method == 'POST':
        try:
            logger.info(f"🔍 ONBOARDING DEBUG: Processing POST data: {dict(request.form)}")
            
            data_option = request.form.get('data_option')
            data_options: Dict[str, Any] = {'option': data_option}
            
            logger.info(f"🔍 ONBOARDING DEBUG: Selected data option: {data_option}")
            
            if data_option == 'migrate':
                # Check for pre-detected database selection
                selected_db = request.form.get('migration_source')
                
                # Check for custom database file upload
                custom_db_file = request.files.get('custom_db_path')
                
                if selected_db:
                    # User selected a pre-detected database
                    data_options['selected_database'] = selected_db
                    logger.info(f"🔍 ONBOARDING DEBUG: Selected pre-detected database: {selected_db}")
                    
                    # Find analysis for selected database
                    for db in db_analysis:
                        if db['path'] == selected_db:
                            data_options['database_analysis'] = db
                            logger.info(f"🔍 ONBOARDING DEBUG: Found database analysis: {db}")
                            break
                            
                elif custom_db_file and custom_db_file.filename:
                    # User uploaded a custom database file
                    import tempfile
                    import os
                    
                    # Save uploaded database file to temporary location
                    temp_dir = tempfile.gettempdir()
                    temp_fd, temp_path = tempfile.mkstemp(
                        suffix=f"_{custom_db_file.filename}",
                        prefix="onboarding_db_",
                        dir=temp_dir
                    )
                    
                    try:
                        # Save the file
                        with os.fdopen(temp_fd, 'wb') as tmp_file:
                            custom_db_file.save(tmp_file)
                        
                        logger.info(f"🔍 ONBOARDING DEBUG: Custom database file saved: {custom_db_file.filename} -> {temp_path}")
                        
                        # Analyze the uploaded database
                        migration_system = AdvancedMigrationSystem()
                        version, analysis = migration_system.detect_database_version(Path(temp_path))
                        
                        # Store database info
                        data_options['selected_database'] = temp_path
                        custom_db_info = {
                            'path': temp_path,
                            'name': custom_db_file.filename,
                            'version': version,
                            'analysis': analysis
                        }
                        data_options['database_analysis'] = custom_db_info
                        
                        logger.info(f"🔍 ONBOARDING DEBUG: Custom database analysis: version={version}, analysis={analysis}")
                        
                    except Exception as e:
                        # Clean up on error
                        try:
                            os.unlink(temp_path)
                        except:
                            pass
                        logger.error(f"❌ Failed to process custom database file: {e}")
                        flash(f'Error processing database file: {e}', 'error')
                        return render_template('onboarding/step3_data_options.html',
                                             databases=db_analysis,
                                             step=3,
                                             total_steps=5)
                                             
                else:
                    # No database selected
                    logger.warning(f"🔍 ONBOARDING DEBUG: No database selected for migration")
                    flash('Please select a database to migrate from before proceeding.', 'error')
                    return render_template('onboarding/step3_data_options.html',
                                         databases=db_analysis,
                                         step=3,
                                         total_steps=5)
            
            elif data_option == 'import':
                # Handle file upload for import
                uploaded_file = request.files.get('import_file')
                if not uploaded_file or not uploaded_file.filename:
                    logger.warning(f"🔍 ONBOARDING DEBUG: No import file provided")
                    flash('Please select a CSV file to import before proceeding.', 'error')
                    # Return to the same step instead of advancing
                    return render_template('onboarding/step3_data_options.html',
                                         databases=db_analysis,
                                         step=3,
                                         total_steps=5)
                
                if uploaded_file and uploaded_file.filename:
                    import tempfile
                    import os
                    
                    # Save uploaded file to temporary location
                    temp_dir = tempfile.gettempdir()
                    # Create a secure temporary filename
                    temp_fd, temp_path = tempfile.mkstemp(
                        suffix=f"_{uploaded_file.filename}",
                        prefix="onboarding_import_",
                        dir=temp_dir
                    )
                    
                    try:
                        # Save the file
                        with os.fdopen(temp_fd, 'wb') as tmp_file:
                            uploaded_file.save(tmp_file)
                        
                        # Store both filename and path
                        data_options['import_file'] = uploaded_file.filename
                        data_options['import_file_path'] = temp_path
                        logger.info(f"🔍 ONBOARDING DEBUG: Import file saved: {uploaded_file.filename} -> {temp_path}")
                        
                    except Exception as e:
                        # Clean up on error
                        try:
                            os.unlink(temp_path)
                        except:
                            pass
                        logger.error(f"❌ Failed to save uploaded file: {e}")
                        flash(f'Error saving uploaded file: {e}', 'error')
                        raise
            
            logger.info(f"🔍 ONBOARDING DEBUG: Saving data_options: {data_options}")
            update_onboarding_data({'data_options': data_options})
            
            # Move to next step
            logger.info(f"🔍 ONBOARDING DEBUG: Moving to step 4")
            set_onboarding_step(4)
            
            logger.info(f"🔍 ONBOARDING DEBUG: Session after update: {dict(session)}")
            
            return redirect(url_for('onboarding.step', step_num=4))
            
        except Exception as e:
            logger.error(f"🔍 ONBOARDING DEBUG: Error in data options: {e}")
            flash(f'Error processing data options: {e}', 'error')
    
    logger.info(f"🔍 ONBOARDING DEBUG: Rendering step 3 template")
    
    return render_template('onboarding/step3_data_options.html',
                         databases=db_analysis,
                         step=3,
                         total_steps=5)


def data_config_step():
    """Step 4: Configure migration/import settings."""
    logger.info(f"🔍 ONBOARDING DEBUG: data_config_step called, method={request.method}")
    
    data_options = get_onboarding_data().get('data_options', {})
    option = data_options.get('option')
    
    logger.info(f"🔍 ONBOARDING DEBUG: data_options: {data_options}")
    logger.info(f"🔍 ONBOARDING DEBUG: option: {option}")
    
    if option == 'migrate':
        logger.info(f"🔍 ONBOARDING DEBUG: Calling migration_config_step")
        return migration_config_step(data_options)
    elif option == 'import':
        logger.info(f"🔍 ONBOARDING DEBUG: Calling import_config_step")
        return import_config_step(data_options)
    else:
        # Skip configuration for fresh start
        logger.info(f"🔍 ONBOARDING DEBUG: Fresh start, skipping to step 5")
        set_onboarding_step(5)
        return redirect(url_for('onboarding.step', step_num=5))


def migration_config_step(data_options: Dict):
    """Configure migration settings."""
    logger.info(f"🔍 ONBOARDING DEBUG: migration_config_step called, method={request.method}")
    
    database_analysis_data = data_options.get('database_analysis', {})
    
    # Create a simple namespace object for template compatibility
    from types import SimpleNamespace
    
    if database_analysis_data and 'analysis' in database_analysis_data:
        analysis = database_analysis_data['analysis']
        version = database_analysis_data.get('version', 'unknown')
    else:
        analysis = {}
        version = 'unknown'
    
    # Create structured objects for template
    version_obj = SimpleNamespace(value=version)
    database_analysis = SimpleNamespace(
        version=version_obj,
        users=analysis.get('users', []),
        book_count=analysis.get('book_count', 0),
        user_count=analysis.get('user_count', 0),
        detected_version=analysis.get('detected_version', version)
    )
    
    logger.info(f"🔍 ONBOARDING DEBUG: database_analysis_data: {database_analysis_data}")
    logger.info(f"🔍 ONBOARDING DEBUG: structured database_analysis version: {version}")
    
    if request.method == 'POST':
        try:
            logger.info(f"🔍 ONBOARDING DEBUG: Processing POST data: {dict(request.form)}")
            
            migration_config = {'type': 'migration'}
            
            if version == DatabaseVersion.V2_MULTI_USER:
                # Handle user mapping for V2 databases
                admin_user_mapping = request.form.get('admin_user_mapping')
                if not admin_user_mapping:
                    logger.error("❌ No admin_user_mapping provided for V2 migration")
                    flash('Please select a user mapping for migration.', 'error')
                    return redirect(url_for('onboarding.step', step_num=4))
                
                try:
                    migration_config['admin_user_mapping'] = str(int(admin_user_mapping))
                except (ValueError, TypeError) as e:
                    logger.error(f"❌ Invalid admin_user_mapping value: {e}")
                    flash('Invalid user mapping selection.', 'error')
                    return redirect(url_for('onboarding.step', step_num=4))
                
                logger.info(f"🔍 ONBOARDING DEBUG: V2 migration with admin_user_mapping: {admin_user_mapping}")
            
            logger.info(f"🔍 ONBOARDING DEBUG: Saving migration_config: {migration_config}")
            update_onboarding_data({'migration_config': migration_config})
            
            # Move to confirmation step
            logger.info(f"🔍 ONBOARDING DEBUG: Moving to step 5")
            set_onboarding_step(5)
            
            logger.info(f"🔍 ONBOARDING DEBUG: Session after update: {dict(session)}")
            
            return redirect(url_for('onboarding.step', step_num=5))
            
        except Exception as e:
            logger.error(f"🔍 ONBOARDING DEBUG: Error in migration config: {e}")
            flash(f'Error configuring migration: {e}', 'error')
    
    logger.info(f"🔍 ONBOARDING DEBUG: Rendering step 4 migration template")
    
    return render_template('onboarding/step4_migration_config.html',
                         database_analysis=database_analysis,
                         step=4,
                         total_steps=5)


def import_config_step(data_options: Dict):
    """Configure import settings - simplified version that mirrors the library import."""
    logger.info(f"🔍 ONBOARDING DEBUG: import_config_step called, method={request.method}")
    logger.info(f"🔍 ONBOARDING DEBUG: data_options: {data_options}")
    
    if request.method == 'POST':
        try:
            # Get the CSV file path from data_options
            csv_file_path = data_options.get('import_file_path')
            
            if not csv_file_path:
                logger.error("❌ No CSV file path found in data_options")
                flash('No CSV file found. Please go back and select a file.', 'error')
                return redirect(url_for('onboarding.step', step_num=3))
            
            logger.info(f"📂 Analyzing CSV file: {csv_file_path}")
            
            # Detect the file type automatically (same logic as direct_import)
            try:
                with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                    first_line = csvfile.readline()
                    
                # Determine import type based on headers (same logic as direct_import)
                goodreads_signatures = ['Book Id', 'Author l-f', 'Bookshelves', 'Exclusive Shelf']
                storygraph_signatures = ['Read Status', 'Moods', 'Pace', 'Character- or Plot-Driven?']
                
                detected_type = None
                field_mappings = {}
                
                if any(sig in first_line for sig in goodreads_signatures):
                    detected_type = 'goodreads'
                    field_mappings = get_goodreads_field_mappings()
                    logger.info("✅ Detected Goodreads CSV format")
                elif any(sig in first_line for sig in storygraph_signatures):
                    detected_type = 'storygraph'
                    field_mappings = get_storygraph_field_mappings()
                    logger.info("✅ Detected StoryGraph CSV format")
                else:
                    # Check if it's a simple ISBN-only file
                    csvfile.seek(0)  # Reset to beginning
                    lines = csvfile.readlines()[:5]  # Check first 5 lines
                    
                    # If all lines look like ISBNs (10 or 13 digits), treat as ISBN-only
                    isbn_like_lines = 0
                    for line in lines:
                        cleaned = line.strip().replace('-', '').replace(' ', '')
                        if cleaned.isdigit() and len(cleaned) in [10, 13]:
                            isbn_like_lines += 1
                    
                    if isbn_like_lines >= len(lines) * 0.8:  # 80% of lines look like ISBNs
                        detected_type = 'isbn_only'
                        field_mappings = {'isbn': 'isbn'}  # Simple mapping for ISBN-only files
                        logger.info("✅ Detected ISBN-only CSV format")
                    else:
                        logger.warning("❌ Could not detect CSV format")
                        flash('Could not detect the format of your CSV file. Please ensure it is a Goodreads export, StoryGraph export, or a simple list of ISBNs.', 'error')
                        return redirect(url_for('onboarding.step', step_num=3))
                
            except Exception as e:
                logger.error(f"❌ Error analyzing CSV file: {e}")
                flash(f'Error reading CSV file: {e}', 'error')
                return redirect(url_for('onboarding.step', step_num=3))
            
            # Create import configuration using the detected format
            import_config = {
                'type': 'import',
                'csv_file_path': csv_file_path,
                'field_mappings': field_mappings,
                'detected_type': detected_type,
                'default_reading_status': 'library_only',
                'duplicate_handling': 'skip',
                'custom_fields_enabled': True,
                'import_options': {}
            }
            
            logger.info(f"🔧 Created import config: detected_type={detected_type}, mappings={len(field_mappings)} fields")
            
            # Save the import configuration to onboarding data
            update_onboarding_data({'import_config': import_config})
            
            # Move to confirmation step
            set_onboarding_step(5)
            return redirect(url_for('onboarding.step', step_num=5))
            
        except Exception as e:
            logger.error(f"❌ Error in import config: {e}")
            flash(f'Error processing import configuration: {e}', 'error')
            return redirect(url_for('onboarding.step', step_num=3))
    
    # If GET request, auto-configure import and redirect to confirmation
    logger.info("🔍 GET request on import config - auto-configuring import")
    
    try:
        # Get the CSV file path from data_options
        csv_file_path = data_options.get('import_file_path')
        
        if not csv_file_path:
            logger.error("❌ No CSV file path found in data_options")
            flash('No CSV file found. Please go back and select a file.', 'error')
            return redirect(url_for('onboarding.step', step_num=3))
        
        logger.info(f"📂 Auto-configuring for CSV file: {csv_file_path}")
        
        # Detect the file type automatically (same logic as POST)
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            first_line = csvfile.readline()
            
        # Determine import type based on headers
        goodreads_signatures = ['Book Id', 'Author l-f', 'Bookshelves', 'Exclusive Shelf']
        storygraph_signatures = ['Read Status', 'Moods', 'Pace', 'Character- or Plot-Driven?']
        
        detected_type = None
        field_mappings = {}
        
        if any(sig in first_line for sig in goodreads_signatures):
            detected_type = 'goodreads'
            field_mappings = get_goodreads_field_mappings()
            logger.info("✅ Auto-detected Goodreads CSV format")
        elif any(sig in first_line for sig in storygraph_signatures):
            detected_type = 'storygraph'
            field_mappings = get_storygraph_field_mappings()
            logger.info("✅ Auto-detected StoryGraph CSV format")
        else:
            # Check if it's a simple ISBN-only file
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                lines = csvfile.readlines()[:5]  # Check first 5 lines
                
            # If all lines look like ISBNs (10 or 13 digits), treat as ISBN-only
            isbn_like_lines = 0
            for line in lines:
                cleaned = line.strip().replace('-', '').replace(' ', '')
                if cleaned.isdigit() and len(cleaned) in [10, 13]:
                    isbn_like_lines += 1
            
            if isbn_like_lines >= len(lines) * 0.8:  # 80% of lines look like ISBNs
                detected_type = 'isbn_only'
                field_mappings = {'isbn': 'isbn'}
                logger.info("✅ Auto-detected ISBN-only CSV format")
            else:
                logger.warning("❌ Could not detect CSV format, using generic mapping")
                detected_type = 'generic'
                field_mappings = {
                    'Title': 'title',
                    'Author': 'author',
                    'ISBN': 'isbn',
                    'Description': 'description'
                }
        
        # Create import configuration
        import_config = {
            'type': 'import',
            'csv_file_path': csv_file_path,
            'field_mappings': field_mappings,
            'detected_type': detected_type,
            'default_reading_status': 'library_only',
            'duplicate_handling': 'skip',
            'custom_fields_enabled': True,
            'import_options': {}
        }
        
        logger.info(f"🔧 Auto-created import config: detected_type={detected_type}, mappings={len(field_mappings)} fields")
        
        # Save the import configuration to onboarding data
        update_onboarding_data({'import_config': import_config})
        
        # Show success message and move to confirmation step
        flash(f'Import automatically configured for {detected_type} format!', 'success')
        set_onboarding_step(5)
        return redirect(url_for('onboarding.step', step_num=5))
        
    except Exception as e:
        logger.error(f"❌ Error in auto-configuration: {e}")
        flash('Import configuration will be automatically detected based on your file.', 'info')
        set_onboarding_step(5)
        return redirect(url_for('onboarding.step', step_num=5))


def get_goodreads_field_mappings():
    """Get predefined field mappings for Goodreads CSV format (same as direct_import)."""
    return {
        'Title': 'title',
        'Author': 'author',
        'Additional Authors': 'additional_authors',
        'ISBN': 'isbn',
        'ISBN13': 'isbn',
        'My Rating': 'rating',
        'Average Rating': 'custom_global_average_rating',
        'Publisher': 'publisher',
        'Binding': 'custom_global_binding',
        'Number of Pages': 'page_count',
        'Year Published': 'publication_year',
        'Published Date': 'published_date',  # In case Goodreads has full dates
        'Publication Date': 'published_date',  # Alternative field name
        'Original Publication Year': 'custom_global_original_publication_year',
        'Date Read': 'date_read',
        'Date Added': 'date_added',
        'Bookshelves': 'custom_global_bookshelves',  # Keep as custom field - usually reading status, not genres
        'Bookshelves with positions': 'custom_global_bookshelves_with_positions',
        'Exclusive Shelf': 'reading_status',
        'My Review': 'notes',
        'Spoiler': 'custom_global_spoiler',
        'Private Notes': 'custom_global_private_notes',
        'Read Count': 'custom_global_read_count',
        'Recommended For': 'custom_global_recommended_for',
        'Recommended By': 'custom_global_recommended_by',
        'Owned Copies': 'custom_global_owned_copies',
        'Original Purchase Date': 'custom_global_original_purchase_date',
        'Original Purchase Location': 'custom_global_original_purchase_location',
        'Condition': 'custom_global_condition',
        'Condition Description': 'custom_global_condition_description',
        'BCID': 'custom_global_bcid'
    }


def get_storygraph_field_mappings():
    """Get predefined field mappings for StoryGraph CSV format (same as direct_import)."""
    return {
        'Title': 'title',
        'Authors': 'author',
        'Contributors': 'contributors',  # New: Handle Contributors column
        'ISBN/UID': 'isbn',  # Fixed: Use actual StoryGraph column name
        'Star Rating': 'rating',  # Fixed: StoryGraph uses "Star Rating" not "My Rating"
        'Read Status': 'reading_status',
        'Date Started': 'start_date',
        'Last Date Read': 'date_read',  # Fixed: StoryGraph uses "Last Date Read"
        'Publication Year': 'publication_year',  # StoryGraph may have this field
        'Published Date': 'published_date',  # In case StoryGraph has full dates
        'Publication Date': 'published_date',  # Alternative field name
        'Tags': 'categories',
        'Review': 'notes',  # Fixed: StoryGraph uses "Review" not "My Review"
        'Format': 'custom_global_format',
        'Moods': 'custom_global_moods',  # Fixed: Map Moods to custom field instead of categories
        'Pace': 'custom_global_pace',
        'Character- or Plot-Driven?': 'custom_global_character_plot_driven',
        'Strong Character Development?': 'custom_global_strong_character_development',
        'Loveable Characters?': 'custom_global_loveable_characters',
        'Diverse Characters?': 'custom_global_diverse_characters',
        'Flawed Characters?': 'custom_global_flawed_characters',
        'Content Warnings': 'custom_global_content_warnings',
        'Content Warning Description': 'custom_global_content_warning_description',
        'Owned?': 'custom_personal_owned'
    }


def confirmation_step():
    """Step 5: Confirmation and execution."""
    logger.info(f"🔍 ONBOARDING DEBUG: confirmation_step called, method={request.method}")
    
    # DEBUG: Log all form data for POST requests
    if request.method == 'POST':
        logger.info(f"🔍 ONBOARDING DEBUG: POST form data: {dict(request.form)}")
    
    onboarding_data = get_onboarding_data()
    
    logger.info(f"🔍 ONBOARDING DEBUG: onboarding_data: {onboarding_data}")
    
    # Ensure we stay on step 5 even if there are errors
    set_onboarding_step(5)
    
    if request.method == 'POST':
        print(f"🚨🚨🚨 STEP5 DEBUG: POST REQUEST RECEIVED! 🚨🚨🚨")
        print(f"🚨 STEP5 DEBUG: Form data: {dict(request.form)}")
        action = request.form.get('action')
        
        if action == 'execute':
            try:
                logger.info(f"🔍 ONBOARDING DEBUG: Executing onboarding with data keys: {list(onboarding_data.keys())}")
                
                # Validate that we have all required data before proceeding
                if not onboarding_data.get('admin'):
                    logger.error("❌ Missing admin data during execution")
                    flash('Missing admin account information. Please start over.', 'error')
                    return redirect(url_for('onboarding.step', step_num=1))
                
                if not onboarding_data.get('site_config'):
                    logger.error("❌ Missing site config data during execution")
                    flash('Missing site configuration. Please complete site setup.', 'error')
                    return redirect(url_for('onboarding.step', step_num=2))
                
                if not onboarding_data.get('data_options'):
                    logger.error("❌ Missing data options during execution")
                    flash('Missing data configuration. Please complete data setup.', 'error')
                    return redirect(url_for('onboarding.step', step_num=3))
                
                # Check if this is an import setup, migration, or fresh start
                data_options = onboarding_data.get('data_options', {})
                data_option = data_options.get('option', 'fresh')
                
                logger.info(f"🔍 ONBOARDING DEBUG: Data option selected: {data_option}")
                
                if data_option == 'import':
                    # For imports, execute like migrations - stay on same page with progress
                    logger.info(f"Import option selected - executing synchronous import like migration")
                    success = execute_onboarding(onboarding_data)
                    
                    if success:
                        return handle_onboarding_completion(onboarding_data)
                    else:
                        logger.error(f"🔍 ONBOARDING DEBUG: Import failed")
                        flash('Import failed. Please check the error message and try again.', 'error')
                        # Ensure session data is preserved
                        update_onboarding_data(onboarding_data)
                        set_onboarding_step(5)
                elif data_option == 'migrate':
                    # For migrations, execute full migration
                    logger.info(f"Migration option selected - executing full migration")
                    success = execute_onboarding(onboarding_data)
                    
                    if success:
                        return handle_onboarding_completion(onboarding_data)
                    else:
                        logger.error(f"🔍 ONBOARDING DEBUG: Migration failed")
                        flash('Migration failed. Please check the error message and try again.', 'error')
                        # Ensure session data is preserved
                        update_onboarding_data(onboarding_data)
                        set_onboarding_step(5)
                else:
                    # For fresh start or any other option, execute basic setup only
                    logger.info(f"🔍 ONBOARDING DEBUG: Fresh start - executing basic setup only")
                    success = execute_onboarding_setup_only(onboarding_data)
                    
                    if success:
                        logger.info(f"🔍 ONBOARDING DEBUG: Fresh start setup completed, proceeding to completion")
                        return handle_onboarding_completion(onboarding_data)
                    else:
                        logger.error(f"🔍 ONBOARDING DEBUG: Fresh start setup failed")
                        flash('Setup failed. Please check the error message and try again.', 'error')
                        # Ensure session data is preserved
                        update_onboarding_data(onboarding_data)
                        set_onboarding_step(5)
                    
            except Exception as e:
                logger.error(f"🔍 ONBOARDING DEBUG: Error executing onboarding: {e}")
                import traceback
                traceback.print_exc()
                flash(f'Setup failed with error: {str(e)}', 'error')
                # Ensure session data is preserved even on exception
                try:
                    update_onboarding_data(onboarding_data)
                    set_onboarding_step(5)
                except Exception as session_error:
                    logger.error(f"❌ Could not preserve session data: {session_error}")
        else:
            logger.info(f"Non-execute action received: {action}")
    
    logger.info(f"🔍 ONBOARDING DEBUG: Rendering step 5 confirmation template")
    
    return render_template('onboarding/step5_confirmation.html',
                         onboarding_data=onboarding_data,
                         step=5,
                         total_steps=5)


@onboarding_bp.route('/complete')
@debug_route('ONBOARDING_COMPLETE')
def complete():
    """Show onboarding completion page."""
    print(f"🎉 COMPLETION: Showing completion page")
    logger.info(f"🔍 ONBOARDING DEBUG: Showing completion page")
    
    # Check if user is authenticated (they should be after successful setup)
    from flask_login import current_user
    if not current_user.is_authenticated:
        flash('Please log in to continue.', 'info')
        return redirect(url_for('auth.login'))
    
    # Optional: Get any import summary data if available
    import_summary = session.get('import_summary', None)
    
    return render_template('onboarding/complete.html',
                         import_summary=import_summary)

@onboarding_bp.route('/import-progress/<task_id>')
@debug_route('ONBOARDING_IMPORT_PROGRESS')
def import_progress(task_id: str):
    """Show simple import splash screen during onboarding."""
    logger.info(f"🔍 ONBOARDING DEBUG: Showing import splash for task {task_id}")
    
    # Check if user is already authenticated (onboarding completed)
    from flask_login import current_user
    if current_user.is_authenticated:
        logger.info(f"🔍 ONBOARDING DEBUG: User is authenticated, redirecting to library")
        flash('Setup completed successfully! Welcome to your library.', 'success')
        return redirect(url_for('main.library'))
    
    try:
        # For simple onboarding, always show the splash screen regardless of task status
        # Get site configuration for template context
        onboarding_data = get_onboarding_data()
        site_config = onboarding_data.get('site_config', {})
        
        template_context = {
            'task_id': task_id,
            'step': 5,
            'total_steps': 5,
            'site_name': site_config.get('site_name', 'MyBibliotheca'),
            'current_theme': site_config.get('theme', 'light')
        }
        
        logger.info(f"🔍 ONBOARDING DEBUG: Rendering splash screen with context: {template_context}")
        return render_template('onboarding/import_splash.html', **template_context)
        
    except Exception as e:
        logger.error(f"❌ Error rendering import splash template: {e}")
        import traceback
        traceback.print_exc()
        # Return a simple fallback page
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Importing Your Library</title>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; 
                       text-align: center; padding: 50px; background: #f8f9fa; }}
                .spinner {{ width: 50px; height: 50px; border: 4px solid #f3f3f3; 
                           border-top: 4px solid #6f42c1; border-radius: 50%; 
                           animation: spin 1s linear infinite; margin: 20px auto; }}
                @keyframes spin {{ 0% {{ transform: rotate(0deg); }} 100% {{ transform: rotate(360deg); }} }}
            </style>
        </head>
        <body>
            <h1>📚 Importing Your Library</h1>
            <div class="spinner"></div>
            <p>Please be patient while we set up your personal library...</p>
            <p><small>Task ID: {task_id}</small></p>
            <script>
                // Auto-redirect after 3 minutes
                setTimeout(function() {{
                    window.location.href = '/library';
                }}, 180000);
                
                // Check for completion every 5 seconds
                setInterval(function() {{
                    fetch('/onboarding/import-progress-json/{task_id}')
                        .then(response => response.json())
                        .then(data => {{
                            if (data.status === 'completed' || data.status === 'success' || data.status === 'failed') {{
                                setTimeout(() => window.location.href = '/library', 2000);
                            }}
                        }})
                        .catch(() => {{}}); // Ignore errors
                }}, 5000);
            </script>
        </body>
        </html>
        """, 200


@onboarding_bp.route('/import-progress-json/<task_id>')
@debug_route('ONBOARDING_IMPORT_PROGRESS_JSON')
def import_progress_json(task_id: str):
    """Get import progress data as JSON during onboarding."""
    logger.info(f"🔍 ONBOARDING DEBUG: Getting progress JSON for task {task_id}")
    
    try:
        # Check if user is already authenticated (onboarding completed)
        from flask_login import current_user
        if current_user.is_authenticated:
            # If user is logged in, onboarding likely completed - return success status
            logger.info(f"✅ User is authenticated - assuming import completed successfully")
            return jsonify({
                'status': 'completed',
                'processed': 1,
                'success': 1,
                'errors': 0,
                'total': 1,
                'current_book': 'Setup completed',
                'error_messages': [],
                'recent_activity': ['Setup completed successfully - please continue to your library']
            })
        
        # First, try to get job from safe manager for real import tasks
        # Note: We need user context, but during onboarding the user might not be logged in yet
        # Try to get the user ID from the onboarding session data
        onboarding_data = get_onboarding_data()
        admin_data = onboarding_data.get('admin', {})
        
        # If we have admin user ID from session, try to get the job safely
        if admin_data and 'user_id' in admin_data:
            job = safe_get_import_job(admin_data['user_id'], task_id)
        else:
            # Fallback: Try to get from current user if authenticated
            from flask_login import current_user
            if current_user.is_authenticated:
                job = safe_get_import_job(current_user.id, task_id)
            else:
                job = None
        
        if job:
            logger.info(f"📊 Found real job data: {job}")
            return jsonify({
                'status': job.get('status', 'pending'),
                'processed': job.get('processed', 0),
                'success': job.get('success', 0),
                'errors': job.get('errors', 0),
                'total': job.get('total', 0),
                'current_book': job.get('current_book'),
                'error_messages': job.get('error_messages', []),
                'recent_activity': job.get('recent_activity', [])
            })
        
        # For simple onboarding fallback (when no real import was started)
        if task_id == 'simple-onboarding-import':
            # Return completed status immediately for simple imports
            logger.info(f"✅ Simple onboarding import - returning completed status")
            return jsonify({
                'status': 'completed',
                'processed': 1,
                'success': 1,
                'errors': 0,
                'total': 1,
                'current_book': 'Import completed',
                'error_messages': [],
                'recent_activity': ['Import completed successfully - redirecting to library']
            })
        
        # Job not found in memory - might be completed and cleaned up
        # Check if onboarding is complete by checking session
        logger.warning(f"⚠️ Job {task_id} not found in memory - checking if onboarding is complete")
        
        # If the onboarding session is cleared, assume the job completed successfully
        current_step = get_onboarding_step()
        if current_step is None or current_step == 1:  # Default state or cleared
            logger.info(f"✅ No active onboarding step found - assuming import completed")
            return jsonify({
                'status': 'completed',
                'processed': 1,
                'success': 1,
                'errors': 0,
                'total': 1,
                'current_book': 'Import completed',
                'error_messages': [],
                'recent_activity': ['Import completed successfully']
            })
        else:
            logger.warning(f"⚠️ Job {task_id} not found but onboarding still active")
            return jsonify({
                'status': 'not_found',
                'processed': 0,
                'success': 0,
                'errors': 0,
                'total': 0,
                'error_messages': ['Job not found - may have been cleaned up']
            })
    
    except Exception as e:
        logger.error(f"❌ Error getting progress for task {task_id}: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'processed': 0,
            'success': 0,
            'errors': 0,
            'total': 0,
            'error_messages': [str(e)]
        })

def execute_onboarding(onboarding_data: Dict) -> bool:
    """Execute the complete onboarding configuration."""
    try:
        # Step 1: Create admin user
        admin_data = onboarding_data.get('admin', {})
        site_config = onboarding_data.get('site_config', {})
        password_hash = generate_password_hash(admin_data['password'])
        
        admin_user = user_service.create_user_sync(
            username=admin_data['username'],
            email=admin_data['email'],
            password_hash=password_hash,
            is_admin=True,
            is_active=True,
            password_must_change=False,
            timezone=site_config.get('timezone', 'UTC'),  # Set timezone from site config
            display_name=admin_data.get('display_name', ''),
            location=site_config.get('location', '')
        )
        
        print(f"🚀 [EXECUTE] Created admin user: {admin_user}")
        if admin_user:
            print(f"🚀 [EXECUTE] Admin user details: ID={admin_user.id}, username={admin_user.username}, email={admin_user.email}")
        else:
            print(f"🚀 [EXECUTE] Failed to create admin user!")
            return False
        
        # Step 2: Apply site configuration and create location
        site_config = onboarding_data.get('site_config', {})
        
        # Create location if specified
        location_name = site_config.get('location', '').strip()
        if location_name:
            try:
                # Initialize location service
                location_service = LocationService()
                
                # Create the location
                location = location_service.create_location(
                    name=location_name,
                    description=f"Default location set during onboarding",
                    location_type="home",  # Default to home type
                    is_default=site_config.get('location_set_as_default', True)
                    )
                
                logger.info(f"✅ Created location: {location.name} (ID: {location.id})")
                
            except Exception as e:
                logger.error(f"❌ Failed to create location '{location_name}': {e}")
                # Don't fail the entire onboarding if location creation fails
        
        # Apply site configuration settings to system settings
        try:
            from .admin import save_system_config
            system_config = {
                'site_name': site_config.get('site_name', 'MyBibliotheca'),
                'server_timezone': site_config.get('timezone', 'UTC')
            }
            if save_system_config(system_config):
                logger.info(f"✅ Applied site configuration to system settings: {system_config}")
            else:
                logger.warning(f"⚠️ Failed to save site configuration to system settings")
        except Exception as e:
            logger.error(f"❌ Error applying site configuration: {e}")
            # Don't fail onboarding if system config save fails
        
        # Step 3: Handle data migration/import
        data_options = onboarding_data.get('data_options', {})
        if data_options.get('option') == 'migrate':
            migration_system = AdvancedMigrationSystem()
            
            # Create backup
            selected_db = data_options.get('selected_database')
            if selected_db:
                db_path = Path(selected_db)
                migration_system.create_backup(db_path)
                
                # Execute migration based on database version
                database_analysis = data_options.get('database_analysis', {})
                if database_analysis.get('version') == DatabaseVersion.V1_SINGLE_USER:
                    # Ensure admin_user.id is not None
                    if not admin_user.id:
                        logger.error("❌ Admin user has no ID - cannot migrate V1 database")
                        return False
                    success = migration_system.migrate_v1_database(db_path, admin_user.id)
                elif database_analysis.get('version') == DatabaseVersion.V2_MULTI_USER:
                    migration_config = onboarding_data.get('migration_config', {})
                    admin_mapping = migration_config.get('admin_user_mapping')
                    
                    # Ensure we have valid user mapping with correct types
                    if not admin_mapping or not admin_user.id:
                        logger.error("❌ Invalid user mapping for V2 migration")
                        return False
                    
                    # Create user mapping with proper types (Dict[int, str])
                    # At this point we know admin_user.id is not None due to the check above
                    try:
                        user_mapping: Dict[int, str] = {int(admin_mapping): admin_user.id}
                    except (ValueError, TypeError) as e:
                        logger.error(f"❌ Invalid admin_mapping type: {e}")
                        return False
                    
                    success = migration_system.migrate_v2_database(db_path, user_mapping, fetch_api_metadata=True)
                else:
                    success = False
                
                if not success:
                    return False
        
        elif data_options.get('option') == 'import':
            # Handle file import with custom metadata support
            import_config = onboarding_data.get('import_config', {})
            custom_fields = import_config.get('custom_fields', {})
            
            # Create custom metadata fields first
            if custom_fields:
                try:
                    from .services import custom_field_service
                    from .domain.models import CustomFieldDefinition, CustomFieldType
                    
                    for csv_field, field_config in custom_fields.items():
                        field_name = field_config['name']
                        field_type_str = field_config['type']
                        
                        # Convert string type to CustomFieldType enum
                        field_type_map = {
                            'string': CustomFieldType.TEXT,
                            'integer': CustomFieldType.NUMBER,
                            'date': CustomFieldType.DATE,
                            'boolean': CustomFieldType.BOOLEAN,
                            'decimal': CustomFieldType.NUMBER,  # Use NUMBER for decimal too
                            'textarea': CustomFieldType.TEXTAREA,
                            'tags': CustomFieldType.TAGS,
                            'list': CustomFieldType.LIST
                        }
                        field_type = field_type_map.get(field_type_str, CustomFieldType.TEXT)
                        
                        # Create CustomFieldDefinition object
                        if not admin_user.id:
                            logger.error("❌ Admin user has no ID - cannot create custom fields")
                            continue
                        
                        field_definition = CustomFieldDefinition(
                            name=field_name,
                            display_name=csv_field,  # Use original CSV header as display name
                            field_type=field_type,
                            description=f"Auto-created from CSV import: {csv_field}",
                            created_by_user_id=admin_user.id,
                            is_shareable=False,
                            is_global=False
                        )
                        
                        # Create the custom field
                        field_data = {
                            'name': field_definition.name,
                            'display_name': field_definition.display_name,
                            'field_type': field_definition.field_type.value if hasattr(field_definition.field_type, 'value') else str(field_definition.field_type),
                            'description': field_definition.description,
                            'is_global': field_definition.is_global,
                            'is_shareable': field_definition.is_shareable
                        }
                        custom_field = custom_field_service.create_field_sync(admin_user.id, field_data)
                        
                        logger.info(f"✅ Created custom field: {field_name} ({field_type}) from CSV column: {csv_field}")
                        
                except Exception as e:
                    logger.error(f"❌ Failed to create custom metadata fields: {e}")
                    # Continue with import even if custom fields fail
            
            # Execute actual CSV file import synchronously (like migration)
            logger.info(f"📋 Import configuration saved: {import_config}")
            logger.info(f"🔧 Custom fields configured: {len(custom_fields)}")
            
            if not admin_user.id:
                logger.error("❌ Admin user has no ID")
                return False
                
            # Import synchronously using the same pattern as migration system
            try:
                import csv
                
                logger.info(f"🚀 Starting CSV import for common library model")
                
                csv_file_path = import_config.get('csv_file_path')
                field_mappings = import_config.get('field_mappings', {})
                
                if not csv_file_path or not Path(csv_file_path).exists():
                    logger.error(f"❌ CSV file not found: {csv_file_path}")
                    return False
                
                # Use the migration system's approach: direct book service, no user relationships
                migration_system = AdvancedMigrationSystem()
                books_imported = 0
                errors = 0
                
                with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    
                    for row in reader:
                        try:
                            # Extract basic book data using field mappings
                            book_data = {}
                            for csv_field, domain_field in field_mappings.items():
                                if csv_field in row and row[csv_field]:
                                    value = row[csv_field].strip()
                                    if value:
                                        book_data[domain_field] = value
                            
                            # Skip rows without title
                            if not book_data.get('title'):
                                continue
                            
                            # Create Book domain object (minimal for common library)
                            from .domain.models import Book
                            book = Book(
                                title=book_data.get('title', ''),
                                isbn13=book_data.get('isbn', ''),
                                isbn10=None,
                                subtitle=book_data.get('subtitle', ''),
                                description=book_data.get('description', ''),
                                publisher=book_data.get('publisher', ''),
                                published_date=book_data.get('published_date', ''),
                                page_count=int(book_data.get('page_count', 0)) if book_data.get('page_count', '').isdigit() else None,
                                language=book_data.get('language', 'en'),
                            )
                            
                            # Create book using the same pattern as migration system
                            created_book = migration_system.book_service.create_book_sync(
                                domain_book=book
                            )
                            
                            if created_book:
                                books_imported += 1
                                logger.info(f"✅ Imported book: {book.title}")
                            else:
                                errors += 1
                                logger.warning(f"❌ Failed to import book: {book.title}")
                                
                        except Exception as book_error:
                            errors += 1
                            logger.error(f"❌ Error importing book from row: {book_error}")
                            continue
                
                logger.info(f"✅ CSV import completed: {books_imported} books imported, {errors} errors")
                
            except Exception as e:
                logger.error(f"❌ Failed to execute CSV import: {e}")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error executing onboarding: {e}")
        return False


def execute_onboarding_setup_only(onboarding_data: Dict) -> bool:
    """Execute only the basic onboarding setup (user, location, custom fields) without CSV import."""
    try:
        print(f"🚀 [SETUP] ============ STARTING ONBOARDING SETUP ============")
        logger.info(f"🚀 Starting onboarding setup with data keys: {list(onboarding_data.keys())}")
        
        # Log full onboarding data for debugging (without passwords)
        debug_data = onboarding_data.copy()
        if 'admin' in debug_data and 'password' in debug_data['admin']:
            debug_data['admin'] = debug_data['admin'].copy()
            debug_data['admin']['password'] = '[REDACTED]'
        print(f"🚀 [SETUP] Full onboarding data: {debug_data}")
        logger.info(f"Full onboarding data (passwords redacted): {debug_data}")
        
        # Step 1: Create admin user
        admin_data = onboarding_data.get('admin', {})
        site_config = onboarding_data.get('site_config', {})
        
        if not admin_data:
            logger.error("❌ Missing admin data in execute_onboarding_setup_only")
            return False
        
        print(f"🚀 [SETUP] ============ STEP 1: CREATING ADMIN USER ============")
        print(f"🚀 [SETUP] Admin data keys: {list(admin_data.keys())}")
        print(f"🚀 [SETUP] Username: {admin_data.get('username')}")
        print(f"🚀 [SETUP] Email: {admin_data.get('email')}")
        print(f"🚀 [SETUP] Has password: {bool(admin_data.get('password'))}")
        print(f"🚀 [SETUP] Site config: {site_config}")
        logger.info(f"Creating admin user: {admin_data.get('username')} with email: {admin_data.get('email')}")
        
        try:
            password_hash = generate_password_hash(admin_data['password'])
            print(f"🚀 [SETUP] Password hash generated successfully")
        except Exception as hash_error:
            logger.error(f"Failed to generate password hash: {hash_error}")
            return False
        
        try:
            print(f"🚀 [SETUP] Calling user_service.create_user_sync...")
            admin_user = user_service.create_user_sync(
                username=admin_data['username'],
                email=admin_data['email'],
                password_hash=password_hash,
                is_admin=True,
                is_active=True,
                password_must_change=False,
                timezone=site_config.get('timezone', 'UTC'),  # Set timezone from site config
                display_name=admin_data.get('display_name', ''),
                location=site_config.get('location', '')
            )
            print(f"🚀 [SETUP] user_service.create_user_sync returned: {admin_user}")
        except Exception as user_create_error:
            logger.error(f"Exception creating admin user: {user_create_error}")
            import traceback
            traceback.print_exc()
            return False
        
        if not admin_user:
            logger.error("❌ Failed to create admin user - service returned None")
            return False
        
        logger.info(f"✅ Admin user created: {admin_user.username} (ID: {admin_user.id})")
        
        # Log in the user immediately after creation
        try:
            print(f"🚀 [SETUP] Attempting to log in admin user...")
            login_user(admin_user)
            logger.info(f"✅ Admin user logged in: {admin_user.username}")
            
            # Verify login worked
            from flask_login import current_user
        except Exception as login_error:
            logger.error(f"Failed to log in admin user: {login_error}")
            # Don't return False here - continue with setup even if login fails
        
        print(f"🚀 [SETUP] ============ STEP 2: CREATING LOCATION ============")
        # Step 2: Apply site configuration and create location
        site_config = onboarding_data.get('site_config', {})
        
        # Create location if specified
        location_name = site_config.get('location', '').strip()
        if location_name:
            try:
                print(f"🏠 [SETUP] Location name specified: '{location_name}'")
                print(f"🏠 [SETUP] Site config: {site_config}")
                logger.info(f"Creating location: {location_name}")
                
                # Initialize location service with safe Kuzu connection
                print(f"🏠 [SETUP] Getting safe Kuzu connection...")
                from app.utils.safe_kuzu_manager import safe_get_connection
                
                # Create location service
                admin_user_id = getattr(admin_user, 'id', None)
                print(f"🏠 [SETUP] Admin user ID for location creation: {admin_user_id}")
                if not admin_user_id:
                    logger.error(f"❌ Admin user has no ID for location creation")
                    raise Exception("Admin user has no ID")
                
                print(f"🏠 [SETUP] Creating LocationService...")
                location_service = LocationService()
                print(f"🏠 [SETUP] LocationService created: {location_service}")
                
                # Create the location
                print(f"🏠 [SETUP] Calling location_service.create_location...")
                location = location_service.create_location(
                    name=location_name,
                    description=f"Default location set during onboarding",
                    location_type="home",  # Default to home type
                    is_default=site_config.get('location_set_as_default', True)
                )
                print(f"🏠 [SETUP] create_location returned: {location}")
                
                if location:
                    logger.info(f"✅ Created location: {location.name} (ID: {location.id})")
                else:
                    logger.error(f"❌ Location creation returned None")
                
            except Exception as e:
                logger.error(f"❌ Failed to create location '{location_name}': {e}")
                import traceback
                traceback.print_exc()
                # Don't fail the entire onboarding if location creation fails
        else:
            print(f"🏠 [SETUP] No location specified (location='{location_name}'), skipping location creation")
            logger.info("No location specified, skipping location creation")
        
        # Apply site configuration settings to system settings
        print(f"🚀 [SETUP] ============ APPLYING SITE CONFIGURATION ============")
        try:
            from .admin import save_system_config
            system_config = {
                'site_name': site_config.get('site_name', 'MyBibliotheca'),
                'server_timezone': site_config.get('timezone', 'UTC')
            }
            print(f"🚀 [SETUP] Applying system config: {system_config}")
            if save_system_config(system_config):
                logger.info(f"✅ Applied site configuration to system settings: {system_config}")
                print(f"🚀 [SETUP] Site configuration applied successfully")
            else:
                logger.warning(f"⚠️ Failed to save site configuration to system settings")
                print(f"🚀 [SETUP] Failed to apply site configuration")
        except Exception as e:
            logger.error(f"❌ Error applying site configuration: {e}")
            print(f"🚀 [SETUP] Error applying site configuration: {e}")
            # Don't fail onboarding if system config save fails
        
        print(f"🚀 [SETUP] ============ STEP 3: CREATING CUSTOM FIELDS ============")
        # Step 3: Create custom metadata fields for import
        import_config = onboarding_data.get('import_config', {})
        custom_fields = import_config.get('custom_fields', {})
        
        print(f"🏷️ [SETUP] Import config: {import_config}")
        print(f"🏷️ [SETUP] Custom fields to create: {len(custom_fields)} fields")
        print(f"🏷️ [SETUP] Custom fields: {custom_fields}")
        
        # Create custom metadata fields first
        if custom_fields:
            try:
                print(f"🏷️ [SETUP] Starting creation of {len(custom_fields)} custom fields")
                logger.info(f"Creating {len(custom_fields)} custom fields")
                
                print(f"🏷️ [SETUP] Importing custom field service...")
                from .services import custom_field_service
                from .domain.models import CustomFieldDefinition, CustomFieldType
                print(f"🏷️ [SETUP] Imports successful")
                
                for csv_field, field_config in custom_fields.items():
                    print(f"🏷️ [SETUP] Processing custom field: {csv_field} -> {field_config}")
                    
                    field_name = field_config['name']
                    field_type_str = field_config['type']
                    
                    print(f"🏷️ [SETUP] Field name: {field_name}, type: {field_type_str}")
                    
                    # Convert string type to CustomFieldType enum
                    field_type_map = {
                        'string': CustomFieldType.TEXT,
                        'integer': CustomFieldType.NUMBER,
                        'date': CustomFieldType.DATE,
                        'boolean': CustomFieldType.BOOLEAN,
                        'decimal': CustomFieldType.NUMBER,  # Use NUMBER for decimal too
                        'textarea': CustomFieldType.TEXTAREA,
                        'tags': CustomFieldType.TAGS,
                        'list': CustomFieldType.LIST
                    }
                    field_type = field_type_map.get(field_type_str, CustomFieldType.TEXT)
                    print(f"🏷️ [SETUP] Mapped field type: {field_type_str} -> {field_type}")
                    
                    # Check if admin user has an ID
                    admin_user_id = getattr(admin_user, 'id', None)
                    print(f"🏷️ [SETUP] Admin user ID for custom field creation: {admin_user_id}")
                    if not admin_user_id:
                        logger.error(f"❌ Admin user has no ID for custom field creation")
                        continue  # Skip this custom field
                    
                    try:
                        print(f"🏷️ [SETUP] Creating CustomFieldDefinition object...")
                        # Create CustomFieldDefinition object
                        field_definition = CustomFieldDefinition(
                            name=field_name,
                            display_name=csv_field,  # Use original CSV header as display name
                            field_type=field_type,
                            description=f"Auto-created from CSV import: {csv_field}",
                            created_by_user_id=admin_user_id,  # Use the checked ID
                            is_shareable=False,
                            is_global=False
                        )
                        print(f"🏷️ [SETUP] CustomFieldDefinition created: {field_definition}")
                        
                        # Create the custom field
                        print(f"🏷️ [SETUP] Preparing field data for service...")
                        field_data = {
                            'name': field_definition.name,
                            'display_name': field_definition.display_name,
                            'field_type': field_definition.field_type.value if hasattr(field_definition.field_type, 'value') else str(field_definition.field_type),
                            'description': field_definition.description,
                            'is_global': field_definition.is_global,
                            'is_shareable': field_definition.is_shareable
                        }
                        print(f"🏷️ [SETUP] Field data: {field_data}")
                        
                        print(f"🏷️ [SETUP] Calling custom_field_service.create_field_sync...")
                        custom_field = custom_field_service.create_field_sync(admin_user_id, field_data)  # Use the checked ID
                        print(f"🏷️ [SETUP] Custom field service returned: {custom_field}")
                        
                        if custom_field:
                            logger.info(f"✅ Created custom field: {field_name} ({field_type}) from CSV column: {csv_field}")
                        else:
                            logger.error(f"❌ Custom field creation returned None for: {field_name}")
                    
                    except Exception as field_error:
                        logger.error(f"❌ Exception creating custom field {field_name}: {field_error}")
                        import traceback
                        traceback.print_exc()
                        continue  # Continue with next field
                    
            except Exception as e:
                logger.error(f"❌ Failed to create custom metadata fields: {e}")
                import traceback
                traceback.print_exc()
                # Continue even if custom fields fail
        else:
            print(f"🏷️ [SETUP] No custom fields to create (custom_fields is empty)")
            logger.info("No custom fields to create")
        
        print(f"🚀 [SETUP] ============ STEP 4: CSV IMPORT JOB ============")
        # Step 4: Start the CSV import as a background job (only if import option is selected)
        data_options = onboarding_data.get('data_options', {})
        data_option = data_options.get('option', 'unknown')
        print(f"📂 [SETUP] Data option: {data_option}")
        print(f"📂 [SETUP] Data options: {data_options}")
        
        if data_option == 'import':
            print(f"📂 [SETUP] Import option selected, starting CSV import job")
            logger.info("Starting CSV import job")
            
            admin_user_id = getattr(admin_user, 'id', None)
            print(f"📂 [SETUP] Admin user ID for import: {admin_user_id}")
            if not admin_user_id:
                logger.error("❌ Admin user has no ID")
                return False
            
            try:
                print(f"📂 [SETUP] Calling start_onboarding_import_job...")
                print(f"📂 [SETUP] Import config: {import_config}")
                import_task_id = start_onboarding_import_job(admin_user_id, import_config)
                print(f"📂 [SETUP] start_onboarding_import_job returned: {import_task_id}")
                
                if not import_task_id:
                    logger.error("❌ Failed to start CSV import job")
                    return False
                else:
                    logger.info(f"✅ CSV import job started with ID: {import_task_id}")
                    # Store the task ID for progress tracking
                    session['onboarding_import_task_id'] = import_task_id
                    session.modified = True
            except Exception as import_error:
                logger.error(f"Exception starting CSV import job: {import_error}")
                import traceback
                traceback.print_exc()
                return False
        else:
            print(f"📂 [SETUP] No import selected (option='{data_option}'), skipping CSV import")
            logger.info("No import selected, skipping CSV import")
        
        print(f"🎉 [SETUP] ============ SETUP COMPLETED SUCCESSFULLY ============")
        logger.info("🎉 Onboarding setup completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error executing onboarding setup: {e}")
        import traceback
        traceback.print_exc()
        return False


def handle_onboarding_completion(onboarding_data: Dict):
    """Handle the completion of onboarding (login user and redirect)."""
    try:
        print(f"🎉 [COMPLETION] ============ STARTING COMPLETION PROCESS ============")
        logger.info(f"🔍 ONBOARDING DEBUG: Starting completion process")
        
        # Check if the user is already logged in from the setup process
        from flask_login import current_user
        print(f"🎉 [COMPLETION] Current user status: authenticated={current_user.is_authenticated}")
        if hasattr(current_user, 'id'):
            print(f"🎉 [COMPLETION] Current user ID: {current_user.id}")
            print(f"🎉 [COMPLETION] Current user username: {getattr(current_user, 'username', 'NO_USERNAME')}")
        
        if current_user.is_authenticated:
            print(f"🎉 [COMPLETION] User already logged in from setup process!")
            logger.info(f"User already logged in: {getattr(current_user, 'username', 'UNKNOWN')}")
            
            # Clear onboarding session and redirect
            print(f"🎉 [COMPLETION] Clearing onboarding session")
            clear_onboarding_session()
            logger.info(f"🔍 ONBOARDING DEBUG: Onboarding session cleared")
            
            print(f"🎉 [COMPLETION] Success! Redirecting to main index")
            logger.info(f"🔍 ONBOARDING DEBUG: Success! Redirecting to main index")
            flash('🎉 Welcome to MyBibliotheca! Your library is ready.', 'success')
            
            return redirect(url_for('main.index'))
        
        # If not logged in, try to find and log in the user
        print(f"🎉 [COMPLETION] User not logged in, attempting to find and log in admin user")
        
        # Get the created admin user and log them in
        admin_data = onboarding_data.get('admin', {})
        admin_username = admin_data.get('username', 'UNKNOWN')
        print(f"🎉 [COMPLETION] Looking for admin user: {admin_username}")
        print(f"🎉 [COMPLETION] Admin data: {admin_data}")
        
        # Add debugging: let's see what users actually exist
        try:
            from .kuzu_integration import KuzuIntegrationService
            kuzu_service = KuzuIntegrationService()
            kuzu_service.initialize()
            if kuzu_service.db is not None:
                all_users = kuzu_service.db.query("MATCH (u:User) RETURN u.username, u.id, u.email LIMIT 10")
                logger.info(f"All users in database: {all_users}")
            else:
                logger.warning("Database connection is None during completion")
        except Exception as debug_error:
            logger.error(f"Could not query users during completion: {debug_error}")
        
        admin_user = user_service.get_user_by_username_sync(admin_username)
        
        if not admin_user:
            logger.error(f"🔍 ONBOARDING DEBUG: Could not find admin user: {admin_username}")
            # Don't clear session - stay on step 5 with error
            flash('Setup completed but login failed. Please try logging in manually.', 'warning')
            return redirect(url_for('auth.login'))
        
        print(f"🎉 [COMPLETION] Found admin user, logging in: {admin_user.username}")
        login_user(admin_user)
        logger.info(f"🔍 ONBOARDING DEBUG: Admin user logged in successfully: {admin_user.username}")
        
        # Verify user is actually logged in
        from flask_login import current_user
        print(f"🎉 [COMPLETION] Current user after login: {current_user.is_authenticated}, ID: {getattr(current_user, 'id', 'NO_ID')}")
        
        # Simple verification - just check if we're authenticated
        if not current_user.is_authenticated:
            logger.error("Login verification failed - user not authenticated")
            flash('Setup completed but login failed. Please log in manually.', 'warning')
            return redirect(url_for('auth.login'))
        
        # Only clear onboarding session after successful verification
        print(f"🎉 [COMPLETION] Clearing onboarding session")
        clear_onboarding_session()
        logger.info(f"🔍 ONBOARDING DEBUG: Onboarding session cleared")
        
        print(f"🎉 [COMPLETION] Success! Redirecting to library")
        logger.info(f"🔍 ONBOARDING DEBUG: Success! Redirecting to library")
        flash('🎉 Welcome to MyBibliotheca! Your library is ready.', 'success')
        
        # Redirect directly to library after successful onboarding
        return redirect(url_for('main.library'))
        
    except Exception as e:
        logger.error(f"Error handling onboarding completion: {e}")
        import traceback
        traceback.print_exc()
        flash(f'Setup completed but login failed: {e}. Please log in manually.', 'warning')
        return redirect(url_for('auth.login'))


def start_onboarding_import_job(user_id: str, import_config: Dict) -> Optional[str]:
    """Start a background import job for onboarding using the proven import system."""
    try:
        print(f"🚀 [IMPORT_JOB] ============ STARTING IMPORT JOB ============")
        print(f"🚀 [IMPORT_JOB] User ID: {user_id}")
        print(f"🚀 [IMPORT_JOB] Import config: {import_config}")
        logger.info(f"🚀 Starting simplified onboarding import job for user {user_id}")
        
        # Import the proven import functions from routes
        from flask import current_app
        
        # Create a unique task ID
        task_id = str(uuid.uuid4())
        print(f"🚀 [IMPORT_JOB] Generated task ID: {task_id}")
        
        logger.info(f"🚀 Starting simplified onboarding import job {task_id} for user {user_id}")
        
        # Create job data structure exactly like the working post-onboarding import
        detected_type = import_config.get('detected_type', 'unknown')
        job_data = {
            'task_id': task_id,
            'user_id': user_id,
            'csv_file_path': import_config.get('csv_file_path'),
            'field_mappings': import_config.get('field_mappings', {}),
            'default_reading_status': 'library_only',
            'duplicate_handling': 'skip',
            'custom_fields_enabled': True,
            'format_type': detected_type,  # Pass the detected format type
            'status': 'pending',
            'processed': 0,
            'success': 0,
            'errors': 0,
            'total': 0,
            'start_time': datetime.utcnow().isoformat(),
            'current_book': None,
            'error_messages': [],
            'recent_activity': []
        }
        print(f"🚀 [IMPORT_JOB] Created job data: {job_data}")
        
        # Count total rows in the CSV
        try:
            print(f"🚀 [IMPORT_JOB] Counting CSV rows...")
            import csv
            csv_file_path = import_config.get('csv_file_path')
            print(f"🚀 [IMPORT_JOB] CSV file path: {csv_file_path}")
            if csv_file_path:
                with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    job_data['total'] = sum(1 for _ in reader)
                    print(f"🚀 [IMPORT_JOB] CSV row count: {job_data['total']}")
        except Exception as e:
            logger.warning(f"⚠️ Could not count CSV rows: {e}")
            job_data['total'] = 0
        
        # Store job data using the safe import manager
        print(f"🚀 [IMPORT_JOB] Storing job in Kuzu...")
        kuzu_success = store_job_in_kuzu(task_id, job_data)
        print(f"🚀 [IMPORT_JOB] Kuzu storage result: {kuzu_success}")
        
        print(f"🚀 [IMPORT_JOB] Storing job safely with user isolation...")
        safe_success = safe_create_import_job(user_id, task_id, job_data)
        print(f"🚀 [IMPORT_JOB] Safe storage complete: {safe_success}")
        
        logger.info(f"🏗️ Created onboarding import job {task_id} for user {user_id}")
        logger.info(f"📊 Kuzu storage: {'✅' if kuzu_success else '❌'}")
        logger.info(f"� Safe storage: {'✅' if safe_success else '❌'}")
        
        # Auto-create custom fields (same as post-onboarding)
        field_mappings = import_config.get('field_mappings', {})
        print(f"🚀 [IMPORT_JOB] Auto-creating custom fields for mappings: {field_mappings}")
        auto_create_custom_fields(field_mappings, user_id)
        print(f"🚀 [IMPORT_JOB] Custom fields auto-creation complete")
        
        # Start the import using the proven working import system
        def run_import():
            try:
                print(f"🚀 [IMPORT_JOB] Starting background import thread for task {task_id}")
                print(f"🚀 [IMPORT_JOB] Import config: {import_config}")
                
                # Update job status to running safely
                running_update = {
                    'status': 'running',
                    'current_book': 'Starting import...'
                }
                safe_update_import_job(user_id, task_id, running_update)
                print(f"🚀 [IMPORT_JOB] Updated job status to running")
                
                start_import_job(
                    task_id=task_id,
                    csv_file_path=import_config.get('csv_file_path'),
                    field_mappings=import_config.get('field_mappings', {}),
                    user_id=user_id,
                    format_type=import_config.get('detected_type', 'unknown')
                )
                
                # Update job status to completed safely
                completion_update = {
                    'status': 'completed',
                    'current_book': 'Import completed successfully'
                }
                safe_update_import_job(user_id, task_id, completion_update)
                    
            except Exception as e:
                import traceback
                traceback.print_exc()
                
                # Update job status to failed safely
                error_update = {
                    'status': 'failed',
                    'current_book': f'Import failed: {str(e)}',
                    'error_messages': [str(e)]
                }
                safe_update_import_job(user_id, task_id, error_update)
                logger.error(f"Onboarding import job {task_id} failed: {e}")
        
        print(f"🚀 [IMPORT_JOB] Creating background thread...")
        import threading
        thread = threading.Thread(target=run_import)
        thread.daemon = True
        print(f"🚀 [IMPORT_JOB] Starting background thread...")
        thread.start()
        
        # Wait a moment to let the thread start and update job status
        import time
        time.sleep(0.5)
        
        # Verify the job is stored safely after thread start
        verification_job = safe_get_import_job(user_id, task_id)
        if verification_job:
            print(f"🚀 [IMPORT_JOB] Job {task_id} confirmed in safe storage after thread start")
            logger.info(f"Job {task_id} confirmed in safe storage after thread start")
        else:
            print(f"❌ [IMPORT_JOB] Job {task_id} missing from safe storage after thread start")
            logger.warning(f"Job {task_id} missing from safe storage after thread start")
        
        logger.info(f"✅ Onboarding import job {task_id} started successfully")
        return task_id
        
    except Exception as e:
        logger.error(f"❌ Failed to start onboarding import job: {e}")
        import traceback
        traceback.print_exc()
        return None


# ==============================================================================
# CSV IMPORT EXECUTION - THREAD-SAFE VERSION
# ==============================================================================
# 
# SECURITY NOTE: All legacy functions with global import_jobs access have been 
# removed to prevent privacy violations and race conditions. Import job 
# management now uses SafeImportJobManager with proper user isolation.

def execute_csv_import_with_progress(task_id: str, csv_file_path: str, field_mappings: Dict[str, str], user_id: str, default_locations: List[str]) -> bool:
    """Execute CSV import with progress tracking."""
    try:
        import csv
        from .utils import normalize_goodreads_value
        
        # Get the job safely (for onboarding, jobs are stored in safe manager)
        job = safe_get_import_job(user_id, task_id)
        if not job:
            logger.error(f"❌ Job {task_id} not found")
            return False
        
        # Setup default locations for the onboarding process
        # During onboarding, we need to ensure the user has a default location
        if not default_locations:
            from .location_service import LocationService
            from app.utils.safe_kuzu_manager import safe_get_connection
            
            # Initialize location service
            location_service = LocationService()
            
            # Get or create default location for this user
            default_location = location_service.get_default_location(user_id)
            if not default_location:
                logger.info(f"🏠 Creating default location for user during onboarding import")
                created_locations = location_service.setup_default_locations()
                if created_locations:
                    default_location = location_service.get_default_location(user_id)
                    if default_location and default_location.id:
                        default_locations = [default_location.id]
                        logger.info(f"✅ Created and using default location: {default_location.name} (ID: {default_location.id})")
                    else:
                        logger.error(f"❌ Failed to get default location after creation")
                        default_locations = []
                else:
                    logger.error(f"❌ Failed to create default locations")
                    default_locations = []
            else:
                if default_location.id:
                    default_locations = [default_location.id]
                    logger.info(f"✅ Using existing default location: {default_location.name} (ID: {default_location.id})")
                else:
                    logger.error(f"❌ Default location has no ID")
                    default_locations = []
        
        logger.info(f"�📊 Starting CSV import with mappings: {field_mappings}")
        logger.info(f"📍 Using default locations: {default_locations}")
        
        # Read and process the CSV file
        with open(csv_file_path, 'r', encoding='utf-8', errors='ignore') as csvfile:
            # Try to detect if CSV has headers
            first_line = csvfile.readline().strip()
            csvfile.seek(0)  # Reset to beginning
            
            # Check if first line looks like an ISBN or a header
            has_headers = not (first_line.isdigit() or len(first_line) in [10, 13])
            
            logger.info(f"📄 First line: '{first_line}', Has headers: {has_headers}")
            
            if has_headers:
                reader = csv.DictReader(csvfile)
            else:
                # For headerless CSV (like ISBN-only files), create a simple reader
                reader = csv.reader(csvfile)
                # Convert to dict format for consistency
                dict_reader = []
                for row_data in reader:
                    if row_data and row_data[0].strip():  # Skip empty rows
                        dict_reader.append({'isbn': row_data[0].strip()})
                reader = dict_reader
            
            for row_num, row in enumerate(reader, 1):
                try:
                    # Update current book in job
                    job['current_book'] = f"Row {row_num}"
                    job['processed'] = row_num - 1  # Zero-based for processed count
                    
                    # Update progress every 5 rows to avoid too many updates
                    if row_num % 5 == 0:
                        update_job_in_kuzu(task_id, {
                            'processed': job['processed'],
                            'current_book': job['current_book']
                        })
                    
                    logger.info(f"📖 Processing row {row_num}: {row}")
                    
                    # Extract book data based on mappings
                    book_data = {}
                    personal_custom_metadata = {}
                    
                    if has_headers:
                        # Use field mappings for CSV with headers
                        for csv_field, book_field in field_mappings.items():
                            raw_value = row.get(csv_field, '')
                            
                            # Apply Goodreads normalization to all values
                            if book_field == 'isbn':
                                value = normalize_goodreads_value(raw_value, 'isbn')
                            else:
                                value = normalize_goodreads_value(raw_value, 'text')
                            
                            if value:  # Only process non-empty values
                                # Check if this is a custom field
                                if book_field.startswith('custom_'):
                                    # Extract custom field name and add to personal metadata
                                    custom_field_name = book_field.replace('custom_', '')
                                    personal_custom_metadata[custom_field_name] = value
                                    logger.info(f"📝 Added custom metadata: {custom_field_name} = {value}")
                                else:
                                    book_data[book_field] = value
                    else:
                        # For headerless CSV, assume it's ISBN-only
                        isbn_value = row.get('isbn', '').strip()
                        if isbn_value:
                            book_data['isbn'] = isbn_value
                    
                    # Skip if no title or ISBN
                    if not book_data.get('title') and not book_data.get('isbn'):
                        logger.info(f"⏭️ Row {row_num}: Skipped - no title or ISBN")
                        job['recent_activity'].append(f"Row {row_num}: Skipped - no title or ISBN")
                        continue
                    
                    # Update current book name
                    title = book_data.get('title', book_data.get('isbn', 'Unknown Title'))
                    job['current_book'] = title
                    
                    # Process the book with custom metadata
                    success = process_single_book_import(book_data, user_id, default_locations, personal_custom_metadata)
                    
                    if success:
                        job['success'] += 1
                        job['recent_activity'].append(f"Row {row_num}: Successfully imported '{title}'")
                        logger.info(f"✅ Row {row_num}: Successfully imported '{title}'")
                    else:
                        job['errors'] += 1
                        job['recent_activity'].append(f"Row {row_num}: Failed to import")
                        logger.error(f"❌ Row {row_num}: Failed to import")
                    
                    # Keep only the last 10 activities to avoid memory bloat
                    if len(job['recent_activity']) > 10:
                        job['recent_activity'] = job['recent_activity'][-10:]
                
                except Exception as e:
                    logger.error(f"❌ Exception in row {row_num}: {e}")
                    job['errors'] += 1
                    job['recent_activity'].append(f"Row {row_num}: Error - {str(e)}")
                
                # Update final processed count
                job['processed'] = row_num
        
        # Final progress update
        update_job_in_kuzu(task_id, {
            'processed': job['processed'],
            'success': job['success'],
            'errors': job['errors'],
            'current_book': None
        })
        
        logger.info(f"📊 CSV processing completed. Success: {job['success']}, Errors: {job['errors']}")
        
        # Clean up temp file
        try:
            import os
            os.unlink(csv_file_path)
            logger.info(f"🗑️ Cleaned up temporary file: {csv_file_path}")
        except:
            pass
        
        # Return success if at least one book was imported
        return job['success'] > 0
        
    except Exception as e:
        logger.error(f"❌ Error executing CSV import with progress: {e}")
        return False

def process_single_book_import(book_data: Dict, user_id: str, default_locations: List, personal_custom_metadata: Dict) -> bool:
    """Process a single book import with full API metadata enrichment."""
    try:
        # Import required models and utilities
        from .domain.models import (
            Book as DomainBook, 
            Person, 
            BookContribution, 
            ContributionType, 
            Publisher, 
            ReadingStatus, 
            OwnershipStatus
        )
        from .services import book_service
        from .utils import fetch_book_data, get_google_books_cover
        
        # Extract essential book information
        title = book_data.get('title', '').strip()
        if not title:
            logger.error("❌ Book title is required")
            return False
        
        csv_author = book_data.get('author', '').strip()
        isbn = book_data.get('isbn', '').strip()
        
        logger.info(f"📚 Processing book: {title} by {csv_author}")
        
        # API Enrichment Phase
        google_data = None
        ol_data = None
        api_categories = None
        published_date = book_data.get('published_date')
        
        # Try API enrichment if ISBN is available
        if isbn:
            api_isbn = ''.join(filter(str.isdigit, isbn))
            logger.info(f"🔍 Attempting API enrichment for ISBN: {api_isbn}")
            
            try:
                # Google Books API call
                google_data = get_google_books_cover(api_isbn, fetch_title_author=True)
                if google_data:
                    logger.info(f"✅ Got Google Books data: {list(google_data.keys())}")
                    if google_data.get('categories'):
                        api_categories = google_data['categories']
                        logger.info(f"📚 Got categories from Google Books: {api_categories}")
                    if google_data.get('published_date'):
                        published_date = published_date or google_data.get('published_date')
            except Exception as google_error:
                logger.warning(f"⚠️ Google Books API error for ISBN {api_isbn}: {google_error}")
            
            try:
                # OpenLibrary API call
                ol_data = fetch_book_data(api_isbn)
                if ol_data:
                    logger.info(f"✅ Got OpenLibrary data: {list(ol_data.keys())}")
                    if ol_data.get('categories'):
                        if not api_categories:  # Only use OL categories if Google didn't provide any
                            api_categories = ol_data['categories']
                            logger.info(f"📚 Got categories from OpenLibrary: {api_categories}")
                    if ol_data.get('published_date'):
                        published_date = published_date or ol_data.get('published_date')
                        if not api_categories:
                            api_categories = ol_data.get('categories')
                            if api_categories:
                                logger.info(f"📚 Got categories from OpenLibrary: {api_categories}")
            except Exception as api_error:
                logger.warning(f"⚠️ Error fetching metadata for ISBN {api_isbn}: {api_error}")
        
        # Determine final categories (API categories if available)
        final_categories = book_data.get('categories') or api_categories
        if final_categories:
            logger.info(f"📚 Final categories to process: {final_categories}")
        
        # Helper function to find or create person with deduplication
        def find_or_create_person(person_name: str) -> Person:
            """Find existing person by name or create new one."""
            try:
                # Normalize name for search
                normalized_name = person_name.strip().lower()
                
                # Get all existing persons and check for matches
                all_persons = book_service.list_all_persons_sync()
                if all_persons:
                    for person_data in all_persons:
                        existing_name = person_data.get('name', '').strip().lower()
                        existing_normalized = person_data.get('normalized_name', '').strip().lower()
                        
                        # Check for exact match or normalized match
                        if (existing_name == normalized_name or 
                            existing_normalized == normalized_name or
                            existing_name == person_name.strip().lower()):
                            
                            logger.info(f"📝 Found existing person: {person_name} (ID: {person_data.get('id')})")
                            return Person(
                                id=person_data.get('id'),
                                name=person_data.get('name', person_name),
                                normalized_name=person_data.get('normalized_name', ''),
                                birth_year=person_data.get('birth_year'),
                                death_year=person_data.get('death_year'),
                                birth_place=person_data.get('birth_place'),
                                bio=person_data.get('bio'),
                                website=person_data.get('website'),
                                openlibrary_id=person_data.get('openlibrary_id'),
                                image_url=person_data.get('image_url'),
                                created_at=person_data.get('created_at', datetime.utcnow()),
                                updated_at=person_data.get('updated_at', datetime.utcnow())
                            )
                
                # If no match found, create new person
                logger.info(f"📝 Creating new person: {person_name}")
                return Person(name=person_name)
                    
            except Exception as e:
                logger.warning(f"⚠️ Error during person lookup/creation for {person_name}: {e}")
                # Fallback to creating new person
                return Person(name=person_name)
        
        # Create contributors with deduplication
        contributors = []
        added_author_names = set()
        
        # Prioritize API authors if available
        if google_data and google_data.get('authors_list'):
            for i, author_name in enumerate(google_data['authors_list']):
                author_name = author_name.strip()
                if author_name and author_name.lower() not in added_author_names:
                    person = find_or_create_person(author_name)
                    contribution = BookContribution(
                        person=person,
                        contribution_type=ContributionType.AUTHORED,
                        order=i
                    )
                    contributors.append(contribution)
                    added_author_names.add(author_name.lower())
                    logger.info(f"📝 Added author from Google Books: {author_name}")
        elif ol_data and ol_data.get('authors_list'):
            for i, author_name in enumerate(ol_data['authors_list']):
                author_name = author_name.strip()
                if author_name and author_name.lower() not in added_author_names:
                    person = find_or_create_person(author_name)
                    contribution = BookContribution(
                        person=person,
                        contribution_type=ContributionType.AUTHORED,
                        order=i
                    )
                    contributors.append(contribution)
                    added_author_names.add(author_name.lower())
                    logger.info(f"📝 Added author from OpenLibrary: {author_name}")
        else:
            # Fallback to CSV author
            if csv_author and csv_author.lower() not in added_author_names:
                person = find_or_create_person(csv_author)
                contribution = BookContribution(
                    person=person,
                    contribution_type=ContributionType.AUTHORED,
                    order=0
                )
                contributors.append(contribution)
                added_author_names.add(csv_author.lower())
                logger.info(f"📝 Added author from CSV: {csv_author}")
        
        # Handle additional authors from CSV
        if book_data.get('additional_authors'):
            additional_names = book_data['additional_authors'].split(',')
            for name in additional_names:
                name = name.strip()
                if name and name.lower() not in added_author_names:
                    person = find_or_create_person(name)
                    contribution = BookContribution(
                        person=person,
                        contribution_type=ContributionType.AUTHORED,
                        order=len(contributors)
                    )
                    contributors.append(contribution)
                    added_author_names.add(name.lower())
                    logger.info(f"📝 Added additional author: {name}")
        
        # Extract other book metadata 
        description = book_data.get('description', '') or book_data.get('summary', '')
        publisher_name = book_data.get('publisher', '')
        page_count = book_data.get('page_count') or book_data.get('pages')
        language = book_data.get('language', 'en')
        cover_url = book_data.get('cover_url', '')
        average_rating = book_data.get('average_rating')
        rating_count = book_data.get('rating_count')
        reading_status = book_data.get('reading_status', 'library_only')
        
        # Process ISBN data 
        isbn13 = None
        isbn10 = None
        if isbn:
            cleaned_isbn = ''.join(filter(str.isdigit, isbn))
            if len(cleaned_isbn) == 13:
                isbn13 = cleaned_isbn
            elif len(cleaned_isbn) == 10:
                isbn10 = cleaned_isbn
        
        # Convert date helper function
        def _convert_published_date_to_date(date_str):
            """Convert published date string to date object."""
            if not date_str:
                return None
            try:
                from datetime import datetime
                if isinstance(date_str, str):
                    # Try different date formats
                    for fmt in ['%Y-%m-%d', '%Y-%m', '%Y', '%B %d, %Y', '%d %B %Y']:
                        try:
                            return datetime.strptime(date_str, fmt).date()
                        except ValueError:
                            continue
                return None
            except Exception:
                return None
        
        # Create domain book object with full metadata
        domain_book = DomainBook(
            title=title,
            contributors=contributors,
            isbn13=isbn13,
            isbn10=isbn10,
            description=description,
            publisher=Publisher(name=publisher_name) if publisher_name else None,
            page_count=int(page_count) if page_count and str(page_count).isdigit() else None,
            language=language,
            cover_url=cover_url,
            average_rating=average_rating,
            rating_count=rating_count,
            published_date=_convert_published_date_to_date(published_date),
            raw_categories=final_categories,  # This will trigger automatic category processing
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        logger.info(f"📚 Creating book: {domain_book.title} with {len(domain_book.contributors)} contributors")
        
        # Create book using the service (will auto-process categories)
        created_book = book_service.find_or_create_book_sync(domain_book)
        
        if created_book and created_book.id:
            # Convert reading status
            reading_status_enum = ReadingStatus.LIBRARY_ONLY  # default
            if reading_status:
                if reading_status == 'read':
                    reading_status_enum = ReadingStatus.READ
                elif reading_status == 'reading':
                    reading_status_enum = ReadingStatus.READING
                elif reading_status == 'plan_to_read':
                    reading_status_enum = ReadingStatus.PLAN_TO_READ
                elif reading_status == 'on_hold':
                    reading_status_enum = ReadingStatus.ON_HOLD
                elif reading_status == 'did_not_finish':
                    reading_status_enum = ReadingStatus.DNF
            
            # Add to user's library with custom metadata
            custom_metadata_to_pass = personal_custom_metadata if personal_custom_metadata else {}
            success = book_service.add_book_to_user_library_sync(
                user_id=str(user_id),
                book_id=created_book.id,
                reading_status=reading_status_enum.value if hasattr(reading_status_enum, 'value') else str(reading_status_enum),
                ownership_status=OwnershipStatus.OWNED.value if hasattr(OwnershipStatus.OWNED, 'value') else str(OwnershipStatus.OWNED),
                locations=default_locations,
                custom_metadata=custom_metadata_to_pass
            )
            
            if success:
                logger.info(f"✅ Successfully added {title} to user's library")
                return True
            else:
                logger.error(f"❌ Failed to add {title} to user's library")
                return False
        else:
            logger.error(f"❌ Failed to create book: {title}")
            return False
        
    except Exception as e:
        logger.error(f"❌ Error processing single book: {e}")
        import traceback
        traceback.print_exc()
        return False
