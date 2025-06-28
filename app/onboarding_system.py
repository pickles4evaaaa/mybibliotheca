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
from pathlib import Path
from typing import Dict, List, Optional
import pytz
from datetime import datetime

from .advanced_migration_system import AdvancedMigrationSystem, DatabaseVersion
from .services import user_service
from .location_service import LocationService
from .forms import SetupForm
from .debug_utils import debug_route
from .infrastructure.kuzu_graph import get_graph_storage

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
        print(f"🔍 SESSION DEBUG: Main onboarding_data missing but backup found, restoring...")
        session['onboarding_data'] = backup
        session.modified = True
        data = backup
        print(f"🔍 SESSION DEBUG: Restored data: {data}")
    
    print(f"🔍 SESSION DEBUG: get_onboarding_data returning: {data}")
    print(f"🔍 SESSION DEBUG: backup available: {backup}")
    logger.info(f"🔍 SESSION DEBUG: get_onboarding_data returning: {data}")
    return data


def update_onboarding_data(data: Dict):
    """Update onboarding data in session."""
    print(f"🔍 SESSION DEBUG: update_onboarding_data called with: {data}")
    logger.info(f"🔍 SESSION DEBUG: update_onboarding_data called with: {data}")
    
    # Force session to be permanent for better persistence
    session.permanent = True
    
    # Ensure onboarding_data exists in session
    if 'onboarding_data' not in session:
        session['onboarding_data'] = {}
    
    current_data = session['onboarding_data']
    print(f"🔍 SESSION DEBUG: current_data before update: {current_data}")
    logger.info(f"🔍 SESSION DEBUG: current_data before update: {current_data}")
    
    # Update the data
    current_data.update(data)
    session['onboarding_data'] = current_data
    session.modified = True
    
    # Also store in a backup location to help with debugging
    session['onboarding_backup'] = current_data.copy()
    
    print(f"🔍 SESSION DEBUG: session after update: {dict(session)}")
    print(f"🔍 SESSION DEBUG: onboarding_data in session: {session.get('onboarding_data', 'NOT_FOUND')}")
    print(f"🔍 SESSION DEBUG: backup data: {session.get('onboarding_backup', 'NOT_FOUND')}")
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
    """Clear all onboarding data from session."""
    session.pop('onboarding_step', None)
    session.pop('onboarding_data', None)
    session.pop('onboarding_backup', None)
    session.pop('onboarding_import_task_id', None)
    session.modified = True
    logger.info(f"🔍 SESSION DEBUG: Onboarding session cleared")
    print(f"🔍 SESSION DEBUG: Onboarding session cleared")


@onboarding_bp.route('/start')
@debug_route('ONBOARDING_START')  
def start():
    """Start the onboarding process."""
    print(f"🔍 ONBOARDING START: ============ START ROUTE CALLED ============")
    print(f"🔍 ONBOARDING START: Called from {request.environ.get('HTTP_REFERER', 'unknown')}")
    print(f"🔍 ONBOARDING START: User agent: {request.environ.get('HTTP_USER_AGENT', 'unknown')[:100]}...")
    print(f"🔍 ONBOARDING START: Request method: {request.method}")
    print(f"🔍 ONBOARDING START: Request path: {request.path}")
    print(f"🔍 ONBOARDING START: Request args: {dict(request.args)}")
    print(f"🔍 ONBOARDING START: Current session before check: {dict(session)}")
    
    try:
        # Only check user count if we're not in the middle of onboarding
        # Skip this check if onboarding is in progress
        current_step = session.get('onboarding_step', 0)
        onboarding_data = session.get('onboarding_data', {})
        
        print(f"🔍 ONBOARDING START: Current step: {current_step}, Has data: {bool(onboarding_data)}")
        
        if current_step == 0 and not onboarding_data:  # Only check on fresh start
            user_count = user_service.get_user_count_sync()
            print(f"🔍 ONBOARDING START: User count: {user_count}")
            if user_count > 0:
                print(f"🔍 ONBOARDING START: Users exist and no active onboarding, should redirect to login")
                flash('Setup has already been completed.', 'info')
                return redirect(url_for('auth.login'))
        else:
            print(f"🔍 ONBOARDING START: Onboarding in progress (step {current_step}) or has data, allowing continuation")
    except Exception as e:
        print(f"❌ ONBOARDING START: Error checking user count: {e}")
        logger.error(f"Error checking user count: {e}")
    
    print(f"🔍 ONBOARDING START: Proceeding to session check and initialization")
    
    # Only clear session if we're not in the middle of active onboarding
    current_step = session.get('onboarding_step', 0)
    onboarding_data = session.get('onboarding_data', {})
    
    if current_step > 0 and onboarding_data:
        print(f"🔍 ONBOARDING START: Active onboarding detected (step {current_step}), NOT clearing session")
        print(f"🔍 ONBOARDING START: Redirecting to current onboarding step")
        return redirect(url_for('onboarding.step', step_num=current_step))
    else:
        print(f"🔍 ONBOARDING START: No active onboarding, initializing fresh session")
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
        print(f"🔍 ONBOARDING START: Generated CSRF token: {csrf_token[:10]}...")
    except Exception as e:
        print(f"🔍 ONBOARDING START: Could not generate CSRF token: {e}")
    
    print(f"🔍 ONBOARDING START: Session initialized: {dict(session)}")
    
    return redirect(url_for('onboarding.step', step_num=1))


@onboarding_bp.route('/step/<int:step_num>', methods=['GET', 'POST'])
@debug_route('ONBOARDING_STEP')
def step(step_num: int):
    """Handle individual onboarding steps."""
    print(f"🔍 STEP DEBUG: Accessing step {step_num}, method={request.method}")
    current_step = get_onboarding_step()
    print(f"🔍 STEP DEBUG: Current step: {current_step}")
    
    # DEBUG: Log step access attempt
    print(f"🔍 STEP DEBUG: Session keys: {list(session.keys())}")
    print(f"🔍 STEP DEBUG: Session data: {dict(session)}")
    logger.info(f"🔍 ONBOARDING DEBUG: User accessing step {step_num}, current_step={current_step}")
    logger.info(f"🔍 ONBOARDING DEBUG: Session data: {dict(session)}")
    logger.info(f"🔍 ONBOARDING DEBUG: Request method: {request.method}")
    
    # Allow backward navigation always
    # For forward navigation, check if we have completed previous steps
    if step_num > current_step:
        print(f"🔍 STEP DEBUG: Forward navigation check for step {step_num}")
        # Check if we have the required data for this step
        onboarding_data = get_onboarding_data()
        print(f"🔍 STEP DEBUG: Onboarding data keys: {list(onboarding_data.keys())}")
        
        logger.info(f"🔍 ONBOARDING DEBUG: Forward navigation check - onboarding_data keys: {list(onboarding_data.keys())}")
        
        # Step 2 requires admin data from step 1
        if step_num >= 2 and 'admin' not in onboarding_data:
            print(f"🔍 ONBOARDING DEBUG: Missing admin data for step {step_num}")
            print(f"🔍 ONBOARDING DEBUG: Available data: {onboarding_data}")
            print(f"🔍 ONBOARDING DEBUG: Session backup: {session.get('onboarding_backup', 'NO_BACKUP')}")
            
            # Try to recover from backup
            backup = session.get('onboarding_backup', {})
            if 'admin' in backup:
                print(f"🔍 ONBOARDING DEBUG: Found admin data in backup, restoring session")
                session['onboarding_data'] = backup
                session.modified = True
                onboarding_data = backup
            else:
                print(f"🔍 ONBOARDING DEBUG: No admin data in backup either, redirecting to step 1")
                logger.warning(f"🔍 ONBOARDING DEBUG: Missing admin data for step {step_num}, redirecting to step 1")
                flash('Please complete the admin setup first.', 'warning')
                return redirect(url_for('onboarding.step', step_num=1))
        
        # Step 3 requires site config from step 2  
        if step_num >= 3 and 'site_config' not in onboarding_data:
            print(f"🔍 ONBOARDING DEBUG: Missing site_config data for step {step_num}, redirecting to step 2")
            logger.warning(f"🔍 ONBOARDING DEBUG: Missing site_config data for step {step_num}, redirecting to step 2")
            return redirect(url_for('onboarding.step', step_num=2))
        
        # Step 4+ requires data options from step 3
        if step_num >= 4 and 'data_options' not in onboarding_data:
            print(f"🔍 ONBOARDING DEBUG: Missing data_options for step {step_num}, redirecting to step 3")
            logger.warning(f"🔍 ONBOARDING DEBUG: Missing data_options for step {step_num}, redirecting to step 3")
            return redirect(url_for('onboarding.step', step_num=3))
        
        # Step 5 requires additional validation for import option
        if step_num >= 5:
            data_options = onboarding_data.get('data_options', {})
            if data_options.get('option') == 'import':
                # Check if import file was selected
                if 'import_file' not in data_options or not data_options.get('import_file'):
                    print(f"🔍 ONBOARDING DEBUG: Import option selected but no file chosen, redirecting to step 3")
                    logger.warning(f"🔍 ONBOARDING DEBUG: Import option selected but no file chosen for step {step_num}")
                    flash('Please select a file to import before proceeding.', 'error')
                    return redirect(url_for('onboarding.step', step_num=3))
            
        # Allow access if we have the required data
        print(f"🔍 STEP DEBUG: Requirements met, setting step to {step_num}")
        logger.info(f"🔍 ONBOARDING DEBUG: Access granted, setting step to {step_num}")
        set_onboarding_step(step_num)
    elif step_num == current_step:
        # User is accessing their current step - this is always allowed
        print(f"🔍 STEP DEBUG: User accessing current step {step_num}")
        logger.info(f"🔍 ONBOARDING DEBUG: User accessing current step {step_num}")
    else:
        # Backward navigation - always allowed, but don't change the current step
        print(f"🔍 STEP DEBUG: Backward navigation to step {step_num} from {current_step}")
        logger.info(f"🔍 ONBOARDING DEBUG: Backward navigation to step {step_num} from {current_step}")
        # Don't call set_onboarding_step() for backward navigation
        # This preserves the user's actual progress
    
    print(f"🔍 STEP DEBUG: Proceeding to handle step {step_num}")
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
    print(f"🔍 ADMIN_SETUP DEBUG: admin_setup_step called, method={request.method}")
    print(f"🔍 ADMIN_SETUP DEBUG: Session keys: {list(session.keys())}")
    print(f"🔍 ADMIN_SETUP DEBUG: Current onboarding data: {get_onboarding_data()}")
    logger.info(f"🔍 ONBOARDING DEBUG: admin_setup_step called")
    
    # Force session to be permanent to ensure persistence
    session.permanent = True
    
    form = SetupForm()
    
    print(f"🔍 ADMIN_SETUP DEBUG: Form created, submitted={form.is_submitted()}")
    logger.info(f"🔍 ONBOARDING DEBUG: Form created, is_submitted={form.is_submitted()}")
    
    if request.method == 'POST':
        print(f"🔍 ADMIN_SETUP DEBUG: POST request detected")
        print(f"🔍 ADMIN_SETUP DEBUG: Form data: username={request.form.get('username')}, email={request.form.get('email')}")
        print(f"🔍 ADMIN_SETUP DEBUG: Form is_submitted: {form.is_submitted()}")
        
        # Manual form validation for better debugging
        form_valid = form.validate()
        print(f"🔍 ADMIN_SETUP DEBUG: Manual form validation: {form_valid}")
        print(f"🔍 ADMIN_SETUP DEBUG: Form errors: {form.errors}")
        print(f"🔍 ADMIN_SETUP DEBUG: CSRF token in form: {request.form.get('csrf_token', 'MISSING')}")
        print(f"🔍 ADMIN_SETUP DEBUG: CSRF token in session: {session.get('csrf_token', 'MISSING')}")
        
        # Check if basic required fields are present
        username = request.form.get('username', '').strip()
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '').strip()
        password2 = request.form.get('password2', '').strip()
        
        print(f"🔍 ADMIN_SETUP DEBUG: Extracted data - username='{username}', email='{email}', password_len={len(password)}, password2_len={len(password2)}")
        
        # If form validation fails, try to process anyway if we have required data
        if form_valid or (username and email and password and password == password2):
            try:
                print(f"🔍 ADMIN_SETUP DEBUG: Processing form data (form_valid={form_valid})")
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
                
                print(f"🔍 ADMIN_SETUP DEBUG: Admin data prepared: username={admin_data['username']}, email={admin_data['email']}")
                logger.info(f"🔍 ONBOARDING DEBUG: Saving admin data: {admin_data['username']}, {admin_data['email']}")
                
                print(f"🔍 ADMIN_SETUP DEBUG: Before update - session keys: {list(session.keys())}")
                
                # Use direct session assignment instead of helper function
                if 'onboarding_data' not in session:
                    session['onboarding_data'] = {}
                session['onboarding_data']['admin'] = admin_data
                session.permanent = True
                session.modified = True
                
                # Also create a backup
                session['onboarding_backup'] = session['onboarding_data'].copy()
                
                print(f"🔍 ADMIN_SETUP DEBUG: After update - session keys: {list(session.keys())}")
                print(f"🔍 ADMIN_SETUP DEBUG: Session onboarding_data: {session.get('onboarding_data', 'MISSING')}")
                
                # Move to next step
                print(f"🔍 ADMIN_SETUP DEBUG: Setting step to 2")
                logger.info(f"🔍 ONBOARDING DEBUG: Moving to step 2")
                session['onboarding_step'] = 2
                session.modified = True
                print(f"🔍 ADMIN_SETUP DEBUG: Step set, current step: {session.get('onboarding_step')}")
                
                print(f"🔍 ADMIN_SETUP DEBUG: Full session after all updates: {dict(session)}")
                logger.info(f"🔍 ONBOARDING DEBUG: Session after update: {dict(session)}")
                
                flash('Admin account configured successfully!', 'success')
                print(f"🔍 ADMIN_SETUP DEBUG: About to redirect to step 2")
                return redirect(url_for('onboarding.step', step_num=2))
                
            except Exception as e:
                print(f"❌ ADMIN_SETUP ERROR: {e}")
                import traceback
                traceback.print_exc()
                logger.error(f"🔍 ONBOARDING DEBUG: Error in admin setup: {e}")
                flash(f'Error configuring admin account: {e}', 'error')
        else:
            print(f"❌ ADMIN_SETUP DEBUG: Form validation failed and insufficient data")
            print(f"❌ ADMIN_SETUP DEBUG: Missing or invalid data - username='{username}', email='{email}', passwords_match={password == password2}")
            if not form_valid:
                for field, errors in form.errors.items():
                    for error in errors:
                        flash(f'{field}: {error}', 'error')
    
    print(f"🔍 ADMIN_SETUP DEBUG: Rendering step 1 template")
    logger.info(f"🔍 ONBOARDING DEBUG: Rendering step 1 template")
    
    return render_template('onboarding/step1_admin_setup.html', 
                         form=form, 
                         step=1, 
                         total_steps=5)


def site_config_step():
    """Step 2: Site configuration (location, timezone, site name)."""
    print(f"🔍 SITE_CONFIG DEBUG: site_config_step called, method={request.method}")
    logger.info(f"🔍 ONBOARDING DEBUG: site_config_step called, method={request.method}")
    
    # Force session to be permanent for better persistence
    session.permanent = True
    
    if request.method == 'POST':
        try:
            print(f"🔍 SITE_CONFIG DEBUG: Processing POST data: {dict(request.form)}")
            logger.info(f"🔍 ONBOARDING DEBUG: Processing POST data: {dict(request.form)}")
            
            site_config = {
                'site_name': request.form.get('site_name', 'MyBibliotheca'),
                'timezone': request.form.get('timezone', 'UTC'),
                'location': request.form.get('location', ''),
                'location_set_as_default': 'location_set_as_default' in request.form
            }
            
            print(f"🔍 SITE_CONFIG DEBUG: Saving site_config: {site_config}")
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
            print(f"🔍 SITE_CONFIG DEBUG: Moving to step 3")
            logger.info(f"🔍 ONBOARDING DEBUG: Moving to step 3")
            session['onboarding_step'] = 3
            session.modified = True
            
            print(f"🔍 SITE_CONFIG DEBUG: Session after all updates: {dict(session)}")
            logger.info(f"🔍 ONBOARDING DEBUG: Session after update: {dict(session)}")
            
            flash('Site configuration saved!', 'success')
            print(f"🔍 SITE_CONFIG DEBUG: About to redirect to step 3")
            return redirect(url_for('onboarding.step', step_num=3))
            
        except Exception as e:
            print(f"❌ SITE_CONFIG ERROR: {e}")
            import traceback
            traceback.print_exc()
            logger.error(f"🔍 ONBOARDING DEBUG: Error in site config: {e}")
            flash(f'Error saving site configuration: {e}', 'error')
    
    # Get timezone list
    print(f"🔍 SITE_CONFIG DEBUG: GET request, preparing template render")
    print(f"🔍 SITE_CONFIG DEBUG: Current session: {dict(session)}")
    timezones = pytz.common_timezones
    current_config = get_onboarding_data().get('site_config', {})
    print(f"🔍 SITE_CONFIG DEBUG: Retrieved current_config: {current_config}")
    
    logger.info(f"🔍 ONBOARDING DEBUG: Rendering step 2 template with current_config: {current_config}")
    print(f"🔍 SITE_CONFIG DEBUG: About to render template")
    
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
            data_options = {'option': data_option}
            
            logger.info(f"🔍 ONBOARDING DEBUG: Selected data option: {data_option}")
            
            if data_option == 'migrate' and db_analysis:
                selected_db = request.form.get('selected_database')
                data_options['selected_database'] = selected_db
                
                logger.info(f"🔍 ONBOARDING DEBUG: Selected database: {selected_db}")
                
                # Find analysis for selected database
                for db in db_analysis:
                    if db['path'] == selected_db:
                        data_options['database_analysis'] = db
                        logger.info(f"🔍 ONBOARDING DEBUG: Found database analysis: {db}")
                        break
            
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
    
    database_analysis = data_options.get('database_analysis', {})
    
    logger.info(f"🔍 ONBOARDING DEBUG: database_analysis: {database_analysis}")
    
    if request.method == 'POST':
        try:
            logger.info(f"🔍 ONBOARDING DEBUG: Processing POST data: {dict(request.form)}")
            
            migration_config = {'type': 'migration'}
            
            if database_analysis.get('version') == DatabaseVersion.V2_MULTI_USER:
                # Handle user mapping for V2 databases
                admin_user_mapping = request.form.get('admin_user_mapping')
                migration_config['admin_user_mapping'] = int(admin_user_mapping)
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
        'Original Publication Year': 'custom_global_original_publication_year',
        'Date Read': 'date_read',
        'Date Added': 'date_added',
        'Bookshelves': 'custom_global_bookshelves',
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
        'ISBN': 'isbn',
        'Star Rating': 'rating',
        'Read Status': 'reading_status',
        'Date Started': 'start_date',
        'Date Finished': 'date_read',
        'Date Added': 'date_added',
        'Tags': 'custom_global_tags',
        'Moods': 'custom_global_moods',
        'Pace': 'custom_global_pace',
        'Character- or Plot-Driven?': 'custom_global_character_or_plot_driven',
        'Strong Character Development?': 'custom_global_strong_character_development',
        'Loveable Characters?': 'custom_global_loveable_characters',
        'Diverse Characters?': 'custom_global_diverse_characters',
        'Flawed Characters?': 'custom_global_flawed_characters',
        'Content Warnings': 'custom_global_content_warnings',
        'Review': 'notes'
    }


def confirmation_step():
    """Step 5: Confirmation and execution."""
    logger.info(f"🔍 ONBOARDING DEBUG: confirmation_step called, method={request.method}")
    
    onboarding_data = get_onboarding_data()
    
    logger.info(f"🔍 ONBOARDING DEBUG: onboarding_data: {onboarding_data}")
    
    # Ensure we stay on step 5 even if there are errors
    set_onboarding_step(5)
    
    if request.method == 'POST' and request.form.get('action') == 'execute':
        try:
            logger.info(f"🔍 ONBOARDING DEBUG: Executing onboarding with data: {onboarding_data}")
            
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
            
            # Check if this is an import setup or migration
            data_options = onboarding_data.get('data_options', {})
            
            if data_options.get('option') == 'import':
                # For imports, execute the basic setup first (user, location, custom fields)
                # but handle the CSV import as a background job
                success = execute_onboarding_setup_only(onboarding_data)
                
                if success:
                    # Get the import task ID from session (set during setup)
                    import_task_id = session.get('onboarding_import_task_id')
                    
                    if import_task_id:
                        logger.info(f"🔍 ONBOARDING DEBUG: Redirecting to import progress with task_id: {import_task_id}")
                        return redirect(url_for('onboarding.import_progress', task_id=import_task_id))
                    else:
                        # No import task, proceed to completion
                        logger.info(f"🔍 ONBOARDING DEBUG: No import task, proceeding to completion")
                        return handle_onboarding_completion(onboarding_data)
                else:
                    logger.error(f"🔍 ONBOARDING DEBUG: Setup failed")
                    flash('Setup failed. Please check the error message and try again.', 'error')
                    # Ensure session data is preserved
                    update_onboarding_data(onboarding_data)
                    set_onboarding_step(5)
            else:
                # For migrations or other setups, execute normally
                success = execute_onboarding(onboarding_data)
                
                if success:
                    return handle_onboarding_completion(onboarding_data)
                else:
                    logger.error(f"🔍 ONBOARDING DEBUG: Execution failed")
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
    
    logger.info(f"🔍 ONBOARDING DEBUG: Rendering step 5 confirmation template")
    
    return render_template('onboarding/step5_confirmation.html',
                         onboarding_data=onboarding_data,
                         step=5,
                         total_steps=5)


@onboarding_bp.route('/complete')
@debug_route('ONBOARDING_COMPLETE')
def complete():
    """Onboarding completion page."""
    if not current_user.is_authenticated:
        flash('Please complete the setup process.', 'error')
        return redirect(url_for('onboarding.start'))
    
    # Clear all onboarding session data including backup
    clear_onboarding_session()
    session.pop('onboarding_backup', None)
    session.modified = True
    
    # Set a success message
    flash('Welcome to Bibliotheca! Your setup is complete.', 'success')
    
    # Redirect to the main application
    return redirect(url_for('main.library'))


@onboarding_bp.route('/import-progress/<task_id>')
@debug_route('ONBOARDING_IMPORT_PROGRESS')
def import_progress(task_id: str):
    """Show import progress during onboarding."""
    logger.info(f"🔍 ONBOARDING DEBUG: Showing import progress for task {task_id}")
    
    try:
        # Get total books from job data if available
        try:
            from .routes import import_jobs
            job = import_jobs.get(task_id)
            total_books = job.get('total', 0) if job else 0
        except Exception as e:
            logger.warning(f"⚠️ Could not get job data: {e}")
            total_books = 0
        
        # Make sure we have default template context
        template_context = {
            'task_id': task_id,
            'total_books': total_books,
            'start_time': datetime.utcnow().isoformat(),
            'step': 5,
            'total_steps': 5,
            'site_name': 'MyBibliotheca',  # Default fallback
            'current_theme': 'light'  # Default fallback
        }
        
        return render_template('onboarding/import_books_progress.html', **template_context)
        
    except Exception as e:
        logger.error(f"❌ Error rendering import progress template: {e}")
        import traceback
        traceback.print_exc()
        # Return a simple error page
        return f"""
        <html>
        <head><title>Import Progress</title></head>
        <body>
        <h1>Import in Progress</h1>
        <p>Your books are being imported. Task ID: {task_id}</p>
        <p>Please wait while we set up your library...</p>
        <script>
        setTimeout(function() {{
            window.location.href = '/library';
        }}, 30000);  // Redirect after 30 seconds
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
        # Check both regular import jobs and Kuzu storage
        from .routes import import_jobs, get_job_from_kuzu
        
        # Try to get job from memory first
        job = import_jobs.get(task_id)
        
        # If not in memory, try Kuzu
        if not job:
            job = get_job_from_kuzu(task_id)
        
        if job:
            logger.info(f"📊 Job data: {job}")
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
        else:
            logger.warning(f"⚠️ Job {task_id} not found")
            return jsonify({
                'status': 'not_found',
                'processed': 0,
                'success': 0,
                'errors': 0,
                'total': 0,
                'error_messages': ['Job not found']
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
                # Initialize location service with Kuzu connection
                storage = get_graph_storage()
                location_service = LocationService(storage.kuzu_conn)
                
                # Create the location
                location = location_service.create_location(
                    user_id=admin_user.id,
                    name=location_name,
                    description=f"Default location set during onboarding",
                    location_type="home",  # Default to home type
                    is_default=site_config.get('location_set_as_default', True)
                )
                
                logger.info(f"✅ Created location: {location.name} (ID: {location.id})")
                
            except Exception as e:
                logger.error(f"❌ Failed to create location '{location_name}': {e}")
                # Don't fail the entire onboarding if location creation fails
        
        # TODO: Apply other site configuration settings to system settings
        
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
                    success = migration_system.migrate_v1_database(db_path, admin_user.id)
                elif database_analysis.get('version') == DatabaseVersion.V2_MULTI_USER:
                    migration_config = onboarding_data.get('migration_config', {})
                    admin_mapping = migration_config.get('admin_user_mapping')
                    user_mapping = {admin_mapping: admin_user.id} if admin_mapping else {}
                    success = migration_system.migrate_v2_database(db_path, user_mapping)
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
                        custom_field = custom_field_service.create_field_sync(field_definition)
                        
                        logger.info(f"✅ Created custom field: {field_name} ({field_type}) from CSV column: {csv_field}")
                        
                except Exception as e:
                    logger.error(f"❌ Failed to create custom metadata fields: {e}")
                    # Continue with import even if custom fields fail
            
            # Execute actual CSV file import with the configured field mappings
            logger.info(f"📋 Import configuration saved: {import_config}")
            logger.info(f"🔧 Custom fields configured: {len(custom_fields)}")
            
            # Start the CSV import as a background job
            import_task_id = start_onboarding_import_job(admin_user.id, import_config)
            if not import_task_id:
                logger.error("❌ Failed to start CSV import job")
                return False
            else:
                logger.info(f"✅ CSV import job started with ID: {import_task_id}")
                # Store the task ID for progress tracking
                session['onboarding_import_task_id'] = import_task_id
                session.modified = True
        
        return True
        
    except Exception as e:
        logger.error(f"Error executing onboarding: {e}")
        return False


def execute_onboarding_setup_only(onboarding_data: Dict) -> bool:
    """Execute only the basic onboarding setup (user, location, custom fields) without CSV import."""
    try:
        print(f"🚀 [SETUP] Starting onboarding setup")
        logger.info(f"🚀 Starting onboarding setup with data keys: {list(onboarding_data.keys())}")
        
        # Step 1: Create admin user
        admin_data = onboarding_data.get('admin', {})
        site_config = onboarding_data.get('site_config', {})
        
        if not admin_data:
            logger.error("❌ Missing admin data in execute_onboarding_setup_only")
            return False
        
        print(f"🚀 [SETUP] Creating admin user: {admin_data.get('username')}")
        logger.info(f"Creating admin user: {admin_data.get('username')}")
        
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
        
        if not admin_user:
            logger.error("❌ Failed to create admin user")
            return False
        
        print(f"✅ [SETUP] Admin user created: {admin_user.username}")
        logger.info(f"✅ Admin user created: {admin_user.username}")
        
        # Log in the user immediately after creation
        login_user(admin_user)
        print(f"✅ [SETUP] Admin user logged in: {admin_user.username}")
        logger.info(f"✅ Admin user created and logged in: {admin_user.username}")
        
        # Step 2: Apply site configuration and create location
        site_config = onboarding_data.get('site_config', {})
        
        # Create location if specified
        location_name = site_config.get('location', '').strip()
        if location_name:
            try:
                print(f"🏠 [SETUP] Creating location: {location_name}")
                logger.info(f"Creating location: {location_name}")
                
                # Initialize location service with Kuzu connection
                storage = get_graph_storage()
                location_service = LocationService(storage.kuzu_conn)
                
                # Create the location
                location = location_service.create_location(
                    user_id=admin_user.id,
                    name=location_name,
                    description=f"Default location set during onboarding",
                    location_type="home",  # Default to home type
                    is_default=site_config.get('location_set_as_default', True)
                )
                
                print(f"✅ [SETUP] Created location: {location.name} (ID: {location.id})")
                logger.info(f"✅ Created location: {location.name} (ID: {location.id})")
                
            except Exception as e:
                print(f"❌ [SETUP] Failed to create location '{location_name}': {e}")
                logger.error(f"❌ Failed to create location '{location_name}': {e}")
                # Don't fail the entire onboarding if location creation fails
        else:
            print(f"🏠 [SETUP] No location specified, skipping location creation")
            logger.info("No location specified, skipping location creation")
        
        # Step 3: Create custom metadata fields for import
        import_config = onboarding_data.get('import_config', {})
        custom_fields = import_config.get('custom_fields', {})
        
        # Create custom metadata fields first
        if custom_fields:
            try:
                print(f"🏷️ [SETUP] Creating {len(custom_fields)} custom fields")
                logger.info(f"Creating {len(custom_fields)} custom fields")
                
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
                    custom_field = custom_field_service.create_field_sync(field_definition)
                    
                    print(f"✅ [SETUP] Created custom field: {field_name} ({field_type}) from CSV column: {csv_field}")
                    logger.info(f"✅ Created custom field: {field_name} ({field_type}) from CSV column: {csv_field}")
                    
            except Exception as e:
                print(f"❌ [SETUP] Failed to create custom metadata fields: {e}")
                logger.error(f"❌ Failed to create custom metadata fields: {e}")
                # Continue even if custom fields fail
        else:
            print(f"🏷️ [SETUP] No custom fields to create")
            logger.info("No custom fields to create")
        
        # Step 4: Start the CSV import as a background job
        data_options = onboarding_data.get('data_options', {})
        if data_options.get('option') == 'import':
            print(f"📂 [SETUP] Starting CSV import job")
            logger.info("Starting CSV import job")
            
            import_task_id = start_onboarding_import_job(admin_user.id, import_config)
            if not import_task_id:
                print(f"❌ [SETUP] Failed to start CSV import job")
                logger.error("❌ Failed to start CSV import job")
                return False
            else:
                print(f"✅ [SETUP] CSV import job started with ID: {import_task_id}")
                logger.info(f"✅ CSV import job started with ID: {import_task_id}")
                # Store the task ID for progress tracking
                session['onboarding_import_task_id'] = import_task_id
                session.modified = True
        else:
            print(f"📂 [SETUP] No import selected, skipping CSV import")
            logger.info("No import selected, skipping CSV import")
        
        print(f"🎉 [SETUP] Onboarding setup completed successfully")
        logger.info("🎉 Onboarding setup completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ [SETUP] Error executing onboarding setup: {e}")
        logger.error(f"Error executing onboarding setup: {e}")
        import traceback
        traceback.print_exc()
        return False


def handle_onboarding_completion(onboarding_data: Dict):
    """Handle the completion of onboarding (login user and redirect)."""
    try:
        print(f"🎉 [COMPLETION] Starting onboarding completion process")
        logger.info(f"🔍 ONBOARDING DEBUG: Starting completion process")
        
        # Get the created admin user and log them in first
        admin_data = onboarding_data.get('admin', {})
        print(f"🎉 [COMPLETION] Looking for admin user: {admin_data.get('username')}")
        
        # Add debugging: let's see what users actually exist
        try:
            from .kuzu_integration import KuzuIntegrationService
            kuzu_service = KuzuIntegrationService()
            kuzu_service.initialize()
            all_users = kuzu_service.db.query("MATCH (u:User) RETURN u.username, u.id, u.email LIMIT 10")
            print(f"🔍 [COMPLETION DEBUG] All users in database: {all_users}")
        except Exception as debug_error:
            print(f"🔍 [COMPLETION DEBUG] Could not query users: {debug_error}")
        
        admin_user = user_service.get_user_by_username_sync(admin_data['username'])
        if not admin_user:
            print(f"❌ [COMPLETION] Could not find admin user: {admin_data.get('username')}")
            logger.error(f"🔍 ONBOARDING DEBUG: Could not find admin user: {admin_data.get('username')}")
            # Don't clear session - stay on step 5 with error
            flash('Setup failed: Could not create admin user. Please try again.', 'error')
            return render_template('onboarding/step5_confirmation.html',
                                   step=5, 
                                   onboarding_data=onboarding_data,
                                   setup_error=True)
        
        print(f"🎉 [COMPLETION] Found admin user, logging in: {admin_user.username}")
        login_user(admin_user)
        logger.info(f"🔍 ONBOARDING DEBUG: Admin user logged in successfully: {admin_user.username}")
        
        # Verify user is actually logged in
        from flask_login import current_user
        print(f"🎉 [COMPLETION] Current user after login: {current_user.is_authenticated}, ID: {getattr(current_user, 'id', 'NO_ID')}")
        
        # Check user count to verify user was created
        # Add retry logic in case of database timing issues
        import time
        max_retries = 3
        for attempt in range(max_retries):
            final_user_count = user_service.get_user_count_sync()
            print(f"🎉 [COMPLETION] User count check attempt {attempt + 1}: {final_user_count}")
            
            if final_user_count > 0:
                print(f"🎉 [COMPLETION] User count confirmed: {final_user_count}")
                break
            elif attempt < max_retries - 1:
                print(f"🎉 [COMPLETION] User count still 0, retrying in 0.5s...")
                time.sleep(0.5)
            else:
                print(f"❌ [COMPLETION] User count still 0 after {max_retries} attempts!")
                # Don't clear session if verification fails
                flash('Setup warning: User creation could not be verified. Please check if you can log in.', 'warning')
                return render_template('onboarding/step5_confirmation.html',
                                       step=5, 
                                       onboarding_data=onboarding_data,
                                       setup_warning=True)
        
        # Only clear onboarding session after successful verification
        print(f"🎉 [COMPLETION] Clearing onboarding session")
        clear_onboarding_session()
        logger.info(f"🔍 ONBOARDING DEBUG: Onboarding session cleared")
        
        print(f"🎉 [COMPLETION] Success! Redirecting to main index")
        logger.info(f"🔍 ONBOARDING DEBUG: Success! Redirecting to main index")
        flash('Welcome to Bibliotheca! Your library is ready.', 'success')
        
        # Instead of redirecting to library, redirect to index first to avoid potential route issues
        # The user can navigate to library from there
        return redirect(url_for('main.index'))
        
    except Exception as e:
        print(f"❌ [COMPLETION] Error handling onboarding completion: {e}")
        logger.error(f"Error handling onboarding completion: {e}")
        flash(f'Setup completed but login failed: {e}', 'warning')
        return redirect(url_for('auth.login'))


def start_onboarding_import_job(user_id: str, import_config: Dict) -> str:
    """Start a background import job for onboarding using the proven import system."""
    import uuid
    from datetime import datetime
    
    try:
        # Import the proven import functions from routes
        from .routes import store_job_in_kuzu, import_jobs, start_import_job, auto_create_custom_fields
        from flask import current_app
        
        # Create a unique task ID
        task_id = str(uuid.uuid4())
        
        logger.info(f"🚀 Starting simplified onboarding import job {task_id} for user {user_id}")
        
        # Create job data structure exactly like the working post-onboarding import
        job_data = {
            'task_id': task_id,
            'user_id': user_id,
            'csv_file_path': import_config.get('csv_file_path'),
            'field_mappings': import_config.get('field_mappings', {}),
            'default_reading_status': 'library_only',
            'duplicate_handling': 'skip',
            'custom_fields_enabled': True,
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
        
        # Count total rows in the CSV
        try:
            import csv
            csv_file_path = import_config.get('csv_file_path')
            if csv_file_path:
                with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    job_data['total'] = sum(1 for _ in reader)
        except Exception as e:
            logger.warning(f"⚠️ Could not count CSV rows: {e}")
            job_data['total'] = 0
        
        # Store job data using the same proven method as post-onboarding import
        kuzu_success = store_job_in_kuzu(task_id, job_data)
        import_jobs[task_id] = job_data
        
        logger.info(f"🏗️ Created onboarding import job {task_id} for user {user_id}")
        logger.info(f"📊 Kuzu storage: {'✅' if kuzu_success else '❌'}")
        logger.info(f"💾 Memory storage: ✅")
        
        # Auto-create custom fields (same as post-onboarding)
        field_mappings = import_config.get('field_mappings', {})
        auto_create_custom_fields(field_mappings, user_id)
        
        # Start the import using the proven working import system
        app = current_app._get_current_object()
        def run_import():
            with app.app_context():
                try:
                    start_import_job(task_id)
                except Exception as e:
                    if task_id in import_jobs:
                        import_jobs[task_id]['status'] = 'failed'
                        if 'error_messages' not in import_jobs[task_id]:
                            import_jobs[task_id]['error_messages'] = []
                        import_jobs[task_id]['error_messages'].append(str(e))
                    logger.error(f"Onboarding import job {task_id} failed: {e}")
        
        import threading
        thread = threading.Thread(target=run_import)
        thread.daemon = True
        thread.start()
        
        return task_id
        
    except Exception as e:
        logger.error(f"❌ Failed to start onboarding import job: {e}")
        return None

# Removed complex onboarding-specific import functions.
# Now using the proven post-onboarding import system directly via start_import_job().
    """Execute the onboarding import job in the background."""
    try:
        # Import the job functions from routes
        from .routes import import_jobs, update_job_in_kuzu
        
        # Get the job data
        job = import_jobs.get(task_id)
        if not job:
            logger.error(f"❌ Onboarding import job {task_id} not found")
            return
        
        logger.info(f"🚀 Starting onboarding import job {task_id}")
        
        # Mark job as running
        job['status'] = 'running'
        update_job_in_kuzu(task_id, {'status': 'running'})
        
        # Get import configuration
        user_id = job['user_id']
        import_config = {
            'csv_file_path': job['csv_file_path'],
            'field_mappings': job['field_mappings'],
            'custom_fields': job['custom_fields'],
            'import_options': job['import_options']
        }
        
        # Execute the actual CSV import with progress tracking
        success = execute_csv_import_with_progress(user_id, import_config, task_id)
        
        # Update final job status
        if success:
            job['status'] = 'completed'
            update_job_in_kuzu(task_id, {'status': 'completed'})
            logger.info(f"✅ Onboarding import job {task_id} completed successfully")
        else:
            job['status'] = 'failed'
            if 'error_messages' not in job:
                job['error_messages'] = []
            job['error_messages'].append('Import failed')
            update_job_in_kuzu(task_id, {'status': 'failed', 'error_messages': job['error_messages']})
            logger.error(f"❌ Onboarding import job {task_id} failed")
            
    except Exception as e:
        logger.error(f"❌ Error in onboarding import job {task_id}: {e}")
        # Update job status to failed
        try:
            from .routes import import_jobs, update_job_in_kuzu
            job = import_jobs.get(task_id)
            if job:
                job['status'] = 'failed'
                if 'error_messages' not in job:
                    job['error_messages'] = []
                job['error_messages'].append(str(e))
                update_job_in_kuzu(task_id, {'status': 'failed', 'error_messages': job['error_messages']})
        except:
            pass

def execute_csv_import_with_progress(user_id: str, import_config: Dict, task_id: str) -> bool:
    """Execute CSV import with progress tracking for onboarding."""
    try:
        # Import the job functions
        from .routes import import_jobs, update_job_in_kuzu
        
        # Get the job for progress tracking
        job = import_jobs.get(task_id)
        if not job:
            logger.error(f"❌ Job {task_id} not found for progress tracking")
            return False
        
        import csv
        
        # Get import configuration
        csv_file_path = import_config.get('csv_file_path')
        field_mappings = import_config.get('field_mappings', {})
        
        if not csv_file_path:
            logger.error("❌ No CSV file path provided")
            return False
            
        logger.info(f"📂 Importing CSV file: {csv_file_path}")
        
        # Import services we need
        from .services import book_service
        from .utils import normalize_goodreads_value, get_google_books_cover, fetch_book_data
        from .domain.models import Book, Person, BookContribution, ContributionType, Publisher, ReadingStatus, OwnershipStatus
        from .location_service import LocationService
        from config import Config
        
        # Get user's default location for importing books
        default_locations = []
        try:
            # Kuzu version: simplified connection
            storage = get_graph_storage()
            location_service = LocationService(storage.kuzu_conn)
            user_locations = location_service.get_user_locations(str(user_id))
            default_location = location_service.get_default_location(str(user_id))
            if default_location:
                default_locations = [default_location.id]
            elif user_locations:
                default_locations = [user_locations[0].id]  # Use first location as default
            logger.info(f"📍 Default locations for import: {default_locations}")
        except Exception as e:
            logger.warning(f"⚠️ Could not get user locations: {e}")
        
        # Read and process CSV
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
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
                    
                    # Process the book (simplified version for progress tracking)
                    success = process_single_book_import(book_data, user_id, default_locations, {})
                    
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
        from .services import book_service, normalize_isbn_upc
        from .domain.models import Book as DomainBook, Person, BookContribution, ContributionType, Publisher, ReadingStatus, OwnershipStatus
        from .utils import get_google_books_cover, fetch_book_data
        from datetime import datetime
        
        # Extract basic book data
        title = book_data.get('title', 'Unknown Title')
        csv_author = book_data.get('author', '')
        isbn = book_data.get('isbn', '')
        description = book_data.get('description')
        page_count = book_data.get('page_count')
        publisher_name = book_data.get('publisher')
        language = book_data.get('language', 'en')
        cover_url = book_data.get('cover_url')
        reading_status = book_data.get('reading_status', 'library_only')
        
        # Clean and normalize ISBN
        isbn10 = None
        isbn13 = None
        if isbn:
            normalized_isbn = normalize_isbn_upc(isbn)
            if normalized_isbn:
                if len(normalized_isbn) == 10:
                    isbn10 = normalized_isbn
                elif len(normalized_isbn) == 13:
                    isbn13 = normalized_isbn
        
        # Initialize metadata variables
        average_rating = None
        rating_count = None
        published_date = None
        google_data = None
        ol_data = None
        api_categories = None
        
        # Try to enrich with API data if we have an ISBN
        api_isbn = isbn13 or isbn10
        if api_isbn:
            logger.info(f"🔍 Fetching metadata for ISBN: {api_isbn}")
            try:
                # Try Google Books first
                google_data = get_google_books_cover(api_isbn, fetch_title_author=True)
                if google_data:
                    logger.info(f"✅ Got Google Books data for {title}")
                    # Update with API data
                    if google_data.get('title'):
                        title = google_data['title']
                    if google_data.get('author'):
                        csv_author = google_data['author']
                    description = description or google_data.get('description')
                    api_publisher = google_data.get('publisher')
                    if api_publisher:
                        api_publisher = api_publisher.strip('"\'')
                        publisher_name = publisher_name or api_publisher
                    page_count = page_count or google_data.get('page_count')
                    language = language or google_data.get('language', 'en')
                    cover_url = google_data.get('cover')
                    average_rating = google_data.get('average_rating')
                    rating_count = google_data.get('rating_count')
                    published_date = google_data.get('published_date')
                    api_categories = google_data.get('categories')
                    if api_categories:
                        logger.info(f"📚 Got categories from Google Books: {api_categories}")
                else:
                    # Fallback to OpenLibrary
                    logger.info("📚 No Google Books data, trying OpenLibrary...")
                    ol_data = fetch_book_data(api_isbn)
                    if ol_data:
                        logger.info(f"✅ Got OpenLibrary data for {title}")
                        if ol_data.get('title'):
                            title = ol_data['title']
                        if ol_data.get('author'):
                            csv_author = ol_data['author']
                        description = description or ol_data.get('description')
                        api_publisher = ol_data.get('publisher')
                        if api_publisher:
                            api_publisher = api_publisher.strip('"\'')
                            publisher_name = publisher_name or api_publisher
                        page_count = page_count or ol_data.get('page_count')
                        language = language or ol_data.get('language', 'en')
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
        
        # Create contributors
        contributors = []
        added_author_names = set()
        
        # Prioritize API authors if available
        if google_data and google_data.get('authors_list'):
            for i, author_name in enumerate(google_data['authors_list']):
                author_name = author_name.strip()
                if author_name and author_name.lower() not in added_author_names:
                    person = Person(name=author_name)
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
                    person = Person(name=author_name)
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
                person = Person(name=csv_author)
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
                    person = Person(name=name)
                    contribution = BookContribution(
                        person=person,
                        contribution_type=ContributionType.AUTHORED,
                        order=len(contributors)
                    )
                    contributors.append(contribution)
                    added_author_names.add(name.lower())
                    logger.info(f"📝 Added additional author: {name}")
        
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
        
        if created_book:
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
                    reading_status_enum = ReadingStatus.DID_NOT_FINISH
            
            # Add to user's library
            success = book_service.add_book_to_user_library_sync(
                user_id=str(user_id),
                book_id=created_book.id,
                reading_status=reading_status_enum,
                ownership_status=OwnershipStatus.OWNED,
                locations=default_locations
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
