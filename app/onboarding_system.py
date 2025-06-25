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

from flask import Blueprint, render_template, request, redirect, url_for, flash, session, current_app
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
from .infrastructure.redis_graph import RedisGraphConnection

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

def get_onboarding_step() -> int:
    """Get current onboarding step from session."""
    return session.get('onboarding_step', 1)

def set_onboarding_step(step: int):
    """Set current onboarding step in session."""
    logger.info(f"🔍 SESSION DEBUG: set_onboarding_step called with step: {step}")
    session['onboarding_step'] = step
    session.modified = True
    logger.info(f"🔍 SESSION DEBUG: onboarding_step set to: {session.get('onboarding_step', 'NOT_FOUND')}")

def get_onboarding_data() -> Dict:
    """Get all onboarding data from session."""
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
    current_data = get_onboarding_data()
    print(f"🔍 SESSION DEBUG: current_data before update: {current_data}")
    logger.info(f"🔍 SESSION DEBUG: current_data before update: {current_data}")
    current_data.update(data)
    session['onboarding_data'] = current_data
    session.modified = True
    
    # Also store in a backup location to help with debugging
    session['onboarding_backup'] = current_data.copy()
    session.permanent = True  # Make session permanent to help with persistence
    
    print(f"🔍 SESSION DEBUG: session after update: {dict(session)}")
    print(f"🔍 SESSION DEBUG: onboarding_data in session: {session.get('onboarding_data', 'NOT_FOUND')}")
    print(f"🔍 SESSION DEBUG: backup data: {session.get('onboarding_backup', 'NOT_FOUND')}")
    logger.info(f"🔍 SESSION DEBUG: session after update: {dict(session)}")
    logger.info(f"🔍 SESSION DEBUG: onboarding_data in session: {session.get('onboarding_data', 'NOT_FOUND')}")

def clear_onboarding_session():
    """Clear all onboarding data from session."""
    session.pop('onboarding_step', None)
    session.pop('onboarding_data', None)
    session.modified = True


@onboarding_bp.route('/start')
@debug_route('ONBOARDING_START')
def start():
    """Start the onboarding process."""
    try:
        # Check if users already exist
        user_count = user_service.get_user_count_sync()
        if user_count > 0:
            flash('Setup has already been completed.', 'info')
            return redirect(url_for('auth.login'))
    except Exception as e:
        logger.error(f"Error checking user count: {e}")
    
    # Initialize onboarding session
    clear_onboarding_session()
    set_onboarding_step(1)
    
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
                return redirect(url_for('onboarding.step', step_num=1))
        
        # Step 3 requires site config from step 2  
        if step_num >= 3 and 'site_config' not in onboarding_data:
            print(f"🔍 ONBOARDING DEBUG: Missing site_config data for step {step_num}, redirecting to step 2")
            logger.warning(f"🔍 ONBOARDING DEBUG: Missing site_config data for step {step_num}, redirecting to step 2")
            return redirect(url_for('onboarding.step', step_num=2))
            
        # Allow access if we have the required data
        print(f"🔍 STEP DEBUG: Requirements met, setting step to {step_num}")
        logger.info(f"🔍 ONBOARDING DEBUG: Access granted, setting step to {step_num}")
        set_onboarding_step(step_num)
    
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
    
    form = SetupForm()
    
    print(f"🔍 ADMIN_SETUP DEBUG: Form created, submitted={form.is_submitted()}")
    logger.info(f"🔍 ONBOARDING DEBUG: Form created, is_submitted={form.is_submitted()}, is_valid={form.validate() if form.is_submitted() else 'N/A'}")
    
    if form.validate_on_submit():
        try:
            print(f"🔍 ADMIN_SETUP DEBUG: Form validated successfully!")
            logger.info(f"🔍 ONBOARDING DEBUG: Form validated, processing data")
            
            # Store admin data for later user creation
            admin_data = {
                'username': form.username.data,
                'email': form.email.data,
                'password': form.password.data
            }
            
            print(f"🔍 ADMIN_SETUP DEBUG: Admin data prepared: username={admin_data['username']}, email={admin_data['email']}")
            logger.info(f"🔍 ONBOARDING DEBUG: Saving admin data: {admin_data['username']}, {admin_data['email']}")
            
            print(f"🔍 ADMIN_SETUP DEBUG: Before update - session keys: {list(session.keys())}")
            update_onboarding_data({'admin': admin_data})
            print(f"🔍 ADMIN_SETUP DEBUG: After update - session keys: {list(session.keys())}")
            print(f"🔍 ADMIN_SETUP DEBUG: Updated onboarding data: {get_onboarding_data()}")
            
            # Move to next step
            print(f"🔍 ADMIN_SETUP DEBUG: Setting step to 2")
            logger.info(f"🔍 ONBOARDING DEBUG: Moving to step 2")
            set_onboarding_step(2)
            print(f"🔍 ADMIN_SETUP DEBUG: Step set, current step: {get_onboarding_step()}")
            
            print(f"🔍 ADMIN_SETUP DEBUG: Full session after all updates: {dict(session)}")
            logger.info(f"🔍 ONBOARDING DEBUG: Session after update: {dict(session)}")
            
            flash('Admin account configured successfully!', 'success')
            print(f"🔍 ADMIN_SETUP DEBUG: About to redirect to step 2")
            return redirect(url_for('onboarding.step', step_num=2))
            
        except Exception as e:
            print(f"❌ ADMIN_SETUP ERROR: {e}")
            logger.error(f"🔍 ONBOARDING DEBUG: Error in admin setup: {e}")
            flash(f'Error configuring admin account: {e}', 'error')
    else:
        if request.method == 'POST':
            print(f"❌ ADMIN_SETUP DEBUG: Form validation failed: {form.errors}")
    
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
            update_onboarding_data({'site_config': site_config})
            
            # Move to next step
            print(f"🔍 SITE_CONFIG DEBUG: Moving to step 3")
            logger.info(f"🔍 ONBOARDING DEBUG: Moving to step 3")
            set_onboarding_step(3)
            
            print(f"🔍 SITE_CONFIG DEBUG: Session after all updates: {dict(session)}")
            logger.info(f"🔍 ONBOARDING DEBUG: Session after update: {dict(session)}")
            
            flash('Site configuration saved!', 'success')
            print(f"🔍 SITE_CONFIG DEBUG: About to redirect to step 3")
            return redirect(url_for('onboarding.step', step_num=3))
            
        except Exception as e:
            print(f"❌ SITE_CONFIG ERROR: {e}")
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
                if uploaded_file and uploaded_file.filename:
                    # Save uploaded file temporarily
                    # For now, just store filename
                    data_options['import_file'] = uploaded_file.filename
                    logger.info(f"🔍 ONBOARDING DEBUG: Import file: {uploaded_file.filename}")
            
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
    """Configure import settings."""
    if request.method == 'POST':
        try:
            import_config = {
                'type': 'import',
                'field_mappings': {}
            }
            
            # Handle field mappings from form
            for key, value in request.form.items():
                if key.startswith('field_'):
                    field_name = key[6:]  # Remove 'field_' prefix
                    import_config['field_mappings'][field_name] = value
            
            update_onboarding_data({'import_config': import_config})
            
            # Move to confirmation step
            set_onboarding_step(5)
            return redirect(url_for('onboarding.step', step_num=5))
            
        except Exception as e:
            logger.error(f"Error in import config: {e}")
            flash(f'Error configuring import: {e}', 'error')
    
    # Mock field mappings for now
    available_fields = ['title', 'author', 'isbn', 'description', 'published_date']
    csv_headers = ['Title', 'Author', 'ISBN', 'Summary', 'Publication Year']
    
    return render_template('onboarding/step4_import_config.html',
                         available_fields=available_fields,
                         csv_headers=csv_headers,
                         step=4,
                         total_steps=5)


def confirmation_step():
    """Step 5: Confirmation and execution."""
    logger.info(f"🔍 ONBOARDING DEBUG: confirmation_step called, method={request.method}")
    
    onboarding_data = get_onboarding_data()
    
    logger.info(f"🔍 ONBOARDING DEBUG: onboarding_data: {onboarding_data}")
    
    if request.method == 'POST' and request.form.get('action') == 'execute':
        try:
            logger.info(f"🔍 ONBOARDING DEBUG: Executing onboarding with data: {onboarding_data}")
            
            # Execute the onboarding configuration
            success = execute_onboarding(onboarding_data)
            
            logger.info(f"🔍 ONBOARDING DEBUG: Execution result: {success}")
            
            if success:
                # Clear onboarding session
                clear_onboarding_session()
                
                # Get the created admin user and log them in
                admin_data = onboarding_data.get('admin', {})
                admin_user = user_service.get_user_by_username_sync(admin_data['username'])
                if admin_user:
                    login_user(admin_user)
                
                logger.info(f"🔍 ONBOARDING DEBUG: Success! Redirecting to complete")
                flash('Welcome to Bibliotheca! Your library is ready.', 'success')
                return redirect(url_for('onboarding.complete'))
            else:
                logger.error(f"🔍 ONBOARDING DEBUG: Execution failed")
                flash('Setup failed. Please try again.', 'error')
                
        except Exception as e:
            logger.error(f"🔍 ONBOARDING DEBUG: Error executing onboarding: {e}")
            flash(f'Setup failed: {e}', 'error')
    
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
    
    return render_template('onboarding/complete.html')


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
        
        # Step 2: Apply site configuration and create location
        site_config = onboarding_data.get('site_config', {})
        
        # Create location if specified
        location_name = site_config.get('location', '').strip()
        if location_name:
            try:
                # Initialize location service
                from config import Config
                redis_connection = RedisGraphConnection(Config.REDIS_URL)
                location_service = LocationService(redis_connection.client)
                
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
            # TODO: Handle file import
            pass
        
        return True
        
    except Exception as e:
        logger.error(f"Error executing onboarding: {e}")
        return False
