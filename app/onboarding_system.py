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
    """Configure import settings."""
    logger.info(f"🔍 ONBOARDING DEBUG: import_config_step called, method={request.method}")
    logger.info(f"🔍 ONBOARDING DEBUG: data_options: {data_options}")
    
    if request.method == 'POST':
        try:
            import_config = {
                'type': 'import',
                'field_mappings': {},
                'custom_fields': {},
                'import_options': {}
            }
            
            # Add CSV file path from data_options
            if 'import_file_path' in data_options:
                import_config['csv_file_path'] = data_options['import_file_path']
                logger.info(f"🔍 ONBOARDING DEBUG: Added CSV file path: {data_options['import_file_path']}")
            else:
                logger.warning(f"🔍 ONBOARDING DEBUG: No import_file_path in data_options: {data_options}")
            
            # Handle field mappings from form
            for key, value in request.form.items():
                if key.startswith('field_'):
                    field_name = key[6:]  # Remove 'field_' prefix
                    if value:  # Only include non-empty mappings
                        if value.startswith('custom_metadata:'):
                            # This is a custom metadata field
                            custom_field_name = value.split(':', 1)[1]
                            custom_type_key = f'custom_type_{field_name}'
                            custom_type = request.form.get(custom_type_key, 'string')
                            
                            import_config['custom_fields'][field_name] = {
                                'name': custom_field_name,
                                'type': custom_type,
                                'target_field': f'custom_{custom_field_name.lower().replace(" ", "_")}'
                            }
                            import_config['field_mappings'][field_name] = f'custom_{custom_field_name.lower().replace(" ", "_")}'
                        else:
                            # Standard field mapping
                            import_config['field_mappings'][field_name] = value
                elif key.startswith('custom_type_'):
                    # Skip these as they're handled above
                    continue
                elif key in ['skip_duplicates', 'auto_fetch_metadata', 'create_collections']:
                    import_config['import_options'][key] = bool(request.form.get(key))
            
            logger.info(f"🔍 ONBOARDING DEBUG: Saving import_config: {import_config}")
            update_onboarding_data({'import_config': import_config})
            
            # Move to confirmation step
            logger.info(f"🔍 ONBOARDING DEBUG: Moving to step 5")
            set_onboarding_step(5)
            return redirect(url_for('onboarding.step', step_num=5))
            
        except Exception as e:
            logger.error(f"🔍 ONBOARDING DEBUG: Error in import config: {e}")
            flash(f'Error configuring import: {e}', 'error')
    
    # Get real CSV headers if possible, otherwise use mock data
    csv_headers = []
    available_fields = [
        'title', 'author', 'additional_authors', 'isbn', 'description', 
        'published_date', 'publisher', 'page_count', 'rating', 'reading_status',
        'date_read', 'date_added', 'notes', 'categories', 'format'
    ]
    
    import_file = data_options.get('import_file')
    if import_file:
        logger.info(f"🔍 ONBOARDING DEBUG: Attempting to read headers from {import_file}")
        try:
            # Try to detect CSV headers from common sample files
            if import_file.lower() == 'storygraphcsv.csv':
                csv_headers = [
                    "Title", "Authors", "Contributors", "ISBN/UID", "Format",
                    "Read Status", "Date Added", "Last Date Read", "Dates Read", 
                    "Read Count", "Moods", "Pace", "Character- or Plot-Driven?",
                    "Strong Character Development?", "Loveable Characters?", 
                    "Diverse Characters?", "Flawed Characters?", "Star Rating",
                    "Review", "Content Warnings", "Content Warning Description", 
                    "Tags", "Owned?"
                ]
                logger.info(f"🔍 ONBOARDING DEBUG: Using StoryGraph headers")
            elif 'goodreads' in import_file.lower():
                csv_headers = [
                    "Book Id", "Title", "Author", "Author l-f", "Additional Authors", 
                    "ISBN", "ISBN13", "My Rating", "Average Rating", "Publisher", 
                    "Binding", "Number of Pages", "Year Published", "Original Publication Year", 
                    "Date Read", "Date Added", "Bookshelves", "Bookshelves with positions", 
                    "Exclusive Shelf", "My Review", "Spoiler", "Private Notes", "Read Count", "Owned Copies"
                ]
                logger.info(f"🔍 ONBOARDING DEBUG: Using Goodreads headers")
            else:
                # Generic CSV headers - try to read actual file if possible
                import_file_path = data_options.get('import_file')
                if import_file_path:
                    try:
                        # Try to read from common locations
                        possible_paths = [
                            f'/app/test_files/{import_file_path}',
                            f'./test_files/{import_file_path}',
                            f'/app/{import_file_path}',
                            import_file_path
                        ]
                        
                        csv_content = None
                        for path in possible_paths:
                            try:
                                with open(path, 'r', encoding='utf-8') as f:
                                    csv_content = f.read()
                                    logger.info(f"🔍 ONBOARDING DEBUG: Successfully read CSV from {path}")
                                    break
                            except:
                                continue
                        
                        if csv_content:
                            import csv
                            from io import StringIO
                            reader = csv.reader(StringIO(csv_content))
                            csv_headers = next(reader, [])
                            logger.info(f"🔍 ONBOARDING DEBUG: Read actual CSV headers: {csv_headers}")
                        else:
                            raise FileNotFoundError("Could not read CSV file")
                            
                    except Exception as e:
                        logger.warning(f"🔍 ONBOARDING DEBUG: Could not read actual CSV, using generic headers: {e}")
                        csv_headers = ['Title', 'Author', 'ISBN', 'Description', 'Publication Date', 'Rating', 'Status']
                else:
                    csv_headers = ['Title', 'Author', 'ISBN', 'Description', 'Publication Date', 'Rating', 'Status']
                logger.info(f"🔍 ONBOARDING DEBUG: Using generic CSV headers")
        except Exception as e:
            logger.error(f"🔍 ONBOARDING DEBUG: Error reading CSV headers: {e}")
            csv_headers = ['Title', 'Author', 'ISBN', 'Description', 'Publication Date']
    else:
        # Default mock headers
        csv_headers = ['Title', 'Author', 'ISBN', 'Description', 'Publication Date']
        logger.info(f"🔍 ONBOARDING DEBUG: No import file, using default headers")
    
    logger.info(f"🔍 ONBOARDING DEBUG: Final csv_headers: {csv_headers}")
    logger.info(f"🔍 ONBOARDING DEBUG: available_fields: {available_fields}")
    
    return render_template('onboarding/step4_import_config.html',
                         available_fields=available_fields,
                         csv_headers=csv_headers,
                         data_options=data_options,
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
                    flash('Setup failed. Please try again.', 'error')
            else:
                # For migrations or other setups, execute normally
                success = execute_onboarding(onboarding_data)
                
                if success:
                    return handle_onboarding_completion(onboarding_data)
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
        # Check both regular import jobs and Redis storage
        from .routes import import_jobs, get_job_from_redis
        
        # Try to get job from memory first
        job = import_jobs.get(task_id)
        
        # If not in memory, try Redis
        if not job:
            job = get_job_from_redis(task_id)
        
        if job:
            logger.info(f"📊 Job data: {job}")
            return jsonify({
                'status': job.get('status', 'pending'),
                'processed': job.get('processed', 0),
                'success': job.get('success', 0),
                'errors': job.get('errors', 0),
                'total': job.get('total', 0),
                'current_book': job.get('current_book'),
                'error_message': job.get('error_message'),
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
                'error_message': 'Job not found'
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
            'error_message': str(e)
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
        
        # Log in the user immediately after creation
        login_user(admin_user)
        logger.info(f"✅ Admin user created and logged in: {admin_user.username}")
        
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
        
        # Step 3: Create custom metadata fields for import
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
                # Continue even if custom fields fail
        
        # Step 4: Start the CSV import as a background job
        data_options = onboarding_data.get('data_options', {})
        if data_options.get('option') == 'import':
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
        logger.error(f"Error executing onboarding setup: {e}")
        return False


def handle_onboarding_completion(onboarding_data: Dict):
    """Handle the completion of onboarding (login user and redirect)."""
    try:
        # Clear onboarding session
        clear_onboarding_session()
        
        # Get the created admin user and log them in
        admin_data = onboarding_data.get('admin', {})
        admin_user = user_service.get_user_by_username_sync(admin_data['username'])
        if admin_user:
            login_user(admin_user)
        
        logger.info(f"🔍 ONBOARDING DEBUG: Success! Redirecting to library")
        flash('Welcome to Bibliotheca! Your library is ready.', 'success')
        return redirect(url_for('main.library'))
        
    except Exception as e:
        logger.error(f"Error handling onboarding completion: {e}")
        flash(f'Setup completed but login failed: {e}', 'warning')
        return redirect(url_for('auth.login'))


def start_onboarding_import_job(user_id: str, import_config: Dict) -> str:
    """Start a background import job for onboarding."""
    import uuid
    from datetime import datetime
    
    try:
        # Import the Redis job functions from routes
        from .routes import store_job_in_redis, import_jobs
        
        # Create a unique task ID
        task_id = str(uuid.uuid4())
        
        # Prepare job data similar to regular import jobs
        job_data = {
            'task_id': task_id,
            'user_id': user_id,
            'csv_file_path': import_config.get('csv_file_path'),
            'field_mappings': import_config.get('field_mappings', {}),
            'custom_fields': import_config.get('custom_fields', {}),
            'import_options': import_config.get('import_options', {}),
            'status': 'pending',
            'processed': 0,
            'success': 0,
            'errors': 0,
            'total': 0,
            'start_time': datetime.utcnow().isoformat(),
            'current_book': None,
            'error_messages': [],
            'recent_activity': [],
            'onboarding': True  # Flag to indicate this is an onboarding import
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
        
        # Store job data in Redis and memory
        redis_success = store_job_in_redis(task_id, job_data)
        import_jobs[task_id] = job_data
        
        logger.info(f"🏗️ Created onboarding import job {task_id} for user {user_id}")
        logger.info(f"📊 Redis storage: {'✅' if redis_success else '❌'}")
        
        # Start the import job in the background
        import threading
        thread = threading.Thread(target=execute_onboarding_import_job, args=(task_id,))
        thread.daemon = True
        thread.start()
        
        return task_id
        
    except Exception as e:
        logger.error(f"❌ Failed to start onboarding import job: {e}")
        return None

def execute_onboarding_import_job(task_id: str):
    """Execute the onboarding import job in the background."""
    try:
        # Import the job functions from routes
        from .routes import import_jobs, update_job_in_redis
        
        # Get the job data
        job = import_jobs.get(task_id)
        if not job:
            logger.error(f"❌ Onboarding import job {task_id} not found")
            return
        
        logger.info(f"🚀 Starting onboarding import job {task_id}")
        
        # Mark job as running
        job['status'] = 'running'
        update_job_in_redis(task_id, {'status': 'running'})
        
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
            update_job_in_redis(task_id, {'status': 'completed'})
            logger.info(f"✅ Onboarding import job {task_id} completed successfully")
        else:
            job['status'] = 'failed'
            job['error_message'] = 'Import failed'
            update_job_in_redis(task_id, {'status': 'failed', 'error_message': 'Import failed'})
            logger.error(f"❌ Onboarding import job {task_id} failed")
            
    except Exception as e:
        logger.error(f"❌ Error in onboarding import job {task_id}: {e}")
        # Update job status to failed
        try:
            from .routes import import_jobs, update_job_in_redis
            job = import_jobs.get(task_id)
            if job:
                job['status'] = 'failed'
                job['error_message'] = str(e)
                update_job_in_redis(task_id, {'status': 'failed', 'error_message': str(e)})
        except:
            pass

def execute_csv_import_with_progress(user_id: str, import_config: Dict, task_id: str) -> bool:
    """Execute CSV import with progress tracking for onboarding."""
    try:
        # Import the job functions
        from .routes import import_jobs, update_job_in_redis
        
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
        from .infrastructure.redis_graph import RedisGraphConnection
        from config import Config
        
        # Get user's default location for importing books
        default_locations = []
        try:
            redis_connection = RedisGraphConnection(Config.REDIS_URL)
            location_service = LocationService(redis_connection.client)
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
                        update_job_in_redis(task_id, {
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
        update_job_in_redis(task_id, {
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
