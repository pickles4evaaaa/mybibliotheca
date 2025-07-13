"""
Advanced Migration Routes for Bibliotheca
==========================================

Web interface for the advanced migration system that handles:
1. Database detection and analysis
2. First-time setup with admin user creation
3. V1/V2 database migration workflows
4. User mapping for V2 migrations
5. Migration status and progress tracking
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
from flask_login import login_required, current_user
from werkzeug.security import generate_password_hash
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

from .advanced_migration_system import AdvancedMigrationSystem, DatabaseVersion, MigrationStatus
from .services import user_service
from .forms import SetupForm
from .debug_utils import debug_route

# Create blueprint
migration_bp = Blueprint('advanced_migration', __name__, url_prefix='/migration')

logger = logging.getLogger(__name__)


@migration_bp.route('/detect', methods=['GET'])
@debug_route('MIGRATION_DETECT')
def detect_databases():
    """Detect and analyze available SQLite databases."""
    try:
        migration_system = AdvancedMigrationSystem()
        databases = migration_system.find_sqlite_databases()
        
        db_analysis = []
        for db_path in databases:
            version, analysis = migration_system.detect_database_version(db_path)
            db_analysis.append({
                'path': str(db_path),
                'name': db_path.name,
                'version': version,
                'analysis': analysis
            })
        
        return render_template('migration/detect.html', databases=db_analysis)
        
    except Exception as e:
        logger.error(f"Error detecting databases: {e}")
        flash(f'Error detecting databases: {e}', 'error')
        return redirect(url_for('main.index'))


@migration_bp.route('/setup', methods=['GET', 'POST'])
@debug_route('MIGRATION_SETUP')
def first_time_setup():
    """Handle first-time setup with admin user creation and optional migration."""
    try:
        # Check if users already exist
        user_count = user_service.get_user_count_sync()
        if user_count > 0:
            flash('Setup has already been completed.', 'info')
            return redirect(url_for('main.index'))
    except Exception as e:
        logger.error(f"Error checking user count: {e}")
        # Continue with setup if we can't check
    
    # Detect available databases
    migration_system = AdvancedMigrationSystem()
    databases = migration_system.find_sqlite_databases()
    
    db_analysis = []
    for db_path in databases:
        version, analysis = migration_system.detect_database_version(db_path)
        db_analysis.append({
            'path': str(db_path),
            'name': db_path.name,
            'version': version,
            'analysis': analysis
        })
    
    # Check if we have form data from the original setup route
    setup_form_data = session.get('setup_form_data')
    
    form = SetupForm()
    
    # Pre-populate form if we have data from original setup
    if setup_form_data and not form.is_submitted():
        form.username.data = setup_form_data.get('username', '')
        form.email.data = setup_form_data.get('email', '')
        # Don't pre-fill password for security
    
    if form.validate_on_submit():
        try:
            # Use form data or fallback to session data
            username = form.username.data or (setup_form_data and setup_form_data.get('username'))
            email = form.email.data or (setup_form_data and setup_form_data.get('email'))
            password = form.password.data or (setup_form_data and setup_form_data.get('password'))
            
            if not all([username, email, password]):
                flash('All fields are required.', 'error')
                return render_template('migration/first_time_setup.html', 
                                     form=form, 
                                     databases=db_analysis)
            
            # Type safety: ensure all values are strings (the check above guarantees they're truthy)
            username = str(username)
            email = str(email)
            password = str(password)
            
            # Create the first admin user
            admin_user = migration_system.create_first_admin_user(
                username=username,
                email=email,
                password=password
            )
            
            if admin_user:
                # Store admin user ID in session for potential migration
                session['setup_admin_user_id'] = admin_user.id
                session['setup_admin_username'] = admin_user.username
                
                # Clear the original setup form data
                session.pop('setup_form_data', None)
                
                flash(f'Admin user "{admin_user.username}" created successfully!', 'success')
                
                # If databases were found, offer migration
                if databases:
                    return redirect(url_for('advanced_migration.choose_migration'))
                else:
                    # No databases to migrate, setup complete
                    from flask_login import login_user
                    login_user(admin_user)
                    return redirect(url_for('main.index'))
            else:
                flash('Failed to create admin user. Please try again.', 'error')
                
        except Exception as e:
            logger.error(f"Error creating admin user: {e}")
            flash(f'Error creating admin user: {e}', 'error')
    
    return render_template('migration/first_time_setup.html', 
                         form=form, 
                         databases=db_analysis)


@migration_bp.route('/choose', methods=['GET', 'POST'])
@debug_route('MIGRATION_CHOOSE')
def choose_migration():
    """Choose which database to migrate after admin user creation."""
    admin_user_id = session.get('setup_admin_user_id')
    if not admin_user_id:
        flash('Setup session expired. Please restart setup.', 'error')
        return redirect(url_for('advanced_migration.first_time_setup'))
    
    migration_system = AdvancedMigrationSystem()
    databases = migration_system.find_sqlite_databases()
    
    db_analysis = []
    for db_path in databases:
        version, analysis = migration_system.detect_database_version(db_path)
        db_analysis.append({
            'path': str(db_path),
            'name': db_path.name,
            'version': version,
            'analysis': analysis
        })
    
    if request.method == 'POST':
        selected_db = request.form.get('selected_database')
        if selected_db:
            session['migration_database'] = selected_db
            
            # Find the selected database analysis
            selected_analysis = None
            for db in db_analysis:
                if db['path'] == selected_db:
                    selected_analysis = db
                    break
            
            if selected_analysis:
                if selected_analysis['version'] == DatabaseVersion.V1_SINGLE_USER:
                    return redirect(url_for('advanced_migration.migrate_v1'))
                elif selected_analysis['version'] == DatabaseVersion.V2_MULTI_USER:
                    return redirect(url_for('advanced_migration.setup_v2_migration'))
                else:
                    flash('Unknown database version. Cannot migrate.', 'error')
        else:
            flash('Please select a database to migrate.', 'error')
    
    return render_template('migration/choose_migration.html', 
                         databases=db_analysis,
                         admin_username=session.get('setup_admin_username'))


@migration_bp.route('/migrate-v1', methods=['GET', 'POST'])
@debug_route('MIGRATION_V1')
def migrate_v1():
    """Migrate a V1 (single-user) database."""
    admin_user_id = session.get('setup_admin_user_id')
    selected_db = session.get('migration_database')
    
    if not admin_user_id or not selected_db:
        flash('Migration session invalid. Please restart setup.', 'error')
        return redirect(url_for('advanced_migration.first_time_setup'))
    
    if request.method == 'POST':
        if request.form.get('action') == 'migrate':
            try:
                migration_system = AdvancedMigrationSystem()
                
                # Create backup
                db_path = Path(selected_db)
                if not migration_system.create_backup(db_path):
                    flash('Failed to create backup. Migration aborted.', 'error')
                    return redirect(url_for('advanced_migration.choose_migration'))
                
                # Perform migration
                success = migration_system.migrate_v1_database(db_path, admin_user_id)
                
                if success:
                    # Store migration results
                    session['migration_results'] = migration_system.get_migration_summary()
                    
                    # Clear setup session data
                    session.pop('setup_admin_user_id', None)
                    session.pop('setup_admin_username', None)
                    session.pop('migration_database', None)
                    
                    # Login the admin user
                    from flask_login import login_user
                    admin_user = user_service.get_user_by_id_sync(admin_user_id)
                    if admin_user:
                        login_user(admin_user)
                    
                    return redirect(url_for('advanced_migration.migration_complete'))
                else:
                    flash('Migration failed. Please check the logs.', 'error')
                    
            except Exception as e:
                logger.error(f"Error during V1 migration: {e}")
                flash(f'Migration error: {e}', 'error')
    
    # Analyze the database for display
    migration_system = AdvancedMigrationSystem()
    db_path = Path(selected_db)
    version, analysis = migration_system.detect_database_version(db_path)
    
    return render_template('migration/migrate_v1.html', 
                         database=selected_db,
                         analysis=analysis,
                         admin_username=session.get('setup_admin_username'))


@migration_bp.route('/setup-v2', methods=['GET', 'POST'])
@debug_route('MIGRATION_V2_SETUP')
def setup_v2_migration():
    """Setup V2 migration by selecting which old user corresponds to the new admin."""
    admin_user_id = session.get('setup_admin_user_id')
    selected_db = session.get('migration_database')
    
    if not admin_user_id or not selected_db:
        flash('Migration session invalid. Please restart setup.', 'error')
        return redirect(url_for('advanced_migration.first_time_setup'))
    
    # Analyze the V2 database
    migration_system = AdvancedMigrationSystem()
    db_path = Path(selected_db)
    version, analysis = migration_system.detect_database_version(db_path)
    
    if version != DatabaseVersion.V2_MULTI_USER:
        flash('Selected database is not a V2 multi-user database.', 'error')
        return redirect(url_for('advanced_migration.choose_migration'))
    
    if request.method == 'POST':
        admin_mapping = request.form.get('admin_user_mapping')
        if admin_mapping:
            # Store the user mapping configuration
            user_mapping = {int(admin_mapping): admin_user_id}
            session['v2_user_mapping'] = user_mapping
            session['v2_admin_mapping'] = int(admin_mapping)
            
            return redirect(url_for('advanced_migration.migrate_v2'))
        else:
            flash('Please select which old user corresponds to your admin account.', 'error')
    
    return render_template('migration/setup_v2_migration.html',
                         database=selected_db,
                         analysis=analysis,
                         old_users=analysis.get('users', []),
                         admin_username=session.get('setup_admin_username'))


@migration_bp.route('/migrate-v2', methods=['GET', 'POST'])
@debug_route('MIGRATION_V2')
def migrate_v2():
    """Migrate a V2 (multi-user) database with user mapping."""
    admin_user_id = session.get('setup_admin_user_id')
    selected_db = session.get('migration_database')
    user_mapping = session.get('v2_user_mapping', {})
    
    if not admin_user_id or not selected_db or not user_mapping:
        flash('Migration session invalid. Please restart setup.', 'error')
        return redirect(url_for('advanced_migration.first_time_setup'))
    
    if request.method == 'POST':
        if request.form.get('action') == 'migrate':
            try:
                # Get user choice for API metadata enhancement
                fetch_api_metadata = request.form.get('api_metadata', 'false').lower() == 'true'
                
                migration_system = AdvancedMigrationSystem()
                
                # Create backup
                db_path = Path(selected_db)
                if not migration_system.create_backup(db_path):
                    flash('Failed to create backup. Migration aborted.', 'error')
                    return redirect(url_for('advanced_migration.setup_v2_migration'))
                
                # Convert string keys back to integers for user mapping
                int_user_mapping = {int(k): v for k, v in user_mapping.items()}
                
                # Perform migration with user's choice for API enhancement
                success = migration_system.migrate_v2_database(db_path, int_user_mapping, fetch_api_metadata=fetch_api_metadata)
                
                if success:
                    # Store migration results
                    session['migration_results'] = migration_system.get_migration_summary()
                    
                    # Clear setup session data
                    session.pop('setup_admin_user_id', None)
                    session.pop('setup_admin_username', None)
                    session.pop('migration_database', None)
                    session.pop('v2_user_mapping', None)
                    session.pop('v2_admin_mapping', None)
                    
                    # Login the admin user
                    from flask_login import login_user
                    admin_user = user_service.get_user_by_id_sync(admin_user_id)
                    if admin_user:
                        login_user(admin_user)
                    
                    return redirect(url_for('advanced_migration.migration_complete'))
                else:
                    flash('Migration failed. Please check the logs.', 'error')
                    
            except Exception as e:
                logger.error(f"Error during V2 migration: {e}")
                flash(f'Migration error: {e}', 'error')
    
    # Analyze the database for display
    migration_system = AdvancedMigrationSystem()
    db_path = Path(selected_db)
    version, analysis = migration_system.detect_database_version(db_path)
    
    admin_mapping = session.get('v2_admin_mapping')
    mapped_user = None
    if admin_mapping:
        for user in analysis.get('users', []):
            if user['id'] == admin_mapping:
                mapped_user = user
                break
    
    return render_template('migration/migrate_v2.html',
                         database=selected_db,
                         analysis=analysis,
                         mapped_user=mapped_user,
                         admin_username=session.get('setup_admin_username'))


@migration_bp.route('/complete')
@debug_route('MIGRATION_COMPLETE')
def migration_complete():
    """Display migration completion status and results."""
    migration_results = session.get('migration_results')
    if not migration_results:
        flash('No migration results found.', 'error')
        return redirect(url_for('main.index'))
    
    # Clear the results from session after displaying
    session.pop('migration_results', None)
    
    return render_template('migration/complete.html', results=migration_results)


@migration_bp.route('/status/<migration_id>')
@debug_route('MIGRATION_STATUS')
def migration_status(migration_id):
    """Get migration status via AJAX (for future progress tracking)."""
    # This would be used for real-time migration progress
    # For now, return a simple status
    return jsonify({
        'status': 'completed',
        'progress': 100,
        'message': 'Migration completed successfully'
    })


# Admin-only routes for database management
@migration_bp.route('/admin/databases')
@login_required
@debug_route('MIGRATION_ADMIN_DB')
def admin_database_list():
    """Admin view of all available databases (requires admin login)."""
    if not current_user.is_admin:
        flash('Admin access required.', 'error')
        return redirect(url_for('main.index'))
    
    try:
        migration_system = AdvancedMigrationSystem()
        databases = migration_system.find_sqlite_databases()
        
        db_analysis = []
        for db_path in databases:
            version, analysis = migration_system.detect_database_version(db_path)
            db_analysis.append({
                'path': str(db_path),
                'name': db_path.name,
                'version': version,
                'analysis': analysis
            })
        
        return render_template('migration/admin_databases.html', databases=db_analysis)
        
    except Exception as e:
        logger.error(f"Error in admin database list: {e}")
        flash(f'Error retrieving database information: {e}', 'error')
        return redirect(url_for('main.index'))


@migration_bp.route('/admin/migrate/<path:db_path>')
@login_required
@debug_route('MIGRATION_ADMIN_MIGRATE')
def admin_migrate_database(db_path):
    """Admin-initiated migration for existing installations."""
    if not current_user.is_admin:
        flash('Admin access required.', 'error')
        return redirect(url_for('main.index'))
    
    # Implementation for admin-initiated migrations
    # This would be similar to the setup flow but for existing installations
    flash('Admin migration feature coming soon.', 'info')
    return redirect(url_for('advanced_migration.admin_database_list'))
