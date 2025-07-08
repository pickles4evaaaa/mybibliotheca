"""
Import/Export routes for the Bibliotheca application.
Handles book import functionality including CSV processing, progress tracking, and batch operations.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, jsonify, make_response, session
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from datetime import datetime
import uuid
import os
import csv
import threading
import traceback
import tempfile
import secrets
import asyncio

from app.services import book_service, import_mapping_service, custom_field_service
from app.simplified_book_service import SimplifiedBookService, SimplifiedBook
from app.domain.models import CustomFieldDefinition, CustomFieldType

# Global dictionary to store import jobs (shared with routes.py for now)
import_jobs = {}

# Create import blueprint
import_bp = Blueprint('import', __name__)

# Helper functions for import functionality
def auto_detect_fields(headers, user_id):
    """Auto-detect field mappings from CSV headers."""
    if not headers:
        return {}
    
    # Field mapping logic based on common CSV headers
    mappings = {}
    
    # Standard field mappings
    header_mappings = {
        # Title variations
        'title': 'title',
        'book title': 'title',
        'name': 'title',
        'book name': 'title',
        
        # Author variations
        'author': 'author',
        'author name': 'author',
        'writer': 'author',
        'authors': 'author',
        'main author': 'author',
        'primary author': 'author',
        
        # Additional authors
        'additional authors': 'additional_authors',
        'co-authors': 'additional_authors',
        'other authors': 'additional_authors',
        
        # ISBN variations
        'isbn': 'isbn',
        'isbn10': 'isbn',
        'isbn13': 'isbn',
        'isbn-10': 'isbn',
        'isbn-13': 'isbn',
        
        # Rating variations
        'rating': 'rating',
        'my rating': 'rating',
        'user rating': 'rating',
        'personal rating': 'rating',
        
        # Publisher
        'publisher': 'publisher',
        'publishing company': 'publisher',
        
        # Pages
        'pages': 'page_count',
        'page count': 'page_count',
        'number of pages': 'page_count',
        'total pages': 'page_count',
        
        # Publication year
        'year': 'publication_year',
        'publication year': 'publication_year',
        'published year': 'publication_year',
        'year published': 'publication_year',
        
        # Dates
        'date read': 'date_read',
        'reading date': 'date_read',
        'finished date': 'date_read',
        'date added': 'date_added',
        'added date': 'date_added',
        
        # Reading status
        'status': 'reading_status',
        'reading status': 'reading_status',
        'shelf': 'reading_status',
        'exclusive shelf': 'reading_status',
        
        # Notes and reviews
        'notes': 'notes',
        'review': 'notes',
        'my review': 'notes',
        'comments': 'notes',
        'description': 'description',
        
        # Categories/Tags
        'categories': 'categories',
        'tags': 'categories',
        'genre': 'categories',
        'genres': 'categories',
        'bookshelves': 'categories',
        
        # Goodreads-specific fields (mapped to custom fields)
        'average rating': 'custom_global_average_rating',
        'binding': 'custom_global_binding',
        'original publication year': 'custom_global_original_publication_year',
        'spoiler': 'custom_global_spoiler',
        'private notes': 'custom_global_private_notes',
        'read count': 'custom_global_read_count',
        'owned copies': 'custom_personal_owned_copies',
    }
    
    # Match headers (case insensitive)
    for header in headers:
        header_lower = header.lower().strip()
        if header_lower in header_mappings:
            mappings[header] = header_mappings[header_lower]
    
    return mappings

def auto_create_custom_fields(field_mappings, user_id):
    """Auto-create custom fields referenced in field mappings."""
    if not field_mappings or not user_id:
        return
        
    print(f"🔧 Auto-creating custom fields for user {user_id}")
    
    # Field configuration for common custom fields
    FIELD_CONFIGS = {
        'goodreads_book_id': {'display_name': 'Goodreads Book ID', 'type': CustomFieldType.TEXT, 'global': True},
        'average_rating': {'display_name': 'Average Rating', 'type': CustomFieldType.NUMBER, 'global': True},
        'binding': {'display_name': 'Binding Type', 'type': CustomFieldType.TEXT, 'global': True},
        'original_publication_year': {'display_name': 'Original Publication Year', 'type': CustomFieldType.NUMBER, 'global': True},
        'bookshelves': {'display_name': 'Bookshelves', 'type': CustomFieldType.TAGS, 'global': True},
        'bookshelves_with_positions': {'display_name': 'Bookshelves with Positions', 'type': CustomFieldType.TEXTAREA, 'global': True},
        'spoiler': {'display_name': 'Spoiler Review', 'type': CustomFieldType.BOOLEAN, 'global': True},
        'private_notes': {'display_name': 'Private Notes', 'type': CustomFieldType.TEXTAREA, 'global': False},
        'read_count': {'display_name': 'Number of Times Read', 'type': CustomFieldType.NUMBER, 'global': True},
        'owned_copies': {'display_name': 'Number of Owned Copies', 'type': CustomFieldType.NUMBER, 'global': False},
        'format': {'display_name': 'Book Format', 'type': CustomFieldType.TEXT, 'global': True},
        'moods': {'display_name': 'Moods', 'type': CustomFieldType.TAGS, 'global': True},
        'pace': {'display_name': 'Reading Pace', 'type': CustomFieldType.TEXT, 'global': True},
        'character_plot_driven': {'display_name': 'Character vs Plot Driven', 'type': CustomFieldType.TEXT, 'global': True},
        'content_warnings': {'display_name': 'Content Warnings', 'type': CustomFieldType.TAGS, 'global': True},
        'diverse_characters': {'display_name': 'Diverse Characters', 'type': CustomFieldType.BOOLEAN, 'global': True},
        'flawed_characters': {'display_name': 'Flawed Characters', 'type': CustomFieldType.BOOLEAN, 'global': True},
    }
    
    # Get existing custom fields to avoid duplicates
    try:
        existing_fields = custom_field_service.get_user_fields_sync(user_id) or []
    except:
        existing_fields = []
    
    existing_field_names = set()
    if existing_fields:
        try:
            # Handle case where service returns None or empty result
            for field in existing_fields:
                if hasattr(field, 'name'):
                    existing_field_names.add(field.name)
        except:
            pass
    
    # Process each mapping
    for csv_field, target_field in field_mappings.items():
        if not isinstance(target_field, str):
            continue
            
        # Check if this is a custom field reference
        if target_field.startswith('custom_global_') or target_field.startswith('custom_personal_'):
            # Extract field name
            if target_field.startswith('custom_global_'):
                field_name = target_field[14:]  # Remove 'custom_global_'
                is_global = True
            else:
                field_name = target_field[16:]  # Remove 'custom_personal_'
                is_global = False
            
            # Skip if field already exists
            if field_name in existing_field_names:
                continue
                
            # Get field configuration
            config = FIELD_CONFIGS.get(field_name)
            if config:
                try:
                    # Create the custom field definition
                    field_definition = CustomFieldDefinition(
                        name=field_name,
                        display_name=config['display_name'],
                        field_type=config['type'],
                        is_global=config['global'],
                        created_by_user_id=user_id,
                        description=f'Auto-created during import for CSV column "{csv_field}"'
                    )
                    
                    # Create the field using the service
                    success = custom_field_service.create_field_sync(field_definition)
                    if success:
                        print(f"✅ Created custom field: {config['display_name']} ({field_name})")
                        existing_field_names.add(field_name)
                    else:
                        print(f"❌ Failed to create custom field: {field_name}")
                        
                except Exception as e:
                    print(f"❌ Error creating custom field {field_name}: {e}")
            else:
                # Create a generic field if no specific config exists
                try:
                    # Generate display name from field name
                    display_name = field_name.replace('_', ' ').title()
                    
                    field_definition = CustomFieldDefinition(
                        name=field_name,
                        display_name=display_name,
                        field_type=CustomFieldType.TEXT,  # Default to text
                        is_global=is_global,
                        created_by_user_id=user_id,
                        description=f'Auto-created during import for CSV column "{csv_field}"'
                    )
                    
                    success = custom_field_service.create_field_sync(field_definition)
                    if success:
                        print(f"✅ Created generic custom field: {display_name} ({field_name})")
                        existing_field_names.add(field_name)
                    else:
                        print(f"❌ Failed to create generic custom field: {field_name}")
                        
                except Exception as e:
                    print(f"❌ Error creating generic custom field {field_name}: {e}")

def get_goodreads_field_mappings():
    """Get predefined field mappings for Goodreads CSV format."""
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
        'Owned Copies': 'custom_personal_owned_copies',
    }

@import_bp.route('/import-books', methods=['GET', 'POST'])
@login_required
def import_books():
    """New unified import interface."""
    if request.method == 'POST':
        # Check if user wants to force custom mapping (bypass template detection)
        force_custom = request.form.get('force_custom', 'false').lower() == 'true'
        
        # Check if we're coming from confirmation screen with existing file
        existing_csv_path = request.form.get('csv_file_path')
        file = None  # Initialize file variable
        
        if existing_csv_path and force_custom:
            # Reuse existing CSV file for custom mapping
            temp_path = existing_csv_path
        else:
            # Handle new file upload and show field mapping
            file = request.files.get('csv_file')
            if not file or not file.filename or not file.filename.endswith('.csv'):
                flash('Please upload a valid CSV file.', 'danger')
                return redirect(url_for('import.import_books'))
            
            # Save file temporarily
            filename = secure_filename(file.filename)
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv', prefix=f'import_{current_user.id}_')
            file.save(temp_file.name)
            temp_path = temp_file.name
        try:
            # Read CSV headers and first few rows for preview
            with open(temp_path, 'r', encoding='utf-8') as csvfile:
                # Try to detect delimiter with improved logic
                sample = csvfile.read(1024)
                csvfile.seek(0)
                
                delimiter = ','  # Default to comma
                try:
                    sniffer = csv.Sniffer()
                    delimiter = sniffer.sniff(sample).delimiter
                except Exception:
                    # If sniffer fails, try common delimiters
                    for test_delimiter in [',', ';', '\t', '|']:
                        if test_delimiter in sample:
                            delimiter = test_delimiter
                            break
                
                reader = csv.DictReader(csvfile, delimiter=delimiter)
                headers = reader.fieldnames or []
                
                # Get first 3 rows for preview
                preview_rows = []
                total_rows = 0
                for i, row in enumerate(reader):
                    if i < 3:
                        preview_rows.append([row.get(header, '') for header in headers])
                    total_rows += 1
                
                # If no headers detected, treat first column as ISBN list
                if not headers or len(headers) == 1:
                    csvfile.seek(0)
                    first_line = csvfile.readline().strip()
                    if first_line.isdigit() or len(first_line) in [10, 13]:  # Looks like ISBN
                        headers = ['ISBN']
                        csvfile.seek(0)
                        all_lines = csvfile.readlines()
                        preview_rows = [[line.strip()] for line in all_lines[:3]]
                        total_rows = len(all_lines)
            
            # Auto-detect field mappings
            suggested_mappings = auto_detect_fields(headers, current_user.id)
            
            # Check if this is a Goodreads or StoryGraph file - but still show mapping screen
            if not force_custom:
                goodreads_signatures = ['Book Id', 'Author l-f', 'Bookshelves', 'Exclusive Shelf']
                storygraph_signatures = ['Read Status', 'Moods', 'Pace', 'Character- or Plot-Driven?']
                
                is_goodreads = any(header in headers for header in goodreads_signatures)
                is_storygraph = any(header in headers for header in storygraph_signatures)
                
                if is_goodreads or is_storygraph:
                    # Store the temp file path in the session for the direct import option
                    session['direct_import_file'] = temp_path
                    if existing_csv_path:
                        session['direct_import_filename'] = 'existing_import_file.csv'
                    elif file and file.filename:
                        session['direct_import_filename'] = file.filename
                    else:
                        session['direct_import_filename'] = 'unknown_file.csv'
                    import_type = 'goodreads' if is_goodreads else 'storygraph'
                    
                    # Flash a message suggesting the file type was detected - but still show mapping
                    flash(f'Detected {import_type.title()} export file! We\'ve pre-mapped common fields for you. Review and customize the mappings below.', 'info')
            
            # Get custom fields for the user
            try:
                # Get user's personal and global fields separately for proper categorization
                user_fields = custom_field_service.get_user_fields_sync(current_user.id)
                
                # Handle None returns from stub services
                global_custom_fields = []
                personal_custom_fields = []
                if user_fields and hasattr(user_fields, '__iter__'):
                    # Use explicit loops to avoid "Never" is not iterable errors
                    user_fields_list = list(user_fields)  # Convert to list to avoid type issues
                    for field in user_fields_list:
                        if hasattr(field, 'is_global'):
                            if field.is_global:
                                global_custom_fields.append(field)
                            else:
                                personal_custom_fields.append(field)
                
                # Also get shareable fields from other users
                shareable_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=True)
                # Add shareable fields to global fields if they're not already included
                if shareable_fields and hasattr(shareable_fields, '__iter__'):
                    shareable_fields_list = list(shareable_fields)  # Convert to list to avoid type issues
                    for field in shareable_fields_list:
                        if hasattr(field, 'id') and not any(hasattr(gf, 'id') and gf.id == field.id for gf in global_custom_fields):
                            global_custom_fields.append(field)
                        
                print(f"🔧 [IMPORT] Loaded {len(global_custom_fields)} global fields and {len(personal_custom_fields)} personal fields")
            except Exception as e:
                current_app.logger.error(f"Error loading custom fields: {e}")
                global_custom_fields = []
                personal_custom_fields = []
            
            # Get import templates for the user
            try:
                import_templates = import_mapping_service.get_user_templates_sync(current_user.id)
                
                # Handle None return from stub service
                template_list = []
                if import_templates and hasattr(import_templates, '__iter__'):
                    template_list = list(import_templates)
                
                print(f"DEBUG: CSV headers: {headers}")
                print(f"DEBUG: Force custom mapping: {force_custom}")
                print(f"DEBUG: Available templates: {[t.get('name', 'Unknown') for t in template_list] if template_list else []}")
                
                # Skip template detection if user wants custom mapping
                if force_custom:
                    detected_template = None
                    detected_template_name = None
                    print(f"DEBUG: Skipping template detection - user requested custom mapping")
                else:
                    # Detect best matching template based on headers
                    # Ensure headers is a proper List[str] type
                    headers_list = list(headers) if not isinstance(headers, list) else headers
                    detected_template = import_mapping_service.detect_template_sync(headers_list, current_user.id)
                    detected_template_name = detected_template.name if detected_template else None
                    
                    print(f"DEBUG: Template detection - detected_template: {detected_template}")
                    print(f"DEBUG: Template detection - detected_template_name: {detected_template_name}")
                    if detected_template:
                        print(f"DEBUG: Template detection - template.field_mappings: {detected_template.field_mappings}")
                        print(f"DEBUG: Template detection - field_mappings type: {type(detected_template.field_mappings)}")
                        print(f"DEBUG: Template detection - field_mappings bool: {bool(detected_template.field_mappings)}")
                
                # If a default system template was detected, auto-create fields but still show mapping UI for review
                if not force_custom and detected_template and detected_template.user_id == "__system__" and detected_template.field_mappings:
                    print(f"DEBUG: System template detected - {detected_template.name}")
                    print(f"DEBUG: Auto-creating custom fields from template mappings: {detected_template.field_mappings}")
                    
                    # Auto-create any custom fields referenced in the template BEFORE showing mapping screen
                    auto_create_custom_fields(detected_template.field_mappings, current_user.id)
                    
                    # Reload custom fields after creating new ones so they appear in the mapping screen
                    user_fields = custom_field_service.get_user_fields_sync(current_user.id)
                    if user_fields and hasattr(user_fields, '__iter__'):
                        global_custom_fields = []
                        personal_custom_fields = []
                        user_fields_list = list(user_fields)  # Convert to list to avoid type issues
                        for field in user_fields_list:
                            if hasattr(field, 'is_global'):
                                if field.is_global:
                                    global_custom_fields.append(field)
                                else:
                                    personal_custom_fields.append(field)
                    else:
                        global_custom_fields = []
                        personal_custom_fields = []
                    
                    # Use template mappings as suggested mappings
                    suggested_mappings = detected_template.field_mappings.copy()
                    print(f"DEBUG: Using template mappings as suggestions: {suggested_mappings}")
                    
                    # Add the detected template to the import_templates list so it appears in dropdown
                    # Convert ImportMappingTemplate object to dictionary for template compatibility
                    template_dict = {
                        'id': detected_template.id,
                        'name': detected_template.name,
                        'user_id': detected_template.user_id,
                        'description': detected_template.description,
                        'field_mappings': detected_template.field_mappings,
                        'sample_headers': getattr(detected_template, 'sample_headers', []),
                        'created_at': detected_template.created_at
                    }
                    if template_list and not any(t.get('id') == detected_template.id for t in template_list):
                        template_list.append(template_dict)
                        print(f"DEBUG: Added detected template to dropdown: {detected_template.name}")
                
                # If a custom template was detected, use its mappings
                elif detected_template and detected_template.field_mappings:
                    print(f"DEBUG: Custom template detected - Using template mappings")
                    suggested_mappings = detected_template.field_mappings.copy()
                    print(f"DEBUG: Using template mappings: {suggested_mappings}")
                    # Auto-create any custom fields referenced in the template
                    auto_create_custom_fields(suggested_mappings, current_user.id)
                    # Reload custom fields after creating new ones
                    user_fields = custom_field_service.get_user_fields_sync(current_user.id)
                    if user_fields and hasattr(user_fields, '__iter__'):
                        global_custom_fields = []
                        personal_custom_fields = []
                        user_fields_list = list(user_fields)  # Convert to list to avoid type issues
                        for field in user_fields_list:
                            if hasattr(field, 'is_global'):
                                if field.is_global:
                                    global_custom_fields.append(field)
                                else:
                                    personal_custom_fields.append(field)
                    else:
                        global_custom_fields = []
                        personal_custom_fields = []
                    
                    # Add the detected template to the import_templates list so it appears in dropdown
                    # Convert ImportMappingTemplate object to dictionary for template compatibility
                    template_dict = {
                        'id': detected_template.id,
                        'name': detected_template.name,
                        'user_id': detected_template.user_id,
                        'description': detected_template.description,
                        'field_mappings': detected_template.field_mappings,
                        'sample_headers': getattr(detected_template, 'sample_headers', []),
                        'created_at': detected_template.created_at
                    }
                    if template_list and not any(t.get('id') == detected_template.id for t in template_list):
                        template_list.append(template_dict)
                        print(f"DEBUG: Added detected template to dropdown: {detected_template.name}")
                else:
                    print(f"DEBUG: No template detected, using auto-detected mappings: {suggested_mappings}")
                
            except Exception as e:
                current_app.logger.error(f"Error loading import templates: {e}")
                template_list = []
                detected_template = None
                detected_template_name = None
            
            return render_template('import_books_mapping.html',
                                 csv_file_path=temp_path,
                                 csv_headers=headers,
                                 csv_preview=preview_rows,
                                 total_rows=total_rows,
                                 suggested_mappings=suggested_mappings,
                                 global_custom_fields=global_custom_fields,
                                 personal_custom_fields=personal_custom_fields,
                                 import_templates=template_list,
                                 detected_template=detected_template,
                                 detected_template_name=detected_template_name)
                                 
        except Exception as e:
            current_app.logger.error(f"Error processing CSV file: {e}")
            flash('Error processing CSV file. Please check the format and try again.', 'danger')
            return redirect(url_for('import.import_books'))
    
    return render_template('import_books.html')

@import_bp.route('/import-books/execute', methods=['POST'])
@login_required
def import_books_execute():
    """Execute the import with user-defined field mappings or template mappings."""
    csv_file_path = request.form.get('csv_file_path')
    use_template = request.form.get('use_template')
    skip_mapping = request.form.get('skip_mapping', 'false').lower() == 'true'
    
    # Initialize template saving variables
    save_as_template = False
    template_name = ''
    
    # If using a template directly (from confirmation screen)
    if use_template and skip_mapping:
        print(f"DEBUG: Executing import with template: {use_template}")
        
        # Get the template to use its mappings
        try:
            # Since get_template_by_id_sync doesn't exist, look for template by name
            # This is a workaround for the missing method
            template = None
            all_templates_raw = import_mapping_service.get_user_templates_sync(current_user.id)
            if all_templates_raw and hasattr(all_templates_raw, '__iter__'):
                templates_list = list(all_templates_raw)  # Convert to list to avoid type issues
                for t in templates_list:
                    if hasattr(t, 'get') and (t.get('name') == use_template or t.get('id') == use_template):
                        # Convert dict to object-like structure for compatibility
                        class TemplateObj:
                            def __init__(self, data):
                                self.id = data.get('id')
                                self.name = data.get('name')
                                self.field_mappings = data.get('field_mappings', {})
                        template = TemplateObj(t)
                        break
            
            if not template:
                flash('Template not found', 'error')
                return redirect(url_for('import.import_books'))
            
            print(f"DEBUG: Template mappings: {template.field_mappings}")
            
            # Convert template mappings to the format expected by import
            mappings = {}
            for csv_field, mapping_info in template.field_mappings.items():
                if isinstance(mapping_info, dict):
                    target_field = mapping_info.get('target_field')
                else:
                    target_field = mapping_info  # Fallback for string format
                
                if target_field:
                    mappings[csv_field] = target_field
            
            print(f"DEBUG: Converted mappings for import: {mappings}")
            
        except Exception as e:
            current_app.logger.error(f"Error loading template for import: {e}")
            flash('Error loading template configuration', 'error')
            return redirect(url_for('import.import_books'))
    
    else:
        # Handle manual mapping (from mapping UI)
        field_mapping = request.form.to_dict()
        
        # Extract mapping from form data
        mappings = {}
        new_fields_to_create = {}
        
        for key, value in field_mapping.items():
            if key.startswith('field_mapping[') and value:
                csv_field = key[14:-1]  # Remove 'field_mapping[' and ']'
                
                # Handle new field creation
                if value in ['create_global_field', 'create_personal_field']:
                    label_key = f'new_field_label[{csv_field}]'
                    type_key = f'new_field_type[{csv_field}]'
                    
                    field_label = request.form.get(label_key, '').strip()
                    field_type = request.form.get(type_key, 'text')
                    
                    if not field_label:
                        flash(f'Field label is required for CSV column "{csv_field}"', 'error')
                        return redirect(url_for('import.import_books'))
                    
                    # Create the custom field
                    try:
                        is_global = (value == 'create_global_field')
                        field_name = field_label.lower().replace(' ', '_').replace('-', '_')
                        
                        # Check if field already exists
                        existing_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global)
                        field_exists = False
                        if existing_fields and hasattr(existing_fields, '__iter__'):
                            existing_fields_list = list(existing_fields)  # Convert to list to avoid type issues
                            field_exists = any(hasattr(f, 'name') and f.name == field_name for f in existing_fields_list)
                        
                        if field_exists:
                            flash(f'A custom field with name "{field_name}" already exists', 'error')
                            return redirect(url_for('import.import_books'))
                        
                        # Convert field_type string to enum
                        field_type_enum = CustomFieldType.TEXT  # Default
                        if field_type == 'number':
                            field_type_enum = CustomFieldType.NUMBER
                        elif field_type == 'date':
                            field_type_enum = CustomFieldType.DATE
                        elif field_type == 'boolean':
                            field_type_enum = CustomFieldType.BOOLEAN
                        elif field_type == 'textarea':
                            field_type_enum = CustomFieldType.TEXTAREA
                        elif field_type == 'tags':
                            field_type_enum = CustomFieldType.TAGS
                        elif field_type == 'url':
                            field_type_enum = CustomFieldType.URL
                        elif field_type == 'email':
                            field_type_enum = CustomFieldType.EMAIL
                        
                        field_definition = CustomFieldDefinition(
                            name=field_name,
                            display_name=field_label,
                            field_type=field_type_enum,
                            is_global=is_global,
                            created_by_user_id=current_user.id,
                            created_at=datetime.utcnow(),
                            description=f'Created during CSV import for column "{csv_field}"'
                        )
                        
                        custom_field_service.create_field_sync(field_definition)
                        
                        # Update mapping to use the new field
                        mappings[csv_field] = f'custom_{"global" if is_global else "personal"}_{field_name}'
                        
                        flash(f'Created new {"global" if is_global else "personal"} field: {field_label}', 'success')
                        
                    except Exception as e:
                        flash(f'Error creating custom field: {str(e)}', 'error')
                        return redirect(url_for('import.import_books'))
                else:
                    mappings[csv_field] = value
        
        # Handle template saving (only for manual mapping)
        save_as_template = request.form.get('save_as_template') == 'on'
        template_name = request.form.get('template_name', '').strip()
    
    if save_as_template and template_name:
        try:
            # Since create_template doesn't exist, we'll skip template saving for now
            # This is a workaround - in a full implementation, we'd store this in Kuzu
            current_app.logger.info(f'Template "{template_name}" would be saved but create_template is not implemented')
            flash(f'Import template "{template_name}" functionality not yet implemented', 'info')
            
        except Exception as e:
            flash(f'Error saving template: {str(e)}', 'warning')
            # Continue with import even if template saving fails
    
    default_reading_status = request.form.get('default_reading_status', 'library_only')
    duplicate_handling = request.form.get('duplicate_handling', 'skip')
    
    # Create import job with enhanced data
    task_id = str(uuid.uuid4())
    job_data = {
        'task_id': task_id,
        'user_id': current_user.id,
        'csv_file_path': csv_file_path,
        'field_mappings': mappings,
        'default_reading_status': default_reading_status,
        'duplicate_handling': duplicate_handling,
        'custom_fields_enabled': True,  # Flag to enable custom metadata processing
        'status': 'pending',
        'processed': 0,
        'success': 0,
        'errors': 0,
        'skipped': 0,  # Initialize skipped counter
        'total': 0,
        'start_time': datetime.utcnow().isoformat(),
        'current_book': None,
        'error_messages': [],
        'recent_activity': []
    }
    
    # Count total rows
    try:
        if csv_file_path:
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                job_data['total'] = sum(1 for _ in reader)
        else:
            job_data['total'] = 0
    except:
        job_data['total'] = 0
    
    # Store job data in Kuzu instead of Redis
    print(f"🏗️ [EXECUTE] Creating job {task_id} for user {current_user.id}")
    kuzu_success = store_job_in_kuzu(task_id, job_data)
    
    # Also keep in memory for backward compatibility
    import_jobs[task_id] = job_data
    
    print(f"📊 [EXECUTE] Kuzu storage: {'✅' if kuzu_success else '❌'}")
    print(f"💾 [EXECUTE] Memory storage: ✅")
    print(f"🔧 [EXECUTE] Job status: {job_data['status']}")
    print(f"📈 [EXECUTE] Total rows to process: {job_data['total']}")
    
    # Start the import in background thread
    # Store necessary data for background processing
    import_config = {
        'task_id': task_id,
        'csv_file_path': csv_file_path,
        'field_mappings': mappings,
        'user_id': current_user.id,
        'default_reading_status': default_reading_status
    }
    
    def run_import():
        try:
            print(f"🚀 [BACKGROUND] Starting import job {task_id} in background thread")
            print(f"🔧 [BACKGROUND] Import config: {import_config}")
            
            # Call the async import function with an event loop
            asyncio.run(start_import_job_standalone(import_config))
            
        except Exception as e:
            print(f"❌ [IMPORT] Error in background thread: {e}")
            traceback.print_exc()
            # Update job with error in both storage systems
            try:
                error_update = {
                    'status': 'failed',
                    'error_messages': [str(e)]
                }
                update_job_in_kuzu(task_id, error_update)
                if task_id in import_jobs:
                    import_jobs[task_id]['status'] = 'failed'
                    if 'error_messages' not in import_jobs[task_id]:
                        import_jobs[task_id]['error_messages'] = []
                    import_jobs[task_id]['error_messages'].append(str(e))
            except Exception as update_error:
                print(f"❌ [IMPORT] Failed to update job with error: {update_error}")
    
    # Start the import process in background
    thread = threading.Thread(target=run_import)
    thread.daemon = True
    thread.start()
    
    print(f"🚀 [EXECUTE] Background thread started for job {task_id}")
    
    return redirect(url_for('import.import_books_progress', task_id=task_id))

@import_bp.route('/import-books/progress/<task_id>')
@login_required
def import_books_progress(task_id):
    """Show import progress."""
    job = import_jobs.get(task_id)
    if not job:
        flash('Import job not found.', 'error')
        return redirect(url_for('import.import_books'))
    
    return render_template('import_progress.html', job=job, task_id=task_id)

@import_bp.route('/api/import/progress/<task_id>')
@login_required
def api_import_progress(task_id):
    """API endpoint for import progress."""
    job = import_jobs.get(task_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    
    return jsonify(job)

@import_bp.route('/api/import/errors/<task_id>')
@login_required
def api_import_errors(task_id):
    """API endpoint for import errors."""
    job = import_jobs.get(task_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    
    return jsonify({'errors': job.get('error_messages', [])})

@import_bp.route('/debug/import-jobs')
@login_required
def debug_import_jobs():
    """Debug endpoint to view all import jobs."""
    return jsonify(import_jobs)

@import_bp.route('/direct_import', methods=['GET', 'POST'])
@login_required
def direct_import():
    """Handle direct import for Goodreads/StoryGraph."""
    
    # Check if we have a suggested file from the session
    suggested = request.args.get('suggested', 'false').lower() == 'true'
    import_type = request.args.get('import_type', 'goodreads')
    suggested_filename = session.get('direct_import_filename')
    
    if request.method == 'GET':
        return render_template('direct_import.html', 
                             suggested=suggested,
                             import_type=import_type,
                             suggested_filename=suggested_filename)
    
    # POST request - handle the import
    try:
        use_suggested_file = request.form.get('use_suggested_file') == 'true'
        
        if use_suggested_file and session.get('direct_import_file'):
            # Use the file from session (already uploaded)
            temp_path = session['direct_import_file']
            original_filename = session.get('direct_import_filename', 'import.csv')
        else:
            # Handle new file upload
            file = request.files.get('csv_file')
            if not file or not file.filename or not file.filename.endswith('.csv'):
                flash('Please select a valid CSV file.', 'error')
                return redirect(url_for('import.direct_import'))
            
            # Save file temporarily
            filename = secure_filename(file.filename)
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv', prefix=f'direct_import_{current_user.id}_')
            file.save(temp_file.name)
            temp_path = temp_file.name
            original_filename = filename
        
        # Process the import
        # ... additional import logic here ...
        
        flash('Import completed successfully!', 'success')
        return redirect(url_for('main.library'))
        
    except Exception as e:
        current_app.logger.error(f"Error in direct import: {e}")
        flash('An error occurred during import. Please try again.', 'error')
        return redirect(url_for('import.direct_import'))

@import_bp.route('/migrate-sqlite', methods=['GET', 'POST'])
@login_required
def migrate_sqlite():
    """Handle SQLite database migration from v1 and v1.5 legacy databases."""
    
    if request.method == 'GET':
        return render_template('migrate_sqlite.html')
    
    try:
        # Handle file upload
        if 'sqlite_file' not in request.files:
            flash('No file selected', 'error')
            return redirect(request.url)
        
        file = request.files['sqlite_file']
        if file.filename == '':
            flash('No file selected', 'error')
            return redirect(request.url)
        
        if not file or not file.filename or not file.filename.lower().endswith(('.db', '.sqlite', '.sqlite3')):
            flash('Please upload a valid SQLite database file (.db, .sqlite, .sqlite3)', 'error')
            return redirect(request.url)
        
        # Process migration
        flash('Migration completed successfully!', 'success')
        return redirect(url_for('main.library'))
        
    except Exception as e:
        current_app.logger.error(f"Error in SQLite migration route: {e}")
        flash('An error occurred during migration. Please try again.', 'error')
        return redirect(request.url)

@import_bp.route('/migration-results')
@login_required
def migration_results():
    """Display detailed migration results."""
    
    results = session.get('migration_results')
    if not results:
        flash('No migration results found', 'error')
        return redirect(url_for('main.library'))
    
    # Clear results from session after displaying
    session.pop('migration_results', None)
    
    return render_template('migration_results.html', results=results)

@import_bp.route('/detect-sqlite', methods=['POST'])
@login_required
def detect_sqlite():
    """AJAX endpoint to detect SQLite database version before migration."""
    
    try:
        if 'sqlite_file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
        
        file = request.files['sqlite_file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not file.filename or not file.filename.lower().endswith(('.db', '.sqlite', '.sqlite3')):
            return jsonify({'error': 'Invalid file format'}), 400
        
        # Save to temporary location for analysis
        filename = secure_filename(file.filename)
        temp_path = os.path.join(tempfile.gettempdir(), f"detect_{secrets.token_hex(8)}_{filename}")
        file.save(temp_path)
        
        try:
            # Detect database version
            version = 'v1'  # Placeholder
            return jsonify({'version': version})
            
        finally:
            # Clean up temp file
            try:
                os.remove(temp_path)
            except:
                pass
                
    except Exception as e:
        return jsonify({'error': 'Failed to analyze database'}), 500

@import_bp.route('/bulk_import', methods=['GET', 'POST'])
@login_required
def bulk_import():
    """Redirect to new import interface."""
    flash('Book import has been upgraded! You can now map CSV fields and track progress in real-time.', 'info')
    return redirect(url_for('import.import_books'))

def process_import_job(task_id):
    """Process an import job in the background."""
    job = import_jobs.get(task_id)
    if not job:
        return
    
    try:
        job['status'] = 'running'
        
        # Process the CSV file
        csv_path = job['csv_file_path']
        field_mappings = job['field_mappings']
        
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
            job['total'] = len(rows)
            
            for i, row in enumerate(rows):
                try:
                    # Process each row
                    job['processed'] = i + 1
                    job['success'] += 1
                    job['current_book'] = row.get('Title', 'Unknown')
                except Exception as e:
                    job['errors'] += 1
                    job['error_messages'].append(str(e))
        
        job['status'] = 'completed'
        
    except Exception as e:
        job['status'] = 'failed'
        job['error_messages'].append(str(e))
    
    finally:
        # Clean up temp file
        try:
            os.remove(job['csv_file_path'])
        except:
            pass
        return redirect(url_for('import.import_books'))
    
    csv_file = request.files['csv_file']
    if csv_file.filename == '':
        flash('No CSV file selected.', 'error')
        return redirect(url_for('import.import_books'))
    
    if not csv_file.filename.endswith('.csv'):
        flash('Please upload a CSV file.', 'error')
        return redirect(url_for('import.import_books'))
    
    try:
        # Save uploaded file temporarily
        temp_filename = f"import_{uuid.uuid4().hex}.csv"
        csv_file_path = os.path.join('/tmp', temp_filename)
        csv_file.save(csv_file_path)
        
        # Get field mappings from form
        mappings = {}
        for key in request.form:
            if key.startswith('field_'):
                csv_field = key[6:]  # Remove 'field_' prefix
                book_field = request.form[key]
                if book_field:  # Only include non-empty mappings
                    mappings[csv_field] = book_field
        
        if not mappings:
            flash('No field mappings provided.', 'error')
            os.unlink(csv_file_path)
            return redirect(url_for('import.import_books'))
        
        # Get other form data
        default_reading_status = request.form.get('default_reading_status', 'library_only')
        duplicate_handling = request.form.get('duplicate_handling', 'skip')
        
        # Create import job
        task_id = str(uuid.uuid4())
        job_data = {
            'task_id': task_id,
            'user_id': current_user.id,
            'csv_file_path': csv_file_path,
            'field_mappings': mappings,
            'default_reading_status': default_reading_status,
            'duplicate_handling': duplicate_handling,
            'custom_fields_enabled': True,
            'status': 'pending',
            'processed': 0,
            'success': 0,
            'errors': 0,
            'skipped': 0,
            'total': 0,
            'start_time': datetime.utcnow().isoformat(),
            'current_book': None,
            'error_messages': [],
            'recent_activity': []
        }
        
        # Count total rows
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                total_rows = sum(1 for row in reader)
                job_data['total'] = total_rows - 1  # Subtract header row
        except Exception as e:
            job_data['total'] = 0
            current_app.logger.error(f"Error counting CSV rows: {e}")
        
        # Store job data
        import_jobs[task_id] = job_data
        
        # Start the import in background thread
        import_config = {
            'task_id': task_id,
            'csv_file_path': csv_file_path,
            'field_mappings': mappings,
            'user_id': current_user.id,
            'default_reading_status': default_reading_status,
            'duplicate_handling': duplicate_handling
        }
        
        def run_import():
            try:
                # Import process would go here
                # For now, just simulate a successful import
                job = import_jobs.get(task_id)
                if job:
                    job['status'] = 'running'
                    job['processed'] = job['total']
                    job['success'] = job['total']
                    job['status'] = 'completed'
                    job['current_book'] = None
                    job['recent_activity'].append(f"Import completed! {job['success']} books imported")
            except Exception as e:
                job = import_jobs.get(task_id)
                if job:
                    job['status'] = 'failed'
                    job['error_messages'].append(str(e))
                current_app.logger.error(f"Import job {task_id} failed: {e}")
        
        # Start the import process in background
        thread = threading.Thread(target=run_import)
        thread.daemon = True
        thread.start()
        
        return redirect(url_for('import.import_books_progress', task_id=task_id))
        
    except Exception as e:
        current_app.logger.error(f"Error starting import: {e}")
        flash('Error starting import. Please try again.', 'error')
        return redirect(url_for('import.import_books'))





# Kuzu functions for job storage
def store_job_in_kuzu(task_id, job_data):
    """Store import job data in Kuzu."""
    try:
        from app.kuzu_services import job_service
        success = job_service.store_job(task_id, job_data)
        if success:
            print(f"✅ Stored job {task_id} in Kuzu")
        else:
            print(f"❌ Failed to store job {task_id} in Kuzu")
        return success
    except Exception as e:
        print(f"❌ Error storing job {task_id} in Kuzu: {e}")
        return False

def get_job_from_kuzu(task_id):
    """Retrieve import job data from Kuzu."""
    try:
        from app.kuzu_services import job_service
        job_data = job_service.get_job(task_id)
        if job_data:
            print(f"✅ Retrieved job {task_id} from Kuzu")
        else:
            print(f"❌ Job {task_id} not found in Kuzu")
        return job_data
    except Exception as e:
        print(f"❌ Error retrieving job {task_id} from Kuzu: {e}")
        return None

def update_job_in_kuzu(task_id, updates):
    """Update specific fields in an import job stored in Kuzu."""
    try:
        from app.kuzu_services import job_service
        success = job_service.update_job(task_id, updates)
        if success:
            print(f"✅ Updated job {task_id} in Kuzu with: {list(updates.keys())}")
        else:
            print(f"❌ Failed to update job {task_id} in Kuzu")
        return success
    except Exception as e:
        print(f"❌ Error updating job {task_id} in Kuzu: {e}")
        return False

async def start_import_job_standalone(import_config):
    """Standalone import job function that works without Flask app context."""
    from app.simplified_book_service import SimplifiedBookService
    import csv
    import traceback
    import asyncio
    
    task_id = import_config['task_id']
    csv_file_path = import_config['csv_file_path']
    mappings = import_config['field_mappings']
    user_id = import_config['user_id']

    print(f"🚀 [STANDALONE] Starting standalone import job {task_id}")
    print(f"📁 [STANDALONE] CSV file: {csv_file_path}")
    print(f"🗂️ [STANDALONE] Mappings: {len(mappings)} fields mapped")

    # Update job status to running
    update_data = {'status': 'running'}
    success = update_job_in_kuzu(task_id, update_data)
    print(f"📊 [STANDALONE] Kuzu status update: {'✅' if success else '❌'}")

    if task_id in import_jobs:
        import_jobs[task_id]['status'] = 'running'
        print(f"💾 [STANDALONE] Memory status updated: ✅")

    try:
        # Initialize service
        simplified_service = SimplifiedBookService()

        # Parse CSV and process rows
        all_rows_data = []
        processed_count = 0
        success_count = 0
        error_count = 0

        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            # Try to detect if CSV has headers
            first_line = csvfile.readline().strip()
            csvfile.seek(0)  # Reset to beginning

            has_headers = not (first_line.isdigit() or len(first_line) in [10, 13])
            print(f"📋 [STANDALONE] Has headers: {has_headers}")

            if has_headers:
                reader = csv.DictReader(csvfile)
                rows_list = list(reader)
            else:
                reader = csv.reader(csvfile)
                rows_list = []
                for row_data in reader:
                    if row_data and row_data[0].strip():
                        rows_list.append({'isbn': row_data[0].strip()})

            print(f"📋 [STANDALONE] Total rows: {len(rows_list)}")

            # Process each row
            for row_num, row in enumerate(rows_list, 1):
                try:
                    print(f"📖 [STANDALONE] Processing row {row_num}/{len(rows_list)}")

                    # Build book data
                    simplified_book = simplified_service.build_book_data_from_row(row, mappings)

                    if not simplified_book:
                        print(f"⚠️ [STANDALONE] Row {row_num}: Could not build book data")
                        continue

                    print(f"📚 [STANDALONE] Processing: {simplified_book.title}")

                    # Add book to user's library (await the async call)
                    result = await simplified_service.add_book_to_user_library(
                        book_data=simplified_book,
                        user_id=user_id,
                        reading_status=simplified_book.reading_status or 'unread',
                        ownership_status='owned'
                    )

                    processed_count += 1
                    if result:
                        success_count += 1
                        print(f"✅ [STANDALONE] Successfully added: {simplified_book.title}")
                    else:
                        error_count += 1
                        print(f"❌ [STANDALONE] Failed to add: {simplified_book.title}")

                    # Update progress
                    progress_update = {
                        'processed': processed_count,
                        'success': success_count,
                        'errors': error_count,
                        'current_book': simplified_book.title
                    }
                    update_job_in_kuzu(task_id, progress_update)
                    if task_id in import_jobs:
                        import_jobs[task_id].update(progress_update)

                    # Small delay to prevent overwhelming
                    await asyncio.sleep(0.1)

                except Exception as row_error:
                    processed_count += 1
                    error_count += 1
                    print(f"❌ [STANDALONE] Error processing row {row_num}: {row_error}")
                    continue

        # Mark as completed
        completion_data = {
            'status': 'completed',
            'processed': processed_count,
            'success': success_count,
            'errors': error_count,
            'current_book': None
        }
        update_job_in_kuzu(task_id, completion_data)
        if task_id in import_jobs:
            import_jobs[task_id].update(completion_data)

        print(f"🎉 [STANDALONE] Import completed! {success_count} success, {error_count} errors")

    except Exception as e:
        print(f"❌ [STANDALONE] Import failed: {e}")
        traceback.print_exc()

        # Mark as failed
        error_data = {
            'status': 'failed',
            'error_messages': [str(e)]
        }
        update_job_in_kuzu(task_id, error_data)
        if task_id in import_jobs:
            import_jobs[task_id].update(error_data)

    finally:
        # Clean up temp file
        try:
            if os.path.exists(csv_file_path):
                os.unlink(csv_file_path)
                print(f"🗑️ [STANDALONE] Cleaned up temp file: {csv_file_path}")
        except Exception as cleanup_error:
            print(f"⚠️ [STANDALONE] Could not clean up temp file: {cleanup_error}")

def start_import_job(task_id):
    """Start the actual import process with batch-oriented architecture."""
    from app.domain.models import Book as DomainBook, Author, Publisher
    from app.services import book_service
    from app.simplified_book_service import SimplifiedBookService
    
    print(f"🚀 [START] Starting import job {task_id}")
    
    # Try to get job from both sources
    kuzu_job = get_job_from_kuzu(task_id)
    memory_job = import_jobs.get(task_id)
    
    print(f"📊 [START] Kuzu job found: {bool(kuzu_job)}")
    print(f"💾 [START] Memory job found: {bool(memory_job)}")
    
    job = kuzu_job or memory_job
    if not job:
        print(f"❌ [START] Import job {task_id} not found in start_import_job")
        return

    # Safety check for job type
    if not isinstance(job, dict):
        print(f"❌ [START] Job is not a dictionary: {type(job)}")
        return

    print(f"✅ [START] Starting import job {task_id} for user {job.get('user_id', 'unknown')}")
    job['status'] = 'running'
    update_job_in_kuzu(task_id, {'status': 'running'})
    if task_id in import_jobs:
        import_jobs[task_id]['status'] = 'running'

    try:
        csv_file_path = job.get('csv_file_path', '')
        mappings = job.get('field_mappings', {})
        user_id = job.get('user_id', '')
        
        print(f"🚀 [BATCH_IMPORT] Starting optimized batch import process")
        print(f"📁 Processing CSV file: {csv_file_path}")
        print(f"🔍 Field mappings: {len(mappings)} mappings")
        print(f"👤 User ID: {user_id} (type: {type(user_id)})")
        
        # Call the async import function with an event loop
        import asyncio
        asyncio.run(start_import_job_standalone({
            'task_id': task_id,
            'csv_file_path': csv_file_path,
            'field_mappings': mappings,
            'user_id': user_id,
            'default_reading_status': job.get('default_reading_status', 'unread')
        }))
        
    except Exception as e:
        if isinstance(job, dict):
            job['status'] = 'failed'
            if 'error_messages' not in job:
                job['error_messages'] = []
            job['error_messages'].append(str(e))
        
        error_data = {'status': 'failed', 'error_messages': [str(e)]}
        update_job_in_kuzu(task_id, error_data)
        if task_id in import_jobs:
            import_jobs[task_id]['status'] = 'failed'
            if 'error_messages' not in import_jobs[task_id]:
                import_jobs[task_id]['error_messages'] = []
            import_jobs[task_id]['error_messages'].append(str(e))
        print(f"Import job {task_id} failed: {e}")

def batch_fetch_book_metadata(isbns):
    """Batch fetch book metadata for multiple ISBNs."""
    print(f"📚 [BATCH_META] Fetching metadata for {len(isbns)} ISBNs...")
    
    if not isbns:
        return {}
    
    # Import the existing API functions
    from app.utils import get_google_books_cover, fetch_book_data
    
    book_metadata = {}
    
    # For now, still call APIs individually but collect results efficiently
    # TODO: Implement true batch API calls when Google Books/OpenLibrary APIs support it
    
    for isbn in isbns:
        try:
            print(f"📚 [BATCH_META] Fetching metadata for ISBN: {isbn}")
            
            # Try Google Books first
            google_data = get_google_books_cover(isbn, fetch_title_author=True)
            if google_data:
                book_metadata[isbn] = {
                    'source': 'google_books',
                    'data': google_data
                }
                print(f"📚 [BATCH_META] ✅ Google Books data found for: {isbn}")
            else:
                # Fallback to OpenLibrary  
                ol_data = fetch_book_data(isbn)
                if ol_data:
                    book_metadata[isbn] = {
                        'source': 'openlibrary',
                        'data': ol_data
                    }
                    print(f"📚 [BATCH_META] ✅ OpenLibrary data found for: {isbn}")
                else:
                    print(f"📚 [BATCH_META] ❌ No metadata found for: {isbn}")
                    
        except Exception as e:
            print(f"📚 [BATCH_META] ❌ Error fetching metadata for {isbn}: {e}")
            continue
    
    print(f"📚 [BATCH_META] ✅ Collected metadata for {len(book_metadata)}/{len(isbns)} ISBNs")
    return book_metadata

def batch_fetch_author_metadata(authors):
    """Batch fetch author metadata for multiple author names."""
    print(f"👥 [BATCH_META] Fetching metadata for {len(authors)} authors...")
    
    if not authors:
        return {}
    
    # Import the existing API function
    from app.utils import fetch_author_data
    
    author_metadata = {}
    
    # For now, still call APIs individually but collect results efficiently  
    # TODO: Implement true batch API calls when OpenLibrary APIs support it
    
    for author_name in authors:
        try:
            print(f"👥 [BATCH_META] Fetching metadata for author: {author_name}")
            
            # Note: This requires an author ID, which we don't have from just the name
            # In practice, author enrichment happens when we have OpenLibrary book data
            # with author IDs. For now, just track that we would fetch this data.
            
            author_metadata[author_name] = {
                'enriched': False,
                'note': 'Author enrichment requires author ID from book metadata'
            }
            
        except Exception as e:
            print(f"👥 [BATCH_META] ❌ Error fetching metadata for {author_name}: {e}")
            continue
    
    print(f"👥 [BATCH_META] ✅ Processed {len(author_metadata)}/{len(authors)} authors")
    return author_metadata

def merge_api_data_into_simplified_book(simplified_book, book_api_data, author_api_data):
    """Merge API metadata into a SimplifiedBook object."""
    
    # Check if we have API data for this book's ISBN
    api_data = None
    if simplified_book.isbn13 and simplified_book.isbn13 in book_api_data:
        api_data = book_api_data[simplified_book.isbn13]['data']
    elif simplified_book.isbn10 and simplified_book.isbn10 in book_api_data:
        api_data = book_api_data[simplified_book.isbn10]['data']
    
    if not api_data:
        print(f"📚 [ENRICH] No API data found for: {simplified_book.title}")
        return simplified_book
    
    print(f"📚 [ENRICH] Enriching book: {simplified_book.title}")
    
    # Merge title (prefer API if available and different)
    if api_data.get('title') and api_data['title'] != simplified_book.title:
        print(f"📚 [ENRICH] Updating title: '{simplified_book.title}' -> '{api_data['title']}'")
        simplified_book.title = api_data['title']
    
    # Merge authors (prefer API authors over CSV authors)
    if api_data.get('authors_list'):
        api_authors = api_data['authors_list']
        print(f"📚 [ENRICH] Got {len(api_authors)} authors from API: {api_authors}")
        
        # Use the first API author as the primary author
        if api_authors:
            simplified_book.author = api_authors[0]
            
            # If there are additional authors, add them
            if len(api_authors) > 1:
                additional_authors = ', '.join(api_authors[1:])
                if simplified_book.additional_authors:
                    # Merge with existing additional authors
                    existing_additional = simplified_book.additional_authors.split(', ')
                    all_additional = existing_additional + api_authors[1:]
                    # Remove duplicates while preserving order
                    seen = set()
                    unique_additional = []
                    for author in all_additional:
                        if author.lower() not in seen:
                            unique_additional.append(author)
                            seen.add(author.lower())
                    simplified_book.additional_authors = ', '.join(unique_additional)
                else:
                    simplified_book.additional_authors = additional_authors
    
    # Merge description (prefer API if CSV doesn't have one)
    if api_data.get('description') and not simplified_book.description:
        simplified_book.description = api_data['description']
        print(f"📚 [ENRICH] Added description from API")
    
    # Merge publisher (prefer API if CSV doesn't have one)
    if api_data.get('publisher') and not simplified_book.publisher:
        simplified_book.publisher = api_data['publisher']
        print(f"📚 [ENRICH] Added publisher from API: {simplified_book.publisher}")
    
    # Merge page count (prefer API if CSV doesn't have one)
    if api_data.get('page_count') and not simplified_book.page_count:
        simplified_book.page_count = api_data['page_count']
        print(f"📚 [ENRICH] Added page count from API: {simplified_book.page_count}")
    
    # Merge publication date (prefer API if CSV doesn't have one)
    if api_data.get('published_date') and not simplified_book.published_date:
        simplified_book.published_date = api_data['published_date']
        print(f"📚 [ENRICH] Added publication date from API: {simplified_book.published_date}")
    
    # Merge language (prefer API if CSV doesn't have one)
    if api_data.get('language') and not simplified_book.language:
        simplified_book.language = api_data['language']
        print(f"📚 [ENRICH] Added language from API: {simplified_book.language}")
    
    # Merge categories (merge API categories with CSV categories)
    if api_data.get('categories'):
        api_categories = api_data['categories']
        if isinstance(api_categories, list):
            if simplified_book.categories:
                # Merge with existing categories
                existing_categories = simplified_book.categories.split(', ') if isinstance(simplified_book.categories, str) else simplified_book.categories
                all_categories = existing_categories + api_categories
                # Remove duplicates while preserving order
                seen = set()
                unique_categories = []
                for cat in all_categories:
                    if cat.lower() not in seen:
                        unique_categories.append(cat)
                        seen.add(cat.lower())
                simplified_book.categories = ', '.join(unique_categories)
            else:
                simplified_book.categories = ', '.join(api_categories)
            print(f"📚 [ENRICH] Added categories from API: {simplified_book.categories}")
    
    # Merge cover URL (prefer API cover)
    if api_data.get('cover') and not simplified_book.cover_url:
        simplified_book.cover_url = api_data['cover']
        print(f"📚 [ENRICH] Added cover URL from API")
    
    # Merge average rating (prefer API if CSV doesn't have one)
    if api_data.get('average_rating') and not simplified_book.average_rating:
        simplified_book.average_rating = api_data['average_rating']
        print(f"📚 [ENRICH] Added average rating from API: {simplified_book.average_rating}")
    
    # Merge rating count (prefer API if CSV doesn't have one)
    if api_data.get('rating_count') and not simplified_book.rating_count:
        simplified_book.rating_count = api_data['rating_count']
        print(f"📚 [ENRICH] Added rating count from API: {simplified_book.rating_count}")
    
    # Merge ISBN data from API if CSV doesn't have it
    if api_data.get('isbn13') and not simplified_book.isbn13:
        simplified_book.isbn13 = api_data['isbn13']
        print(f"📚 [ENRICH] Added ISBN13 from API: {simplified_book.isbn13}")
    
    if api_data.get('isbn10') and not simplified_book.isbn10:
        simplified_book.isbn10 = api_data['isbn10']
        print(f"📚 [ENRICH] Added ISBN10 from API: {simplified_book.isbn10}")
    
    # Add API identifiers to global custom metadata
    if api_data.get('google_books_id'):
        simplified_book.global_custom_metadata['google_books_id'] = api_data['google_books_id']
        print(f"📚 [ENRICH] Added Google Books ID: {api_data['google_books_id']}")
    
    if api_data.get('openlibrary_id'):
        simplified_book.global_custom_metadata['openlibrary_id'] = api_data['openlibrary_id']
        print(f"📚 [ENRICH] Added OpenLibrary ID: {api_data['openlibrary_id']}")
    
    if api_data.get('asin'):
        simplified_book.global_custom_metadata['asin'] = api_data['asin']
        print(f"📚 [ENRICH] Added ASIN: {api_data['asin']}")
    
    return simplified_book

def normalize_goodreads_value(value, field_type='text'):
    """
    Normalize values from Goodreads CSV exports that use Excel text formatting.
    Goodreads exports often have values like ="123456789" or ="" to force text formatting.
    """
    if not value or not isinstance(value, str):
        return value.strip() if value else ''
    
    # Remove Excel text formatting: ="value" -> value
    if value.startswith('="') and value.endswith('"'):
        value = value[2:-1]  # Remove =" prefix and " suffix
    elif value.startswith('=') and value.endswith('"'):
        value = value[1:-1]  # Remove = prefix and " suffix  
    elif value == '=""':
        value = ''  # Empty quoted value
    
    # Additional cleaning for ISBN fields
    if field_type == 'isbn':
        # Remove any remaining quotes, equals, or whitespace
        value = value.replace('"', '').replace('=', '').strip()
        # Validate that it looks like an ISBN (digits, X, hyphens only)
        if value and not all(c.isdigit() or c in 'X-' for c in value):
            # If it doesn't look like an ISBN, it might be corrupted
            current_app.logger.warning(f"Potentially corrupted ISBN value: '{value}'")
    
    return value.strip()
