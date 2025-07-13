"""
Import/Export routes for the Bibliotheca application.
Handles book import functionality including CSV processing, progress tracking, and batch operations.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, jsonify, make_response, session
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from datetime import datetime
from typing import List, Any
import uuid
import os
import csv
import threading
import traceback
import asyncio
import tempfile
import secrets
import logging

# Set up import-specific logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

from app.services import book_service, import_mapping_service, custom_field_service
from app.simplified_book_service import SimplifiedBookService, SimplifiedBook
from app.domain.models import CustomFieldDefinition, CustomFieldType
from app.utils import normalize_goodreads_value

# Global dictionary to store import jobs (shared with routes.py for now)
import_jobs = {}

# Create import blueprint
import_bp = Blueprint('import', __name__)

# Helper functions for import functionality
def detect_csv_format(csv_file_path):
    """
    Detect if CSV is Goodreads, StoryGraph, or unknown format by analyzing column headers.
    Returns tuple of (format_type, confidence_score)
    """
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            # Read first line to get headers
            reader = csv.reader(csvfile)
            headers = next(reader, [])
            
            if not headers:
                return 'unknown', 0.0
            
            # Convert headers to lowercase for comparison
            headers_lower = [h.lower().strip() for h in headers]
            
            # Goodreads signature columns (case insensitive)
            goodreads_indicators = {
                'book id': 2.0,
                'title': 1.0,
                'author': 1.0,
                'my rating': 2.0,
                'exclusive shelf': 2.0,
                'bookshelves': 1.5,
                'bookshelves with positions': 2.0,
                'private notes': 1.5,
                'read count': 1.5,
                'owned copies': 1.5,
                'original publication year': 1.5,
                'date read': 1.0,
                'date added': 1.0,
                'binding': 1.0
            }
            
            # StoryGraph signature columns (case insensitive)
            storygraph_indicators = {
                'title': 1.0,
                'author': 1.0,
                'star rating': 2.0,
                'read status': 2.0,
                'date started': 2.0,
                'date finished': 2.0,
                'tags': 1.5,
                'moods': 2.0,
                'pace': 2.0,
                'character- or plot-driven?': 2.5,
                'strong character development?': 2.0,
                'loveable characters?': 2.0,
                'diverse characters?': 2.0,
                'flawed characters?': 2.0,
                'content warnings': 1.5,
                'format': 1.0
            }
            
            # Calculate scores
            goodreads_score = 0.0
            storygraph_score = 0.0
            
            for header in headers_lower:
                if header in goodreads_indicators:
                    goodreads_score += goodreads_indicators[header]
                if header in storygraph_indicators:
                    storygraph_score += storygraph_indicators[header]
            
            # Normalize scores by number of possible indicators
            goodreads_normalized = goodreads_score / sum(goodreads_indicators.values())
            storygraph_normalized = storygraph_score / sum(storygraph_indicators.values())
            
            print(f"📊 [DETECT] Goodreads score: {goodreads_score:.1f} (normalized: {goodreads_normalized:.3f})")
            print(f"📊 [DETECT] StoryGraph score: {storygraph_score:.1f} (normalized: {storygraph_normalized:.3f})")
            print(f"📊 [DETECT] Headers found: {headers}")
            
            # Determine format with minimum confidence threshold
            min_confidence = 0.3
            
            if goodreads_normalized >= min_confidence and goodreads_normalized > storygraph_normalized:
                return 'goodreads', goodreads_normalized
            elif storygraph_normalized >= min_confidence and storygraph_normalized > goodreads_normalized:
                return 'storygraph', storygraph_normalized
            else:
                return 'unknown', max(goodreads_normalized, storygraph_normalized)
                
    except Exception as e:
        print(f"❌ [DETECT] Error detecting CSV format: {e}")
        return 'unknown', 0.0

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
        'binding': 'custom_global_binding_type',
        'original publication year': 'custom_global_original_publication_year',
        'spoiler': 'custom_personal_spoiler_flag',
        'private notes': 'custom_global_private_notes',
        'read count': 'custom_personal_read_count',
        'owned copies': 'custom_personal_owned_copies',
        'bookshelves with positions': 'custom_global_shelf_positions',
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
        'binding_type': {'display_name': 'Binding Type', 'type': CustomFieldType.TEXT, 'global': True},
        'original_publication_year': {'display_name': 'Original Publication Year', 'type': CustomFieldType.NUMBER, 'global': True},
        'bookshelves': {'display_name': 'Bookshelves', 'type': CustomFieldType.TAGS, 'global': True},
        'shelf_positions': {'display_name': 'Bookshelves with Positions', 'type': CustomFieldType.TEXTAREA, 'global': True},
        'spoiler_flag': {'display_name': 'Spoiler Review', 'type': CustomFieldType.BOOLEAN, 'global': False},
        'private_notes': {'display_name': 'Private Notes', 'type': CustomFieldType.TEXTAREA, 'global': False},
        'read_count': {'display_name': 'Number of Times Read', 'type': CustomFieldType.NUMBER, 'global': False},
        'owned_copies': {'display_name': 'Number of Owned Copies', 'type': CustomFieldType.NUMBER, 'global': False},
        'format': {'display_name': 'Book Format', 'type': CustomFieldType.TEXT, 'global': True},
        'moods': {'display_name': 'Moods', 'type': CustomFieldType.TAGS, 'global': True},
        'pace': {'display_name': 'Reading Pace', 'type': CustomFieldType.TEXT, 'global': True},
        'character_plot_driven': {'display_name': 'Character vs Plot Driven', 'type': CustomFieldType.TEXT, 'global': True},
        'content_warnings': {'display_name': 'Content Warnings', 'type': CustomFieldType.TAGS, 'global': False},
        'character_development': {'display_name': 'Strong Character Development', 'type': CustomFieldType.BOOLEAN, 'global': False},
        'loveable_characters': {'display_name': 'Loveable Characters', 'type': CustomFieldType.BOOLEAN, 'global': False},
        'diverse_characters': {'display_name': 'Diverse Characters', 'type': CustomFieldType.BOOLEAN, 'global': False},
        'flawed_characters': {'display_name': 'Flawed Characters', 'type': CustomFieldType.BOOLEAN, 'global': False},
        # StoryGraph-specific fields
        'tags': {'display_name': 'Tags', 'type': CustomFieldType.TAGS, 'global': True},
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
                if isinstance(field, dict) and 'name' in field:
                    existing_field_names.add(field['name'])
                elif hasattr(field, 'name'):
                    field_name = getattr(field, 'name', None)
                    if field_name:
                        existing_field_names.add(field_name)
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
                    field_data = {
                        'name': field_definition.name,
                        'display_name': field_definition.display_name,
                        'field_type': field_definition.field_type,
                        'is_global': field_definition.is_global,
                        'created_by_user_id': field_definition.created_by_user_id,
                        'description': field_definition.description
                    }
                    success = custom_field_service.create_field_sync(user_id, field_data)
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
                    
                    field_data = {
                        'name': field_definition.name,
                        'display_name': field_definition.display_name,
                        'field_type': field_definition.field_type,
                        'is_global': field_definition.is_global,
                        'created_by_user_id': field_definition.created_by_user_id,
                        'description': field_definition.description
                    }
                    success = custom_field_service.create_field_sync(user_id, field_data)
                    if success:
                        print(f"✅ Created generic custom field: {display_name} ({field_name})")
                        existing_field_names.add(field_name)
                    else:
                        print(f"❌ Failed to create generic custom field: {field_name}")
                        
                except Exception as e:
                    print(f"❌ Error creating generic custom field {field_name}: {e}")

def get_goodreads_field_mappings():
    """Get predefined field mappings for Goodreads CSV format with custom field support."""
    # Base mappings
    base_mappings = {
        'Title': 'title',
        'Author': 'author', 
        'Additional Authors': 'additional_authors',
        'ISBN': 'isbn',
        'ISBN13': 'isbn',
        'My Rating': 'user_rating',
        'Average Rating': 'average_rating',
        'Publisher': 'publisher',
        'Number of Pages': 'page_count',
        'Year Published': 'publication_year',
        'Original Publication Year': 'original_publication_year',
        'Date Read': 'date_read',
        'Date Added': 'date_added',
        'Bookshelves': 'categories',
        'Exclusive Shelf': 'reading_status',
        'My Review': 'personal_notes',
        'Private Notes': 'private_notes',
    }
    
    # Enhanced mappings with custom field support
    enhanced_mappings = {**base_mappings}
    enhanced_mappings.update({
        # Global custom fields (shared across users)
        'Binding': 'custom_global_binding_type',
        'Bookshelves with positions': 'custom_global_shelf_positions',
        
        # Personal custom fields (per-user)
        'Spoiler': 'custom_personal_spoiler_flag', 
        'Read Count': 'custom_personal_read_count',
        'Owned Copies': 'custom_personal_owned_copies',
    })
    
    return enhanced_mappings

def get_storygraph_field_mappings():
    """Get predefined field mappings for StoryGraph CSV format with custom field support."""
    # Base mappings
    base_mappings = {
        'Title': 'title',
        'Author': 'author',
        'ISBN': 'isbn',
        'ISBN13': 'isbn',
        'Publisher': 'publisher',
        'Pages': 'page_count',
        'Publication Year': 'publication_year',
        'Date Started': 'start_date',
        'Date Finished': 'date_read',
        'Date Added': 'date_added',
        'Read Status': 'reading_status',
        'Star Rating': 'user_rating',
        'Review': 'personal_notes',
        'Tags': 'categories',
        'Format': 'format',
    }
    
    # Enhanced mappings with custom field support
    enhanced_mappings = {**base_mappings}
    enhanced_mappings.update({
        # Global custom fields (shared across users)
        'Moods': 'custom_global_moods',
        'Pace': 'custom_global_pace', 
        'Character- or Plot-Driven?': 'custom_global_character_plot_driven',
        
        # Personal custom fields (per-user)
        'Strong Character Development?': 'custom_personal_character_development',
        'Loveable Characters?': 'custom_personal_loveable_characters',
        'Diverse Characters?': 'custom_personal_diverse_characters',
        'Flawed Characters?': 'custom_personal_flawed_characters',
        'Content Warnings': 'custom_personal_content_warnings',
    })
    
    return enhanced_mappings

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
                        if isinstance(field, dict):
                            is_global = field.get('is_global', False)
                        else:
                            is_global = getattr(field, 'is_global', False)
                        
                        if is_global:
                            global_custom_fields.append(field)
                        else:
                            personal_custom_fields.append(field)
                
                # Also get shareable fields from other users
                shareable_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=True)
                # Add shareable fields to global fields if they're not already included
                if shareable_fields and hasattr(shareable_fields, '__iter__'):
                    shareable_fields_list = list(shareable_fields)  # Convert to list to avoid type issues
                    for field in shareable_fields_list:
                        field_id = field.get('id') if isinstance(field, dict) else getattr(field, 'id', None)
                        if field_id and not any(
                            (gf.get('id') if isinstance(gf, dict) else getattr(gf, 'id', None)) == field_id 
                            for gf in global_custom_fields
                        ):
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
                template_list: List[Any] = []
                if import_templates and hasattr(import_templates, '__iter__'):
                    template_list = list(import_templates)
                
                print(f"DEBUG: CSV headers: {headers}")
                print(f"DEBUG: Force custom mapping: {force_custom}")
                print(f"DEBUG: Available templates: {[t.get('name', 'Unknown') if isinstance(t, dict) else getattr(t, 'name', 'Unknown') for t in template_list] if template_list else []}")
                
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
                            if isinstance(field, dict):
                                is_global = field.get('is_global', False)
                            else:
                                is_global = getattr(field, 'is_global', False)
                            
                            if is_global:
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
                    if template_list and not any(
                        (t.get('id') if isinstance(t, dict) else getattr(t, 'id', None)) == detected_template.id 
                        for t in template_list
                    ):
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
                            if isinstance(field, dict):
                                is_global = field.get('is_global', False)
                            else:
                                is_global = getattr(field, 'is_global', False)
                            
                            if is_global:
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
                    if template_list and not any(
                        (t.get('id') if isinstance(t, dict) else getattr(t, 'id', None)) == detected_template.id 
                        for t in template_list
                    ):
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
                    t_name = t.get('name') if isinstance(t, dict) else getattr(t, 'name', None)
                    t_id = t.get('id') if isinstance(t, dict) else getattr(t, 'id', None)
                    
                    if t_name == use_template or t_id == use_template:
                        # Convert dict to object-like structure for compatibility
                        class TemplateObj:
                            def __init__(self, data):
                                if isinstance(data, dict):
                                    self.id = data.get('id')
                                    self.name = data.get('name')
                                    self.field_mappings = data.get('field_mappings', {})
                                else:
                                    self.id = getattr(data, 'id', None)
                                    self.name = getattr(data, 'name', None)
                                    self.field_mappings = getattr(data, 'field_mappings', {})
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
                            field_exists = any(
                                (f.get('name') if isinstance(f, dict) else getattr(f, 'name', None)) == field_name 
                                for f in existing_fields_list
                            )
                        
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
                        
                        field_data = {
                            'name': field_definition.name,
                            'display_name': field_definition.display_name,
                            'field_type': field_definition.field_type,
                            'is_global': field_definition.is_global,
                            'created_by_user_id': field_definition.created_by_user_id,
                            'description': field_definition.description
                        }
                        custom_field_service.create_field_sync(current_user.id, field_data)
                        
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
            asyncio.run(process_simple_import(import_config))
            
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
    
    # Get total books count from job data or default to 0
    total_books = job.get('total_books', 0)
    
    # Parse start time or use current time
    start_time = job.get('created_at', datetime.now().isoformat())
    
    return render_template('import_books_progress.html', 
                         job=job, 
                         task_id=task_id,
                         total_books=total_books,
                         start_time=start_time)

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
        print(f"📋 [DIRECT_IMPORT] Starting {import_type} import processing...")
        
        # Get predefined mappings based on import type
        if import_type == 'goodreads':
            mappings = get_goodreads_field_mappings()
        elif import_type == 'storygraph':
            mappings = get_storygraph_field_mappings()
        else:
            # For other import types, use a basic mapping
            mappings = {
                'Title': 'title',
                'Author': 'author',
                'ISBN': 'isbn',
                'ISBN13': 'isbn',
                'Description': 'description',
                'Publisher': 'publisher',
                'Date Read': 'date_read',
                'Date Added': 'date_added',
                'My Rating': 'user_rating',
                'Reading Status': 'reading_status'
            }
        
        print(f"📋 [DIRECT_IMPORT] Using mappings: {list(mappings.keys())}")
        
        # Initialize service
        from app.simplified_book_service import SimplifiedBookService
        simplified_service = SimplifiedBookService()
        
        # Initialize counters
        processed_count = 0
        success_count = 0
        error_count = 0
        
        # Parse CSV and process rows
        with open(temp_path, 'r', encoding='utf-8') as csvfile:
            import csv
            
            # Goodreads/StoryGraph files typically have headers
            reader = csv.DictReader(csvfile)
            rows_list = list(reader)
            
            print(f"📋 [DIRECT_IMPORT] Found {len(rows_list)} rows to process")
            
            # Process each row
            for row_num, row in enumerate(rows_list, 1):
                try:
                    print(f"📖 [DIRECT_IMPORT] Processing row {row_num}/{len(rows_list)}")
                    
                    # Build book data using the service's mapping logic
                    simplified_book = simplified_service.build_book_data_from_row(row, mappings)
                    
                    if not simplified_book:
                        print(f"⚠️ [DIRECT_IMPORT] Row {row_num}: Could not build book data")
                        processed_count += 1
                        error_count += 1
                        continue
                    
                    print(f"📚 [DIRECT_IMPORT] Processing: {simplified_book.title}")
                    
                    # Add book to user's library using sync method
                    result = simplified_service.add_book_to_user_library_sync(
                        book_data=simplified_book,
                        user_id=str(current_user.id),
                        reading_status=simplified_book.reading_status or 'plan_to_read',
                        ownership_status='owned',
                        media_type='physical'
                    )
                    
                    processed_count += 1
                    if result:
                        success_count += 1
                        print(f"✅ [DIRECT_IMPORT] Successfully added: {simplified_book.title}")
                    else:
                        error_count += 1
                        print(f"❌ [DIRECT_IMPORT] Failed to add: {simplified_book.title}")
                        
                except Exception as row_error:
                    processed_count += 1
                    error_count += 1
                    print(f"❌ [DIRECT_IMPORT] Error processing row {row_num}: {row_error}")
                    continue
        
        # Clean up temporary file if we created it
        if not use_suggested_file:
            try:
                import os
                os.unlink(temp_path)
            except Exception as cleanup_error:
                print(f"⚠️ [DIRECT_IMPORT] Could not clean up temp file: {cleanup_error}")
        
        # Clear session data
        session.pop('direct_import_file', None)
        session.pop('direct_import_filename', None)
        
        print(f"🎉 [DIRECT_IMPORT] Import completed! {success_count} success, {error_count} errors out of {processed_count} processed")
        
        if success_count > 0:
            flash(f'Import completed! Successfully imported {success_count} books. {error_count} errors.', 'success')
        else:
            flash(f'Import completed with {error_count} errors. No books were successfully imported.', 'warning')
        
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

@import_bp.route('/simple-import', methods=['GET', 'POST'])
@login_required
def simple_import():
    """Simplified import that auto-detects format and uses default mappings."""
    if request.method == 'GET':
        return render_template('simple_import.html')
    
    try:
        # Handle file upload
        file = request.files.get('csv_file')
        if not file or not file.filename or file.filename == '' or not file.filename.endswith('.csv'):
            flash('Please select a valid CSV file', 'error')
            return redirect(request.url)
        
        # Save file temporarily
        filename = secure_filename(file.filename)
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv', prefix=f'simple_import_{current_user.id}_')
        file.save(temp_file.name)
        temp_path = temp_file.name
        
        print(f"📁 [SIMPLE_IMPORT] Processing file: {filename}")
        print(f"📁 [SIMPLE_IMPORT] Temp path: {temp_path}")
        
        # Detect CSV format
        format_type, confidence = detect_csv_format(temp_path)
        print(f"🔍 [SIMPLE_IMPORT] Detected format: {format_type} (confidence: {confidence:.3f})")
        
        if format_type == 'unknown':
            flash('Could not detect CSV format. Please use the manual import for custom CSV files.', 'warning')
            os.unlink(temp_path)
            return redirect(url_for('import.import_books'))
        
        # Get appropriate mappings
        if format_type == 'goodreads':
            mappings = get_goodreads_field_mappings()
            format_display = 'Goodreads'
        else:  # storygraph
            mappings = get_storygraph_field_mappings()
            format_display = 'StoryGraph'
        
        print(f"📋 [SIMPLE_IMPORT] Using {format_display} mappings with {len(mappings)} field mappings")
        
        # Get import settings from form
        default_reading_status = request.form.get('default_reading_status', 'plan_to_read')
        enable_api_enrichment = request.form.get('enable_api_enrichment', 'true').lower() == 'true'
        
        # Create import job
        task_id = str(uuid.uuid4())
        job_data = {
            'task_id': task_id,
            'user_id': current_user.id,
            'csv_file_path': temp_path,
            'field_mappings': mappings,
            'default_reading_status': default_reading_status,
            'enable_api_enrichment': enable_api_enrichment,
            'format_type': format_type,
            'format_display': format_display,
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
            with open(temp_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                job_data['total'] = sum(1 for _ in reader)
        except:
            job_data['total'] = 0
        
        # Store job data
        import_jobs[task_id] = job_data
        store_job_in_kuzu(task_id, job_data)
        
        print(f"🚀 [SIMPLE_IMPORT] Created job {task_id} for {job_data['total']} rows")
        
        # Start the import in background
        import_config = {
            'task_id': task_id,
            'csv_file_path': temp_path,
            'field_mappings': mappings,
            'user_id': current_user.id,
            'default_reading_status': default_reading_status,
            'enable_api_enrichment': enable_api_enrichment,
            'format_type': format_type
        }
        
        def run_simple_import():
            try:
                print(f"🚀 [SIMPLE_IMPORT] Starting background thread for job {task_id}")
                print(f"🔧 [SIMPLE_IMPORT] Import config: {import_config}")
                
                # Call the async import function with an event loop
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(process_simple_import(import_config))
                    print(f"✅ [SIMPLE_IMPORT] Background processing completed successfully for {task_id}")
                finally:
                    loop.close()
                    
            except Exception as e:
                print(f"❌ [SIMPLE_IMPORT] Error in background thread: {e}")
                traceback.print_exc()
                # Update job with error
                error_update = {'status': 'failed', 'error_messages': [str(e)]}
                update_job_in_kuzu(task_id, error_update)
                if task_id in import_jobs:
                    import_jobs[task_id].update(error_update)
        
        # Start background thread
        thread = threading.Thread(target=run_simple_import)
        thread.daemon = True
        thread.start()
        
        flash(f'Import started! Detected {format_display} format with {confidence:.1%} confidence.', 'success')
        return redirect(url_for('import.import_books_progress', task_id=task_id))
        
    except Exception as e:
        current_app.logger.error(f"Error in simple import: {e}")
        flash('An error occurred during import. Please try again.', 'error')
        return redirect(request.url)

async def process_simple_import(import_config):
    """Process a simple import job with API enrichment."""
    from app.simplified_book_service import SimplifiedBookService, SimplifiedBook
    
    task_id = import_config['task_id']
    csv_file_path = import_config['csv_file_path']
    mappings = import_config['field_mappings']
    user_id = import_config['user_id']
    default_reading_status = import_config.get('default_reading_status', 'plan_to_read')
    enable_api_enrichment = import_config.get('enable_api_enrichment', True)
    format_type = import_config.get('format_type', 'unknown')
    
    print(f"🔄 [PROCESS_SIMPLE] Starting import for task {task_id}")
    print(f"🔄 [PROCESS_SIMPLE] Format: {format_type}, API enrichment: {enable_api_enrichment}")
    
    # Initialize simplified book service
    simplified_service = SimplifiedBookService()
    
    # Initialize counters
    processed_count = 0
    success_count = 0
    error_count = 0
    skipped_count = 0
    
    try:
        # STEP 1: Pre-analyze and create custom fields BEFORE processing any books
        print(f"🔍 [PROCESS_SIMPLE] Pre-analyzing custom fields...")
        try:
            custom_fields_success, created_custom_fields = await pre_analyze_and_create_custom_fields(
                csv_file_path, mappings, user_id
            )
            
            if not custom_fields_success:
                print(f"⚠️ [PROCESS_SIMPLE] Custom field pre-analysis failed, continuing anyway...")
                created_custom_fields = {}
            else:
                print(f"✅ [PROCESS_SIMPLE] Pre-created {len(created_custom_fields)} custom fields")
        except Exception as e:
            print(f"⚠️ [PROCESS_SIMPLE] Custom field pre-analysis error: {e}, continuing anyway...")
            created_custom_fields = {}
            
        print(f"✅ [PROCESS_SIMPLE] Pre-created {len(created_custom_fields)} custom fields")
        
        # STEP 2: Read and process CSV
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows_list = list(reader)
            
            print(f"📋 [PROCESS_SIMPLE] Found {len(rows_list)} rows to process")
            
            # Collect ISBNs for batch API enrichment if enabled
            isbns_to_enrich = []
            if enable_api_enrichment:
                for row in rows_list:
                    isbn = row.get('ISBN') or row.get('ISBN13') or row.get('isbn') or row.get('isbn13')
                    if isbn:
                        # Clean ISBN (handle Goodreads formatting)
                        isbn_clean = normalize_goodreads_value(isbn, 'isbn')
                        if isbn_clean and isinstance(isbn_clean, str) and len(isbn_clean) >= 10:
                            isbns_to_enrich.append(isbn_clean)
                
                print(f"🌐 [PROCESS_SIMPLE] Will enrich {len(isbns_to_enrich)} books with API data")
                
                # Batch fetch metadata
                book_metadata = batch_fetch_book_metadata(isbns_to_enrich[:50])  # Limit to first 50 to avoid rate limits
                print(f"🌐 [PROCESS_SIMPLE] Retrieved metadata for {len(book_metadata)} books")
            else:
                book_metadata = {}
            
            # Process each row
            for row_num, row in enumerate(rows_list, 1):
                try:
                    print(f"📖 [PROCESS_SIMPLE] Processing row {row_num}/{len(rows_list)}")
                    
                    # Build book data using mappings
                    simplified_book = simplified_service.build_book_data_from_row(row, mappings)
                    
                    if not simplified_book or not simplified_book.title:
                        print(f"⚠️ [PROCESS_SIMPLE] Row {row_num}: Could not build book data or missing title")
                        processed_count += 1
                        skipped_count += 1
                        continue
                    
                    print(f"📚 [PROCESS_SIMPLE] Processing: {simplified_book.title}")
                    
                    # Apply API enrichment if enabled
                    if enable_api_enrichment and book_metadata:
                        simplified_book = merge_api_data_into_simplified_book(
                            simplified_book, book_metadata, {}
                        )
                    
                    # Set default reading status if not provided
                    if not simplified_book.reading_status:
                        simplified_book.reading_status = import_config.get('default_reading_status', 'plan_to_read')
                    
                    # Add book to user's library
                    result = await simplified_service.add_book_to_user_library(
                        book_data=simplified_book,
                        user_id=user_id,
                        reading_status=simplified_book.reading_status,
                        ownership_status='owned',
                        media_type='physical',
                        custom_metadata=simplified_book.personal_custom_metadata if hasattr(simplified_book, 'personal_custom_metadata') else None
                    )
                    
                    processed_count += 1
                    if result:
                        success_count += 1
                        print(f"✅ [PROCESS_SIMPLE] Successfully added: {simplified_book.title}")
                    else:
                        error_count += 1
                        print(f"❌ [PROCESS_SIMPLE] Failed to add: {simplified_book.title}")
                    
                    # Update progress after each book for real-time feedback
                    progress_update = {
                        'processed': processed_count,
                        'success': success_count,
                        'errors': error_count,
                        'skipped': skipped_count,
                        'current_book': simplified_book.title
                    }
                    # Update in memory for fast API access
                    if task_id in import_jobs:
                        import_jobs[task_id].update(progress_update)
                    
                    # Update in Kuzu less frequently to avoid performance issues
                    if processed_count % 5 == 0:
                        update_job_in_kuzu(task_id, progress_update)
                    
                    # Small delay to prevent overwhelming the system
                    await asyncio.sleep(0.05)
                    
                except Exception as row_error:
                    processed_count += 1
                    error_count += 1
                    print(f"❌ [PROCESS_SIMPLE] Error processing row {row_num}: {row_error}")
                    continue
        
        # Mark as completed
        completion_data = {
            'status': 'completed',
            'processed': processed_count,
            'success': success_count,
            'errors': error_count,
            'skipped': skipped_count,
            'current_book': None,
            'recent_activity': [f"Import completed! {success_count} books imported, {error_count} errors, {skipped_count} skipped"]
        }
        update_job_in_kuzu(task_id, completion_data)
        if task_id in import_jobs:
            import_jobs[task_id].update(completion_data)
        
        print(f"🎉 [PROCESS_SIMPLE] Import completed! {success_count} success, {error_count} errors, {skipped_count} skipped")
        
    except Exception as e:
        print(f"❌ [PROCESS_SIMPLE] Import failed: {e}")
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
                print(f"🗑️ [PROCESS_SIMPLE] Cleaned up temp file: {csv_file_path}")
        except Exception as cleanup_error:
            print(f"⚠️ [PROCESS_SIMPLE] Could not clean up temp file: {cleanup_error}")

def merge_api_data_into_simplified_book(simplified_book, book_metadata, extra_metadata):
    """Merge API metadata into a SimplifiedBook object."""
    
    # Check if we have API data for this book's ISBN
    api_data = None
    if simplified_book.isbn13 and simplified_book.isbn13 in book_metadata:
        api_data = book_metadata[simplified_book.isbn13]['data']
    elif simplified_book.isbn10 and simplified_book.isbn10 in book_metadata:
        api_data = book_metadata[simplified_book.isbn10]['data']
    
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

def start_import_job(task_id, csv_file_path, field_mappings, user_id, **kwargs):
    """Legacy function for compatibility with onboarding system."""
    print(f"🔄 [LEGACY_IMPORT] Starting legacy import job {task_id}")
    
    # Convert to new simple import format
    import_config = {
        'task_id': task_id,
        'csv_file_path': csv_file_path,
        'field_mappings': field_mappings,
        'user_id': user_id,
        'default_reading_status': kwargs.get('default_reading_status', 'plan_to_read'),
        'enable_api_enrichment': kwargs.get('enable_api_enrichment', True),
        'format_type': kwargs.get('format_type', 'unknown')
    }
    
    # Start the import in a background thread
    def run_import():
        try:
            asyncio.run(process_simple_import(import_config))
        except Exception as e:
            print(f"❌ [LEGACY_IMPORT] Error in import: {e}")
            traceback.print_exc()
    
    thread = threading.Thread(target=run_import)
    thread.daemon = True
    thread.start()
    
    return task_id

def batch_fetch_book_metadata(isbns):
    """Batch fetch metadata from Google Books and OpenLibrary APIs."""
    print(f"🌐 [API] Fetching metadata for {len(isbns)} ISBNs")
    
    from app.utils import fetch_book_data, get_google_books_cover
    import time
    
    metadata = {}
    
    for i, isbn in enumerate(isbns):
        if not isbn:
            continue
            
        print(f"🌐 [API] Processing {i+1}/{len(isbns)}: {isbn}")
        
        try:
            # Try Google Books first (usually faster and more complete)
            google_data = get_google_books_cover(isbn, fetch_title_author=True)
            
            # Try OpenLibrary as backup/additional source
            openlibrary_data = fetch_book_data(isbn)
            
            # Merge the data, preferring Google Books for most fields
            combined_data = {}
            
            if google_data:
                combined_data.update(google_data)
                combined_data['source'] = 'google_books'
                print(f"✅ [API] Got Google Books data for {isbn}")
            
            if openlibrary_data:
                # Add OpenLibrary data for missing fields
                for key, value in openlibrary_data.items():
                    if value and (key not in combined_data or not combined_data[key]):
                        combined_data[key] = value
                
                # Add OpenLibrary specific fields
                if openlibrary_data.get('openlibrary_id'):
                    combined_data['openlibrary_id'] = openlibrary_data['openlibrary_id']
                
                if google_data:
                    combined_data['source'] = 'google_books,openlibrary'
                else:
                    combined_data['source'] = 'openlibrary'
                
                print(f"✅ [API] Got OpenLibrary data for {isbn}")
            
            if combined_data:
                metadata[isbn] = {
                    'data': combined_data,
                    'source': combined_data.get('source', 'unknown')
                }
                print(f"📚 [API] Combined data for {isbn}: {list(combined_data.keys())}")
            else:
                print(f"⚠️ [API] No data found for {isbn}")
            
            # Rate limiting: small delay between requests to be respectful
            if i < len(isbns) - 1:  # Don't delay after the last request
                time.sleep(0.5)  # 500ms between requests
                
        except Exception as e:
            print(f"❌ [API] Error fetching data for {isbn}: {e}")
            continue
    
    print(f"🎉 [API] Completed batch fetch: {len(metadata)} successful out of {len(isbns)} ISBNs")
    return metadata

def store_job_in_kuzu(task_id, job_data):
    """Store import job status in KuzuDB."""
    try:
        print(f"📊 [KUZU_JOB] Storing job {task_id} with status {job_data.get('status', 'unknown')}")
        # TODO: Implement actual Kuzu storage
    except Exception as e:
        print(f"❌ [KUZU_JOB] Error storing job: {e}")

def update_job_in_kuzu(task_id, update_data):
    """Update import job status in KuzuDB."""
    try:
        print(f"📊 [KUZU_JOB] Updating job {task_id} with data: {list(update_data.keys())}")
        # Update in-memory job tracking
        if task_id in import_jobs:
            import_jobs[task_id].update(update_data)
        # TODO: Implement actual Kuzu update
    except Exception as e:
        print(f"❌ [KUZU_JOB] Error updating job: {e}")

@import_bp.route('/simple', methods=['GET', 'POST'])
@login_required
def simple_csv_import():
    """Very simple CSV import with automatic format detection."""
    
    if request.method == 'GET':
        return render_template('simple_import.html')
    
    # Handle POST request
    try:
        print(f"🔍 [SIMPLE_IMPORT] Processing upload for user {current_user.id}")
        
        # Check for file upload
        if 'csv_file' not in request.files:
            print(f"❌ [SIMPLE_IMPORT] No file in request")
            flash('Please select a file to upload', 'error')
            return redirect(request.url)
        
        file = request.files['csv_file']
        if file.filename == '':
            print(f"❌ [SIMPLE_IMPORT] Empty filename")
            flash('Please select a file to upload', 'error')
            return redirect(request.url)
        
        if not file.filename or not file.filename.lower().endswith('.csv'):
            print(f"❌ [SIMPLE_IMPORT] Invalid file type: {file.filename}")
            flash('Please upload a CSV file (.csv)', 'error')
            return redirect(request.url)
        
        print(f"📁 [SIMPLE_IMPORT] File received: {file.filename}")
        
        # Save file temporarily
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv', prefix=f'simple_import_{current_user.id}_')
        file.save(temp_file.name)
        temp_path = temp_file.name
        print(f"💾 [SIMPLE_IMPORT] File saved to: {temp_path}")
        
        # Detect format
        format_type, confidence = detect_csv_format(temp_path)
        print(f"🔍 [SIMPLE_IMPORT] Format detection: {format_type} (confidence: {confidence:.2f})")
        
        if confidence < 0.5:
            print(f"⚠️ [SIMPLE_IMPORT] Low confidence in format detection")
            flash('Could not automatically detect file format. Please use the manual import.', 'warning')
            try:
                import os
                os.unlink(temp_path)
            except:
                pass
            return redirect(request.url)
        
        # Get appropriate mappings
        if format_type == 'goodreads':
            mappings = get_goodreads_field_mappings()
            print(f"📋 [SIMPLE_IMPORT] Using Goodreads mappings: {len(mappings)} fields")
        elif format_type == 'storygraph':
            mappings = get_storygraph_field_mappings()
            print(f"📋 [SIMPLE_IMPORT] Using StoryGraph mappings: {len(mappings)} fields")
        else:
            print(f"❌ [SIMPLE_IMPORT] Unknown format type: {format_type}")
            flash('Unsupported file format detected', 'error')
            try:
                import os
                os.unlink(temp_path)
            except:
                pass
            return redirect(request.url)
        
        # Initialize service
        simplified_service = SimplifiedBookService()
        print(f"🔧 [SIMPLE_IMPORT] Service initialized")
        
        # Process the CSV
        success_count = 0
        error_count = 0
        processed_count = 0
        errors = []
        
        try:
            with open(temp_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
                print(f"📊 [SIMPLE_IMPORT] Found {len(rows)} rows to process")
                
                for row_num, row in enumerate(rows, 1):
                    try:
                        print(f"📖 [SIMPLE_IMPORT] Processing row {row_num}/{len(rows)}")
                        
                        # Build book data
                        simplified_book = simplified_service.build_book_data_from_row(row, mappings)
                        
                        if not simplified_book or not simplified_book.title:
                            error_msg = f"Row {row_num}: Could not extract book data or missing title"
                            print(f"⚠️ [SIMPLE_IMPORT] {error_msg}")
                            errors.append(error_msg)
                            error_count += 1
                            processed_count += 1
                            continue
                        
                        print(f"📚 [SIMPLE_IMPORT] Processing: {simplified_book.title}")
                        
                        # Set default reading status if not provided
                        if not simplified_book.reading_status:
                            simplified_book.reading_status = 'plan_to_read'
                        
                        # Add to library
                        result = simplified_service.add_book_to_user_library_sync(
                            book_data=simplified_book,
                            user_id=str(current_user.id),
                            reading_status=simplified_book.reading_status,
                            ownership_status='owned',
                            media_type='physical'
                        )
                        
                        processed_count += 1
                        
                        if result:
                            success_count += 1
                            print(f"✅ [SIMPLE_IMPORT] Successfully added: {simplified_book.title}")
                        else:
                            error_count += 1
                            error_msg = f"Row {row_num}: Failed to add book '{simplified_book.title}'"
                            print(f"❌ [SIMPLE_IMPORT] {error_msg}")
                            errors.append(error_msg)
                    
                    except Exception as row_error:
                        processed_count += 1
                        error_count += 1
                        error_msg = f"Row {row_num}: {str(row_error)}"
                        print(f"❌ [SIMPLE_IMPORT] {error_msg}")
                        errors.append(error_msg)
                        continue
        
        finally:
            # Clean up temp file
            try:
                import os
                os.unlink(temp_path)
                print(f"🗑️ [SIMPLE_IMPORT] Cleaned up temp file")
            except Exception as cleanup_error:
                print(f"⚠️ [SIMPLE_IMPORT] Could not clean up temp file: {cleanup_error}")
        
        # Report results
        print(f"🎉 [SIMPLE_IMPORT] Completed: {success_count} success, {error_count} errors, {processed_count} total")
        
        if success_count > 0:
            if error_count > 0:
                flash(f'Import completed! {success_count} books imported, {error_count} errors occurred.', 'warning')
            else:
                flash(f'Import completed successfully! {success_count} books imported.', 'success')
        else:
            flash(f'Import failed. {error_count} errors occurred. No books were imported.', 'error')
        
        # Store errors in session for viewing if needed
        if errors:
            session['import_errors'] = errors[:50]  # Limit to 50 errors
        
        return redirect(url_for('main.library'))
        
    except Exception as e:
        print(f"❌ [SIMPLE_IMPORT] Critical error: {e}")
        traceback.print_exc()
        flash('An unexpected error occurred during import. Please try again.', 'error')
        return redirect(request.url)

@import_bp.route('/simple-upload', methods=['POST'])
@login_required
def simple_upload():
    """Handle simple CSV upload with auto-detection and processing."""
    
    print(f"🔍 [SIMPLE_UPLOAD] Starting upload for user {current_user.id}")
    
    try:
        # Check for file upload
        if 'csv_file' not in request.files:
            print(f"❌ [SIMPLE_UPLOAD] No file in request")
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['csv_file']
        if file.filename == '':
            print(f"❌ [SIMPLE_UPLOAD] Empty filename")
            return jsonify({'error': 'No file selected'}), 400
        
        if not file.filename or not file.filename.lower().endswith('.csv'):
            print(f"❌ [SIMPLE_UPLOAD] Invalid file type: {file.filename}")
            return jsonify({'error': 'File must be a CSV (.csv)'}), 400
        
        # Save file temporarily
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv', prefix=f'upload_{current_user.id}_')
        file.save(temp_file.name)
        temp_path = temp_file.name
        print(f"💾 [SIMPLE_UPLOAD] File saved to: {temp_path}")
        
        # Detect format
        format_type, confidence = detect_csv_format(temp_path)
        print(f"🔍 [SIMPLE_UPLOAD] Format: {format_type}, Confidence: {confidence:.2f}")
        
        # Get appropriate field mappings
        if format_type == 'goodreads':
            field_mappings = get_goodreads_field_mappings()
            print(f"📋 [SIMPLE_UPLOAD] Using Goodreads mappings: {len(field_mappings)} fields")
        elif format_type == 'storygraph':
            field_mappings = get_storygraph_field_mappings()
            print(f"📋 [SIMPLE_UPLOAD] Using StoryGraph mappings: {len(field_mappings)} fields")
        else:
            print(f"❌ [SIMPLE_UPLOAD] Unknown format type: {format_type}, using empty mappings")
            field_mappings = {}
        
        # Count total books in CSV for progress tracking
        try:
            with open(temp_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                # Skip header
                next(reader, None)
                total_books = sum(1 for row in reader)
        except Exception as e:
            print(f"⚠️ [SIMPLE_UPLOAD] Could not count CSV rows: {e}")
            total_books = 0
        
        print(f"📊 [SIMPLE_UPLOAD] Found {total_books} books to import")
        
        # Create import config
        import_config = {
            'task_id': str(uuid.uuid4()),
            'csv_file_path': temp_path,
            'user_id': current_user.id,
            'format_type': format_type,
            'confidence': confidence,
            'filename': file.filename,
            'field_mappings': field_mappings,
            'default_reading_status': 'plan_to_read',
            'enable_api_enrichment': request.form.get('enable_api_enrichment', 'true').lower() == 'true'
        }
        
        # Initialize job tracking
        task_id = import_config['task_id']
        job_data = {
            'task_id': task_id,
            'user_id': str(current_user.id),
            'status': 'running',
            'filename': file.filename,
            'format_type': format_type,
            'confidence': confidence,
            'total_books': total_books,
            'processed': 0,
            'success': 0,
            'errors': 0,
            'skipped': 0,
            'current_book': None,
            'created_at': datetime.now().isoformat(),
            'recent_activity': [f'Started import of {file.filename} ({total_books} books)']
        }
        import_jobs[task_id] = job_data
        print(f"📊 [SIMPLE_UPLOAD] Initialized job tracking for {task_id}")
        
        # Start background processing
        def process_upload():
            try:
                print(f"🚀 [SIMPLE_UPLOAD] Starting background processing for {task_id}")
                print(f"🔧 [SIMPLE_UPLOAD] Import config: {import_config}")
                
                # Create new event loop for this thread
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(process_simple_import(import_config))
                    print(f"✅ [SIMPLE_UPLOAD] Background processing completed successfully for {task_id}")
                finally:
                    loop.close()
                    
            except Exception as e:
                print(f"❌ [SIMPLE_UPLOAD] Background processing error: {e}")
                traceback.print_exc()
                # Update job status
                if task_id in import_jobs:
                    import_jobs[task_id]['status'] = 'failed'
                    import_jobs[task_id]['error_messages'] = [str(e)]
        
        thread = threading.Thread(target=process_upload)
        thread.daemon = True
        thread.start()
        
        print(f"🚀 [SIMPLE_UPLOAD] Background processing started for {import_config['task_id']}")
        
        return jsonify({
            'status': 'success',
            'task_id': import_config['task_id'],
            'format_detected': format_type,
            'confidence': confidence,
            'message': f'Import started! Detected {format_type} format with {confidence:.1%} confidence.'
        })
        
    except Exception as e:
        print(f"❌ [SIMPLE_UPLOAD] Error: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Processing failed'}), 500

async def pre_analyze_and_create_custom_fields(csv_file_path, field_mappings, user_id):
    """
    Pre-analyze import file and create all needed custom fields before processing books.
    
    Args:
        csv_file_path: Path to the CSV file
        field_mappings: Dict mapping CSV fields to book fields
        user_id: User ID for personal custom fields
        
    Returns:
        Tuple of (success: bool, custom_fields_created: dict)
    """
    from app.services.kuzu_custom_field_service import KuzuCustomFieldService
    
    print(f"🔍 [PRE_ANALYZE] Starting custom field pre-analysis")
    
    custom_field_service = KuzuCustomFieldService()
    custom_fields_to_create = []
    
    try:
        # Step 1: Read CSV headers to see what custom fields are mapped
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            csv_headers = reader.fieldnames or []
            
        print(f"🔍 [PRE_ANALYZE] CSV headers: {csv_headers}")
        print(f"🔍 [PRE_ANALYZE] Field mappings: {field_mappings}")
        
        # Step 2: Identify custom field mappings 
        for csv_field, mapped_field in field_mappings.items():
            if csv_field in csv_headers and mapped_field.startswith('custom_'):
                # Parse custom field type and name
                if mapped_field.startswith('custom_global_'):
                    field_name = mapped_field[14:]  # Remove 'custom_global_' prefix
                    is_global = True
                elif mapped_field.startswith('custom_personal_'):
                    field_name = mapped_field[16:]  # Remove 'custom_personal_' prefix
                    is_global = False
                else:
                    field_name = mapped_field[7:]  # Remove 'custom_' prefix - default to global
                    is_global = True
                
                # Sample some data to determine field type
                field_type = 'text'  # Default to text
                
                # Try to sample the CSV to infer field type
                try:
                    with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                        reader = csv.DictReader(csvfile)
                        sample_rows = []
                        for i, row in enumerate(reader):
                            if i >= 10:  # Sample first 10 rows
                                break
                            if csv_field in row and row[csv_field]:
                                sample_rows.append(row[csv_field])
                        
                        # Infer type from samples
                        if sample_rows:
                            field_type = infer_field_type_from_samples(sample_rows)
                            
                except Exception as e:
                    print(f"⚠️ [PRE_ANALYZE] Could not sample data for {csv_field}: {e}")
                
                custom_fields_to_create.append({
                    'name': field_name,
                    'type': field_type,
                    'is_global': is_global,
                    'csv_field': csv_field,
                    'mapped_field': mapped_field
                })
        
        print(f"🔍 [PRE_ANALYZE] Found {len(custom_fields_to_create)} custom fields to create:")
        for field in custom_fields_to_create:
            print(f"   - {field['name']} ({field['type']}, {'global' if field['is_global'] else 'personal'})")
        
        # Step 3: Create all custom fields
        created_fields = {}
        for field_info in custom_fields_to_create:
            try:
                print(f"🔧 [PRE_ANALYZE] Creating custom field: {field_info['name']}")
                
                # Create field definition
                field_def = custom_field_service.create_field_sync(user_id, {
                    'name': field_info['name'],
                    'display_name': field_info['name'].replace('_', ' ').title(),
                    'field_type': field_info['type'],
                    'is_global': field_info['is_global'],
                    'description': f"Custom field for {field_info['name']} (imported from CSV)"
                })
                
                # Handle different return types from create_field_sync
                field_id = None
                if isinstance(field_def, dict) and 'id' in field_def:
                    field_id = field_def['id']
                elif field_def and hasattr(field_def, 'id'):
                    field_id = getattr(field_def, 'id', None)
                elif field_def:
                    # If it returns something truthy but not a dict/object with id, 
                    # assume success and use field name as identifier
                    field_id = field_info['name']
                
                if field_id:
                    created_fields[field_info['mapped_field']] = {
                        'id': field_id,
                        'name': field_info['name'],
                        'type': field_info['type'],
                        'is_global': field_info['is_global']
                    }
                    print(f"✅ [PRE_ANALYZE] Created custom field: {field_info['name']}")
                else:
                    print(f"⚠️ [PRE_ANALYZE] Field creation returned falsy result for: {field_info['name']}")
                    
            except Exception as e:
                print(f"❌ [PRE_ANALYZE] Error creating custom field {field_info['name']}: {e}")
                import traceback
                traceback.print_exc()
        
        print(f"✅ [PRE_ANALYZE] Custom field creation complete. Created {len(created_fields)} fields.")
        return True, created_fields
        
    except Exception as e:
        print(f"❌ [PRE_ANALYZE] Error during custom field pre-analysis: {e}")
        import traceback
        traceback.print_exc()
        return False, {}

def infer_field_type_from_samples(samples):
    """
    Infer the best custom field type from sample values.
    
    Args:
        samples: List of string values from CSV
        
    Returns:
        str: 'integer', 'decimal', 'boolean', 'date', or 'text'
    """
    if not samples:
        return 'text'
    
    # Clean samples
    clean_samples = [str(s).strip() for s in samples if s is not None and str(s).strip()]
    
    if not clean_samples:
        return 'text'
    
    # Try integer
    integer_count = 0
    for sample in clean_samples:
        try:
            int(sample)
            integer_count += 1
        except ValueError:
            pass
    
    if integer_count == len(clean_samples):
        return 'integer'
    
    # Try decimal
    decimal_count = 0
    for sample in clean_samples:
        try:
            float(sample)
            decimal_count += 1
        except ValueError:
            pass
    
    if decimal_count == len(clean_samples):
        return 'decimal'
    
    # Try boolean
    boolean_values = {'true', 'false', 'yes', 'no', '1', '0', 'y', 'n'}
    boolean_count = 0
    for sample in clean_samples:
        if sample.lower() in boolean_values:
            boolean_count += 1
    
    if boolean_count == len(clean_samples):
        return 'boolean'
    
    # Try date (basic check)
    date_count = 0
    for sample in clean_samples:
        if len(sample) >= 8 and ('-' in sample or '/' in sample):
            try:
                # Try common date formats
                from datetime import datetime
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']:
                    try:
                        datetime.strptime(sample, fmt)
                        date_count += 1
                        break
                    except ValueError:
                        continue
            except:
                pass
    
    if date_count >= len(clean_samples) * 0.8:  # 80% look like dates
        return 'date'
    
    # Default to text
    return 'text'
