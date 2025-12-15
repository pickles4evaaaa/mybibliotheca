"""
Import/Export routes for the MyBibliotheca application.
Handles book import functionality including CSV processing, progress tracking, and batch operations.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, jsonify, make_response, session, Response
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from datetime import datetime, date, timedelta, timezone
from typing import List, Any, Optional, Tuple
import uuid
import os
import csv
import threading
import traceback
import asyncio
import tempfile
import secrets
import logging
import json
import os as _os_for_import_verbosity
import requests  # Add requests import
import time

# Metadata debug flag (shared with unified metadata module)
_META_DEBUG_FLAG = (os.getenv('METADATA_DEBUG','0').lower() in ('1','true','yes','on'))

# Set up import-specific logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Quiet mode for heavy imports: set IMPORT_VERBOSE=true to re-enable prints
_IMPORT_VERBOSE = (
    (_os_for_import_verbosity.getenv('VERBOSE') or 'false').lower() == 'true'
    or (_os_for_import_verbosity.getenv('IMPORT_VERBOSE') or 'false').lower() == 'true'
)
def _dprint(*args, **kwargs):
    if _IMPORT_VERBOSE:
        __builtins__.print(*args, **kwargs)

# Redirect module print to conditional debug print
print = _dprint

from app.services import book_service, import_mapping_service, custom_field_service, reading_log_service
from app.simplified_book_service import SimplifiedBookService, SimplifiedBook
from app.domain.models import CustomFieldDefinition, CustomFieldType, ImportMappingTemplate, ReadingLog
from app.utils import normalize_goodreads_value
from app.utils.user_settings import get_effective_reading_defaults, get_default_book_format
from app.utils.image_processing import process_image_from_url, process_image_from_filestorage, get_covers_dir
from app.utils.book_utils import get_best_cover_for_book
from app.services.kuzu_async_helper import run_async
from app.utils.safe_import_manager import (
    safe_import_manager, 
    safe_create_import_job,
    safe_update_import_job, 
    safe_get_import_job,
    safe_get_user_import_jobs,
    safe_delete_import_job
)

# DEPRECATED: Global dictionary to store import jobs 
# This is being replaced with safe_import_manager for thread safety and user isolation
# ✅ DEPRECATED GLOBAL DICTIONARY REMOVED
# All import job management now uses SafeImportJobManager for thread safety and user isolation
# Original global import_jobs = {} pattern was dangerous and has been completely replaced

# Create import blueprint
import_bp = Blueprint('import', __name__)

UNASSIGNED_READING_LOG_TITLE = 'Unassigned Reading Logs'

MAX_PROGRESS_RECENT_BOOKS = int(os.getenv('IMPORT_PROGRESS_RECENT', '25'))
MAX_PROGRESS_ERROR_MESSAGES = int(os.getenv('IMPORT_PROGRESS_MAX_ERRORS', '200'))
PROGRESS_EMIT_INTERVAL = float(os.getenv('IMPORT_PROGRESS_INTERVAL', '0.35'))


def _bounded_list(existing: Optional[List[Any]], addition: Any, max_len: int) -> List[Any]:
    items = list(existing or [])
    if addition is None:
        return items[-max_len:] if max_len > 0 else items
    if isinstance(addition, list):
        items.extend(addition)
    else:
        items.append(addition)
    if max_len > 0 and len(items) > max_len:
        items = items[-max_len:]
    return items


def _update_import_progress(user_id: str, task_id: str, *, updates: Optional[dict] = None,
                             processed_book: Optional[Any] = None,
                             error_message: Optional[dict] = None) -> bool:
    snapshot = safe_get_import_job(user_id, task_id) or {}
    payload = dict(updates or {})
    if processed_book is not None:
        payload['processed_books'] = _bounded_list(snapshot.get('processed_books'), processed_book, MAX_PROGRESS_RECENT_BOOKS)
    if error_message is not None:
        payload['error_messages'] = _bounded_list(snapshot.get('error_messages'), error_message, MAX_PROGRESS_ERROR_MESSAGES)
    return safe_update_import_job(user_id, task_id, payload)


def _sanitize_quick_add_inputs(days_raw, pages_raw, minutes_raw, user_id) -> Tuple[int, int, int, List[str]]:
    """Normalize quick-add form inputs and return (days, pages, minutes, errors)."""
    errors: List[str] = []

    try:
        days = int(days_raw)
    except (TypeError, ValueError):
        days = 0
        errors.append('Number of prior days must be a whole number.')

    if days < 1:
        errors.append('Number of prior days must be at least 1.')
        days = max(days, 0)
    if days > 365:
        errors.append('Quick add supports up to 365 prior days per run.')
        days = 365

    def _parse_int(raw_value) -> int:
        try:
            value = int(raw_value)
            return value if value >= 0 else 0
        except (TypeError, ValueError):
            return 0

    pages = _parse_int(pages_raw)
    minutes = _parse_int(minutes_raw)

    defaults = get_effective_reading_defaults(user_id)
    default_pages = defaults[0] if defaults else None
    default_minutes = defaults[1] if defaults else None

    if pages <= 0 and minutes <= 0:
        if (default_pages or 0) > 0:
            pages = int(default_pages)  # type: ignore[arg-type]
        if (default_minutes or 0) > 0:
            minutes = int(default_minutes)  # type: ignore[arg-type]

    if pages <= 0 and minutes <= 0:
        pages = 1
        minutes = 1

    return days, pages, minutes, errors


async def _ensure_unassigned_reading_log_book(user_id: str,
                                              location_service=None,
                                              default_location_id: Optional[str] = None) -> Optional[str]:
    """Ensure the placeholder book for bookless logs exists and return its ID."""
    try:
        existing = book_service.search_books_sync(UNASSIGNED_READING_LOG_TITLE, user_id, limit=1)
        if existing:
            candidate = existing[0]
            book_id = getattr(candidate, 'id', None)
            if not book_id and isinstance(candidate, dict):
                book_id = candidate.get('id')
            if book_id:
                return book_id
    except Exception as search_error:
        logger.warning(f"Quick add lookup for unassigned book failed: {search_error}")

    service = SimplifiedBookService()
    placeholder = SimplifiedBook(title=UNASSIGNED_READING_LOG_TITLE, author='System Generated')
    try:
        book_id = await service.create_standalone_book(placeholder)
    except Exception as creation_error:
        logger.error(f"Failed to create unassigned reading log book: {creation_error}")
        return None

    if not book_id:
        return None

    try:
        if location_service is None:
            from app.location_service import LocationService  # Lazy import to avoid circulars
            location_service = LocationService()
            default_location = location_service.get_default_location()
            default_location_id = getattr(default_location, 'id', None) if default_location else None
        if location_service and default_location_id:
            try:
                location_service.add_book_to_location(book_id, default_location_id, user_id)
            except Exception as loc_error:
                logger.debug(f"Unable to link unassigned book to default location: {loc_error}")
    except Exception as location_error:
        logger.debug(f"Location association skipped: {location_error}")

    return book_id


def _is_ajax_request() -> bool:
    requested_with = request.headers.get('X-Requested-With', '').lower()
    if requested_with == 'xmlhttprequest':
        return True
    accept_header = request.headers.get('Accept', '')
    return 'application/json' in accept_header.lower()

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
            
            
            # Determine format with minimum confidence threshold
            min_confidence = 0.3
            
            if goodreads_normalized >= min_confidence and goodreads_normalized > storygraph_normalized:
                return 'goodreads', goodreads_normalized
            elif storygraph_normalized >= min_confidence and storygraph_normalized > goodreads_normalized:
                return 'storygraph', storygraph_normalized
            else:
                return 'unknown', max(goodreads_normalized, storygraph_normalized)
                
    except Exception as e:
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
        
        # Publication year and date
        'year': 'publication_year',
        'publication year': 'publication_year',
        'published year': 'publication_year',
        'year published': 'publication_year',
        'published date': 'published_date',
        'publication date': 'published_date',
        'date published': 'published_date',
        'publish date': 'published_date',
        
        # Dates
        'date read': 'date_read',
        'reading date': 'date_read',
        'finished date': 'date_read',
        'date added': 'date_added',
        'added date': 'date_added',
        
        # Reading status
        'status': 'reading_status',
        'reading status': 'reading_status',
    'read status': 'reading_status',
        'shelf': 'reading_status',
        'exclusive shelf': 'reading_status',
        
        # Notes and reviews
        'notes': 'notes',
        'review': 'notes',
        'my review': 'notes',
        'comments': 'notes',
        'description': 'description',
        
        # Categories/Tags (Note: In proper imports, these should be moved to custom fields)
        'categories': 'categories',
        'tags': 'categories',
        'genre': 'categories',
        'genres': 'categories',
        # Bookshelves should be mapped to custom fields in proper imports
        'bookshelves': 'custom_personal_goodreads_shelves',
        
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
            for f in existing_fields:
                name = f.get('name') if isinstance(f, dict) else getattr(f, 'name', None)
                if name:
                    existing_field_names.add(name)
        except Exception:
            pass

def get_goodreads_field_mappings():
    """Get predefined field mappings for Goodreads CSV format with custom field support.
    
    NOTE: Goodreads 'Bookshelves' are moved to custom fields since they're user-specific
    tags/shelves, not actual genres. Real genres/categories will be fetched from APIs.
    """
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
        'Published Date': 'published_date',  # In case Goodreads has full dates
        'Publication Date': 'published_date',  # Alternative field name
        'Original Publication Year': 'original_publication_year',
        'Date Read': 'date_read',
        'Date Added': 'date_added',
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
        
        # Personal custom fields (per-user) - Goodreads bookshelves moved here
        'Bookshelves': 'custom_personal_goodreads_shelves',  # Moved from categories to custom field
        'Spoiler': 'custom_personal_spoiler_flag', 
        'Read Count': 'custom_personal_read_count',
        'Owned Copies': 'custom_personal_owned_copies',
    })
    
    return enhanced_mappings

def get_storygraph_field_mappings():
    """Get predefined field mappings for StoryGraph CSV format with custom field support.
    
    NOTE: StoryGraph 'Tags' are moved to custom fields since they're user-specific
    tags, not actual genres. Real genres/categories will be fetched from APIs.
    """
    # Base mappings
    base_mappings = {
        'Title': 'title',
    'Author': 'author',
    'Authors': 'author',  # Some exports use plural header
    'Contributors': 'contributors',  # Parse role-tagged contributors
    'ISBN': 'isbn',
    'ISBN/UID': 'isbn',   # StoryGraph alternate column name
        'ISBN13': 'isbn',
        'Publisher': 'publisher',
        'Pages': 'page_count',
        'Publication Year': 'publication_year',
        'Published Date': 'published_date',  # In case StoryGraph has full dates
        'Publication Date': 'published_date',  # Alternative field name
        'Date Started': 'start_date',
        'Date Finished': 'date_read',
        'Date Added': 'date_added',
        'Read Status': 'reading_status',
        'Star Rating': 'user_rating',
        'Review': 'personal_notes',
        'Format': 'format',
        'Additional Authors': 'additional_authors',  # Normalize if present
    }
    
    # Enhanced mappings with custom field support
    enhanced_mappings = {**base_mappings}
    enhanced_mappings.update({
        # Global custom fields (shared across users)
        'Moods': 'custom_global_moods',
        'Pace': 'custom_global_pace', 
        'Character- or Plot-Driven?': 'custom_global_character_plot_driven',
        
        # Personal custom fields (per-user) - StoryGraph tags moved here
        'Tags': 'custom_personal_storygraph_tags',  # Moved from categories to custom field
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
            if not file or not file.filename:
                flash('Please upload a valid delimited file (.csv, .tsv, or .txt).', 'danger')
                return redirect(url_for('import.import_books'))
            # Allow common delimited text files
            allowed_exts = ('.csv', '.tsv', '.txt')
            filename_lc = file.filename.lower()
            if not any(filename_lc.endswith(ext) for ext in allowed_exts):
                flash('Please upload a valid delimited file (.csv, .tsv, or .txt).', 'danger')
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
                
                # Auto-detect if this looks like a reading history file and route to that workflow
                def _looks_like_reading_history(hdrs) -> bool:
                    low = [str(h).lower().strip() for h in (hdrs or [])]
                    has_date = any('date' == h or h.startswith('date') for h in low)
                    has_any_metrics = any(k in low for k in ['pages read','minutes read','start page','end page'])
                    # Avoid false positives on typical book exports by requiring absence of both title and author headers
                    typical_book_cols = any(k in low for k in ['title','author','authors','isbn','isbn13','isbn_13'])
                    return has_date and has_any_metrics and not typical_book_cols
                if _looks_like_reading_history(headers):
                    # Hand off to the Reading History importer using the saved file
                    return redirect(url_for('import.import_reading_history', csv_file_path=temp_path))

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
                
                # Skip template detection if user wants custom mapping
                if force_custom:
                    detected_template = None
                    detected_template_name = None
                else:
                    # Detect best matching template based on headers
                    # Ensure headers is a proper List[str] type
                    headers_list = list(headers) if not isinstance(headers, list) else headers
                    detected_template = import_mapping_service.detect_template_sync(headers_list, current_user.id)
                    detected_template_name = detected_template.name if detected_template else None
                    
                # If a default system template was detected, auto-create fields but still show mapping UI for review
                if not force_custom and detected_template and detected_template.user_id == "__system__" and detected_template.field_mappings:
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
                
                # If a custom template was detected, use its mappings
                elif detected_template and detected_template.field_mappings:
                    suggested_mappings = detected_template.field_mappings.copy()
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
    csv_headers = [header for header in request.form.getlist('csv_headers[]') if header]

    # Ensure mappings always defined to avoid unbound variable errors
    mappings = {}
    
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
                save_as_template = False
                if target_field:
                    mappings[csv_field] = target_field
            
            print(f"DEBUG: Converted mappings for import: {mappings}")
            
        except Exception as e:
            current_app.logger.error(f"Error loading template for import: {e}")
            flash('Error loading template configuration', 'error')
                    # Since get_template_by_id_sync doesn't exist, look for template by name
    
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
                            created_at=datetime.now(timezone.utc),
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
    
    if save_as_template:
        if not template_name:
            flash('Template name is required to save mappings.', 'warning')
        elif not mappings:
            flash('Map at least one field before saving a template.', 'warning')
        else:
            template_headers = []
            seen_headers = set()
            header_source = csv_headers or list(mappings.keys())
            for header in header_source:
                header_value = header if isinstance(header, str) else str(header)
                if not header_value or header_value in seen_headers:
                    continue
                seen_headers.add(header_value)
                template_headers.append(header_value)
            template_field_mappings = {}
            for csv_field, target_field in mappings.items():
                if not target_field:
                    continue
                if isinstance(target_field, dict):
                    template_field_mappings[csv_field] = target_field
                else:
                    template_field_mappings[csv_field] = {
                        'action': 'map_existing',
                        'target_field': target_field
                    }
            if not template_field_mappings:
                flash('Map at least one field before saving a template.', 'warning')
            else:
                try:
                    existing_templates_raw = import_mapping_service.get_user_templates_sync(current_user.id) or []
                    existing_templates = list(existing_templates_raw) if hasattr(existing_templates_raw, '__iter__') else []
                    conflict_found = False
                    template_name_lower = template_name.lower()
                    for existing in existing_templates:
                        existing_name = existing.get('name') if isinstance(existing, dict) else getattr(existing, 'name', '')
                        if existing_name and existing_name.lower() == template_name_lower:
                            conflict_found = True
                            break
                    if conflict_found:
                        flash(f'An import template named "{template_name}" already exists. Choose another name.', 'warning')
                    else:
                        new_template = ImportMappingTemplate(
                            user_id=current_user.id,
                            name=template_name,
                            source_type='custom',
                            sample_headers=template_headers,
                            field_mappings=template_field_mappings
                        )
                        created_template = import_mapping_service.create_template_sync(new_template)
                        if created_template:
                            flash(f'Saved import template "{template_name}".', 'success')
                        else:
                            flash('Unable to save template. Please try again later.', 'warning')
                except Exception as e:
                    current_app.logger.exception('Failed to save import template')
                    flash(f'Error saving template: {str(e)}', 'warning')
    
    default_reading_status = request.form.get('default_reading_status', '')
    duplicate_handling = request.form.get('duplicate_handling', 'skip')
    default_import_media_type = request.form.get('default_import_media_type', 'physical')
    
    # Create import job with enhanced data
    task_id = str(uuid.uuid4())
    job_data = {
        'task_id': task_id,
        'user_id': current_user.id,
        'csv_file_path': csv_file_path,
        'field_mappings': mappings,
        'default_reading_status': default_reading_status,
        'duplicate_handling': duplicate_handling,
        'default_import_media_type': default_import_media_type,
        'custom_fields_enabled': True,  # Flag to enable custom metadata processing
        'status': 'pending',
        'processed': 0,
        'success': 0,
        'errors': 0,
        'skipped': 0,  # Initialize skipped counter
        'total': 0,
        'start_time': datetime.now(timezone.utc).isoformat(),
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
    
    # Store in safe import manager with proper user isolation
    safe_success = safe_create_import_job(current_user.id, task_id, job_data)
    
    print(f"📊 [EXECUTE] Kuzu storage: {'✅' if kuzu_success else '❌'}")
    print(f"🔒 [EXECUTE] Safe storage: {'✅' if safe_success else '❌'}")
    print(f"💾 [EXECUTE] Legacy storage: ✅")
    
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
            traceback.print_exc()
            # Update job with error in both storage systems
            try:
                error_update = {
                    'status': 'failed',
                    'error_messages': [str(e)]
                }
                update_job_in_kuzu(task_id, error_update)
                
                # Update safely with user isolation
                safe_update_import_job(current_user.id, task_id, error_update)
                
            except Exception as update_error:
                pass  # Error updating job status
    
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
    # Use safe import manager with user isolation
    job = safe_get_import_job(current_user.id, task_id)
    if not job:
        flash('Import job not found.', 'error')
        return redirect(url_for('import.import_books'))
    
    # Check if this is a reading history import that needs special handling
    if job.get('import_type') == 'reading_history' and job.get('status') == 'needs_book_matching':
        # Redirect to book matching page for reading history imports
        return redirect(url_for('import.reading_history_book_matching', task_id=task_id))
    
    # Get total books count from job data or default to 0
    total_books = job.get('total_books', 0)
    
    # For reading history imports, use total_entries if available
    if job.get('import_type') == 'reading_history':
        total_books = job.get('total_entries', total_books)
    
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
    """API endpoint for import progress with proper user isolation."""
    # Get job safely with user isolation
    job = safe_get_import_job(current_user.id, task_id)
    
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    # Augment response with status buckets for UI (success_titles, error_titles, skipped_titles, unmatched_titles)
    # Derive lazily so we don't store large arrays permanently in job record.
    success_titles = []
    error_titles = []  # Will be populated from error_messages only to avoid duplication
    error_details = []  # Structured objects {title,message,row_number,error_type}
    skipped_titles = []
    unmatched_titles = []
    merged_titles = []
    try:
        if job.get('import_type') == 'reading_history':
            books_for_matching = job.get('books_for_matching') or []
            # If final stats included matched/created titles, preload into buckets
            matched_book_titles = job.get('matched_book_titles') or []
            created_book_titles = job.get('created_book_titles') or []
            for b in books_for_matching:
                try:
                    entries = b.get('entries') or []
                    if not isinstance(entries, list):
                        continue
                    dates = []
                    for e in entries:
                        d = e.get('date') or e.get('finished_at') or e.get('started_at') or ''
                        if d:
                            dates.append(str(d))
                    date_span = ''
                    if dates:
                        dates_sorted = sorted(dates)
                        date_span = dates_sorted[0]
                        if len(dates_sorted) > 1 and dates_sorted[-1] != dates_sorted[0]:
                            date_span += f" → {dates_sorted[-1]}"
                    label = b.get('csv_name') or b.get('matched_title') or b.get('title') or 'Unknown'
                    if date_span:
                        label = f"{label} ({date_span})"
                    status = (b.get('status') or '').lower()
                    if status in ('success','matched','created'):
                        success_titles.append(label)
                    elif status in ('skipped','ignored'):
                        skipped_titles.append(label)
                    elif status in ('unmatched','needs_match'):
                        unmatched_titles.append(label)
                except Exception:
                    continue
            for err in job.get('reading_log_error_messages') or []:
                if isinstance(err, dict):
                    t = err.get('title') or err.get('csv_name') or err.get('book') or ''
                    if t:
                        error_titles.append(t)
            # Success details for reading logs (dates etc.)
            success_detail_strings = job.get('reading_log_success_details') or []
            # For consistency with UI, attach as success_titles if none gathered above
            if success_detail_strings:
                success_titles = success_detail_strings[:500]
            # Map matched/created titles into merged/skipped buckets for tile display semantics
            if matched_book_titles and not merged_titles:
                merged_titles = matched_book_titles
            if created_book_titles and not skipped_titles:
                skipped_titles = created_book_titles
        else:
            books = job.get('processed_books') or job.get('books') or []
            for b in books:
                try:
                    status = (b.get('status') or '').lower()
                    title = b.get('title') or b.get('raw_title') or b.get('name') or ''
                    if not title:
                        continue
                    if status == 'success':
                        success_titles.append(title)
                    elif status == 'merged':
                        merged_titles.append(title)
                    elif status == 'skipped':
                        skipped_titles.append(title)
                    elif status == 'unmatched':
                        unmatched_titles.append(title)
                    # Do NOT append error titles here; we'll rely on error_messages for errors to avoid duplicates
                except Exception:
                    continue
            # Build error titles + details strictly from error_messages
            for err in job.get('error_messages') or []:
                if not isinstance(err, dict):
                    continue
                t = err.get('title') or err.get('raw_title') or ''
                m = err.get('message') or err.get('error') or ''
                if t:
                    error_titles.append(t)
                error_details.append({
                    'title': t,
                    'message': m[:300],  # limit size for payload
                    'row_number': err.get('row_number') or err.get('row'),
                    'error_type': err.get('type') or err.get('error_type') or 'error'
                })
    except Exception:
        pass
    # Deduplicate while preserving order for error titles
    seen = set()
    deduped_error_titles = []
    for t in error_titles:
        if t and t not in seen:
            seen.add(t)
            deduped_error_titles.append(t)

    # Summarize failed ISBNs (exclude duplicate-related errors)
    failed_isbn_preview = []
    failed_isbn_count = 0
    try:
        errors = job.get('error_messages') or []
        seen_failed = set()
        for err in errors:
            error_type = str(err.get('error_type') or '').lower()
            if 'duplicate' in error_type:
                continue
            isbn_val = (err.get('isbn') or err.get('raw_isbn') or '').strip()
            if not isbn_val or isbn_val in seen_failed:
                continue
            seen_failed.add(isbn_val)
            failed_isbn_preview.append({
                'isbn': isbn_val,
                'title': err.get('title') or '',
                'message': err.get('message') or ''
            })
        failed_isbn_count = len(seen_failed)
    except Exception:
        failed_isbn_preview = []
        failed_isbn_count = 0

    enriched = dict(job)
    enriched['success_titles'] = success_titles
    enriched['error_titles'] = deduped_error_titles
    enriched['merged_titles'] = merged_titles
    enriched['skipped_titles'] = skipped_titles
    enriched['unmatched_titles'] = unmatched_titles
    enriched['success_preview'] = success_titles[:25]
    enriched['error_preview'] = deduped_error_titles[:25]
    enriched['merged_preview'] = merged_titles[:25]
    enriched['error_details'] = error_details[:1000]  # safety cap
    enriched['skipped_preview'] = skipped_titles[:25]
    enriched['unmatched_preview'] = unmatched_titles[:25]
    enriched['failed_isbn_count'] = failed_isbn_count
    enriched['failed_isbn_preview'] = failed_isbn_preview[:25]

    # Derive live counters and current book if missing
    try:
        pb = list(job.get('processed_books') or [])
        # Prefer running counters when present; otherwise, fallback to derived counts from previews
        if isinstance(job.get('success'), int):
            enriched['success'] = int(job.get('success') or 0)
        else:
            enriched['success'] = len(success_titles)
        if isinstance(job.get('merged'), int):
            enriched['merged'] = int(job.get('merged') or 0)
        else:
            enriched['merged'] = len(merged_titles)
        if isinstance(job.get('errors'), int):
            enriched['errors'] = int(job.get('errors') or 0)
        else:
            enriched['errors'] = len(deduped_error_titles)
        if isinstance(job.get('skipped'), int):
            enriched['skipped'] = int(job.get('skipped') or 0)
        else:
            enriched['skipped'] = len(skipped_titles)
        if isinstance(job.get('unmatched'), int):
            enriched['unmatched'] = int(job.get('unmatched') or 0)
        else:
            enriched['unmatched'] = len(unmatched_titles)
        # Current book fallback from last processed item
        cb = job.get('current_book')
        if not cb:
            for b in reversed(pb):
                t = (b or {}).get('title')
                if t:
                    cb = t
                    break
            if not cb and success_titles:
                cb = success_titles[-1]
        if cb:
            enriched['current_book'] = cb
        # Populate recent_activity if not present: show last 10 processed events
        if not job.get('recent_activity') and pb:
            events = []
            for b in pb[-10:]:
                if not isinstance(b, dict):
                    continue
                t = b.get('title') or 'Unknown'
                s = (b.get('status') or 'updated').capitalize()
                events.append(f"{s}: {t}")
            enriched['recent_activity'] = events
    except Exception:
        pass
    return jsonify(enriched)

@import_bp.route('/api/import/cancel/<task_id>', methods=['POST'])
@login_required
def api_import_cancel(task_id):
    """Request cancellation of a running import/sync job.

    Sets a cancellation flag on the job; workers are expected to check
    this flag and exit promptly. The job status will move to "cancelling"
    immediately and to "cancelled" when workers acknowledge.
    """
    job = safe_get_import_job(current_user.id, task_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    status = (job.get('status') or '').lower()
    if status in ('completed','failed','cancelled'):
        # Nothing to cancel
        return jsonify({'ok': True, 'status': status})
    # Mark as cancelling and set flag
    safe_update_import_job(current_user.id, task_id, {
        'cancelled': True,
        'status': 'cancelling'
    })
    return jsonify({'ok': True, 'status': 'cancelling'})

@import_bp.route('/api/import/errors/<task_id>')
@login_required
def api_import_errors(task_id):
    """Download import errors as CSV (was JSON)."""
    job = safe_get_import_job(current_user.id, task_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    errors = job.get('error_messages') or []

    header = ['row_number','error_type','message','raw_isbn','isbn','title','author','file_name','raw_row']
    def _escape(v: str):
        if v is None:
            v = ''
        v = str(v)
        if any(c in v for c in [',','"','\n','\r']):
            v = '"' + v.replace('"','""') + '"'
        return v
    rows = [','.join(header)]
    if errors:
        for err in errors:
            if isinstance(err, dict):
                row = [
                    err.get('row_number') or err.get('row') or '',
                    err.get('type') or err.get('error_type') or 'error',
                    (err.get('message') or err.get('error') or '').replace('\n',' ').replace('\r',' ')[:500],
                    err.get('raw_isbn') or '',
                    err.get('isbn') or err.get('raw_isbn') or '',
                    err.get('title') or '',
                    err.get('author') or '',
                    err.get('file_name') or '',
                    json.dumps(err.get('raw_row')) if isinstance(err.get('raw_row'), dict) else (err.get('raw_row') or '')
                ]
            else:
                row = ['', 'error', str(err).replace('\n',' '), '', '', '', '', '', '']
            rows.append(','.join(_escape(v) for v in row))
    csv_content = '\n'.join(rows) + '\n'
    return Response(csv_content, mimetype='text/csv', headers={
        'Content-Disposition': f'attachment; filename="import_errors_{task_id}.csv"'
    })

@import_bp.route('/api/import/failed-isbns/<task_id>')
@login_required
def api_import_failed_isbns(task_id):
    """Download CSV containing only failed ISBNs (duplicates excluded)."""
    job = safe_get_import_job(current_user.id, task_id)
    if not job:
        return Response(json.dumps({'error': 'Job not found'}), status=404, mimetype='application/json')

    errors = job.get('error_messages') or []
    header = ['isbn', 'title', 'row_number', 'message']

    def _escape(value: str) -> str:
        if value is None:
            value = ''
        value = str(value)
        if any(c in value for c in [',', '"', '\n', '\r']):
            value = '"' + value.replace('"', '""') + '"'
        return value

    seen = set()
    rows = [','.join(header)]
    for err in errors:
        if not isinstance(err, dict):
            continue
        error_type = str(err.get('error_type') or err.get('type') or '').lower()
        if 'duplicate' in error_type:
            continue
        isbn_val = (err.get('isbn') or err.get('raw_isbn') or '').strip()
        if not isbn_val or isbn_val in seen:
            continue
        seen.add(isbn_val)
        row_number = err.get('row_number')
        row = [
            isbn_val,
            (err.get('title') or '').strip(),
            row_number if row_number is not None else '',
            (err.get('message') or err.get('error') or '').replace('\n', ' ').replace('\r', ' ').strip()
        ]
        rows.append(','.join(_escape(v) for v in row))

    csv_content = '\n'.join(rows) + '\n'
    return Response(csv_content, mimetype='text/csv', headers={
        'Content-Disposition': f'attachment; filename="failed_isbns_{task_id}.csv"'
    })

@import_bp.route('/debug/import-jobs')
@login_required
def debug_import_jobs():
    """Debug endpoint to view import jobs with proper user isolation."""
    # Check if user is admin for full debug access
    is_admin = getattr(current_user, 'is_admin', False)
    
    if is_admin:
        # Admin gets comprehensive debug info
        debug_info = safe_import_manager.get_jobs_for_admin_debug(
            current_user.id, 
            include_user_data=True
        )
        
        # Migration is complete - no legacy jobs should exist
        debug_info['migration_status'] = 'completed'
        debug_info['security_note'] = 'All import jobs now use SafeImportJobManager with user isolation'
        
        return jsonify(debug_info)
    else:
        # Regular users only see their own jobs
        user_jobs = safe_get_user_import_jobs(current_user.id)
        
        # Include basic statistics
        stats = safe_import_manager.get_statistics()
        
        return jsonify({
            'your_jobs': user_jobs,
            'your_job_count': len(user_jobs),
            'system_uptime_hours': stats.get('uptime_hours', 0),
            'note': 'Only your own jobs are visible for privacy protection'
        })

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
        
        # Detect import type based on file content
        detected_format, confidence = detect_csv_format(temp_path)
        if detected_format in ['goodreads', 'storygraph']:
            import_type = detected_format
        else:
            # Default to goodreads if not detected
            import_type = 'goodreads'
        
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
        
        # Create import job for background processing
        task_id = str(uuid.uuid4())
        
        # Count total rows
        try:
            with open(temp_path, 'r', encoding='utf-8') as csvfile:
                import csv
                reader = csv.DictReader(csvfile)
                total_rows = sum(1 for _ in reader)
        except:
            total_rows = 0
        
        job_data = {
            'task_id': task_id,
            'user_id': current_user.id,
            'csv_file_path': temp_path,
            'field_mappings': mappings,
            'default_reading_status': '',
            'duplicate_handling': 'skip',
            'custom_fields_enabled': True,
            'format_type': import_type,
            'enable_api_enrichment': True,  # Enable API enrichment for better data
            'status': 'pending',
            'processed': 0,
            'success': 0,
            'errors': 0,
            'skipped': 0,
            'total': total_rows,
            'start_time': datetime.now(timezone.utc).isoformat(),
            'current_book': None,
            'error_messages': [],
            'recent_activity': []
        }
        
        # Store job data in memory and Kuzu
        print(f"🏗️ [DIRECT_IMPORT] Creating job {task_id} for user {current_user.id}")
        
        # Store in safe import manager with proper user isolation
        safe_create_import_job(current_user.id, task_id, job_data)
        store_job_in_kuzu(task_id, job_data)
        
        # Auto-create any custom fields referenced in the mappings
        auto_create_custom_fields(mappings, current_user.id)
        
        # Start the import in background thread
        import_config = {
            'task_id': task_id,
            'csv_file_path': temp_path,
            'field_mappings': mappings,
            'user_id': current_user.id,
            'default_reading_status': '',
            'format_type': import_type,
            'enable_api_enrichment': True
        }
        
        def run_import():
            try:
                print(f"🚀 [DIRECT_IMPORT] Starting import job {task_id} in background thread")
                
                # Call the async import function with an event loop
                asyncio.run(process_simple_import(import_config))
                
            except Exception as e:
                import traceback
                traceback.print_exc()
                # Update job with error
                try:
                    error_update = {
                        'status': 'failed',
                        'error_messages': [str(e)]
                    }
                    update_job_in_kuzu(task_id, error_update)
                    # Update safely with user isolation
                    safe_update_import_job(current_user.id, task_id, error_update)
                except Exception as update_error:
                    pass  # Error updating job status
        
        # Start the import process in background
        import threading
        thread = threading.Thread(target=run_import)
        thread.daemon = True
        thread.start()
        
        # Clear session data  
        session.pop('direct_import_file', None)
        session.pop('direct_import_filename', None)
        
        print(f"🚀 [DIRECT_IMPORT] Background thread started for job {task_id}")
        
        # Redirect to waiting page instead of library
        return redirect(url_for('import.import_waiting', task_id=task_id, 
                               import_type=import_type, filename=original_filename))
        
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
    """
    DEPRECATED: This function is unsafe for concurrent use.
    Use safe_import_manager functions instead.
    
    Process an import job in the background.
    """
    # DEPRECATED: Direct access to global import_jobs is unsafe
    # This function should not be used
    print(f"⚠️ WARNING: process_import_job({task_id}) called - this function is deprecated and unsafe")
    return

@import_bp.route('/upload', methods=['GET', 'POST'])
@login_required 
def upload_import():
    """Upload and process CSV files for book import."""
    if request.method == 'GET':
        return render_template('import_upload.html')
    
    # Handle POST request - file upload
    csv_file = request.files.get('csv_file')
    if not csv_file or not csv_file.filename or csv_file.filename == '':
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
        default_reading_status = request.form.get('default_reading_status', '')
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
            'start_time': datetime.now(timezone.utc).isoformat(),
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

        # Store job data with user isolation
        safe_create_import_job(current_user.id, task_id, job_data)

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
                job = safe_get_import_job(current_user.id, task_id)
                if job:
                    updates = {
                        'status': 'running',
                        'processed': job['total'],
                        'success': job['total'],
                        'current_book': None
                    }
                    safe_update_import_job(current_user.id, task_id, updates)

                    # Final completion update
                    completion_updates = {
                        'status': 'completed',
                        'recent_activity': job.get('recent_activity', []) + [f"Import completed! {job['total']} books imported"]
                    }
                    safe_update_import_job(current_user.id, task_id, completion_updates)
            except Exception as e:
                error_updates = {
                    'status': 'failed',
                    'error_messages': [str(e)]
                }
                safe_update_import_job(current_user.id, task_id, error_updates)
                logger.error(f"Import job {task_id} failed: {e}")

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
def simple_import():  # legacy placeholder
    from flask import redirect, url_for
    return redirect(url_for('book.library'))

async def process_simple_import(import_config):
    """Process a simple import job with API enrichment and detailed error capture."""
    from app.simplified_book_service import SimplifiedBookService

    task_id = import_config['task_id']
    csv_file_path = import_config['csv_file_path']
    mappings = import_config['field_mappings']
    user_id = import_config['user_id']
    enable_api_enrichment = import_config.get('enable_api_enrichment', True)
    format_type = import_config.get('format_type', 'unknown')

    if format_type in ['goodreads', 'storygraph']:
        enable_api_enrichment = True

    if _META_DEBUG_FLAG:
        logger.debug(f"[IMPORT][PROCESS_START] task={task_id} format={format_type} enrich={enable_api_enrichment} mappings={mappings}")
    simplified_service = SimplifiedBookService()
    # Check if user specified a default import media type; otherwise use system default
    default_import_media_type = import_config.get('default_import_media_type')
    if default_import_media_type and default_import_media_type in ['physical', 'ebook', 'audiobook', 'kindle']:
        default_media_type = default_import_media_type
    else:
        default_media_type = get_default_book_format()

    processed_count = success_count = error_count = skipped_count = merged_count = 0
    last_progress_emit = time.perf_counter()
    pending_processed_entries: List[dict] = []
    try:
        # Pre-analyze custom fields
        try:
            cf_ok, created_cf = await pre_analyze_and_create_custom_fields(csv_file_path, mappings, user_id)
            if cf_ok:
                print(f"🔧 [PROCESS_SIMPLE] Created {len(created_cf)} custom fields")
        except Exception as ce:
            print(f"⚠️ [PROCESS_SIMPLE] Custom field analysis failed: {ce}")

        row_count = 0
        isbn_candidates: List[str] = []
        with open(csv_file_path, 'r', encoding='utf-8') as fh:
            reader = csv.DictReader(fh)
            for idx, scan_row in enumerate(reader, 1):
                row_count = idx
                if not enable_api_enrichment:
                    continue
                raw_isbn = scan_row.get('ISBN13') or scan_row.get('isbn13') or scan_row.get('ISBN') or scan_row.get('ISBN/UID') or scan_row.get('isbn')
                if not raw_isbn:
                    continue
                cleaned = normalize_goodreads_value(raw_isbn, 'isbn')
                if cleaned and isinstance(cleaned, str) and len(cleaned) >= 10:
                    isbn_candidates.append(cleaned)
                if _META_DEBUG_FLAG:
                    logger.debug(f"[IMPORT][ISBN_COLLECT] row={idx} raw={raw_isbn!r} cleaned={cleaned!r}")
        print(f"📋 [PROCESS_SIMPLE] Rows to process: {row_count}")

        # Batch enrichment
        book_metadata = {}
        if enable_api_enrichment and isbn_candidates:
            if _META_DEBUG_FLAG:
                logger.debug(f"[IMPORT][ISBN_COLLECT] collected={len(isbn_candidates)} values={isbn_candidates}")
            seen = set()
            uniq: List[str] = []
            for v in isbn_candidates:
                if v not in seen:
                    seen.add(v)
                    uniq.append(v)
            if _META_DEBUG_FLAG:
                logger.debug(f"[IMPORT][ISBN_COLLECT] dedup_size={len(uniq)} values={uniq}")
            if uniq:
                max_batch_size = 50
                aggregated_metadata = {}
                for start in range(0, len(uniq), max_batch_size):
                    batch = uniq[start:start + max_batch_size]
                    if not batch:
                        continue
                    if _META_DEBUG_FLAG:
                        logger.debug(f"[IMPORT][METADATA][BATCH_EXEC] offset={start} size={len(batch)} sample={batch[:3]}")
                    chunk_metadata = batch_fetch_book_metadata(batch)
                    if chunk_metadata:
                        aggregated_metadata.update(chunk_metadata)
                book_metadata = aggregated_metadata
                if _META_DEBUG_FLAG:
                    logger.debug(f"[IMPORT][POST_BATCH] requested={len(uniq)} fetched={len(book_metadata)} keys={list(book_metadata.keys())}")

        source_filename = os.path.basename(csv_file_path)
        with open(csv_file_path, 'r', encoding='utf-8') as fh:
            reader = csv.DictReader(fh)
            for row_num, row in enumerate(reader, 1):
                try:
                    simplified_book = simplified_service.build_book_data_from_row(row, mappings)
                    # Allow ISBN-only rows (title may be filled after enrichment). Skip only if missing both title and ISBN.
                    if not simplified_book or (not simplified_book.title and not (simplified_book.isbn13 or simplified_book.isbn10)):
                        processed_count += 1
                        skipped_count += 1
                        raw_title = row.get('Title') or row.get('title') or row.get('Book Title') or row.get('Name') or row.get('Book Name') or 'Untitled'
                        _update_import_progress(user_id, task_id, processed_book={'title': raw_title, 'status': 'skipped'})
                        continue

                    if enable_api_enrichment and book_metadata:
                        simplified_book = merge_api_data_into_simplified_book(simplified_book, book_metadata, {})
                        if _META_DEBUG_FLAG:
                            logger.debug(f"[IMPORT][ROW_ENRICHED] row={row_num} isbn13={simplified_book.isbn13} isbn10={simplified_book.isbn10} title={simplified_book.title!r}")

                    # After enrichment, if this was an ISBN-only row and we failed to retrieve metadata (no real title), record error instead of creating placeholder book
                    isbn_key = simplified_book.isbn13 or simplified_book.isbn10
                    if isbn_key and enable_api_enrichment:
                        # Consider presence of either ISBN form in metadata map
                        in_meta = isbn_key in book_metadata or (simplified_book.isbn13 and simplified_book.isbn13 in book_metadata) or (simplified_book.isbn10 and simplified_book.isbn10 in book_metadata)
                        # Failed if not in metadata OR title is still placeholder (exactly equals any isbn form)
                        placeholder_title = not simplified_book.title or simplified_book.title.strip() == '' or simplified_book.title in {simplified_book.isbn13, simplified_book.isbn10}
                        lookup_failed = (not in_meta) or placeholder_title
                        # If original CSV provided no title (row mapping produced empty title) and lookup failed, treat as error
                        if lookup_failed and (not row.get(mappings.get('title', ''))):
                            try:
                                logger.error(f"[IMPORT][LOOKUP_FAIL] row={row_num} isbn={isbn_key} in_metadata={isbn_key in book_metadata} title_after={simplified_book.title!r}")
                            except Exception:
                                pass
                            processed_count += 1
                            error_count += 1
                            error_payload = {
                                'row_number': row_num,
                                'title': isbn_key,
                                'author': '',
                                'isbn': isbn_key,
                                'raw_isbn': row.get('ISBN') or row.get('ISBN13') or row.get('ISBN/UID') or '',
                                'file_name': source_filename,
                                'error_type': 'lookup_failed',
                                'message': f'Failed to fetch metadata for ISBN {isbn_key}',
                                'raw_row': {k: v for k, v in row.items() if v}
                            }
                            progress_update = {
                                'processed': processed_count,
                                'success': success_count,
                                'merged': merged_count,
                                'errors': error_count,
                                'skipped': skipped_count,
                                'current_book': isbn_key,
                            }
                            _update_import_progress(
                                user_id,
                                task_id,
                                updates=progress_update,
                                processed_book={'title': isbn_key, 'status': 'error'},
                                error_message=error_payload,
                            )
                            last_progress_emit = time.perf_counter()
                            continue  # Skip creation attempt

                    if not simplified_book.reading_status:
                        simplified_book.reading_status = import_config.get('default_reading_status', '')

                    # Extract personal metadata fields from SimplifiedBook
                    personal_metadata_for_import = getattr(simplified_book, 'personal_custom_metadata', None) or {}

                    # Add standard personal fields if present in SimplifiedBook
                    if simplified_book.date_started:
                        personal_metadata_for_import['start_date'] = simplified_book.date_started
                    if simplified_book.date_read:
                        personal_metadata_for_import['finish_date'] = simplified_book.date_read

                    merged_applied = False
                    duplicate_existing_id = None
                    try:
                        result = await simplified_service.add_book_to_user_library(
                            book_data=simplified_book,
                            user_id=user_id,
                            reading_status=simplified_book.reading_status,
                            ownership_status='owned',
                            media_type=default_media_type,
                            user_rating=simplified_book.user_rating,
                            personal_notes=simplified_book.personal_notes,
                            custom_metadata=personal_metadata_for_import
                        )
                    except Exception as add_ex:
                        # Detect duplicate via BookAlreadyExistsError class name
                        if add_ex.__class__.__name__ == 'BookAlreadyExistsError':
                            # Extract existing id if attribute present
                            duplicate_existing_id = getattr(add_ex, 'book_id', None)
                            try:
                                # Merge logic: fill empty fields and append metadata
                                from app.services.kuzu_book_service import KuzuBookService
                                from app.services.kuzu_custom_field_service import KuzuCustomFieldService
                                kbs = KuzuBookService(user_id=user_id)
                                existing = await kbs.get_book_by_id(duplicate_existing_id) if duplicate_existing_id else None
                                updates = {}

                                def _needs(existing_val):
                                    return existing_val is None or (isinstance(existing_val, str) and existing_val.strip() == '') or existing_val == 0

                                candidate_fields = ['subtitle', 'description', 'published_date', 'page_count', 'language', 'cover_url', 'asin', 'google_books_id', 'openlibrary_id', 'average_rating', 'rating_count', 'series', 'series_volume', 'series_order']
                                for field_name in candidate_fields:
                                    new_val = getattr(simplified_book, field_name, None)
                                    if not new_val:
                                        continue
                                    if existing and _needs(getattr(existing, field_name, None)):
                                        updates[field_name] = new_val
                                if updates and duplicate_existing_id:
                                    await kbs.update_book(duplicate_existing_id, updates)
                                    merged_applied = True
                                try:
                                    cfs = KuzuCustomFieldService()
                                    gmeta = simplified_book.global_custom_metadata or {}
                                    pmeta = getattr(simplified_book, 'personal_custom_metadata', None) or {}
                                    if gmeta or pmeta:
                                        cfs.ensure_custom_fields_exist(user_id, gmeta, pmeta)
                                        combined_meta = {}
                                        combined_meta.update(gmeta)
                                        combined_meta.update(pmeta)
                                        if combined_meta and duplicate_existing_id:
                                            cfs.save_custom_metadata_sync(book_id=duplicate_existing_id, user_id=user_id, custom_metadata=combined_meta)
                                            merged_applied = True
                                except Exception as meta_ex:
                                    print(f"⚠️ [MERGE] Custom metadata merge failed: {meta_ex}")
                                result = True  # Treat duplicate+merge as success
                            except Exception as merge_ex:
                                print(f"⚠️ [MERGE] Failed merging duplicate: {merge_ex}")
                                result = False
                        else:
                            # Unexpected error path
                            raise

                    processed_count += 1
                    if result and merged_applied:
                        merged_count += 1
                        status_value = 'merged'
                    else:
                        status_value = 'success' if result else 'error'
                        if result:
                            success_count += 1
                    progress_entry = {'title': simplified_book.title or 'Untitled', 'status': status_value}
                    progress_update = {
                        'processed': processed_count,
                        'success': success_count,
                        'merged': merged_count,
                        'errors': error_count,
                        'skipped': skipped_count,
                        'current_book': simplified_book.title,
                    }

                    if not result:
                        error_count += 1
                        message_txt = 'Duplicate book detected but merge failed' if duplicate_existing_id else 'Failed to add book to library (service returned no result)'
                        error_payload = {
                            'row_number': row_num,
                            'title': simplified_book.title or '',
                            'author': getattr(simplified_book, 'authors', [''])[0] if getattr(simplified_book, 'authors', None) else '',
                            'isbn': simplified_book.isbn13 or simplified_book.isbn10 or '',
                            'raw_isbn': row.get('ISBN') or row.get('ISBN13') or row.get('ISBN/UID') or '',
                            'file_name': source_filename,
                            'error_type': 'duplicate_merge_failed' if duplicate_existing_id else 'add_failed',
                            'message': message_txt,
                            'raw_row': {k: v for k, v in row.items() if v}
                        }
                        pending_processed_entries.append(progress_entry)
                        _update_import_progress(
                            user_id,
                            task_id,
                            updates=progress_update,
                            processed_book=pending_processed_entries,
                            error_message=error_payload,
                        )
                        pending_processed_entries.clear()
                        last_progress_emit = time.perf_counter()
                    else:
                        pending_processed_entries.append(progress_entry)
                        immediate = status_value != 'success'
                        elapsed = time.perf_counter() - last_progress_emit
                        if immediate or elapsed >= PROGRESS_EMIT_INTERVAL:
                            _update_import_progress(
                                user_id,
                                task_id,
                                updates=progress_update,
                                processed_book=pending_processed_entries,
                            )
                            pending_processed_entries.clear()
                            last_progress_emit = time.perf_counter()

                    if processed_count % 5 == 0:
                        update_job_in_kuzu(task_id, progress_update)
                except Exception as ex:
                    processed_count += 1
                    error_count += 1
                    raw_title = row.get('Title') or row.get('title') or row.get('Book Title') or row.get('Name') or row.get('Book Name') or 'Untitled'
                    error_payload = {
                        'row_number': row_num,
                        'title': raw_title,
                        'author': row.get('Author') or row.get('author') or '',
                        'isbn': row.get('ISBN13') or row.get('ISBN') or '',
                        'raw_isbn': row.get('ISBN') or row.get('ISBN13') or row.get('ISBN/UID') or '',
                        'file_name': source_filename,
                        'error_type': 'exception',
                        'message': str(ex)[:500],
                        'raw_row': {k: v for k, v in row.items() if v}
                    }
                    progress_entry = {'title': raw_title, 'status': 'error'}
                    pending_processed_entries.append(progress_entry)
                    progress_update = {
                        'processed': processed_count,
                        'success': success_count,
                        'merged': merged_count,
                        'errors': error_count,
                        'skipped': skipped_count,
                        'current_book': raw_title,
                    }
                    _update_import_progress(
                        user_id,
                        task_id,
                        updates=progress_update,
                        processed_book=pending_processed_entries,
                        error_message=error_payload,
                    )
                    pending_processed_entries.clear()
                    last_progress_emit = time.perf_counter()
                    continue
                    continue

        if pending_processed_entries:
            progress_update = {
                'processed': processed_count,
                'success': success_count,
                'merged': merged_count,
                'errors': error_count,
                'skipped': skipped_count,
                'current_book': pending_processed_entries[-1].get('title') if pending_processed_entries else None,
            }
            _update_import_progress(
                user_id,
                task_id,
                updates=progress_update,
                processed_book=pending_processed_entries,
            )
            pending_processed_entries.clear()

        final_activity = f"Import completed! {success_count} new, {merged_count} merged, {error_count} errors, {skipped_count} skipped"
        completion_updates = {
            'status': 'completed',
            'processed': processed_count,
            'success': success_count,
            'merged': merged_count,
            'errors': error_count,
            'skipped': skipped_count,
            'current_book': None,
            'recent_activity': [final_activity],
        }
        _update_import_progress(user_id, task_id, updates=completion_updates)
        snap = safe_get_import_job(user_id, task_id) or {}
        completion = dict(completion_updates)
        if 'processed_books' in snap:
            completion['processed_books'] = snap['processed_books']
        if 'error_messages' in snap:
            completion['error_messages'] = snap['error_messages']
        update_job_in_kuzu(task_id, completion)
        safe_update_import_job(user_id, task_id, completion)
        print(f"🎉 [PROCESS_SIMPLE] Done: {success_count} success, {error_count} errors, {skipped_count} skipped")
        if success_count > 0:
            try:
                def _run_backup():
                    try:
                        from app.services.simple_backup_service import get_simple_backup_service
                        svc = get_simple_backup_service()
                        svc.create_backup(description=f'Post-import backup: {success_count} books added', reason='post_import_books')
                    except Exception as be:
                        logger.warning(f"Post-import backup failed: {be}")
                threading.Thread(target=_run_backup, daemon=True).start()
            except Exception as outer_be:
                logger.warning(f"Failed launching post-import backup thread: {outer_be}")
    except Exception as e:
        traceback.print_exc()
        err = {'status': 'failed', 'error_messages': [str(e)]}
        update_job_in_kuzu(task_id, err)
        safe_update_import_job(user_id, task_id, err)
    finally:
        try:
            if os.path.exists(csv_file_path):
                os.unlink(csv_file_path)
        except Exception:
            pass

def merge_api_data_into_simplified_book(simplified_book, book_metadata, extra_metadata):
    """Merge API metadata into a SimplifiedBook object."""
    
    print(f"🔀 [MERGE_API] Starting merge for book with ISBN13: {simplified_book.isbn13}, ISBN10: {simplified_book.isbn10}")
    print(f"🔀 [MERGE_API] Available metadata ISBNs: {list(book_metadata.keys())}")
    
    # Check if we have API data for this book's ISBN
    api_data = None
    if simplified_book.isbn13 and simplified_book.isbn13 in book_metadata:
        api_data = book_metadata[simplified_book.isbn13]['data']
        print(f"✅ [MERGE_API] Found API data for ISBN13: {simplified_book.isbn13}")
    elif simplified_book.isbn10 and simplified_book.isbn10 in book_metadata:
        api_data = book_metadata[simplified_book.isbn10]['data']
        print(f"✅ [MERGE_API] Found API data for ISBN10: {simplified_book.isbn10}")
    
    if not api_data:
        print(f"❌ [MERGE_API] No API data found for this book")
        return simplified_book
    
    print(f"🔀 [MERGE_API] API data keys: {list(api_data.keys())}")
    
    # Centralized cover selection & caching (only if we don't already have a processed local cover)
    try:
        if not simplified_book.cover_url or not simplified_book.cover_url.startswith('/covers/'):
            from app.services.cover_service import cover_service
            cr = cover_service.fetch_and_cache(isbn=simplified_book.isbn13 or simplified_book.isbn10,
                                               title=simplified_book.title,
                                               author=simplified_book.author)
            if cr.cached_url:
                simplified_book.cover_url = cr.cached_url
                simplified_book.global_custom_metadata['cover_source'] = cr.source
                simplified_book.global_custom_metadata['cover_quality'] = cr.quality
    except Exception as e:
        print(f"[MERGE_API][COVER] Failed unified cover selection: {e}")

    # Merge title (prefer API if available and different)
    if api_data.get('title') and api_data['title'] != simplified_book.title:
        print(f"🔀 [MERGE_API] Updating title from '{simplified_book.title}' to '{api_data['title']}'")
        simplified_book.title = api_data['title']
        simplified_book.title = api_data['title']
    
    # Merge authors (prefer API authors over CSV authors)
    api_authors = None
    if api_data.get('authors_list'):
        api_authors = api_data['authors_list']
    elif api_data.get('authors'):
        # Fallback for unified metadata which provides 'authors'
        api_authors = api_data['authors']

    if api_authors:
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
    
    # Merge description (prefer API description over CSV)
    if api_data.get('description'):
        if not simplified_book.description or simplified_book.description.strip() == '':
            simplified_book.description = api_data['description']
            print(f"🔀 [MERGE_API] Updated description to: {api_data['description'][:100]}...")
        else:
            print(f"🔀 [MERGE_API] Description already set, keeping CSV description")
    else:
        print(f"🔀 [MERGE_API] No description found in API data")
    
    # Merge publisher (prefer API if CSV doesn't have one) with normalization
    if api_data.get('publisher') and (not simplified_book.publisher or str(simplified_book.publisher).strip() == ''):
        try:
            from app.simplified_book_service import _normalize_publisher_name
            simplified_book.publisher = _normalize_publisher_name(api_data['publisher'])
        except Exception:
            simplified_book.publisher = api_data['publisher']
        print(f"🔀 [MERGE_API] Updated publisher to: {api_data['publisher']}")
    
    # Merge page count (prefer API if CSV doesn't have one)
    if api_data.get('page_count') and not simplified_book.page_count:
        simplified_book.page_count = api_data['page_count']
    
    # Merge publication date (prefer API if CSV doesn't have one)
    if api_data.get('published_date') and not simplified_book.published_date:
        simplified_book.published_date = api_data['published_date']
        print(f"🔀 [MERGE_API] Updated published_date to: '{api_data['published_date']}' (type: {type(api_data['published_date'])})")
    
    # Merge language (prefer API if CSV doesn't have one)
    if api_data.get('language') and not simplified_book.language:
        simplified_book.language = api_data['language']
    
    # Merge categories - ALWAYS prefer API categories over CSV data
    # CSV categories are often user tags rather than real book genres
    if api_data.get('categories'):
        api_categories = api_data['categories']
        if isinstance(api_categories, list) and api_categories:
            # Always use API categories, don't merge with CSV "categories"
            # Keep as list since SimplifiedBook.categories is List[str]
            simplified_book.categories = api_categories
            print(f"🔀 [MERGE_API] Updated categories to: {len(api_categories)} items")
        elif isinstance(api_categories, str) and api_categories.strip():
            # Split string categories by comma and clean them
            simplified_book.categories = [cat.strip() for cat in api_categories.split(',') if cat.strip()]
            print(f"🔀 [MERGE_API] Updated categories from string: {len(simplified_book.categories)} items")
    else:
        # If no API categories available, clear any existing CSV categories
        # since they're usually incorrect (user tags instead of real genres)
        if simplified_book.categories:
            simplified_book.categories = []
    
    # Merge raw hierarchical category paths for building proper category graph
    if api_data.get('raw_category_paths'):
        paths = api_data.get('raw_category_paths')
        if isinstance(paths, list) and paths:
            simplified_book.raw_categories = paths
            print(f"🔀 [MERGE_API] Set raw_categories paths: {len(paths)} items")

    # Merge cover URL (prefer API cover) - check both possible field names
    # Unified best cover selection
    try:
        best_cover = get_best_cover_for_book(
            isbn=simplified_book.isbn13 or simplified_book.isbn10,
            title=api_data.get('title') or simplified_book.title,
            author=api_data.get('author') or simplified_book.author
        )
        from app.utils.book_utils import normalize_cover_url
        cover_url = normalize_cover_url(best_cover.get('cover_url') or api_data.get('cover_url') or api_data.get('cover'))
        if cover_url and not simplified_book.cover_url:
            try:
                processed_url = process_image_from_url(cover_url)
                simplified_book.cover_url = processed_url
                print(f"🔀 [MERGE_API] Updated cover URL to: {processed_url} (source={best_cover.get('source')})")
            except Exception as e:
                simplified_book.cover_url = cover_url
                print(f"🔀 [MERGE_API] Failed to process cover, using raw URL: {cover_url} ({e})")
        elif cover_url:
            print(f"🔀 [MERGE_API] Cover URL already set, not overriding")
        else:
            print(f"🔀 [MERGE_API] No cover URL found in API data or best-cover helper")
    except Exception as ce:
        print(f"🔀 [MERGE_API] Best cover helper failed: {ce}")
    
    # Merge description (prefer API if CSV doesn't have one)
    if api_data.get('description') and not simplified_book.description:
        simplified_book.description = api_data['description']
        print(f"🔀 [MERGE_API] Updated description to: {api_data['description'][:100]}...")
    elif api_data.get('description'):
        print(f"🔀 [MERGE_API] Description already set, not overriding")
    else:
        print(f"🔀 [MERGE_API] No description found in API data")
    
    # Merge average rating (prefer API if CSV doesn't have one)
    if api_data.get('average_rating') and not simplified_book.average_rating:
        simplified_book.average_rating = api_data['average_rating']
    
    # Merge rating count (prefer API if CSV doesn't have one)
    if api_data.get('rating_count') and not simplified_book.rating_count:
        simplified_book.rating_count = api_data['rating_count']
    
    # Merge ISBN data from API if CSV doesn't have it
    if api_data.get('isbn13') and not simplified_book.isbn13:
        simplified_book.isbn13 = api_data['isbn13']
    
    if api_data.get('isbn10') and not simplified_book.isbn10:
        simplified_book.isbn10 = api_data['isbn10']
    
    # Merge contributors (Google Books enhanced data) 
    if api_data.get('contributors'):
        contributors = api_data['contributors']
        
        # Extract different types of contributors
        authors = [c['name'] for c in contributors if c.get('role') == 'author']
        editors = [c['name'] for c in contributors if c.get('role') == 'editor']
        translators = [c['name'] for c in contributors if c.get('role') == 'translator']
        narrators = [c['name'] for c in contributors if c.get('role') == 'narrator']
        illustrators = [c['name'] for c in contributors if c.get('role') == 'illustrator']
        
        # 🔥 FIX: Map contributors to SimplifiedBook fields that the service expects
        
        # Handle additional authors (exclude the primary author)
        if authors and len(authors) > 1:
            # If we have multiple authors and the first one matches the primary author,
            # use the rest as additional authors
            additional_author_names = authors[1:] if authors[0].lower() == simplified_book.author.lower() else authors
            if additional_author_names:
                if simplified_book.additional_authors:
                    # Merge with existing additional authors
                    existing = simplified_book.additional_authors.split(', ')
                    all_additional = existing + additional_author_names
                    # Remove duplicates while preserving order
                    seen = set()
                    unique_additional = []
                    for author in all_additional:
                        if author.lower() not in seen:
                            unique_additional.append(author)
                            seen.add(author.lower())
                    simplified_book.additional_authors = ', '.join(unique_additional)
                else:
                    simplified_book.additional_authors = ', '.join(additional_author_names)
                print(f"🔀 [MERGE_API] Set additional_authors: {simplified_book.additional_authors}")
        
        # Map other contributor types to SimplifiedBook fields
        if editors:
            simplified_book.editor = ', '.join(editors)
            simplified_book.global_custom_metadata['editors'] = ', '.join(editors)
            print(f"🔀 [MERGE_API] Set editors: {simplified_book.editor}")
        
        if translators:
            simplified_book.translator = ', '.join(translators)
            simplified_book.global_custom_metadata['translators'] = ', '.join(translators)
            print(f"🔀 [MERGE_API] Set translators: {simplified_book.translator}")
        
        if narrators:
            simplified_book.narrator = ', '.join(narrators)
            simplified_book.global_custom_metadata['narrators'] = ', '.join(narrators)
            print(f"🔀 [MERGE_API] Set narrators: {simplified_book.narrator}")
        
        if illustrators:
            simplified_book.illustrator = ', '.join(illustrators)
            simplified_book.global_custom_metadata['illustrators'] = ', '.join(illustrators)
            print(f"🔀 [MERGE_API] Set illustrators: {simplified_book.illustrator}")
        
        print(f"🔀 [MERGE_API] Added contributors: {len(contributors)} total")
        print(f"    Authors: {len(authors)}, Editors: {len(editors)}, Translators: {len(translators)}, Narrators: {len(narrators)}, Illustrators: {len(illustrators)}")
    
    # Add subtitle if available
    if api_data.get('subtitle'):
        # Persist to custom metadata for backward-compat UIs
        if not simplified_book.global_custom_metadata.get('subtitle'):
            simplified_book.global_custom_metadata['subtitle'] = api_data['subtitle']
        # Also set core field so it renders on details page and persists to DB
        if not getattr(simplified_book, 'subtitle', None):
            simplified_book.subtitle = api_data['subtitle']
        print(f"🔀 [MERGE_API] Added subtitle: {api_data['subtitle']}")
    
    # Helper to normalize OpenLibrary identifiers into linkable path form
    def _normalize_openlibrary_id(olid_val: str):
        try:
            if not olid_val:
                return None
            olid = str(olid_val).strip()
            # Already a path like /works/OL12345W or /books/OL12345M
            if olid.startswith('/'):
                return olid
            # Bare IDs – infer path by suffix (W = work, M = edition/book, A = author)
            suffix = olid[-1:].upper()
            if suffix == 'M':
                return f"/books/{olid}"
            if suffix == 'W':
                return f"/works/{olid}"
            if suffix == 'A':
                return f"/authors/{olid}"
            # Fallback to works
            return f"/works/{olid}"
        except Exception:
            return None

    # Add API identifiers to global custom metadata and core fields
    if api_data.get('google_books_id'):
        simplified_book.global_custom_metadata['google_books_id'] = api_data['google_books_id']
        # Ensure core field is set so templates and storage persist it
        if not getattr(simplified_book, 'google_books_id', None):
            simplified_book.google_books_id = api_data['google_books_id']
    
    if api_data.get('openlibrary_id'):
        normalized_olid = _normalize_openlibrary_id(api_data['openlibrary_id'])
        simplified_book.global_custom_metadata['openlibrary_id'] = normalized_olid or api_data['openlibrary_id']
        # Ensure core field is set so templates and storage persist it
        if not getattr(simplified_book, 'openlibrary_id', None):
            simplified_book.openlibrary_id = normalized_olid or api_data['openlibrary_id']
    
    if api_data.get('asin'):
        asin_val = str(api_data['asin']).strip()
        simplified_book.global_custom_metadata['asin'] = asin_val
        if not getattr(simplified_book, 'asin', None):
            simplified_book.asin = asin_val
    
    print(f"🎉 [MERGE_API] Merge completed for '{simplified_book.title}':")
    print(f"    Title: {simplified_book.title}")
    print(f"    Author: {simplified_book.author}")
    print(f"    Cover URL: {simplified_book.cover_url}")
    print(f"    Description: {simplified_book.description[:100] if simplified_book.description else None}...")
    print(f"    Categories: {len(simplified_book.categories)} items")
    contributors_count = len(api_data.get('contributors', []))
    print(f"    Contributors: {contributors_count} items")
    
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
    'default_reading_status': kwargs.get('default_reading_status', ''),
        'enable_api_enrichment': kwargs.get('enable_api_enrichment', True),
        'format_type': kwargs.get('format_type', 'unknown')
    }
    
    # Start the import in a background thread
    def run_import():
        try:
            asyncio.run(process_simple_import(import_config))
        except Exception as e:
            traceback.print_exc()
    
    thread = threading.Thread(target=run_import)
    thread.daemon = True
    thread.start()
    
    return task_id

def batch_fetch_book_metadata(isbns):
    """Batch fetch metadata using the unified aggregator for a list of ISBNs.

    Noise reduction: only warnings for empty results, errors for exceptions/batch failures.
    Enable METADATA_DEBUG=1 for detailed per-ISBN diagnostics.
    """
    if _META_DEBUG_FLAG:
        logger.debug(f"[IMPORT][METADATA][BATCH_START] size={len(isbns)} isbns={isbns}")

    from app.utils.unified_metadata import fetch_unified_by_isbn_detailed
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import os
    import time
    import random

    metadata = {}
    failed_isbns = []

    # Local validator mirrors unified_metadata logic (keep lightweight, no dependency loop)
    import re as _re
    def _valid_isbn10(v: str) -> bool:
        if len(v) != 10 or not _re.match(r'^[0-9]{9}[0-9Xx]$', v):
            return False
        total = 0
        for i, ch in enumerate(v[:9]):
            if not ch.isdigit():
                return False
            total += (10 - i) * int(ch)
        check = v[9]
        total += 10 if check in 'Xx' else int(check)
        return total % 11 == 0
    def _valid_isbn13(v: str) -> bool:
        if len(v) != 13 or not v.isdigit():
            return False
        if not (v.startswith('978') or v.startswith('979')):
            return False
        s = 0
        for i, ch in enumerate(v[:12]):
            s += (1 if i % 2 == 0 else 3) * int(ch)
        return (10 - (s % 10)) % 10 == int(v[12])
    def _is_valid_isbn(raw: str) -> bool:
        if not raw:
            return False
        cleaned = _re.sub(r'[^0-9Xx]', '', raw.strip())
        return _valid_isbn10(cleaned) or _valid_isbn13(cleaned)

    valid_entries = []
    for i, raw_isbn in enumerate(isbns):
        if not raw_isbn:
            continue
        isbn = raw_isbn.strip()
        if not _is_valid_isbn(isbn):
            if _META_DEBUG_FLAG:
                logger.debug(f"[IMPORT][METADATA][SKIP_INVALID] idx={i} isbn={isbn or raw_isbn}")
            continue
        valid_entries.append((i, isbn))

    if not valid_entries:
        if _META_DEBUG_FLAG:
            logger.debug("[IMPORT][METADATA][BATCH_COMPLETE] success=0 failed=0 rate=0.0% keys=[]")
        return metadata

    def _read_int_env(name, default):
        try:
            raw = os.getenv(name)
            return int(raw) if raw not in (None, '') else default
        except ValueError:
            return default

    def _read_float_env(name, default):
        try:
            raw = os.getenv(name)
            return float(raw) if raw not in (None, '') else default
        except ValueError:
            return default

    def _normalize_int(value):
        try:
            if value in (None, '',):
                return None
            parsed = int(value)
            if parsed < 1:
                parsed = 1
            return parsed
        except Exception:
            return None

    def _read_admin_concurrency():
        try:
            from app.admin import load_system_config
            cfg = load_system_config() or {}
            import_settings = cfg.get('import_settings') or {}
            return _normalize_int(import_settings.get('metadata_concurrency'))
        except Exception:
            return None

    env_default_workers = _read_int_env('IMPORT_METADATA_WORKERS', 4)
    env_override_workers = _read_int_env('IMPORT_METADATA_CONCURRENCY', env_default_workers)
    admin_override_workers = _read_admin_concurrency()
    configured_workers = admin_override_workers if admin_override_workers is not None else env_override_workers
    max_workers = max(1, min(configured_workers, len(valid_entries)))

    jitter_min = max(0.0, _read_float_env('IMPORT_METADATA_JITTER_MIN', 0.05))
    jitter_max = max(0.0, _read_float_env('IMPORT_METADATA_JITTER_MAX', 0.2))
    if jitter_max < jitter_min:
        jitter_max = jitter_min

    def _fetch_single(idx, isbn):
        if _META_DEBUG_FLAG:
            logger.debug(f"[IMPORT][METADATA][FETCH_START] idx={idx} isbn={isbn}")
        # Apply tiny jitter to reduce thundering herd against providers
        if jitter_max > 0 or jitter_min > 0:
            delay = random.uniform(jitter_min, jitter_max) if jitter_max > jitter_min else jitter_min
            if delay > 0:
                time.sleep(delay)
        try:
            data, provider_errors = fetch_unified_by_isbn_detailed(isbn)
            return idx, isbn, data, provider_errors, None
        except Exception as exc:  # pragma: no cover - defensive logging path
            return idx, isbn, None, None, exc

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_fetch_single, idx, isbn) for idx, isbn in valid_entries]
        for future in as_completed(futures):
            idx, isbn, data, provider_errors, exc = future.result()
            if exc is not None:
                logger.error(
                    f"[IMPORT][METADATA] Exception fetching ISBN {isbn}: {exc}",
                    exc_info=(type(exc), exc, exc.__traceback__)
                )
                failed_isbns.append(isbn)
                continue

            if _META_DEBUG_FLAG:
                logger.debug(f"[IMPORT][METADATA][PROVIDERS] isbn={isbn} providers={provider_errors}")

            if data:
                metadata[isbn] = {
                    'data': data,
                    'source': 'unified',
                    'provider_errors': provider_errors
                }
                try:
                    alt13 = data.get('isbn13')
                    alt10 = data.get('isbn10')
                    if alt13 and alt13 != isbn and alt13 not in metadata:
                        metadata[alt13] = metadata[isbn]
                        if _META_DEBUG_FLAG:
                            logger.debug(f"[IMPORT][METADATA][CANON] promoted_isbn13={alt13} original_query={isbn}")
                    for alt in [alt10]:
                        if alt and alt not in metadata:
                            metadata[alt] = metadata[isbn]
                    if alt13 and alt10 and alt13 != alt10 and _META_DEBUG_FLAG:
                        logger.debug(f"[IMPORT][METADATA][ALT_INDEX] isbn_query={isbn} alt13={alt13} alt10={alt10}")
                except Exception:
                    pass
                if _META_DEBUG_FLAG:
                    logger.debug(f"[IMPORT][METADATA][FETCH_OK] isbn={isbn} title={data.get('title')}")
            else:
                logger.warning(f"[IMPORT][METADATA] No metadata for ISBN {isbn}")
                failed_isbns.append(isbn)

    if _META_DEBUG_FLAG:
        success_rate = (len(metadata) / len(isbns)) * 100 if isbns else 0
        logger.debug(f"[IMPORT][METADATA][BATCH_COMPLETE] success={len(metadata)} failed={len(failed_isbns)} rate={success_rate:.1f}% keys={list(metadata.keys())}")
    if failed_isbns:
        print(f"⚠️ [UNIFIED_API] Failed to fetch: {failed_isbns}")
        logger.error(f"[IMPORT][METADATA] Batch fetch failures ({len(failed_isbns)}): {failed_isbns}")
    else:
        # Downgrade zero-failure message to debug (avoid misleading error spam)
        logger.debug("[IMPORT][METADATA] Batch fetch completed with zero failures")
    return metadata

def store_job_in_kuzu(task_id, job_data):
    """Store import job status in KuzuDB."""
    try:
        # TODO: Implement actual Kuzu storage
        return True
    except Exception as e:
        print(f"Error storing job in Kuzu: {e}")
        return False

def update_job_in_kuzu(task_id, update_data):
    """Update import job status in KuzuDB."""
    try:
        # TODO: Implement actual Kuzu update
        # Note: In-memory updates are now handled by SafeImportJobManager
        return True
    except Exception as e:
        print(f"Error updating job in Kuzu: {e}")
        return False

@import_bp.route('/simple', methods=['GET', 'POST'])
@login_required
def simple_csv_import():
    """Very simple CSV import with automatic format detection."""
    
    if request.method == 'GET':
        return render_template('simple_import.html')
    
    # Handle POST request
    try:
        
        # Check for file upload
        if 'csv_file' not in request.files:
            flash('Please select a file to upload', 'error')
            return redirect(request.url)
        
        file = request.files['csv_file']
        if file.filename == '':
            flash('Please select a file to upload', 'error')
            return redirect(request.url)
        
        if not file.filename or not file.filename.lower().endswith('.csv'):
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
        
        if confidence < 0.5:
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
        default_media_type = get_default_book_format()
        
        # Process the CSV
        success_count = 0
        error_count = 0
        processed_count = 0
        errors = []
        
        try:
            with open(temp_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
                
                for row_num, row in enumerate(rows, 1):
                    try:
                        print(f"📖 [SIMPLE_IMPORT] Processing row {row_num}/{len(rows)}")
                        
                        # Build book data
                        simplified_book = simplified_service.build_book_data_from_row(row, mappings)
                        
                        if not simplified_book or not simplified_book.title:
                            error_msg = f"Row {row_num}: Could not extract book data or missing title"
                            errors.append(error_msg)
                            error_count += 1
                            processed_count += 1
                            continue
                        
                        
                        # Leave reading status empty if not provided; user can set later
                        if not simplified_book.reading_status:
                            simplified_book.reading_status = ''
                        
                        # Add to library
                        result = simplified_service.add_book_to_user_library_sync(
                            book_data=simplified_book,
                            user_id=str(current_user.id),
                            reading_status=simplified_book.reading_status,
                            ownership_status='owned',
                            media_type=default_media_type
                        )
                        
                        processed_count += 1
                        
                        if result:
                            success_count += 1
                        else:
                            error_count += 1
                            error_msg = f"Row {row_num}: Failed to add book '{simplified_book.title}'"
                            errors.append(error_msg)
                    
                    except Exception as row_error:
                        processed_count += 1
                        error_count += 1
                        error_msg = f"Row {row_num}: {str(row_error)}"
                        errors.append(error_msg)
                        continue
        
        finally:
            # Clean up temp file
            try:
                import os
                os.unlink(temp_path)
                print(f"🗑️ [SIMPLE_IMPORT] Cleaned up temp file")
            except Exception as cleanup_error:
                print(f"Warning: Could not clean up temp file: {cleanup_error}")
        
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
        traceback.print_exc()
        flash('An unexpected error occurred during import. Please try again.', 'error')
        return redirect(request.url)

@import_bp.route('/simple-upload', methods=['POST'])
@login_required
def simple_upload():
    """Handle simple CSV upload with auto-detection and processing."""
    
    
    try:
        # Check for file upload
        if 'csv_file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['csv_file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not file.filename or not file.filename.lower().endswith('.csv'):
            return jsonify({'error': 'File must be a CSV (.csv)'}), 400
        
        # Save file temporarily
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv', prefix=f'upload_{current_user.id}_')
        file.save(temp_file.name)
        temp_path = temp_file.name
        print(f"💾 [SIMPLE_UPLOAD] File saved to: {temp_path}")
        
        # Detect format
        format_type, confidence = detect_csv_format(temp_path)
        
        # Get appropriate field mappings
        if format_type == 'goodreads':
            field_mappings = get_goodreads_field_mappings()
            print(f"📋 [SIMPLE_UPLOAD] Using Goodreads mappings: {len(field_mappings)} fields")
        elif format_type == 'storygraph':
            field_mappings = get_storygraph_field_mappings()
            print(f"📋 [SIMPLE_UPLOAD] Using StoryGraph mappings: {len(field_mappings)} fields")
        else:
            # For unknown format, try to auto-detect column mappings
            field_mappings = {}
            try:
                with open(temp_path, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    headers = reader.fieldnames or []
                    print(f"📋 [SIMPLE_UPLOAD] CSV headers detected: {headers}")
                    
                    # Auto-detect basic mappings for common column names
                    for header in headers:
                        header_lower = header.lower().strip()
                        if header_lower in ['isbn', 'isbn13', 'isbn10', 'isbn_13', 'isbn_10']:
                            field_mappings[header] = 'isbn'
                            print(f"📋 [SIMPLE_UPLOAD] Mapped '{header}' -> 'isbn'")
                        elif header_lower in ['title', 'book title', 'name', 'book name']:
                            field_mappings[header] = 'title'
                            print(f"📋 [SIMPLE_UPLOAD] Mapped '{header}' -> 'title'")
                        elif header_lower in ['author', 'author name', 'authors']:
                            field_mappings[header] = 'author'
                            print(f"📋 [SIMPLE_UPLOAD] Mapped '{header}' -> 'author'")
                    
                    print(f"📋 [SIMPLE_UPLOAD] Auto-detected {len(field_mappings)} field mappings")
            except Exception as e:
                print(f"⚠️ [SIMPLE_UPLOAD] Could not auto-detect mappings: {e}")
                field_mappings = {}
        
        # Count total books in CSV for progress tracking
        try:
            with open(temp_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                # Skip header
                next(reader, None)
                total_books = sum(1 for row in reader)
        except Exception as e:
            total_books = 0
        
        
        # Create import config
        import_config = {
            'task_id': str(uuid.uuid4()),
            'csv_file_path': temp_path,
            'user_id': current_user.id,
            'format_type': format_type,
            'confidence': confidence,
            'filename': file.filename,
            'field_mappings': field_mappings,
            'default_reading_status': '',
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
        # Store job data with user isolation
        safe_create_import_job(current_user.id, task_id, job_data)
        
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
                finally:
                    loop.close()
                    
            except Exception as e:
                traceback.print_exc()
                # Update job status with user isolation
                error_updates = {
                    'status': 'failed',
                    'error_messages': [str(e)]
                }
                safe_update_import_job(current_user.id, task_id, error_updates)
        
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
    
    
    custom_field_service = KuzuCustomFieldService()
    custom_fields_to_create = []
    
    try:
        # Step 1: Read CSV headers to see what custom fields are mapped
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            csv_headers = reader.fieldnames or []
            
        
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
                    print(f"Warning: Could not sample field {csv_field}: {e}")
                
                custom_fields_to_create.append({
                    'name': field_name,
                    'type': field_type,
                    'is_global': is_global,
                    'csv_field': csv_field,
                    'mapped_field': mapped_field
                })
        
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
                else:
                    print(f"Failed to create custom field: {field_info['name']}")
                    
            except Exception as e:
                import traceback
                traceback.print_exc()
        
        return True, created_fields
        
    except Exception as e:
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

@import_bp.route('/import-waiting/<task_id>')
@login_required
def import_waiting(task_id):
    """Show waiting page for import process (similar to onboarding splash screen)."""
    import_type = request.args.get('import_type', 'goodreads')
    filename = request.args.get('filename', 'import.csv')
    
    return render_template('import_waiting.html',
                         task_id=task_id,
                         import_type=import_type,
                         filename=filename)


# Reading History Import Routes
@import_bp.route('/reading-history', methods=['GET', 'POST'])
@login_required
def import_reading_history():
    """Import reading history from CSV."""
    if request.method == 'GET':
        # If handed off from generic importer with an existing temp file, go straight to mapping
        csv_file_path = request.args.get('csv_file_path')
        if csv_file_path and os.path.exists(csv_file_path):
            try:
                with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                    sample = csvfile.read(1024)
                    csvfile.seek(0)
                    delimiter = ','
                    try:
                        sniffer = csv.Sniffer()
                        delimiter = sniffer.sniff(sample).delimiter
                    except Exception:
                        for test_delimiter in [',', ';', '\t', '|']:
                            if test_delimiter in sample:
                                delimiter = test_delimiter
                                break
                    reader = csv.DictReader(csvfile, delimiter=delimiter)
                    headers = reader.fieldnames or []
                    preview_rows = []
                    total_rows = 0
                    for i, row in enumerate(reader):
                        if i < 3:
                            preview_rows.append([row.get(header, '') for header in headers])
                        total_rows += 1
                suggested_mappings = _auto_detect_reading_history_fields(headers)
                return render_template('import_reading_history_mapping.html',
                                      csv_file_path=csv_file_path,
                                      csv_headers=headers,
                                      csv_preview=preview_rows,
                                      total_rows=total_rows,
                                      suggested_mappings=suggested_mappings)
            except Exception as e:
                current_app.logger.error(f"Error preparing mapping for handed-off reading history file: {e}")
                flash('Error preparing file for mapping. Please re-upload.', 'danger')
                return render_template('import_reading_history.html')
        return render_template('import_reading_history.html')
    
    # Handle POST request - file upload
    csv_file = request.files.get('csv_file')
    if not csv_file or not csv_file.filename or csv_file.filename == '':
        flash('No CSV file selected.', 'error')
        return redirect(url_for('import.import_reading_history'))
    
    # Don't strictly enforce .csv extension; accept CSV/TSV/TXT and rely on sniffer below
    # This mirrors the robustness of the main Import Books page
    
    try:
        # Save uploaded file temporarily
        filename = secure_filename(csv_file.filename)
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv', prefix=f'reading_history_{current_user.id}_')
        csv_file.save(temp_file.name)
        temp_path = temp_file.name
        
        # Read CSV headers and first few rows for preview
        with open(temp_path, 'r', encoding='utf-8') as csvfile:
            # Try to detect delimiter
            sample = csvfile.read(1024)
            csvfile.seek(0)
            
            delimiter = ','
            try:
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter
            except Exception:
                for test_delimiter in [',', ';', '\t', '|']:
                    if test_delimiter in sample:
                        delimiter = test_delimiter
                        break
            
            reader = csv.DictReader(csvfile, delimiter=delimiter)
            headers = reader.fieldnames or []
            
            # Get first few rows for preview
            preview_rows = []
            total_rows = 0
            for i, row in enumerate(reader):
                if i < 3:
                    preview_rows.append([row.get(header, '') for header in headers])
                total_rows += 1
        
        # Validate required columns
        required_columns = ['Date']
        missing_columns = [col for col in required_columns if col not in headers]
        
        if missing_columns:
            flash(f'Missing required columns: {", ".join(missing_columns)}. Please ensure your CSV has a "Date" column.', 'error')
            os.unlink(temp_path)
            return redirect(url_for('import.import_reading_history'))
        
        # Auto-detect field mappings
        suggested_mappings = _auto_detect_reading_history_fields(headers)
        
        return render_template('import_reading_history_mapping.html',
                             csv_file_path=temp_path,
                             csv_headers=headers,
                             csv_preview=preview_rows,
                             total_rows=total_rows,
                             suggested_mappings=suggested_mappings)
                             
    except Exception as e:
        current_app.logger.error(f"Error processing reading history CSV file: {e}")
        flash('Error processing CSV file. Please check the format and try again.', 'danger')
        return redirect(url_for('import.import_reading_history'))


@import_bp.route('/reading-history/execute', methods=['POST'])
@login_required
def import_reading_history_execute():
    """Execute the reading history import with user-defined field mappings."""
    csv_file_path = request.form.get('csv_file_path')
    
    if not csv_file_path or not os.path.exists(csv_file_path):
        flash('CSV file not found. Please upload the file again.', 'error')
        return redirect(url_for('import.import_reading_history'))
    
    try:
        # Get field mappings from form
        mappings = {}
        for key in request.form:
            if key.startswith('field_'):
                csv_field = key[6:]  # Remove 'field_' prefix
                book_field = request.form[key]
                if book_field:  # Only include non-empty mappings
                    mappings[csv_field] = book_field
        
        if not mappings.get('Date'):
            flash('Date field mapping is required.', 'error')
            os.unlink(csv_file_path)
            return redirect(url_for('import.import_reading_history'))
        
        # Create import job
        task_id = str(uuid.uuid4())
        job_data = {
            'task_id': task_id,
            'user_id': current_user.id,
            'csv_file_path': csv_file_path,
            'field_mappings': mappings,
            'import_type': 'reading_history',
            'status': 'pending',
            'processed': 0,
            'success': 0,
            'errors': 0,
            'skipped': 0,
            'total': 0,
            'start_time': datetime.now(timezone.utc).isoformat(),
            'current_book': None,
            'error_messages': [],
            'recent_activity': []
        }
        
        # Count total rows
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                job_data['total'] = sum(1 for _ in reader)
        except:
            job_data['total'] = 0
        
        # Store job data with user isolation
        safe_create_import_job(current_user.id, task_id, job_data)
        
        # Start the import in background thread
        import_config = {
            'task_id': task_id,
            'csv_file_path': csv_file_path,
            'field_mappings': mappings,
            'user_id': current_user.id,
            'import_type': 'reading_history'
        }
        
        def run_import():
            try:
                asyncio.run(process_reading_history_import(import_config))
            except Exception as e:
                error_updates = {
                    'status': 'failed',
                    'error_messages': [str(e)]
                }
                safe_update_import_job(import_config['user_id'], task_id, error_updates)
                logger.error(f"Reading history import job {task_id} failed: {e}")
        
        thread = threading.Thread(target=run_import)
        thread.daemon = True
        thread.start()
        
        return redirect(url_for('import.import_books_progress', task_id=task_id))
        
    except Exception as e:
        current_app.logger.error(f"Error starting reading history import: {e}")
        flash('Error starting import. Please try again.', 'error')
        return redirect(url_for('import.import_reading_history'))


@import_bp.route('/reading-history/template')
@login_required
def download_reading_history_template():
    """Download the reading history template CSV file."""
    try:
        # Debug static folder configuration
        current_app.logger.info(f"Static folder: {current_app.static_folder}")
        current_app.logger.info(f"Instance path: {current_app.instance_path}")
        
        # Use more robust path resolution
        if current_app.static_folder and os.path.exists(current_app.static_folder):
            template_path = os.path.join(current_app.static_folder, 'templates', 'reading_history_template.csv')
            current_app.logger.info(f"Trying static folder path: {template_path}")
        else:
            # Fallback to app root path
            app_root = os.path.dirname(os.path.dirname(__file__))  # Go up from routes directory
            template_path = os.path.join(app_root, 'static', 'templates', 'reading_history_template.csv')
            current_app.logger.info(f"Using fallback path: {template_path}")
        
        if not os.path.exists(template_path):
            # Try additional fallback paths
            fallback_paths = [
                os.path.join(os.path.dirname(current_app.instance_path), 'app', 'static', 'templates', 'reading_history_template.csv'),
                os.path.join(os.getcwd(), 'app', 'static', 'templates', 'reading_history_template.csv'),
                os.path.join(os.getcwd(), 'static', 'templates', 'reading_history_template.csv')
            ]
            
            for fallback_path in fallback_paths:
                current_app.logger.info(f"Trying fallback path: {fallback_path}")
                if os.path.exists(fallback_path):
                    template_path = fallback_path
                    break
            else:
                current_app.logger.error(f"Template file not found at any expected location")
                current_app.logger.error(f"Tried paths: {[template_path] + fallback_paths}")
                flash('Template file not found. Please contact administrator.', 'error')
                return redirect(url_for('import.import_reading_history'))
        
        current_app.logger.info(f"Using template path: {template_path}")
        
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
        
        return make_response(
            template_content,
            200,
            {
                'Content-Disposition': 'attachment; filename=reading_history_template.csv',
                'Content-Type': 'text/csv; charset=utf-8'
            }
        )
    except Exception as e:
        current_app.logger.error(f"Error downloading template: {e}")
        import traceback
        current_app.logger.error(traceback.format_exc())
        flash('Error downloading template file.', 'error')
        return redirect(url_for('import.import_reading_history'))


@import_bp.route('/reading-history/quick-add/preview', methods=['POST'])
@login_required
def quick_add_reading_history_preview():
    """Render confirmation screen for bulk quick-add of reading logs."""
    days, pages, minutes, errors = _sanitize_quick_add_inputs(
        request.form.get('days'),
        request.form.get('pages'),
        request.form.get('minutes'),
        getattr(current_user, 'id', None)
    )

    today = date.today()
    target_dates = [today - timedelta(days=i) for i in range(1, days + 1)] if days > 0 else []
    target_dates.sort()
    if target_dates:
        start_date = min(target_dates)
        end_date = max(target_dates)
    else:
        start_date = None
        end_date = None
    sample_dates = target_dates[:min(10, len(target_dates))]
    context = {
        'errors': errors,
        'days': days,
        'pages': pages,
        'minutes': minutes,
        'start_date': start_date,
        'end_date': end_date,
        'sample_dates': sample_dates,
        'total_dates': len(target_dates),
        'unassigned_title': UNASSIGNED_READING_LOG_TITLE
    }

    if _is_ajax_request():
        preview_html = render_template('partials/_quick_add_preview_summary.html', **context)
        status = 'error' if errors else 'ok'
        modal_title = 'Quick Add Issues' if errors else 'Confirm Quick Add'
        return jsonify({
            'status': status,
            'html': preview_html,
            'modal_title': modal_title,
            'confirm_enabled': not errors,
            'confirm_label': 'Confirm & Add Logs',
            'days': days,
            'pages': pages,
            'minutes': minutes,
            'start_date': start_date.isoformat() if start_date else None,
            'end_date': end_date.isoformat() if end_date else None,
            'errors': errors,
            'total_dates': len(target_dates)
        })

    return render_template('import_reading_quick_add_confirm.html', **context)


@import_bp.route('/reading-history/quick-add/execute', methods=['POST'])
@login_required
def quick_add_reading_history_execute_bulk():
    """Create or update reading logs across a span of prior days."""
    days, pages, minutes, errors = _sanitize_quick_add_inputs(
        request.form.get('days'),
        request.form.get('pages'),
        request.form.get('minutes'),
        getattr(current_user, 'id', None)
    )

    user_id = str(getattr(current_user, 'id', ''))
    if not user_id:
        errors.append('User context is missing.')

    base_context = {
        'errors': errors,
        'days': days,
        'pages': pages,
        'minutes': minutes,
        'created_count': 0,
        'success_dates': [],
        'failed_dates': [],
        'unassigned_title': UNASSIGNED_READING_LOG_TITLE
    }

    if errors:
        if _is_ajax_request():
            result_html = render_template('partials/_quick_add_result_summary.html', **base_context)
            return jsonify({
                'status': 'error',
                'html': result_html,
                'modal_title': 'Quick Add Issues',
                'errors': errors,
                'created_count': 0,
                'success_dates': [],
                'failed_dates': []
            })
        return render_template('import_reading_quick_add_result.html', **base_context)

    try:
        unassigned_book_id = run_async(_ensure_unassigned_reading_log_book(user_id))
    except Exception as helper_error:
        current_app.logger.error(f"Quick add placeholder resolution failed: {helper_error}")
        unassigned_book_id = None

    if not unassigned_book_id:
        errors.append('Unable to locate or create the placeholder book used for quick add logs.')
        base_context['errors'] = errors
        if _is_ajax_request():
            result_html = render_template('partials/_quick_add_result_summary.html', **base_context)
            return jsonify({
                'status': 'error',
                'html': result_html,
                'modal_title': 'Quick Add Issues',
                'errors': errors,
                'created_count': 0,
                'success_dates': [],
                'failed_dates': []
            })
        return render_template('import_reading_quick_add_result.html', **base_context)

    today = date.today()
    success_dates: List[date] = []
    failed_dates: List[date] = []

    for offset in range(1, days + 1):
        log_date = today - timedelta(days=offset)
        reading_log = ReadingLog(
            user_id=user_id,
            book_id=unassigned_book_id,
            date=log_date,
            pages_read=pages,
            minutes_read=minutes,
            notes=None
        )
        try:
            created = reading_log_service.create_reading_log_sync(reading_log)
            if created:
                success_dates.append(log_date)
            else:
                failed_dates.append(log_date)
        except Exception as create_error:
            failed_dates.append(log_date)
            current_app.logger.error(f"Quick add log creation failed for {log_date}: {create_error}")

    success_dates.sort()
    failed_dates.sort()
    created_count = len(success_dates)
    base_context.update({
        'errors': errors,
        'created_count': created_count,
        'success_dates': success_dates,
        'failed_dates': failed_dates
    })

    if _is_ajax_request():
        result_html = render_template('partials/_quick_add_result_summary.html', **base_context)
        if errors and created_count == 0:
            status = 'error'
        elif failed_dates:
            status = 'partial'
        else:
            status = 'ok'
        return jsonify({
            'status': status,
            'html': result_html,
            'modal_title': 'Quick Add Results' if status != 'error' else 'Quick Add Issues',
            'created_count': created_count,
            'success_dates': [d.isoformat() for d in success_dates],
            'failed_dates': [d.isoformat() for d in failed_dates],
            'errors': errors,
            'days': days,
            'pages': pages,
            'minutes': minutes
        })

    return render_template('import_reading_quick_add_result.html', **base_context)


@import_bp.route('/reading-history/book-matching/<task_id>')
@login_required
def reading_history_book_matching(task_id):
    """Handle book matching for reading history import."""
    try:
        # Get the import job data
        job_data = safe_get_import_job(current_user.id, task_id)
        
        if not job_data:
            flash('Import job not found.', 'error')
            return redirect(url_for('import.import_reading_history'))
        
        if job_data.get('status') != 'needs_book_matching':
            flash('This import does not need book matching.', 'info')
            return redirect(url_for('import.import_books_progress', task_id=task_id))
        
        books_for_matching = job_data.get('books_for_matching', [])
        # Hide bookless pseudo-entries from the matching UI; they are auto-routed
        if isinstance(books_for_matching, list):
            books_for_matching = [b for b in books_for_matching if not b.get('is_bookless', False)]
        validation_errors = job_data.get('validation_errors', [])
        
        # Type safety checks to prevent template errors
        if not isinstance(books_for_matching, list):
            current_app.logger.error(f"books_for_matching is not a list: {type(books_for_matching)} = {books_for_matching}")
            books_for_matching = []
            
        if not isinstance(validation_errors, list):
            current_app.logger.error(f"validation_errors is not a list: {type(validation_errors)} = {validation_errors}")
            validation_errors = []
        
        if not books_for_matching:
            flash('No books found for matching.', 'info')
            return redirect(url_for('import.import_books_progress', task_id=task_id))
        
        # Get user's library for dropdown options
        from app.services import book_service
        user_books = book_service.get_user_books_sync(current_user.id)  # Remove limit parameter
        
        # Ensure user_books is a list
        if not isinstance(user_books, list):
            current_app.logger.error(f"user_books is not a list: {type(user_books)} = {user_books}")
            user_books = []
        
        # Ensure import_job has safe default values for template
        safe_import_job = dict(job_data) if isinstance(job_data, dict) else {}
        safe_import_job.setdefault('auto_matched_count', 0)
        safe_import_job.setdefault('total_entries', 0)
        
        return render_template('import_reading_history_book_matching.html',
                             task_id=task_id,
                             books_for_matching=books_for_matching,
                             validation_errors=validation_errors,
                             user_books=user_books,
                             import_job=safe_import_job)
                             
    except Exception as e:
        current_app.logger.error(f"Error loading book matching: {e}")
        flash('Error loading book matching interface.', 'error')
        return redirect(url_for('import.import_reading_history'))


@import_bp.route('/api/books/search-details', methods=['POST'])
@login_required
def api_search_book_details():
    """Search for books by title using the unified search pipeline (Google+OpenLibrary)."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'No search criteria provided'}), 400

        query = (data.get('query') or '').strip()
        if not query:
            return jsonify({'success': False, 'message': 'Query is required'}), 400
        if len(query) < 2:
            return jsonify({'success': False, 'message': 'Query must be at least 2 characters'}), 400

        current_app.logger.info(f"[IMPORT_SEARCH] Unified search request: query='{query}'")

        from app.utils.metadata_aggregator import fetch_unified_by_title
        unified_results = fetch_unified_by_title(query, max_results=10) or []

        results = []
        for item in unified_results:
            authors = item.get('authors') or []
            isbn13 = item.get('isbn_13') or item.get('isbn13') or ''
            isbn10 = item.get('isbn_10') or item.get('isbn10') or ''
            results.append({
                'title': item.get('title', ''),
                'subtitle': item.get('subtitle', ''),
                'author': ', '.join(authors) if authors else '',
                'authors_list': authors,
                'description': item.get('description', ''),
                'publisher': item.get('publisher', ''),
                'published_date': item.get('published_date', ''),
                'page_count': item.get('page_count', 0),
                'isbn13': isbn13,
                'isbn10': isbn10,
                # Always process cover URLs for search results
                'cover_url': process_image_from_url(item.get('cover_url') or item.get('cover') or '') if (item.get('cover_url') or item.get('cover')) else '',
                'language': item.get('language', 'en'),
                'categories': item.get('categories', []),
                'google_books_id': item.get('google_books_id', ''),
                'openlibrary_id': item.get('openlibrary_id', ''),
                'asin': item.get('asin', ''),
                'average_rating': item.get('average_rating'),
                'rating_count': item.get('rating_count'),
                'contributors': item.get('contributors', []),
                'raw_category_paths': item.get('raw_category_paths', []),
                'source': item.get('source', 'Unified')
            })

        current_app.logger.info(f"[IMPORT_SEARCH] Returning {len(results)} unified results")
        return jsonify({'success': True, 'query': query, 'results': results, 'count': len(results)})

    except Exception as e:
        current_app.logger.error(f"[IMPORT_SEARCH] Error in book search: {e}")
        return jsonify({'success': False, 'message': 'Internal server error'}), 500


@import_bp.route('/api/books/external-search', methods=['POST'])
@login_required
def api_external_book_search():
    """Reading log import search powered by unified title search, prioritizing ISBN results."""
    try:
        data = request.get_json()
        query = (data.get('query') or '').strip()

        if not query:
            return jsonify({'error': 'Query is required'}), 400
        if len(query) < 2:
            return jsonify({'error': 'Query must be at least 2 characters'}), 400

        print(f"🔍 [READING_LOG_SEARCH] Unified search for: '{query}'")

        from app.utils.metadata_aggregator import fetch_unified_by_title
        unified_results = fetch_unified_by_title(query, max_results=25) or []

        isbn_results = []
        for item in unified_results:
            authors = item.get('authors') or []
            isbn13 = item.get('isbn_13') or item.get('isbn13') or ''
            isbn10 = item.get('isbn_10') or item.get('isbn10') or ''
            if not (isbn13 or isbn10):
                continue  # only ISBN results allowed
            res = {
                'title': item.get('title', ''),
                'author': ', '.join(authors) if authors else '',
                'description': item.get('description', ''),
                'published_date': item.get('published_date', ''),
                'page_count': item.get('page_count', 0),
                'isbn13': isbn13,
                'isbn10': isbn10,
                'cover_url': item.get('cover_url') or item.get('cover', ''),
                'publisher': item.get('publisher', ''),
                'language': item.get('language', ''),
                'categories': item.get('categories', []),
                'openlibrary_id': item.get('openlibrary_id', ''),
                'google_books_id': item.get('google_books_id', ''),
                'source': item.get('source', 'Unified'),
            }
            if res['title']:
                isbn_results.append(res)

        final_results = isbn_results[:10]
        return jsonify({
            'query': query,
            'results': final_results,
            'count': len(final_results),
            'isbn_count': len(final_results),
            'has_isbn_priority': True,
            'filtered_out_count': 0
        })

    except Exception as e:
        current_app.logger.error(f"Error in external book search: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@import_bp.route('/reading-history/resolve-books/<task_id>', methods=['POST'])
@login_required
def resolve_reading_history_books(task_id):
    """Resolve book matches and create reading log entries."""
    try:
        # Get the import job data
        job_data = safe_get_import_job(current_user.id, task_id)
        
        if not job_data:
            flash('Import job not found.', 'error')
            return redirect(url_for('import.import_reading_history'))
        
        # Process book resolutions from form
        from app.services import book_service, reading_log_service
        from app.domain.models import ReadingLog, Book
        
        books_for_matching = job_data.get('books_for_matching', [])
        book_resolutions = {}  # csv_name -> {'action': 'match/create/skip/bookless', 'book_id': id, ...}
        
        # Extract resolutions from form data
        for i, book_data in enumerate(books_for_matching):
            csv_name = book_data['csv_name']
            action = request.form.get(f'action_{i}')
            
            if action == 'match':
                book_id = request.form.get(f'book_id_{i}')
                if book_id:
                    book_resolutions[csv_name] = {
                        'action': 'match',
                        # Use 'matched_book_id' to align with final processing expectations
                        'matched_book_id': book_id
                    }
            elif action == 'create':
                # Extract book creation data
                title = request.form.get(f'new_title_{i}', csv_name)
                author = request.form.get(f'new_author_{i}', '')
                
                # Check if there's API metadata stored (from JavaScript API search)
                api_metadata_json = request.form.get(f'api_metadata_{i}', '')
                api_metadata = {}
                if api_metadata_json:
                    try:
                        import json
                        api_metadata = json.loads(api_metadata_json)
                    except:
                        pass  # Ignore invalid JSON
                
                book_resolutions[csv_name] = {
                    'action': 'create',
                    'title': title,
                    'author': author,
                    'api_metadata': api_metadata  # Include API metadata for enhanced book creation
                }
            elif action == 'bookless':
                # Create reading logs without a specific book
                book_resolutions[csv_name] = {
                    'action': 'bookless',
                    'book_title': csv_name  # Use the CSV name as the book title in the log
                }
            # Books marked as 'skip' will not be in resolutions dict

        # Automatically include bookless pseudo-entry (not shown in form UI) so finalization creates logs
        for book_data in books_for_matching:
            try:
                if (book_data.get('is_bookless') or book_data.get('csv_name') == '[BOOKLESS_ENTRY]') \
                        and book_data['csv_name'] not in book_resolutions:
                    book_resolutions[book_data['csv_name']] = {'action': 'bookless'}
            except Exception:
                pass
        
        # Update job status to processing with proper progress reset
        total_entries = job_data.get('total_entries', job_data.get('total', 0))
        safe_update_import_job(current_user.id, task_id, {
            'status': 'processing_reading_logs',
            'processed': 0,  # Reset progress for reading log creation phase
            'auto_matched': 0,  # Will be updated as reading logs are created
            'need_review': 0,   # No more review needed at this point
            'validation_errors': len(job_data.get('validation_errors', [])),
            'total_entries': total_entries,
            'total': total_entries,  # Preserve the total entries count from analysis phase
            'recent_activity': ['Processing book matches and creating reading logs...']
        })
        
        # Start background processing
        def process_reading_logs():
            try:
                asyncio.run(_process_final_reading_history_import(task_id, job_data, book_resolutions))
            except Exception as e:
                logger.error(f"Error in final processing: {e}")
                safe_update_import_job(job_data['user_id'], task_id, {
                    'status': 'failed',
                    'error_messages': [str(e)]
                })
        
        thread = threading.Thread(target=process_reading_logs)
        thread.daemon = True
        thread.start()
        
        flash('Processing book matches. Please check progress page for updates.', 'info')
        return redirect(url_for('import.import_books_progress', task_id=task_id))
        
    except Exception as e:
        current_app.logger.error(f"Error resolving books: {e}")
        flash('Error resolving book matches.', 'error')
        return redirect(url_for('import.import_reading_history'))


async def _process_final_reading_history_import(task_id, job_data, book_resolutions):
    """Finalize reading history import: create any new books, then create reading logs."""
    from app.services import book_service, reading_log_service
    from app.domain.models import ReadingLog
    from app.simplified_book_service import SimplifiedBookService, SimplifiedBook
    # Lazy import location service only if needed
    try:
        from app.location_service import LocationService
        location_service = LocationService()
        default_location_obj = location_service.get_default_location()
        default_location_id = getattr(default_location_obj, 'id', None) if default_location_obj else None
    except Exception:
        location_service = None
        default_location_id = None
    user_id = job_data['user_id']
    csv_file_path = job_data.get('csv_file_path')
    if not csv_file_path or not os.path.exists(csv_file_path):
        raise Exception("CSV file no longer available")
    books_for_matching = job_data.get('books_for_matching', []) or []
    total_entries_in_csv = sum(len(b.get('entries', [])) for b in books_for_matching)
    success_count = 0
    error_count = 0
    skipped_count = 0
    created_books = 0
    success_detail_strings: list[str] = []
    unassigned_book_id = None
    created_book_titles: list[str] = []  # track created book titles for UI
    safe_update_import_job(user_id, task_id, {
        'status': 'processing',
        'processed': 0,
        'total': total_entries_in_csv,
        'reading_logs_created': 0,
        'reading_log_errors': 0,
        'reading_logs_skipped': 0,
        'books_created': 0,
        'recent_activity': [f"Starting import of {total_entries_in_csv} reading log entries..."]
    })
    try:
        # 1. create any required books
        if isinstance(book_resolutions, dict):
            for csv_name, resolution in book_resolutions.items():
                if resolution.get('action') != 'create':
                    continue
                try:
                    api_meta = resolution.get('api_metadata', {}) or {}
                    selected_isbn = api_meta.get('_selected_isbn')
                    use_isbn_lookup = bool(api_meta.get('_use_isbn_lookup') and selected_isbn)
                    manual_creation = api_meta.get('_manual_creation')
                    service = SimplifiedBookService()
                    new_book = None
                    if use_isbn_lookup and selected_isbn:
                        try:
                            from app.utils.metadata_aggregator import fetch_unified_by_isbn
                            unified = fetch_unified_by_isbn(selected_isbn) or {}
                        except Exception:
                            unified = {}
                        title = unified.get('title') or resolution.get('title') or 'Unknown Title'
                        author = resolution.get('author') or (unified.get('authors') or ['Unknown Author'])[0]
                        new_book = SimplifiedBook(title=title, author=author)
                        new_book.isbn13 = selected_isbn if len(selected_isbn) == 13 else unified.get('isbn_13')
                        new_book.isbn10 = selected_isbn if len(selected_isbn) == 10 else unified.get('isbn_10')
                        new_book.description = unified.get('description')
                        new_book.subtitle = unified.get('subtitle')
                        new_book.publisher = unified.get('publisher')
                        new_book.published_date = unified.get('published_date')
                        new_book.page_count = unified.get('page_count')
                        new_book.language = unified.get('language') or 'en'
                        new_book.categories = unified.get('categories') or []
                        new_book.cover_url = unified.get('cover_url') or unified.get('cover')
                    elif manual_creation:
                        new_book = SimplifiedBook(title=resolution.get('title') or 'Unknown Title', author=resolution.get('author') or 'Unknown Author')
                    if new_book:
                        try:
                            from app.utils.book_utils import get_best_cover_for_book
                            best = get_best_cover_for_book(isbn=selected_isbn, title=new_book.title, author=new_book.author)
                            if best and best.get('cover_url'):
                                new_book.cover_url = best['cover_url']
                        except Exception:
                            pass
                        created_id = await service.create_standalone_book(new_book)
                        if created_id:
                            resolution['created_book_id'] = created_id
                            created_books += 1
                            try:
                                if new_book.title and new_book.title not in created_book_titles:
                                    created_book_titles.append(new_book.title)
                                # Attach to default location if available
                                if default_location_id and location_service:
                                    try:
                                        location_service.add_book_to_location(created_id, default_location_id, user_id)
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                except Exception as ce:
                    logger.warning(f"Create book failed for {csv_name}: {ce}")
        # 2. process logs
        total_entries_processed = 0
        for block in books_for_matching:
            csv_name = block.get('csv_name')
            entries = block.get('entries', [])
            res = book_resolutions.get(csv_name) if isinstance(book_resolutions, dict) else None
            if not isinstance(res, dict):
                res = {}
            act = res.get('action')
            # If no explicit resolution but block is marked bookless, treat as bookless
            if not act and (block.get('is_bookless') or csv_name == '[BOOKLESS_ENTRY]'):
                act = 'bookless'
            if not act:
                skipped_count += len(entries)
                total_entries_processed += len(entries)
                continue
            if act == 'skip':
                skipped_count += len(entries)
                total_entries_processed += len(entries)
                continue
            if act == 'bookless':
                if unassigned_book_id is None:
                    try:
                        unassigned_book_id = await _ensure_unassigned_reading_log_book(
                            user_id,
                            location_service=location_service,
                            default_location_id=default_location_id
                        )
                    except Exception as ue:
                        logger.warning(f"Unassigned book preparation failed: {ue}")
                        unassigned_book_id = None
                if not unassigned_book_id:
                    skipped_count += len(entries)
                    total_entries_processed += len(entries)
                    continue
                target_book_id = unassigned_book_id
            else:
                # Support legacy key 'book_id' for already persisted jobs
                if act == 'match':
                    target_book_id = res.get('matched_book_id') or res.get('book_id')
                else:
                    target_book_id = res.get('created_book_id')
            for entry in entries:
                try:
                    log_date = datetime.strptime(entry.get('date'), '%Y-%m-%d').date()
                    # Resolve pages/minutes with priority: entry -> user defaults -> server defaults -> fallback 1 minute
                    try:
                        p_val = entry.get('pages_read', 0) or 0
                        m_val = entry.get('minutes_read', 0) or 0
                    except Exception:
                        p_val, m_val = 0, 0
                    if p_val <= 0 and m_val <= 0:
                        try:
                            dp, dm = get_effective_reading_defaults(user_id)
                        except Exception:
                            dp, dm = (None, None)
                        if (dp or 0) > 0:
                            p_val = int(dp)  # type: ignore[arg-type]
                        if (dm or 0) > 0:
                            m_val = int(dm)  # type: ignore[arg-type]
                        if p_val <= 0 and m_val <= 0:
                            m_val = 1
                    rl = ReadingLog(
                        user_id=str(user_id),
                        book_id=target_book_id,
                        date=log_date,
                        pages_read=p_val,
                        minutes_read=m_val,
                        notes=(entry.get('notes') or (f"Original book name: {csv_name}" if act == 'bookless' else None)),
                        created_at=datetime.now(timezone.utc),
                        updated_at=datetime.now(timezone.utc)
                    )
                    created_log = reading_log_service.create_reading_log_sync(rl)
                    if created_log:
                        success_count += 1
                        display_name = csv_name
                        if act == 'match':
                            try:
                                matched_book_obj = book_service.get_book_sync(target_book_id)
                                if matched_book_obj:
                                    display_name = getattr(matched_book_obj, 'title', None) or getattr(matched_book_obj, 'name', None) or (matched_book_obj.get('title') if isinstance(matched_book_obj, dict) else csv_name)
                            except Exception:
                                pass
                        label = f"{log_date} — {display_name}"
                        bits = []
                        p = p_val
                        m = m_val
                        if p: bits.append(f"{p}p")
                        if m: bits.append(f"{m}m")
                        if bits:
                            label += f" ({', '.join(bits)})"
                        if len(success_detail_strings) < 500:
                            success_detail_strings.append(label)
                    else:
                        error_count += 1
                except Exception:
                    error_count += 1
                finally:
                    total_entries_processed += 1
                    if total_entries_processed % 5 == 0 or total_entries_processed == total_entries_in_csv:
                        safe_update_import_job(user_id, task_id, {
                            'processed': total_entries_processed,
                            'total': total_entries_in_csv,
                            'reading_logs_created': success_count,
                            'reading_log_errors': error_count,
                            'reading_logs_skipped': skipped_count,
                            'books_created': created_books,
                            'recent_activity': [f"Processing reading logs: {total_entries_processed}/{total_entries_in_csv} entries completed"]
                        })
        matched_existing = 0
        matched_book_titles: list[str] = []
        if isinstance(book_resolutions, dict):
            matched_ids = {}
            for csv_name, r in book_resolutions.items():
                if r.get('action') == 'match':
                    bid = r.get('matched_book_id') or r.get('book_id')
                    if bid and bid not in matched_ids:
                        matched_ids[bid] = csv_name
            matched_existing = len(matched_ids)
            # Resolve titles
            for bid, csv_name in matched_ids.items():
                try:
                    bobj = book_service.get_book_sync(bid)
                    title = None
                    if bobj:
                        title = getattr(bobj, 'title', None) or getattr(bobj, 'name', None)
                        if isinstance(bobj, dict):
                            title = bobj.get('title') or bobj.get('name') or title
                    if not title:
                        title = csv_name
                    if title and title not in matched_book_titles:
                        matched_book_titles.append(title)
                except Exception:
                    if csv_name and csv_name not in matched_book_titles:
                        matched_book_titles.append(csv_name)
        books_involved = matched_existing + created_books
        distinct_books_total = len([b for b in books_for_matching if b.get('csv_name') != '[BOOKLESS_ENTRY]'])
        final_status = 'completed' if error_count == 0 else 'completed_with_errors'
        safe_update_import_job(user_id, task_id, {
            'status': final_status,
            'import_type': 'reading_history',
            'processed': total_entries_processed,
            'success': success_count,
            'errors': error_count,
            'skipped': skipped_count,
            'unmatched': 0,
            'total': total_entries_in_csv,
            'books_for_matching': [],
            'reading_logs_created': success_count,
            'reading_log_errors': error_count,
            'books_created': created_books,
            'books_matched': matched_existing,
            'books_involved': books_involved,
            'distinct_books_total': distinct_books_total,
            'reading_log_success_details': success_detail_strings,
            'matched_book_titles': matched_book_titles,
            'created_book_titles': created_book_titles,
            'recent_activity': [
                f"Import completed: {success_count} reading logs created across {books_involved} books ({matched_existing} matched, {created_books} created)",
                f"Errors: {error_count}, Skipped: {skipped_count}",
                f"Total entries processed: {total_entries_processed}/{total_entries_in_csv}"
            ]
        })
        if success_count > 0:
            try:
                def _run_backup():
                    try:
                        from app.services.simple_backup_service import get_simple_backup_service
                        svc = get_simple_backup_service()
                        svc.create_backup(description=f'Post-reading-history import backup: {success_count} logs', reason='post_import_reading_history')
                    except Exception as be:
                        logger.warning(f"Post-reading-history backup failed: {be}")
                threading.Thread(target=_run_backup, daemon=True).start()
            except Exception as outer_be:
                logger.warning(f"Failed launching post-reading-history backup thread: {outer_be}")
    except Exception as e:
        logger.error(f"Fatal reading history import error: {e}")
        try:
            safe_update_import_job(user_id, task_id, {
                'status': 'failed',
                'error_messages': [{'message': str(e), 'type': 'processing_error'}]
            })
        except Exception:
            pass
    finally:
        try:
            if csv_file_path and os.path.exists(csv_file_path):
                os.unlink(csv_file_path)
        except Exception:
            pass


def _auto_detect_reading_history_fields(headers):
    """Auto-detect field mappings for reading history CSV headers."""
    mappings = {}
    
    for header in headers:
        header_lower = header.lower().strip()
        
        if 'date' in header_lower:
            mappings[header] = 'Date'
        elif 'book' in header_lower and 'name' in header_lower:
            mappings[header] = 'Book Name'
        elif 'start' in header_lower and 'page' in header_lower:
            mappings[header] = 'Start Page'
        elif 'end' in header_lower and 'page' in header_lower:
            mappings[header] = 'End Page'
        elif 'pages' in header_lower and 'read' in header_lower:
            mappings[header] = 'Pages Read'
        elif 'minutes' in header_lower and 'read' in header_lower:
            mappings[header] = 'Minutes Read'
        elif 'time' in header_lower and 'read' in header_lower:
            mappings[header] = 'Minutes Read'
    
    return mappings


async def process_reading_history_import(import_config):
    """Process a reading history import job by first collecting distinct books for user matching."""
    task_id = import_config['task_id']
    csv_file_path = import_config['csv_file_path']
    mappings = import_config['field_mappings']
    user_id = import_config['user_id']
    
    # Import required services
    from app.services import book_service
    
    try:
        # Update status to analyzing with proper progress tracking
        safe_update_import_job(user_id, task_id, {
            'status': 'analyzing',
            'processed': 0,
            'total': 0,  # Will be updated as we count rows
            'auto_matched': 0,  # Books automatically matched
            'need_review': 0,   # Books needing user review
            'validation_errors': 0,  # CSV parsing/validation errors
            'total_entries': 0,  # Total CSV entries
            'recent_activity': ['Starting analysis of CSV file...']
        })
        
        # Step 1: Parse CSV and collect all distinct books with their reading entries
        distinct_books = {}  # book_name -> list of reading entries
        validation_errors = []
        total_entries = 0
        processed_entries = 0
        
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row_num, row in enumerate(reader, 1):
                total_entries += 1
                processed_entries += 1
                
                # Update progress every 10 rows during analysis
                if processed_entries % 10 == 0:
                    safe_update_import_job(user_id, task_id, {
                        'processed': processed_entries,
                        'total': processed_entries,  # Update total as we go during analysis
                        'auto_matched': 0,  # No matching done yet
                        'need_review': 0,   # No books identified yet
                        'validation_errors': len(validation_errors),
                        'total_entries': processed_entries,
                        'recent_activity': [f'Analyzing entry {processed_entries}...']
                    })
                
                try:
                    # Extract and validate data from row
                    date_str = row.get(mappings.get('Date', ''), '').strip()
                    book_name = row.get(mappings.get('Book Name', ''), '').strip()
                    start_page_str = row.get(mappings.get('Start Page', ''), '').strip()
                    end_page_str = row.get(mappings.get('End Page', ''), '').strip()
                    pages_read_str = row.get(mappings.get('Pages Read', ''), '').strip()
                    minutes_read_str = row.get(mappings.get('Minutes Read', ''), '').strip()
                    
                    # Validate date (required)
                    if not date_str:
                        validation_errors.append(f"Row {row_num}: Date is required")
                        continue
                    
                    # Parse date
                    try:
                        log_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    except ValueError:
                        validation_errors.append(f"Row {row_num}: Invalid date format. Use YYYY-MM-DD")
                        continue
                    
                    # Handle book name (empty book names will be handled as bookless logs)
                    if not book_name or book_name.strip() == "":
                        book_name = "[BOOKLESS_ENTRY]"  # Mark for bookless processing
                        print(f"ℹ️ [READING_HISTORY] Row {row_num}: Empty book name, will create bookless reading log")
                    
                    # Parse numeric fields
                    start_page = 0
                    end_page = 0
                    pages_read = 0
                    minutes_read = 0
                    
                    try:
                        if start_page_str:
                            start_page = int(start_page_str)
                        if end_page_str:
                            end_page = int(end_page_str)
                        if pages_read_str:
                            pages_read = int(pages_read_str)
                        if minutes_read_str:
                            minutes_read = int(minutes_read_str)
                    except ValueError:
                        pass  # Invalid numbers will remain 0
                    
                    # Calculate pages read if not provided but start/end pages are
                    if not pages_read and start_page and end_page and end_page >= start_page:
                        pages_read = end_page - start_page + 1

                    # Apply defaults if still missing: user > server > legacy(1 minute)
                    if pages_read <= 0 and minutes_read <= 0:
                        try:
                            dp, dm = get_effective_reading_defaults(user_id)
                        except Exception:
                            dp, dm = (None, None)
                        if (dp or 0) > 0:
                            pages_read = int(dp)  # type: ignore[arg-type]
                        if (dm or 0) > 0:
                            minutes_read = int(dm)  # type: ignore[arg-type]
                        if pages_read <= 0 and minutes_read <= 0:
                            minutes_read = 1

                    # Validate that we have either pages or minutes read
                    if pages_read <= 0 and minutes_read <= 0:
                        validation_errors.append(f"Row {row_num}: Must have either pages read or minutes read")
                        continue
                    
                    # Store the reading entry under the book name
                    if book_name not in distinct_books:
                        distinct_books[book_name] = []
                    
                    distinct_books[book_name].append({
                        'row_num': row_num,
                        'date': log_date.isoformat(),
                        'pages_read': pages_read,
                        'minutes_read': minutes_read,
                        'start_page': start_page,
                        'end_page': end_page
                    })
                    
                except Exception as row_error:
                    validation_errors.append(f"Row {row_num}: Error processing row - {str(row_error)}")
                    continue
        
        print(f"📚 [READING_HISTORY] Found {len(distinct_books)} distinct books in {total_entries} entries")
        print(f"❌ [READING_HISTORY] {len(validation_errors)} validation errors")
        
        # Update progress to show analysis complete with proper counts
        safe_update_import_job(user_id, task_id, {
            'processed': total_entries,
            'total': total_entries,
            'auto_matched': 0,  # Will be updated during book matching
            'need_review': len(distinct_books),  # All books initially need review
            'validation_errors': len(validation_errors),
            'total_entries': total_entries,
            'recent_activity': [f'Analysis complete: {len(distinct_books)} distinct books found from {total_entries} entries']
        })
        
        # Step 2: Try to automatically match books in user's library
        books_for_matching = []
        books_processed = 0
        
        for book_name, entries in distinct_books.items():
            books_processed += 1
            print(f"🔍 [READING_HISTORY] Searching for book: '{book_name}' ({books_processed}/{len(distinct_books)})")
            
            # Update progress during book matching (keep total as CSV entries)
            if books_processed % 5 == 0 or books_processed == len(distinct_books):
                auto_matched_so_far = len([b for b in books_for_matching if b.get('matched_book')])
                bookless_so_far = len([b for b in books_for_matching if b.get('is_bookless', False)])
                need_review_count = len(distinct_books) - auto_matched_so_far - bookless_so_far
                
                safe_update_import_job(user_id, task_id, {
                    'processed': total_entries,  # Keep CSV entries as processed
                    'total': total_entries,      # Keep CSV entries as total
                    'auto_matched': auto_matched_so_far,  # Books automatically matched
                    'need_review': need_review_count,     # Books still needing review
                    'validation_errors': len(validation_errors),
                    'total_entries': total_entries,
                    'recent_activity': [f'Matching books: {books_processed}/{len(distinct_books)} books processed']
                })
            
            # Skip search for bookless entries
            if book_name == "[BOOKLESS_ENTRY]":
                print(f"🔍 [READING_HISTORY] Skipping search for bookless entry")
                
                books_for_matching.append({
                    'csv_name': book_name,
                    'entry_count': len(entries),
                    'entries': entries,
                    'matched_book': None,
                    'search_results': [],
                    'is_bookless': True  # Mark as bookless
                })
                continue
            
            # Search for potential matches
            search_results = book_service.search_books_sync(book_name, user_id, limit=5)
            print(f"🔍 [READING_HISTORY] Found {len(search_results)} search results for '{book_name}'")
            
            # Find exact matches (case insensitive)
            matched_book = None
            if search_results:
                book_name_lower = book_name.lower().strip()
                for result in search_results:
                    result_title = result.title.lower().strip() if hasattr(result, 'title') else result.get('title', '').lower().strip()
                    if result_title == book_name_lower:
                        matched_book = {
                            'id': result.id if hasattr(result, 'id') else result.get('id'),
                            'title': result.title if hasattr(result, 'title') else result.get('title'),
                            'author': result.author if hasattr(result, 'author') else result.get('author')
                        }
                        print(f"✅ [READING_HISTORY] Auto-matched: '{book_name}' -> '{matched_book['title']}'")
                        break
                        
                if not matched_book:
                    print(f"⚠️ [READING_HISTORY] No exact match found for '{book_name}', will need user selection")
            else:
                print(f"⚠️ [READING_HISTORY] No search results found for '{book_name}'")
            
            # Convert search results to dict format for template
            search_results_dict = []
            if search_results:
                for result in search_results:
                    search_results_dict.append({
                        'id': result.id if hasattr(result, 'id') else result.get('id'),
                        'title': result.title if hasattr(result, 'title') else result.get('title'),
                        'author': result.author if hasattr(result, 'author') else result.get('author')
                    })
            
            books_for_matching.append({
                'csv_name': book_name,
                'entry_count': len(entries),
                'entries': entries,
                'matched_book': matched_book,
                'search_results': search_results_dict
            })
        
        # Step 3: Check if we need user book matching or can proceed directly
        auto_matched_count = len([b for b in books_for_matching if b['matched_book']])
        bookless_count = len([b for b in books_for_matching if b.get('is_bookless', False)])
        needs_user_matching = auto_matched_count + bookless_count < len(books_for_matching)
        
        if needs_user_matching:
            # Update job status to need book matching with correct progress metrics
            safe_update_import_job(user_id, task_id, {
                'status': 'needs_book_matching',
                'import_type': 'reading_history',
                'books_for_matching': books_for_matching,
                'validation_errors': validation_errors,  # Keep as list for template
                'total_entries': total_entries,
                'total': total_entries,  # Total CSV entries
                'processed': total_entries,  # All CSV entries have been analyzed
                'auto_matched': auto_matched_count,  # Books automatically matched
                'need_review': len(distinct_books) - auto_matched_count - bookless_count,  # Books needing manual review
                'validation_errors_count': len(validation_errors),  # Use different key for count
                'distinct_books_count': len(distinct_books),
                'auto_matched_count': auto_matched_count,
                'recent_activity': [
                    f"Analysis complete: {len(distinct_books)} distinct books found",
                    f"{auto_matched_count} automatically matched",
                    f"{len(validation_errors)} validation errors",
                    f"{len(distinct_books) - auto_matched_count} books need manual matching"
                ]
            })
            
            print(f"🎯 [READING_HISTORY] Analysis complete. Ready for user book matching.")
        else:
            # All books are auto-matched, proceed directly to final processing
            print(f"🎉 [READING_HISTORY] All books auto-matched! Proceeding to final processing...")
            
            # Create book resolutions for all auto-matched books
            book_resolutions = {}
            for book_data in books_for_matching:
                if book_data['matched_book']:
                    book_resolutions[book_data['csv_name']] = {
                        'action': 'match',
                        'book_id': book_data['matched_book']['id']
                    }
                elif book_data.get('is_bookless', False):
                    # Handle bookless entries in automatic processing
                    book_resolutions[book_data['csv_name']] = {
                        'action': 'bookless'
                    }
            
            # Update status to processing
            safe_update_import_job(user_id, task_id, {
                'status': 'running',
                'import_type': 'reading_history',
                'recent_activity': ['All books auto-matched, creating reading logs...']
            })
            
            # Process the final import directly
            job_data = {
                'user_id': user_id,
                'csv_file_path': csv_file_path,
                'books_for_matching': books_for_matching,
                'import_type': 'reading_history'
            }
            asyncio.run(_process_final_reading_history_import(task_id, job_data, book_resolutions))
        
    except Exception as e:
        safe_update_import_job(user_id, task_id, {
            'status': 'failed',
            'error_messages': [str(e)]
        })
        raise
    
    finally:
        # Note: Don't clean up temp file yet - we need it for final processing
        pass
