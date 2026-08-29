"""CSV format detection and mapping helpers.

These functions only interpret import headers.  No database or filesystem
writes belong in this module; import_routes keeps the original names as
compatibility aliases.
"""

from __future__ import annotations

import csv


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

__all__ = [
    "detect_csv_format",
    "auto_detect_fields",
    "get_goodreads_field_mappings",
    "get_storygraph_field_mappings",
]

