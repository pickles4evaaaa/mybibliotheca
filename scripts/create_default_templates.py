#!/usr/bin/env python3
"""
Script to create default import templates for Goodreads and StoryGraph
that are available to all users.
"""

import os
import sys
from datetime import datetime, timezone
from dataclasses import asdict

# Add the parent directory to the path so we can import from app
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set up minimal Flask app context for services
os.environ.setdefault('GRAPH_DATABASE_ENABLED', 'true')
os.environ.setdefault('REDIS_URL', 'redis://localhost:6379')

from app import create_app
from app.domain.models import ImportMappingTemplate
from app.services import import_mapping_service


def create_goodreads_template():
    """Create default Goodreads import template."""
    template = ImportMappingTemplate(
        id="default_goodreads",
        user_id="__system__",  # Special user ID for system templates
        name="Goodreads Export (Default)",
        description="Default template for standard Goodreads library export CSV files. Automatically creates and maps to custom fields for Goodreads-specific metadata.",
        source_type="goodreads",
        sample_headers=[
            "Book Id", "Title", "Author", "Author l-f", "Additional Authors", 
            "ISBN", "ISBN13", "My Rating", "Average Rating", "Publisher", 
            "Binding", "Number of Pages", "Year Published", "Original Publication Year", 
            "Date Read", "Date Added", "Bookshelves", "Bookshelves with positions", 
            "Exclusive Shelf", "My Review", "Spoiler", "Private Notes", "Read Count", "Owned Copies"
        ],
        field_mappings={
            "Book Id": {"action": "map_existing", "target_field": "custom_global_goodreads_book_id"},
            "Title": {"action": "map_existing", "target_field": "title"},
            "Author": {"action": "map_existing", "target_field": "author"}, 
            "Author l-f": {"action": "skip"},  # Skip alternate author format - we'll use primary Author field
            "Additional Authors": {"action": "map_existing", "target_field": "additional_authors"},
            "ISBN": {"action": "map_existing", "target_field": "isbn"}, 
            "ISBN13": {"action": "map_existing", "target_field": "isbn"},
            "My Rating": {"action": "map_existing", "target_field": "rating"},
            "Average Rating": {"action": "map_existing", "target_field": "custom_global_average_rating"},
            "Publisher": {"action": "map_existing", "target_field": "publisher"},
            "Binding": {"action": "map_existing", "target_field": "custom_global_binding"},
            "Number of Pages": {"action": "map_existing", "target_field": "page_count"},
            "Year Published": {"action": "map_existing", "target_field": "publication_year"},
            "Original Publication Year": {"action": "map_existing", "target_field": "custom_global_original_publication_year"},
            "Date Read": {"action": "map_existing", "target_field": "date_read"},
            "Date Added": {"action": "map_existing", "target_field": "date_added"},
            "Bookshelves": {"action": "map_existing", "target_field": "categories"},  # Fixed: Bookshelves are categories, not reading status
            "Bookshelves with positions": {"action": "map_existing", "target_field": "categories"},  # Fixed: These are also categories
            "Exclusive Shelf": {"action": "map_existing", "target_field": "reading_status"},  # Correct: This is the reading status
            "My Review": {"action": "map_existing", "target_field": "notes"},
            "Spoiler": {"action": "map_existing", "target_field": "custom_global_spoiler_review"},
            "Private Notes": {"action": "map_existing", "target_field": "custom_personal_private_notes"},
            "Read Count": {"action": "map_existing", "target_field": "custom_global_read_count"},
            "Owned Copies": {"action": "map_existing", "target_field": "custom_personal_owned_copies"}
        },
        times_used=0,
        last_used=None,
    created_at=datetime.now(timezone.utc),
    updated_at=datetime.now(timezone.utc)
    )
    return template


def create_storygraph_template():
    """Create default StoryGraph import template."""
    template = ImportMappingTemplate(
        id="default_storygraph",
        user_id="__system__",  # Special user ID for system templates
        name="StoryGraph Export (Default)",
        description="Default template for StoryGraph export CSV files. Automatically creates and maps to custom fields for platform-specific metadata.",
        source_type="storygraph", 
        sample_headers=[
            "Title", "Authors", "Contributors", "ISBN/UID", "Format",
            "Read Status", "Date Added", "Last Date Read", "Dates Read", 
            "Read Count", "Moods", "Pace", "Character- or Plot-Driven?",
            "Strong Character Development?", "Loveable Characters?", 
            "Diverse Characters?", "Flawed Characters?", "Star Rating",
            "Review", "Content Warnings", "Content Warning Description", 
            "Tags", "Owned?"
        ],
        field_mappings={
            "Title": {"action": "map_existing", "target_field": "title"},
            "Authors": {"action": "map_existing", "target_field": "author"},
            "Contributors": {"action": "map_existing", "target_field": "additional_authors"}, 
            "ISBN/UID": {"action": "map_existing", "target_field": "isbn"},
            "Format": {"action": "map_existing", "target_field": "custom_global_format"},
            "Read Status": {"action": "map_existing", "target_field": "reading_status"},
            "Date Added": {"action": "map_existing", "target_field": "date_added"},
            "Last Date Read": {"action": "map_existing", "target_field": "date_read"},
            "Dates Read": {"action": "skip"},  # Skip - complex format that's hard to parse
            "Read Count": {"action": "map_existing", "target_field": "custom_global_read_count"},
            "Star Rating": {"action": "map_existing", "target_field": "rating"},
            "Review": {"action": "map_existing", "target_field": "notes"},
            "Tags": {"action": "map_existing", "target_field": "categories"},
            "Moods": {"action": "map_existing", "target_field": "custom_global_moods"},  # Fixed: Create separate field for moods
            "Pace": {"action": "map_existing", "target_field": "custom_global_pace"},
            "Character- or Plot-Driven?": {"action": "map_existing", "target_field": "custom_global_character_plot_driven"},
            "Strong Character Development?": {"action": "map_existing", "target_field": "custom_global_strong_character_development"},
            "Loveable Characters?": {"action": "map_existing", "target_field": "custom_global_loveable_characters"},
            "Diverse Characters?": {"action": "map_existing", "target_field": "custom_global_diverse_characters"},
            "Flawed Characters?": {"action": "map_existing", "target_field": "custom_global_flawed_characters"},
            "Content Warnings": {"action": "map_existing", "target_field": "custom_global_content_warnings"},
            "Content Warning Description": {"action": "map_existing", "target_field": "custom_global_content_warning_description"},
            "Owned?": {"action": "map_existing", "target_field": "custom_personal_owned"}
        },
        times_used=0,
        last_used=None,
    created_at=datetime.now(timezone.utc),
    updated_at=datetime.now(timezone.utc)
    )
    return template


def main():
    """Create and save default templates."""

    def _create_or_update_template(create_func, template_id, template_name):
        """
        Helper function to create a new template or update an existing one.
        """
        try:
            template = create_func()
            existing_template = import_mapping_service.get_template_by_id_sync(template_id)
            
            if existing_template:
                print(f"Found existing {template_name} template - updating with new mappings...")
                updated_template = import_mapping_service.update_template_sync(template)
                print(f"✅ Updated {template_name} template: {updated_template.name}")
            else:
                saved_template = import_mapping_service.create_template_sync(template)
                print(f"✅ Created {template_name} template: {saved_template.name}")

        except Exception as e:
            print(f"Note: Could not check for existing {template_name} template ({e}), creating new one...")
            template = create_func()
            saved_template = import_mapping_service.create_template_sync(template)
            print(f"✅ Created {template_name} template: {saved_template.name}")

    try:
        print("Creating default import templates...")
        
        # Create Flask app context
        app = create_app()
        with app.app_context():
            _create_or_update_template(create_goodreads_template, "default_goodreads", "Goodreads")
            _create_or_update_template(create_storygraph_template, "default_storygraph", "StoryGraph")
            
            print("\n🎉 Default templates created/updated successfully!")
            print("\nThese templates will now be available to all users when importing CSV files.")
        
    except Exception as e:
        print(f"❌ Error creating templates: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
