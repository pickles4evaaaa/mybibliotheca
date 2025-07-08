# Routes Refactoring Guide

## Overview

The original `routes.py` file in the Bibliotheca application has grown to over 6,000 lines, making it difficult to maintain and work with. This document outlines a comprehensive refactoring strategy to split the monolithic file into smaller, more manageable modules.

## Problems with the Current Structure

1. **Size**: Over 6,000 lines in a single file
2. **Mixed Responsibilities**: Route handlers, utility functions, API endpoints, and business logic all mixed together
3. **Difficult Navigation**: Hard to find specific functionality
4. **Testing Challenges**: Difficult to test individual components in isolation
5. **Merge Conflicts**: Multiple developers working on the same large file leads to conflicts
6. **Maintenance Burden**: Changes to one area risk breaking unrelated functionality

## Proposed Structure

### 1. Core Modules

```
app/
├── routes/
│   ├── __init__.py          # Blueprint registration
│   ├── book_routes.py       # Book CRUD operations
│   ├── people_routes.py     # People/author management
│   ├── import_routes.py     # Import/export functionality
│   ├── api_routes.py        # JSON API endpoints
│   ├── stats_routes.py      # Statistics and analytics
│   └── misc_routes.py       # Miscellaneous routes
├── services/
│   ├── import_service.py    # Import processing logic
│   ├── metadata_service.py  # External API calls
│   └── stats_service.py     # Statistics calculations
└── utils/
    ├── field_mapping.py     # Field mapping utilities
    └── csv_helpers.py       # CSV processing helpers
```

### 2. Route Categories

#### Book Routes (`book_routes.py`)
- `/` - Home/library view
- `/library` - Library listing
- `/add` - Add new book
- `/book/<uid>/*` - Book-specific operations
- `/search` - Book search
- `/books/bulk_delete` - Bulk operations

#### People Routes (`people_routes.py`)
- `/people` - People listing
- `/person/<id>` - Person details
- `/person/add` - Add new person
- `/person/<id>/edit` - Edit person
- `/person/<id>/delete` - Delete person
- `/person/<id>/refresh_metadata` - Refresh metadata
- `/persons/bulk_delete` - Bulk operations

#### Import Routes (`import_routes.py`)
- `/import-books` - Import interface
- `/import-books/execute` - Start import
- `/import-books/progress/<task_id>` - Progress tracking
- `/api/import/*` - Import API endpoints

#### API Routes (`api_routes.py`)
- `/api/import/progress/<task_id>` - Import progress
- `/api/import/errors/<task_id>` - Import errors
- `/api/person/search` - Person search
- Other JSON endpoints

#### Stats Routes (`stats_routes.py`)
- `/stats` - Statistics dashboard
- `/month_review/*` - Monthly reviews
- `/community_activity` - Community features
- `/reading_history` - Reading history

## Migration Strategy

### Phase 1: Create New Structure (DONE)
✅ Created `app/routes/` directory
✅ Created initial blueprint files
✅ Created blueprint registration system

### Phase 2: Extract Core Functionality
- Move people management routes to `people_routes.py`
- Move import functionality to `import_routes.py`
- Move book operations to `book_routes.py`

### Phase 3: Service Layer Extraction
- Extract import logic to `import_service.py`
- Extract metadata fetching to `metadata_service.py`
- Extract field mapping to `field_mapping.py`

### Phase 4: API Consolidation
- Move all JSON endpoints to `api_routes.py`
- Standardize API response formats
- Add proper error handling

### Phase 5: Testing and Validation
- Add unit tests for each module
- Integration testing
- Performance testing

### Phase 6: Cleanup
- Remove original `routes.py`
- Update imports throughout the application
- Update documentation

## Implementation Details

### Blueprint Registration

The new structure uses Flask blueprints to organize routes:

```python
# app/routes/__init__.py
from .book_routes import book_bp
from .people_routes import people_bp
from .import_routes import import_bp

def register_blueprints(app):
    app.register_blueprint(book_bp, url_prefix='/books')
    app.register_blueprint(people_bp, url_prefix='/people')
    app.register_blueprint(import_bp, url_prefix='/import')
```

### Service Layer

Business logic is extracted to service modules:

```python
# app/services/import_service.py
class ImportService:
    def process_csv_file(self, file_path, mappings):
        """Process CSV import with field mappings."""
        pass
    
    def detect_csv_format(self, file_path):
        """Auto-detect CSV format (Goodreads, etc.)."""
        pass
```

### Utility Functions

Helper functions are organized by purpose:

```python
# app/utils/field_mapping.py
def auto_detect_fields(headers):
    """Auto-detect field mappings from CSV headers."""
    pass

def get_goodreads_mappings():
    """Get standard Goodreads field mappings."""
    pass
```

## Benefits

1. **Maintainability**: Smaller, focused files are easier to understand and modify
2. **Testability**: Each module can be tested in isolation
3. **Scalability**: New features can be added without affecting existing code
4. **Collaboration**: Multiple developers can work on different areas without conflicts
5. **Debugging**: Easier to trace issues to specific modules
6. **Documentation**: Each module can have focused documentation

## Backward Compatibility

To maintain backward compatibility during the transition:

1. **URL Preservation**: Keep existing URLs working through redirects or route aliasing
2. **Template Compatibility**: Ensure templates continue to work with new route names
3. **Gradual Migration**: Move functionality incrementally, testing at each step
4. **Fallback Routes**: Keep original routes as fallbacks during transition

## Example Migration

Here's how a typical route would be migrated:

### Before (in routes.py)
```python
@bp.route('/people')
@login_required
def people():
    # 50+ lines of code
    pass
```

### After (in people_routes.py)
```python
@people_bp.route('/people')
@login_required
def people():
    # Same functionality, but in dedicated module
    pass
```

## Testing Strategy

Each new module should include:

1. **Unit Tests**: Test individual functions and methods
2. **Integration Tests**: Test blueprint registration and routing
3. **Functional Tests**: Test complete user workflows
4. **Performance Tests**: Ensure no regression in performance

## Next Steps

1. **Complete the extraction** of remaining functionality from `routes.py`
2. **Update application initialization** to use new blueprint registration
3. **Add comprehensive tests** for each module
4. **Update documentation** to reflect new structure
5. **Plan deployment strategy** for production environments

This refactoring will significantly improve the maintainability and scalability of the Bibliotheca application while preserving all existing functionality.
