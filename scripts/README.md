# Default Import Templates

This directory contains scripts and functionality for managing default import templates in MyBibliotheca.

## Overview

Default import templates are pre-configured mapping templates that make it easy for users to import data from popular book tracking services like Goodreads and StoryGraph without having to manually map fields every time.

## Features

### Available Default Templates

1. **Goodreads Export Template**
   - Maps all standard Goodreads library export CSV fields
   - Handles ratings, reading status, dates, categories, and reviews
   - Automatically detects Goodreads CSV format

2. **StoryGraph Export Template**
   - Maps all standard StoryGraph library export CSV fields
   - Includes unique StoryGraph fields like moods, pace, and character analysis
   - Creates custom fields for StoryGraph-specific metadata

### System Templates vs User Templates

- **System Templates**: Pre-installed default templates available to all users
  - Cannot be deleted by users
  - Marked with "System Default" badge
  - Automatically updated with application upgrades
  
- **User Templates**: Custom templates created by individual users
  - Can be edited and deleted by the owner
  - Personal to each user account
  - Created when users save their own mapping configurations

## Usage

### Automatic Initialization

Default templates are automatically created when the application starts if they don't already exist. This happens during the Flask app initialization process.

### Manual Creation

You can also manually create or recreate the default templates using the provided script:

```bash
python scripts/create_default_templates.py
```

### Template Detection

When users upload a CSV file, the system automatically:
1. Analyzes the CSV headers
2. Attempts to match them against available templates
3. Suggests the best matching template (usually 70%+ similarity)
4. Pre-selects the suggested template in the import interface

## Technical Details

### Template Structure

Each template contains:
- **ID**: Unique identifier (e.g., "default_goodreads")
- **User ID**: "__system__" for default templates
- **Name**: Display name shown to users
- **Description**: Helpful description of the template's purpose
- **Source Type**: "goodreads", "storygraph", or "custom"
- **Sample Headers**: List of expected CSV column names
- **Field Mappings**: Dictionary mapping CSV columns to book fields

### Field Mapping Actions

Templates can specify different actions for each CSV column:
- `map_existing`: Map to a standard book field
- `create_custom`: Create a new custom metadata field
- `skip`: Ignore this column during import

### Custom Field Creation

StoryGraph template includes custom fields for unique metadata:
- **Moods**: Tags for reading atmosphere (e.g., "dark", "hopeful")
- **Pace**: Reading speed perception (e.g., "fast", "slow")
- **Character vs Plot**: Whether the book is character or plot driven
- **Content Warnings**: Tags for sensitive content

## Benefits

1. **Faster Imports**: Users don't need to manually map common CSV formats
2. **Consistency**: Standardized field mappings across all users
3. **Onboarding**: New users can immediately import from popular services
4. **Rich Metadata**: Preserves unique metadata from different services
5. **Extensibility**: Easy to add new default templates for other services

## Future Enhancements

- Add templates for other book tracking services (LibraryThing, Bookly, etc.)
- Allow community sharing of custom templates
- Automatic template updates and versioning
- Template marketplace or repository
- AI-powered template suggestions based on CSV structure
