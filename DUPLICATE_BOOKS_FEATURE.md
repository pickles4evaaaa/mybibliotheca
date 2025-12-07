# Duplicate Books Feature Implementation

## Overview
This feature adds options for handling duplicate books when adding a new book to the library. Instead of automatically redirecting to the existing entry, users are now presented with three clear options.

## User Options

### 1. Increment Count (+1)
- **Purpose**: Increase the quantity count for the existing book entry
- **Use Case**: User has multiple copies but wants to track them together
- **Action**: Updates the existing book's `quantity` field by +1
- **Result**: User is redirected to the existing book's detail page

### 2. Add as Separate Entry
- **Purpose**: Create a new entry to track unique photos or notes for each physical copy
- **Use Case**: Collector wants to document each physical copy individually (e.g., different editions, conditions)
- **Action**: Bypasses duplicate check and creates a new standalone book entry
- **Result**: New book entry is created and user is redirected to it

### 3. Navigate to Existing Entry
- **Purpose**: Simply view the existing book without making changes
- **Use Case**: User realizes they already have the book and just wants to check it
- **Action**: No database changes, just navigation
- **Result**: User is redirected to the existing book's detail page

## Technical Implementation

### Schema Changes

#### Book Node Updates
- Added `quantity` field to Book schema (INT64, default 1)
- Updated `master_schema.json` version from 5 to 6
- Updated KuzuDB Book node creation DDL
- Added to domain `Book` model and `SimplifiedBook` dataclass

### Backend Components

#### 1. API Endpoint: `/book/api/resolve_duplicate`
**Method**: POST  
**Authentication**: Required (login_required)

**Request Body**:
```json
{
  "action": "increment_count" | "add_separate" | "navigate",
  "book_id": "uuid-of-existing-book",
  "book_data": {
    "title": "Book Title",
    "author": "Author Name",
    "isbn13": "9781234567890",
    // ... other book fields
  }
}
```

**Response**:
```json
{
  "success": true,
  "message": "Operation completed",
  "book_id": "uuid",
  "redirect_url": "/book/view_book_enhanced/uuid"
}
```

#### 2. Modified Routes

**`add_book_manual` Route**:
- Detects AJAX requests via `Accept` header or `X-Requested-With`
- Returns JSON instead of redirecting when duplicate detected
- Includes full book data in response for modal display

**JSON Response for Duplicate**:
```json
{
  "success": false,
  "duplicate": true,
  "book_id": "existing-book-uuid",
  "message": "Book 'Title' already exists in your library",
  "book_data": {
    // All book fields for re-submission
  }
}
```

#### 3. Book Service Updates

**`create_standalone_book_sync`**:
- New sync wrapper for creating books without duplicate check
- Used by "Add as Separate Entry" action

**`update_book_sync`**:
- Already supports quantity field updates via generic field dictionary
- No changes needed

### Frontend Components

#### 1. Duplicate Resolution Modal
**Location**: `app/templates/add_book.html`

**Features**:
- Bootstrap modal with static backdrop (prevents accidental closure)
- Three large, clearly labeled buttons with icons
- Book information display (title, author, ISBNs)
- Loading states during API calls
- Error handling with user-friendly alerts

**HTML Structure**:
```html
<div class="modal fade" id="duplicateBookModal">
  <!-- Modal with three option buttons -->
  <!-- Each button has icon + title + description -->
</div>
```

**JavaScript Functions**:
- `showDuplicateModal(duplicateResponse)` - Displays modal with book info
- Event handlers for each button (increment, separate, navigate)
- CSRF token handling
- Async fetch calls to API endpoint

#### 2. AJAX Form Submission
**Location**: `app/templates/add_book.html` form submission handler

**Flow**:
1. Prevent default form submission
2. Submit via `fetch()` with `Accept: application/json` header
3. Parse response:
   - If `duplicate: true` → Show modal
   - If `success: true` → Redirect to book/library
   - If error → Display error message
4. Re-enable form on error

#### 3. Quantity Display & Editing

**Book Detail View** (`view_book_enhanced.html`):
```html
{% if book.quantity and book.quantity > 1 %}
<div class="badge bg-primary">
  <i class="bi bi-stack"></i>
  {{ book.quantity }} copies
</div>
{% endif %}
```

**Book Edit Form**:
```html
<input type="number" name="quantity" 
       value="{{ book.quantity or 1 }}" 
       min="1" max="999" step="1">
```

### Database Migration

#### Migration File: `quantity_field_migration.py`

**Purpose**: Ensure smooth upgrades for existing databases

**Steps**:
1. Check if `quantity` column exists (schema preflight handles adding it)
2. Update all existing books where `quantity IS NULL OR quantity = 0`
3. Set `quantity = 1` for those books
4. Log number of books updated

**Integration**:
- Called automatically by `migrations/runner.py`
- Executed during application startup via schema preflight
- Safe to run multiple times (idempotent)

**Dry Run Support**:
```python
run_quantity_migration(dry_run=True)
# Returns what would be done without applying changes
```

### Schema Preflight System

**Automatic Upgrades**:
1. On startup, schema preflight loads `master_schema.json`
2. Compares with current database schema
3. Detects missing `quantity` column on Book node
4. Creates automatic backup (unless `SKIP_PREFLIGHT_BACKUP=true`)
5. Executes `ALTER TABLE Book ADD quantity INT64`
6. Runs migration to set default values
7. Updates schema marker file

**Environment Flags**:
- `DISABLE_SCHEMA_PREFLIGHT=true` - Skip all preflight checks
- `SKIP_PREFLIGHT_BACKUP=true` - Don't create backup before changes
- `SCHEMA_PREFLIGHT_FORCE=1` - Force preflight even if marker matches

### Error Handling

#### Duplicate Detection
- `BookAlreadyExistsError` exception raised by `SimplifiedBookService`
- Caught in route handler
- Checks for AJAX request
- Returns appropriate response (JSON or redirect)

#### API Failures
- Try-catch blocks around all API operations
- User-friendly error messages
- Logs detailed errors to server log
- Re-enables UI controls on error

#### Migration Failures
- Non-blocking: column addition is most critical
- Default value update failure logged but doesn't stop app
- Quantity defaults to NULL in database, handled gracefully in queries

## Backward Compatibility

### Existing Functionality Preserved
- Non-AJAX form submissions work as before (redirect behavior)
- Books without quantity field display normally (NULL/0 hidden)
- Book edit form defaults to 1 if quantity not set
- All existing book operations unaffected

### Database Compatibility
- Automatic schema upgrade on first run
- Existing books get `quantity = 1` automatically
- No data loss or corruption risk
- Backup created before any changes

## Testing Checklist

### Manual Testing Steps

1. **Schema Migration Test**
   - Start with existing database
   - Verify automatic upgrade logs
   - Check all existing books have quantity = 1
   - Verify no errors during migration

2. **Duplicate Detection Test**
   - Add a book (e.g., ISBN: 9780316769174)
   - Try adding the same ISBN again
   - Verify modal appears with three options

3. **Increment Count Test**
   - Click "Increment Count (+1)"
   - Verify quantity increases from 1 to 2
   - Check book detail page shows "2 copies" badge

4. **Add Separate Entry Test**
   - Try adding the duplicate again
   - Click "Add as Separate Entry"
   - Verify new book entry created
   - Check it has its own unique ID
   - Verify both entries show in library

5. **Navigate Test**
   - Try adding duplicate again
   - Click "Navigate to Existing Entry"
   - Verify redirected to existing book
   - Confirm no changes made

6. **Quantity Edit Test**
   - Open book edit form
   - Change quantity field
   - Save changes
   - Verify quantity updated correctly

7. **Non-AJAX Fallback Test**
   - Disable JavaScript in browser
   - Try adding duplicate book
   - Verify fallback behavior (flash + redirect)

### Edge Cases

- **ISBN normalization**: Test with/without hyphens
- **Title/author matching**: Test case-insensitive matching
- **Multiple duplicates**: Add same book 3+ times
- **Zero quantity**: Set quantity to 0, verify validation
- **Large quantity**: Test upper limit (999)
- **Missing data**: Test books without ISBNs

## Future Enhancements

### Possible Improvements
1. **Bulk quantity adjustment**: Select multiple books and adjust quantity
2. **Quantity history**: Track when quantities were changed
3. **Physical location tracking**: Different shelves for each copy
4. **Condition tracking**: Note condition of each copy separately
5. **Lending tracking**: Mark individual copies as lent out
6. **Visual indicator**: Show quantity in library list view
7. **Search by quantity**: Filter books with multiple copies

### Performance Considerations
- Quantity field is indexed (part of Book node)
- Duplicate check uses existing ISBN/title indexes
- Modal JavaScript is lightweight (<5KB)
- No additional database queries for display
- AJAX reduces full page reloads

## Security Considerations

### CSRF Protection
- All API calls include CSRF token
- Token validated server-side
- Modal includes hidden token field

### Authorization
- Login required for all operations
- User can only modify their own books
- Book ownership verified before updates

### Input Validation
- Quantity must be integer 1-999
- Book data sanitized before storage
- ISBN format validated
- SQL injection prevented (parameterized queries)

## Conclusion

This feature provides a robust, user-friendly solution for managing duplicate books while maintaining backward compatibility and ensuring smooth database migrations. The implementation follows the repository's architectural patterns and coding standards.
