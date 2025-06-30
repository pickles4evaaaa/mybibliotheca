# 📖 Book Details Layout Design - MyBibliotheca

This document defines the layout structure for book detail views, organizing fields from the comprehensive field documentation into logical sections for optimal user experience.

## 🎯 Layout Overview

The book details page is organized into 5 distinct sections, each serving a specific purpose in presenting book information to users:

1. **Book Card** - Essential overview information
2. **Book Description** - Full book description/summary
3. **Additional Details** - Technical and publication metadata
4. **Personal Notes** - User's subjective thoughts and experiences
5. **Custom Fields** - User-defined and import-generated metadata

---

## 📇 Book Card Section

**Purpose**: Provide immediate, essential information for quick book identification and status overview.

### Core Fields (Always Displayed)
- **`title`** (STRING) - Primary book title
- **`subtitle`** (STRING) - Book subtitle (if available)
- **`cover_url`** (STRING) - Book cover image
- **`authors`** (ARRAY) - Primary and additional authors
- **`page_count`** (INT64) - Number of pages
- **`language`** (STRING) - Language code with friendly display

### User Status Fields (Subjective Elements)
- **`user_rating`** (DOUBLE) - Personal rating (1-5 stars) from OWNS relationship
- **`reading_status`** (STRING) - Current reading status from OWNS relationship:
  - `currently_reading` - Currently Reading
  - `read` - Read
  - `plan_to_read` - Plan to Read
  - `on_hold` - On Hold
  - `did_not_finish` - Did Not Finish
  - `library_only` - Library Only

### Ownership & Location Fields
- **`ownership_status`** (STRING) - Ownership status from OWNS relationship:
  - `owned` - Owned
  - `borrowed` - Borrowed
  - `loaned` - Loaned Out
  - `wishlist` - Wishlist
- **`media_type`** (STRING) - Media format from OWNS relationship:
  - `physical` - Physical Book
  - `ebook` - E-book
  - `audiobook` - Audiobook
  - `kindle` - Kindle
- **`location_id`** (STRING) - Storage location reference
- **`location_name`** (STRING) - Friendly location name (via STORED_AT relationship)

### Series Information (If Applicable)
- **`series`** (STRING) - Series name
- **`series_volume`** (INT64) - Volume number in series
- **`series_order`** (DOUBLE) - Order in series (allows decimals)


### Visual Elements
- Cover image with fallback placeholder
- Rating display with star visualization
- Status badges with color coding
- Series progression indicator (if part of series)

---

## 📄 Book Description Section

**Purpose**: Present the full book description/summary for content understanding.

### Primary Content
- **`description`** (TEXT) - Complete book description/summary from Book table

### Display Characteristics
- Full-width text area
- Readable typography with proper line spacing
- Expandable/collapsible for long descriptions
- Source attribution (if from API)

### Fallback Handling
- Display placeholder if no description available
- Option to fetch description from APIs if missing
- User ability to add/edit description

---

## 🔍 Additional Details Section

**Purpose**: Provide comprehensive technical, publication, and identification metadata.

### Publication Information
- **`publisher`** (STRING) - Publisher name
- **`published_date`** (STRING) - Publication date
- **`average_rating`** (DOUBLE) - Global average rating (non-personal)
- **`rating_count`** (INT64) - Number of global ratings

### External Identifiers
- **`isbn13`** (STRING) - 13-digit ISBN (primary identifier)
- **`isbn10`** (STRING) - 10-digit ISBN (secondary identifier)
- **`asin`** (STRING) - Amazon Standard Identification Number
- **`google_books_id`** (STRING) - Google Books API identifier
- **`openlibrary_id`** (STRING) - OpenLibrary API identifier

### Dates & Timeline (Non-Personal)
- **`created_at`** (TIMESTAMP) - Record creation date
- **`updated_at`** (TIMESTAMP) - Last modification date

### Contributors (Extended)
- **Additional Authors** - Secondary authors via AUTHORED relationships
- **Editors** - Editorial contributors via AUTHORED relationships
- **Translators** - Translation contributors via AUTHORED relationships
- **Other Contributors** - Other roles (illustrators, etc.)

### Technical Metadata
- **`normalized_title`** (STRING) - Normalized title for searching
- **External API Links** - Links to Google Books, OpenLibrary, etc.

---

## 💭 Personal Notes Section

**Purpose**: Capture user's subjective thoughts, experiences, and non-custom personal data.

### Core Personal Content
- **`personal_notes`** (TEXT) - General personal notes from OWNS relationship
- **`review`** (TEXT) - Formal book review from OWNS relationship

### Reading Experience
- **`start_date`** (DATE) - Reading start date from OWNS relationship
- **`finish_date`** (DATE) - Reading completion date from OWNS relationship
- **`date_added`** (DATE) - Date added to library from OWNS relationship

### Borrowing/Lending Information
- **`borrowed_from`** (STRING) - Person borrowed from
- **`loaned_to`** (STRING) - Person loaned to
- **`borrowed_date`** (DATE) - Date borrowed
- **`loaned_date`** (DATE) - Date loaned
- **`loaned_due_date`** (DATE) - Expected return date

### Purchase Information
- **`purchase_date`** (DATE) - Date of purchase
- **`purchase_price`** (DOUBLE) - Purchase price

### Reading Sessions (If Tracked)
- **Reading Session History** - From ReadingSession table:
  - `session_date` - Individual reading dates
  - `pages_read` - Pages read per session
  - `duration_minutes` - Time spent reading
  - `notes` - Session-specific notes

### Display Characteristics
- Chronological organization for dates
- Rich text editing for notes and reviews
- Private/public visibility controls
- Export options for personal data

---

## ⚙️ Custom Fields Section

**Purpose**: Display all user-defined and import-generated custom metadata fields.

### Global Custom Fields
Fields shared across all users, typically created from imports:

#### **Goodreads Import Fields**
- **`custom_global_goodreads_book_id`** (TEXT) - Goodreads Book ID
- **`custom_global_binding`** (TEXT) - Book binding type
- **`custom_global_original_publication_year`** (NUMBER) - Original publication year
- **`custom_global_bookshelves`** (TAGS) - Goodreads bookshelves
- **`custom_global_bookshelves_with_positions`** (TEXTAREA) - Bookshelves with positions
- **`custom_global_spoiler_review`** (BOOLEAN) - Review contains spoilers
- **`custom_global_read_count`** (NUMBER) - Number of times read

#### **StoryGraph Import Fields**
- **`custom_global_format`** (TEXT) - Book format
- **`custom_global_dates_read`** (TEXTAREA) - All reading dates
- **`custom_global_moods`** (TAGS) - Reading moods
- **`custom_global_pace`** (TEXT) - Reading pace
- **`custom_global_character_plot_driven`** (TEXT) - Character vs plot driven
- **`custom_global_strong_character_development`** (BOOLEAN) - Strong character development
- **`custom_global_loveable_characters`** (BOOLEAN) - Loveable characters
- **`custom_global_diverse_characters`** (BOOLEAN) - Diverse characters
- **`custom_global_flawed_characters`** (BOOLEAN) - Flawed characters
- **`custom_global_content_warnings`** (TAGS) - Content warnings
- **`custom_global_content_warning_description`** (TEXTAREA) - Content warning details

#### **Extended Global Fields**
- **`custom_global_average_rating`** (NUMBER) - Global average rating
- **`custom_global_recommended_for`** (TEXT) - Recommended for
- **`custom_global_recommended_by`** (TEXT) - Recommended by
- **`custom_global_original_purchase_date`** (DATE) - Original purchase date
- **`custom_global_original_purchase_location`** (TEXT) - Purchase location
- **`custom_global_condition`** (TEXT) - Physical condition
- **`custom_global_condition_description`** (TEXTAREA) - Condition details
- **`custom_global_bcid`** (TEXT) - Bibliographic Control ID

### Personal Custom Fields
Fields private to individual users:

- **`custom_personal_private_notes`** (TEXTAREA) - Private notes
- **`custom_personal_owned_copies`** (NUMBER) - Number of owned copies
- **`custom_personal_owned`** (BOOLEAN) - Owned status
- **User-Created Fields** - Any custom fields created by the user

### Field Type Rendering

#### **TEXT Fields**
- Single-line input with character limits
- Inline editing capability

#### **TEXTAREA Fields**
- Multi-line text areas
- Rich text formatting options
- Expandable height

#### **NUMBER Fields**
- Numeric input with validation
- Range controls where applicable

#### **BOOLEAN Fields**
- Checkbox or toggle switch
- Clear true/false indicators

#### **SELECT Fields**
- Dropdown menus
- Option management interface

#### **TAGS Fields**
- Tag-based input with autocomplete
- Visual tag chips
- Easy add/remove functionality

#### **DATE Fields**
- Date picker controls
- Multiple date format support
- Calendar integration

### Organization & Management
- **Grouped by Type** - Global vs Personal sections
- **Alphabetical Sorting** - Within each group
- **Collapsible Sections** - To manage large numbers of fields
- **Field Management** - Add/edit/delete custom fields
- **Import Integration** - Auto-creation from CSV imports

---

## 🎨 Layout Implementation Notes

### Responsive Design
- **Mobile-First** - Stacked sections on small screens
- **Desktop Layout** - Side-by-side where appropriate
- **Tablet Optimization** - Balanced layout for medium screens

### Visual Hierarchy
1. **Book Card** - Most prominent, card-style layout
2. **Description** - Secondary prominence with clear typography
3. **Additional Details** - Compact, organized lists
4. **Personal Notes** - Personal styling cues
5. **Custom Fields** - Organized, manageable sections

### User Experience
- **Progressive Disclosure** - Show most important info first
- **Quick Actions** - Edit buttons where appropriate
- **Search & Filter** - Within custom fields section
- **Export Options** - For personal data sections

### Accessibility
- **Screen Reader Support** - Proper heading hierarchy
- **Keyboard Navigation** - Tab order follows logical flow
- **Color Contrast** - Meets WCAG guidelines
- **Alternative Text** - For all images and icons

---

## 🔄 Field Population Priority

### High Priority (Always Show)
1. Book Card core fields
2. Description (if available)
3. User rating and status

### Medium Priority (Show When Available)
1. Additional details metadata
2. Personal notes and dates
3. Common custom fields

### Low Priority (Show If Populated)
1. Extended identifiers
2. Rarely-used custom fields
3. Technical metadata

### Dynamic Sections
- Hide empty sections entirely
- Show "Add" prompts for user-editable sections
- Lazy-load custom fields for performance

---

*This layout design provides a comprehensive, user-friendly organization of all book detail fields while maintaining clear separation between different types of information and ensuring optimal user experience across all device types.*
