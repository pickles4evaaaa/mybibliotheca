# Rate Limiting Implementation for Bibliotheca

## Overview
This document describes the rate limiting implementation added to address issue #23: preventing API throttling during bulk imports.

## Changes Made

### 1. Utils.py - Core Rate Limiting
- **New Configuration Constants**: Added configurable rate limiting parameters at the top of `utils.py`
  - `API_RATE_LIMIT_DELAY = 1.0` - seconds between API calls
  - `MAX_RETRIES = 3` - maximum retry attempts for failed requests
  - `RETRY_DELAY = 2.0` - base delay before retrying (with exponential backoff)

- **New Function: `rate_limited_request()`**
  - Wraps all HTTP requests with rate limiting
  - Implements exponential backoff retry logic
  - Supports URL parameters
  - Comprehensive error logging
  - Automatically raises exceptions after max retries

- **Updated Functions**:
  - `fetch_book_data()` - Now uses rate-limited OpenLibrary API calls
  - `get_google_books_cover()` - Now uses rate-limited Google Books API calls
  - `generate_month_review_image()` - Cover downloads now rate-limited

### 2. Routes.py - Bulk Import Improvements
- **Enhanced Progress Logging**: Added detailed logging for bulk import progress
- **Better Error Handling**: Individual book failures don't stop the entire import
- **Improved User Feedback**: Updated flash messages to indicate rate limiting is active
- **Import Counting**: Shows progress as "Processing book X/Y"

- **Updated Search Function**: Added rate limiting to Google Books API search

### 3. Templates - User Interface Updates
- **bulk_import.html**: Added informational alert explaining rate limiting
- **Rate Limiting Notice**: Users are informed that large imports may take time due to rate limiting

### 4. Testing
- **test_rate_limiting.py**: Simple test script to verify rate limiting configuration

## Technical Details

### Rate Limiting Strategy
1. **Fixed Delay**: 1 second between each API call (configurable)
2. **Retry Logic**: Up to 3 attempts with exponential backoff (2s, 4s, 6s)
3. **Error Handling**: Comprehensive logging and graceful degradation
4. **API Coverage**: Applied to all external API calls:
   - OpenLibrary book data
   - Google Books API (search and metadata)
   - Cover image downloads

### Benefits
- **Prevents API throttling** during large bulk imports
- **Automatic retry** for transient network issues
- **Configurable delays** can be adjusted based on API provider requirements
- **Detailed logging** for troubleshooting import issues
- **User-friendly** progress feedback

### API Provider Considerations
- **OpenLibrary**: No official rate limits, but recommends being respectful
- **Google Books**: 1000 requests per day for free tier
- **Cover downloads**: Various CDNs, generally permissive

## Configuration
Rate limiting can be adjusted by modifying the constants at the top of `app/utils.py`:

```python
API_RATE_LIMIT_DELAY = 0.5  # Increase for more aggressive rate limiting (reduced from 1.0s)
MAX_RETRIES = 3             # Increase for unreliable networks
RETRY_DELAY = 2.0           # Increase for stricter backoff
```

## Future Improvements
- ✅ **Background job processing for bulk imports** - COMPLETED 
- Dynamic rate limiting based on API response headers
- Progress bar for bulk imports (replaced with real-time progress tracking)
- Configurable rate limits per API provider

## Background Task System
**NEW in this update**: Bulk imports now run as background tasks to prevent web server timeouts!

### Features:
- **Real-time Progress Tracking**: Live updates on import progress with success/error counts
- **No Timeout Issues**: Web requests return immediately while imports run in background
- **Task Management**: View and monitor all background tasks through the "Tasks" link in navigation
- **Persistent State**: Task progress is stored in database and survives container restarts
- **Improved UX**: Users can navigate away and return to check progress

### Technical Implementation:
- **Task Model**: New `Task` table tracks job status, progress, and results
- **Threading**: Python threading for background job execution with proper Flask app context
- **API Endpoints**: RESTful endpoints for task status monitoring (`/api/task/<id>`)
- **Auto-refresh UI**: JavaScript automatically polls for progress updates
- **Rate Limiting**: Still applies (0.5s delays) but doesn't block the web interface
