# Cover Art Issue Analysis

## Issue Description
Book cover art is lost on app restart. Users report that covers that were previously visible are no longer displayed after restarting the application.

## Investigation Results

### Database Analysis
Using the `check_covers.py` script, we found:

**Cover URLs in Database:**
1. `bf5fb33b-da66-4320-941e-2dfc0946d940` (Mistborn: the Wax and Wayne Series)
   - Cover URL: `/static/covers/ed4ee529-9656-4c48-8175-d467c6d7725f.jpg`
   - File exists: **FALSE**

2. `2c2e65b1-dce9-45a0-becd-91bccb02b9a2` (Wind and Truth)
   - Cover URL: `/static/covers/7a81ada3-d441-4df1-bf5a-c7037c24bbb2.jpg`
   - File exists: **FALSE**

### File System Analysis
- Static directory: `app/static/` exists
- Covers directory: `app/static/covers/` exists but is **EMPTY**
- No cover image files are present in the file system

## Root Cause
The issue is **NOT** with the database or URL storage. The cover URLs are correctly stored in the Kuzu database. The problem is that the actual cover image files have been lost from the file system.

**What's happening:**
1. Cover URLs are stored correctly as `/static/covers/{uuid}.jpg`
2. The `app/static/covers/` directory exists
3. But the actual `.jpg` files referenced by the URLs are missing

## Why This Happens
Cover image files can be lost due to:
1. **Container restarts** - If covers are stored in a non-persistent volume
2. **File system cleanup** - Temporary files getting deleted
3. **Deployment issues** - Static files not being properly persisted
4. **Docker volume mounting** - If the static directory isn't properly mounted

## Solution Approaches

### 1. Immediate Fix
- Re-download missing cover images from the original sources
- Ensure the cover images are stored in a persistent location

### 2. Long-term Fix
- Store cover images in a persistent volume or cloud storage
- Add fallback mechanism to re-download covers if files are missing
- Consider storing covers as base64 data in the database (for small images)
- Implement a cover validation/repair function

### 3. Prevention
- Ensure `app/static/covers/` is in a persistent volume in Docker
- Add health checks to detect missing cover files
- Implement automatic cover re-download on missing files

## Current State
- ✅ Book navigation (clicking on books) works correctly
- ✅ Book details pages load without errors (setattr issue was already fixed)
- ✅ All books are visible to all users (query was fixed)
- ❌ Cover images are missing from the file system
- ✅ Cover URLs are correctly stored in database

## Files Checked
- `/Users/jeremiah/Documents/Python Projects/bibliotheca/check_covers.py` - Cover inspection script
- `/Users/jeremiah/Documents/Python Projects/bibliotheca/app/static/covers/` - Empty covers directory
- Database query results showing correct URL storage

## Next Steps
1. Implement a cover repair function that re-downloads missing covers
2. Add persistent volume mounting for the covers directory
3. Consider adding a fallback placeholder image for missing covers
