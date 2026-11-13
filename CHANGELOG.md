# Changelog

All notable changes to MyBibliotheca will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Admin-only "New update available" notice backed by a cached, silent GitHub releases check (disable with `UPDATE_CHECK_ENABLED=false`).

## [3.0.3] - 2026-11-13

### Changed
- Footer and package version show 3.0.3.

### Fixed
- The "Currently Reading" tile no longer drops to 0 when browsing "Total Books" on PostgreSQL; the Wishlist tile is counted in that view too (#261).
- Category aliases: the edit form no longer turns an empty alias list into a `[]` alias, existing `[]` / `['[]']` aliases are cleaned up on display and on the next save, and clearing the alias field now actually removes the aliases (#262).
- "View Reading Log" on a book opens a normal, styled page instead of unstyled text, and its totals count only that book's sessions instead of all of your reading logs. Paging and the "entries per page" selector (including "All") work on that page and from the stats dashboard (#263).

## [3.0.2] - 2026-11-08

### Added
- Google Books API key can be entered in the GUI: a required choice in onboarding step 2 (enter a key, or explicitly acknowledge that skipping makes lookups slow and unstable) with a step-by-step "how do I get a key" popup, and a verify-and-save card in Settings → Server → Metadata. The key is stored in `data/google_books_settings.json`, takes priority over `GOOGLE_BOOKS_API_KEY`, and is now sent on every Google Books request (previously ISBN lookups and the add-book search ran on the shared anonymous quota).
- API limit awareness: daily Google Books request counter (stops calling Google at `GOOGLE_BOOKS_DAILY_LIMIT`, default 1000, or when Google reports its quota is used up, and resumes when the quota resets at midnight Pacific), and an OpenLibrary rate limit (default 3 requests per 2 seconds; `OPENLIBRARY_MAX_REQUESTS` / `OPENLIBRARY_WINDOW_SECONDS`, e.g. 1 per 2s). Shared across all workers; usage and pause status shown in Settings → Server → Metadata and at `/api/api-limits`. Disable with `API_LIMITS_ENABLED=false`.

### Changed
- Google Books is now the preferred metadata/cover provider; OpenLibrary is only queried when Google is paused, over quota, or returns nothing. The add-book search no longer waits on OpenLibrary first.
- Cover candidate searches return several stable, ranked results per provider instead of one best match that could change between searches (#254).
- Library filter tiles are now a uniform size and alignment.
- Footer and package version show 3.0.2.

### Fixed
- Background settings now apply: a custom background is no longer overridden by the dark theme, and the page reloads after saving (#259).
- Backup manager settings are stored reliably across Gunicorn workers (#258); "Backup not found" on download and backups missing from the list no longer happen; backup index writes are atomic.
- A book's location shows its name instead of the internal ID (#257).
- A newly added (or edited/deleted) book now appears on the library page immediately instead of after a second refresh; the library cache version is now shared across Gunicorn workers via Redis.
- Backup page no longer mentions Kuzu when running on PostgreSQL and reports the PostgreSQL database size.
- Book lookups are much faster when the Google Books quota is exhausted: no more sleep-and-retry on 429 responses, parallel OpenLibrary cover checks, concurrent and cached cover-size probes.

## [3.0.1] - 2026-11-02

### Fixed
- Library Journey timeline no longer places books without a finish date into a bogus year (e.g. the import year); only the selected date type is used (#208).
- Editing, deleting and viewing locations no longer fails with a server error on PostgreSQL, and a location saved on the book edit form now shows up when you reopen it, including edits that change nothing else and clearing the location (#255).
- The library page shows book authors again, and sorting by author works (#256).
- First-run setup now shows the real reason when it fails instead of a generic "Setup failed" message (#170).
- Goodreads and CSV imports: merging into an existing book no longer silently drops dates, status, rating and notes; Goodreads ratings and reviews are now imported; failed merges are reported as errors instead of successes; and an undecodable byte or BOM no longer aborts an import at any entry point (upload preview, execute, simple upload, format detection, onboarding import); a warning reports how many characters were replaced (#183, #216).
- Simple upload no longer reports "Import started" when the file's format is unrecognized and every row would be skipped; it returns a clear error instead.
- The AI settings page error (`load_ai_config` NameError) was fixed in 2.2.2 and is included (#251).

### Security
- Passwords, password hashes, CSRF tokens and other secrets are no longer written to logs (onboarding/session debug output, user creation); a global log filter masks them as a safeguard.

### Added
- "New update available" notice for administrators that checks GitHub releases and links to the release page. Cached, non-blocking, sends no identifying data, and can be disabled with `UPDATE_CHECK_ENABLED=false`.

### Hotfix
- Fixed an empty glass card appearing under the navbar for administrators: the hidden update notice was wrapped in a styled container. Republished as the same 3.0.1 tags.

### Changed
- The page footer now shows the installed version read from the application instead of a hard-coded number.
- README documentation links now point to docs.mybibliotheca.org.

## [3.0.0] - 2026-10-20

### Changed
- PostgreSQL is now the primary database for new installations.
- Added a verified in-app migration wizard for existing Kuzu libraries; upgrading the application alone does not migrate data.
- Updated installation, configuration, and migration documentation for the PostgreSQL deployment.

### Migration notes
- Back up the application data directory and PostgreSQL storage before upgrading.
- Existing Kuzu installations remain on Kuzu until an administrator completes the migration wizard.
- Keep the Kuzu database and verified backups until the PostgreSQL library is checked.

## [2.2.2] - 2026-10-19

### Fixed
- Restored the self-hosted Quagga2 camera scanner bundle and removed duplicate camera stream acquisition.
- Restricted CSV import processing to the current user's temporary upload files.
- Fixed the legacy CSV upload route to process imported books instead of reporting a false success.
- Fixed background import error handlers to use captured user IDs outside the request context.
- Restored missing server settings panel loader imports.

### Changed
- Updated Python dependencies and lockfile.

## [2.2.1] - 2026-09-09

### Added
- Added Google Books API support for ISBN metadata lookups and imports.
- Re-enabled camera-based ISBN barcode scanning in Fast Add mode.

### Fixed
- Improved ISBN validation and scanner feedback, with manual and USB/Bluetooth scanner fallbacks.
- Improved CSV import metadata enrichment, timeout handling, and partial-result reporting.
- Fixed static asset URL generation on Settings and book-detail pages.


## [2.2.0] - 2026-08-28

### Added
- Added title-based external book search alongside ISBN lookup when adding books.
- Added support for selecting search results that do not include an ISBN.

### Fixed
- Fixed author card links using an incorrect route on the People page.
- Fixed add-book modal backdrops blocking modal interaction.
- Fixed case-sensitive username login and profile validation.
- Fixed mobile layouts across the library, add-book, and book-detail pages.
- Fixed book-detail covers overflowing their column on mobile devices.

### Improved
- Refreshed project dependencies and lockfile.
- Improved mobile navigation, page controls, pagination, and book-card sizing.
- Removed the decorative bubble from the application header.

## [2.0.1] - 2025-11-13

### Fixed
- Fixed admin blueprint error preventing admins from creating new users without internal server errors ([#110](https://github.com/pickles4evaaaa/mybibliotheca/pull/110))
- Fixed user creation issues and backup settings persistence ([#109](https://github.com/pickles4evaaaa/mybibliotheca/pull/109))
- Fixed security alert by removing user_data dict logging that contained sensitive fields
- Fixed backup settings capture for unchecked state
- Fixed stats mobile display issues
- Fixed dark mode text visibility across UI components ([#104](https://github.com/pickles4evaaaa/mybibliotheca/pull/104))
- Fixed duplicate book detection by removing overly aggressive fuzzy title matching that caused false positives ([#98](https://github.com/pickles4evaaaa/mybibliotheca/pull/98))
- Fixed worker timeout in Goodreads import by adding metadata fetch timeouts ([#99](https://github.com/pickles4evaaaa/mybibliotheca/pull/99))
- Fixed blurry book covers in view book page
- Fixed `@app.before_first_request` decorator issue (removed in Flask 2.2+) that was causing application crashes on startup

### Improved
- Updated Python packages to latest stable versions ([#102](https://github.com/pickles4evaaaa/mybibliotheca/pull/102))
- Enhanced reading journey calendar view with larger, more appealing book covers
- Added pagination controls at bottom of library view for better navigation ([#97](https://github.com/pickles4evaaaa/mybibliotheca/pull/97))
- Simplified header/footer UI and improved mobile layout ([#92](https://github.com/pickles4evaaaa/mybibliotheca/pull/92))
- Updated README with quick start guide and removed outdated setup instructions
- Added comprehensive dark mode CSS overrides for Bootstrap utility classes
- Updated version tag from Alpha 2.0.0 to v2.0.0-beta.1 and then to v2.0.1

### Added
- Added GitHub Copilot instructions and automated setup steps ([#106](https://github.com/pickles4evaaaa/mybibliotheca/pull/106))
- Added wheel files to .gitignore for cleaner repository

### Documentation
- Updated README to version 2.0.1+ with enhanced warning message
- Added feature screenshots showcasing library homepage, reading log, and book details
- Improved documentation for beta deployment

---

## [2.0.0] - 2025-11-12

### Major Changes
- **Complete Database Overhaul**: Migrated from traditional SQL to KuzuDB graph database
  - Advanced relationship modeling for better recommendations
  - Complex queries that were previously impossible
  - Foundation for future social features
  - Better data persistence and integrity

### Added
- **Multi-User Support from Day One**
  - Secure authentication with bcrypt password hashing
  - Complete user data isolation
  - Admin tools for user management
  - No default credentials—full user control
- ISBN book lookup with automatic metadata fetching
- Bulk import from Goodreads CSV
- Detailed reading logs with notes and progress tracking
- Beautiful, responsive UI that works on any device
- Reading stats and streaks to keep users motivated

### Notes
- Version 1.1.1 is deprecated and that branch will no longer be maintained
- For installation documentation, visit: https://mybibliotheca.org/

---

## Earlier Versions

For changes in versions prior to 2.0.0, please refer to the [GitHub Releases](https://github.com/pickles4evaaaa/mybibliotheca/releases) page.
