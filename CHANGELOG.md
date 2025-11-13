# Changelog

All notable changes to MyBibliotheca will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
