# MyBibliotheca

MyBibliotheca is a self-hosted library manager and reading tracker. Organize books, track reading progress, record reading sessions, and keep your collection on infrastructure you control.

## Release 3.0.0

Version **3.0.0** uses PostgreSQL as its primary database. New installations use PostgreSQL automatically. Existing Kuzu installations need to be migrated with the in-app database migration wizard.

## Documentation

The [MyBibliotheca documentation](https://docs.mybibliotheca.org/) is the source of truth for installing, configuring, upgrading, and maintaining the application. It includes the Docker Compose installation guide, the 3.0.0 image tag, and the Kuzu-to-PostgreSQL migration wizard instructions.

- [Install or upgrade MyBibliotheca](https://docs.mybibliotheca.org/stable/install/)
- [Migrate from Kuzu to PostgreSQL](https://docs.mybibliotheca.org/stable/migration/)
- [Backups and restore](https://docs.mybibliotheca.org/stable/backups/)

## Features

- Book and contributor metadata with ISBN lookup and cover fetching
- Imports from Goodreads, StoryGraph, reading-history exports, and custom CSV files
- Reading status, progress, sessions, ratings, reviews, and statistics
- Multiple user accounts with private personal reading data
- Admin tools, backups, restore, and data export
- PostgreSQL persistence with a guided Kuzu migration for existing libraries
- Optional Redis cache

For support and source code, visit the [GitHub repository](https://github.com/pickles4evaaaa/mybibliotheca). MyBibliotheca is licensed under the [MIT License](LICENSE).
