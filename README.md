# MyBibliotheca

MyBibliotheca is a self-hosted library manager and reading tracker. Organize books, track reading progress, record reading sessions, and keep your collection on infrastructure you control.

## Release 3.0.0

Version **3.0.0** uses **PostgreSQL** as its primary database. New installations use PostgreSQL automatically. Existing Kuzu installations must be migrated with the in-app database migration wizard; upgrading the container alone does not move your data.

The migration wizard is available to administrators under **Admin → Database Migration**. It creates and verifies a full backup, checks the PostgreSQL target, copies and audits the data, then switches the application to PostgreSQL. Keep the original Kuzu data and backups until you have checked the migrated library.

## Install with Docker Compose

Requirements: Docker Engine with the Compose plugin, and persistent disk space for the application data and PostgreSQL database.

```bash
git clone https://github.com/pickles4evaaaa/mybibliotheca.git
cd mybibliotheca
cp .env.example .env
```

Edit `.env` and replace `SECRET_KEY`, `SECURITY_PASSWORD_SALT`, and `POSTGRES_PASSWORD` with unique secrets. Generate values with:

```bash
python3 -c 'import secrets; print(secrets.token_urlsafe(32))'
```

Use a different generated value for each setting. Then start MyBibliotheca:

```bash
docker compose up -d
docker compose ps
docker compose logs -f bibliotheca
```

Open [http://localhost:5054](http://localhost:5054) and create the administrator account. Compose starts MyBibliotheca, PostgreSQL, and Redis. It stores uploaded files and other application data in `./data` and PostgreSQL data in the `postgres_data` Docker volume. Back up both locations.

The production Compose file uses the published `pickles4evaaaa/mybibliotheca:3.0.0` image. To update to a later release, pull the repository version that updates the image tag, then run:

```bash
git pull
docker compose pull
docker compose up -d
```

Read the [installation and upgrade guide](https://docs.mybibliotheca.org/stable/install/) before upgrading an existing installation.

## Move an existing Kuzu installation to PostgreSQL

1. Back up the complete `data/` directory and database storage. Keep the backups outside the server if possible.
2. Upgrade to 3.0.0 and start the Compose stack. Do not delete or move `data/kuzu`.
3. Sign in as an administrator and open **Admin → Database Migration**.
4. Follow the wizard in order: create and verify the application backup, check the PostgreSQL target, then run the migration and verification.
5. Confirm the wizard reports PostgreSQL as active. Check users, books, reading history, covers, and integrations before resuming normal use.
6. Keep the original Kuzu files and backups until you are satisfied with the migration.

The wizard will stop if the target database is not suitable or if a data check fails. Resolve the reported issue before retrying. Once PostgreSQL accepts new writes, restore from a PostgreSQL backup if you need to recover; the retained Kuzu copy does not include later PostgreSQL changes. See the [migration guide](https://docs.mybibliotheca.org/stable/install/#upgrade-from-kuzu-to-postgresql) and [backup guide](https://docs.mybibliotheca.org/stable/backups/).

## Features

- Book and contributor metadata with ISBN lookup and cover fetching
- Imports from Goodreads, StoryGraph, reading-history exports, and custom CSV files
- Reading status, progress, sessions, ratings, reviews, and statistics
- Multiple user accounts with private personal reading data
- Admin tools, backups, restore, and data export
- PostgreSQL persistence with a guided Kuzu migration for existing libraries
- Optional Redis cache

## Configuration and documentation

The complete options are documented in [.env.example](.env.example). The [self-hosted documentation](https://docs.mybibliotheca.org/) covers installation, migration, backups, administration, integrations, and troubleshooting.

## Development

```bash
git clone https://github.com/pickles4evaaaa/mybibliotheca.git
cd mybibliotheca
docker compose -f docker-compose.dev.yml up -d --build
```

See [GitHub](https://github.com/pickles4evaaaa/mybibliotheca) for issues and contributions. MyBibliotheca is licensed under the [MIT License](LICENSE).
