# Kuzu to PostgreSQL migration (3.0.0)

MyBibliotheca 3.0.0 uses PostgreSQL for application data. New installs start on PostgreSQL. Existing installs with a Kuzu database remain on Kuzu until an administrator completes the in-app migration wizard.

**Do not delete, move, or modify the Kuzu database during migration.** Upgrading the application does not convert Kuzu data automatically.

## Update your Compose setup

The 3.0.0 production Compose file differs from the old Kuzu-only setup. It adds a PostgreSQL service and the `postgres_data` volume, and pins the app to `pickles4evaaaa/mybibliotheca:3.0.0`. Use the updated file before starting the migration.

- If you use an unmodified Git checkout, update the checkout to get the new `docker-compose.yml`.
- If you use a copied or customized Compose file, replace it with the 3.0.0 file or add the PostgreSQL service, the pinned app image, and its connection settings from [docker-compose.yml](../docker-compose.yml). Keep custom ports and proxy settings and preserve `./data:/app/data`.
- Keep your existing `SECRET_KEY` and `SECURITY_PASSWORD_SALT`. Add a unique `POSTGRES_PASSWORD` to `.env`.
- Do not run `docker compose down -v`, delete `data/`, or remove the new `postgres_data` volume.

## 3.0.0 Compose file

Use this as `docker-compose.yml` for the standard deployment. If you have custom ports, networks, or proxy labels, carry those settings into this file while keeping the PostgreSQL service, app image tag, and persistent data mounts.

```yaml
services:
  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_DB: ${POSTGRES_DB:-bibliotheca}
      POSTGRES_USER: ${POSTGRES_USER:-bibliotheca}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?Set POSTGRES_PASSWORD in .env before starting the stack}
      PGDATA: /var/lib/postgresql/data/pgdata
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U \"$${POSTGRES_USER}\" -d \"$${POSTGRES_DB}\""]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 20s
    restart: unless-stopped

  redis:
    image: redis:7.4-alpine
    command: ["redis-server", "--save", "", "--appendonly", "no", "--maxmemory", "256mb", "--maxmemory-policy", "allkeys-lru"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 5
    restart: unless-stopped

  bibliotheca:
    image: pickles4evaaaa/mybibliotheca:3.0.0
    ports:
      - "5054:5054"
    depends_on:
      redis:
        condition: service_healthy
      postgres:
        condition: service_healthy
    volumes:
      # Mount local data directory for migration support, Kuzu database, and backups
      - ./data:/app/data
  # NOTE: Do NOT bind-mount the static directory on macOS.
  # Docker Desktop's osxfs can trigger EDEADLK (Resource deadlock avoided)
    environment:
      # Security - MUST be set via .env file or environment
      SECRET_KEY: ${SECRET_KEY:?Set SECRET_KEY in .env}
      SECURITY_PASSWORD_SALT: ${SECURITY_PASSWORD_SALT:?Set SECURITY_PASSWORD_SALT in .env}

      # New installs initialize on PostgreSQL. Existing Kuzu installs remain
      # on Kuzu until the administrator completes the migration wizard.
      KUZU_DB_PATH: /app/data/kuzu
      GRAPH_DATABASE_ENABLED: "true"

      # Application settings
      SITE_NAME: ${SITE_NAME:-MyBibliotheca}
      TIMEZONE: ${TIMEZONE:-UTC}
      # PostgreSQL defaults to six workers; the entrypoint keeps Kuzu at one.
      WORKERS: ${WORKERS:-6}
      # Central log level (controls Gunicorn and Flask loggers)
      LOG_LEVEL: ${LOG_LEVEL}

      # Logging settings - disable verbose initialization to prevent duplicate messages
      MYBIBLIOTHECA_VERBOSE_INIT: ${MYBIBLIOTHECA_VERBOSE_INIT:-false}

      # Optional: use your own Google Books quota for metadata lookups/imports.
      # OpenLibrary remains available when this is unset.
      GOOGLE_BOOKS_API_KEY: ${GOOGLE_BOOKS_API_KEY:-}
      OPENLIBRARY_CONTACT_EMAIL: ${OPENLIBRARY_CONTACT_EMAIL:-}

      # Shared Redis cache.  The same cache backend remains enabled after the
      # PostgreSQL cutover; Kuzu query caching is transitional.
      CACHE_REDIS_URL: redis://redis:6379/1
      CACHE_REDIS_PREFIX: ${CACHE_REDIS_PREFIX:-mybibliotheca}
      KUZU_REDIS_CACHE_ENABLED: "true"
      KUZU_REDIS_CACHE_TTL_SECONDS: ${KUZU_REDIS_CACHE_TTL_SECONDS:-10}

      # Primary database for new installs and migration target for Kuzu upgrades.
      POSTGRES_DSN: ${POSTGRES_DSN:-}
      POSTGRES_HOST: ${POSTGRES_HOST:-postgres}
      POSTGRES_PORT: ${POSTGRES_PORT:-5432}
      POSTGRES_DB: ${POSTGRES_DB:-bibliotheca}
      POSTGRES_USER: ${POSTGRES_USER:-bibliotheca}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?Set POSTGRES_PASSWORD in .env}

      # Migration settings (DISABLED for manual migration)
      AUTO_MIGRATE: "false"
      # - MIGRATION_DEFAULT_USER=admin
    restart: unless-stopped

volumes:
  postgres_data:
```

## Before you start

- Schedule a maintenance window. The application should not receive normal user writes during cutover.
- Make a separate copy of the full application `data/` directory and any existing PostgreSQL storage.
- Update the checkout/Compose file, then pull and start the services:

  ```bash
  git pull
  docker compose pull
  docker compose up -d
  docker compose ps
  ```

- Confirm PostgreSQL is healthy and that `POSTGRES_PASSWORD` is set in `.env`:

  ```bash
  docker compose ps
  docker compose logs --tail=100 postgres
  ```

- Keep the Kuzu path mounted at `./data`.
- Sign in with an administrator account.

## Run the migration wizard

Open **Admin → Database Migration** and complete the steps in order:

1. **Create and verify Kuzu backup.** The app safely pauses Kuzu access, creates an archive of the database and related application files, and checks the archive. Download it and keep it with your independent copy.
2. **Check PostgreSQL.** The wizard checks connectivity and inventories the source. The target PostgreSQL database must not already contain application data. If it is not empty, configure a new empty target; do not truncate it manually.
3. **Migrate and verify.** Start the migration during the maintenance window. The app copies the graph and relational data, then checks source preservation and PostgreSQL projections. Keep the wizard page open while it works.
4. **Confirm cutover.** The app activates PostgreSQL after verification. Confirm the page reports PostgreSQL as the active database. If you run outside the supplied Gunicorn container, restart the application when prompted.
5. **Check the library.** Verify users, books, reading history, covers, and integrations before normal use resumes.

If a check fails, the wizard reports the blocker and Kuzu remains authoritative. Address the reported issue and retry. Do not edit persistence state files by hand.

## After cutover

Keep the original Kuzu database and both backups until you have confirmed the new installation. PostgreSQL is authoritative after cutover. Kuzu will not receive writes, so it is not a current rollback copy. For recovery after new PostgreSQL writes, restore a verified PostgreSQL application backup using the [backup and restore guide](BACKUP_RESTORE.md).

Redis remains an optional/shared cache; it is not the primary data store. Continue backing up the PostgreSQL volume and the `data/` directory.

## New installation

For a fresh installation, configure `.env` from `.env.example`, set unique values for `SECRET_KEY`, `SECURITY_PASSWORD_SALT`, and `POSTGRES_PASSWORD`, then run `docker compose pull` and `docker compose up -d`. When no legacy Kuzu database is present, the app initializes PostgreSQL automatically. See the [deployment guide](DEPLOYMENT.md) for the full install steps.
