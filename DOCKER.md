# Docker Compose installation

MyBibliotheca 3.0.0 uses PostgreSQL as its primary database. The supplied Compose stack starts the app, PostgreSQL, and Redis. New installations initialize PostgreSQL automatically; existing Kuzu installations must complete the in-app migration wizard.

## Install

```bash
git clone https://github.com/pickles4evaaaa/mybibliotheca.git
cd mybibliotheca
cp .env.example .env
```

Edit `.env` and set different unique random values for `SECRET_KEY`, `SECURITY_PASSWORD_SALT`, and `POSTGRES_PASSWORD`. Then run:

```bash
docker compose up -d
docker compose ps
docker compose logs -f bibliotheca
```

Open `http://localhost:5054` and create the administrator account. The app files are stored under `./data`; PostgreSQL data uses the `postgres_data` named volume. Back up both.

## Upgrade a Kuzu installation

An application upgrade alone does not move the database. Keep the existing `data/kuzu` directory in place, back up both the complete `data/` directory and PostgreSQL storage, then update the Compose file to the desired release tag and start the stack. Sign in as an administrator, open **Admin → Database Migration**, and complete the backup, PostgreSQL check, migration, and verification steps in order. Keep the Kuzu database and backups until you confirm the migrated library. Read [docs/MIGRATION.md](docs/MIGRATION.md) first.

## Configuration

The `.env.example` file documents the supported settings. Compose defaults connect the application to the bundled PostgreSQL service and Redis. The production Compose file pins the app image to `pickles4evaaaa/mybibliotheca:3.0.0`. For a memory-constrained host, lower `WORKERS`; PostgreSQL defaults to six workers. The app automatically uses one worker while Kuzu remains active during migration.

For production, use HTTPS through a reverse proxy and protect `.env` and backups. See the [deployment guide](docs/DEPLOYMENT.md) and [backup guide](docs/BACKUP_RESTORE.md).
