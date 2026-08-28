# 📚 MyBibliotheca

MyBibliotheca is a self-hosted personal library manager and reading tracker. Organize your collection, track reading progress, record daily reading activity, and keep control of your data.

## Current release

**2.2.0 is the latest stable release.**

Version 2.2.0 uses **KuzuDB** for graph-based library data and relationships. Back up your data before upgrading.

### What's new in 2.2.0

- Search for books by title as well as ISBN
- Select and add search results that do not have an ISBN
- Improved author links and book-detail navigation
- Refined mobile layouts, pagination, and book-card sizing
- Updated dependencies and lockfile

## Features

- 📖 Add books by ISBN or title with automatic metadata and cover fetching
- 📥 Import books from Goodreads and other CSV files
- ✅ Track reading status, progress, ratings, and reviews
- 📅 Log reading sessions and maintain reading streaks
- 📊 View reading statistics and analytics
- 🔐 Multi-user authentication with isolated user libraries
- 👤 Admin tools for user and system management
- 🔒 Privacy controls for shared reading activity and library content
- 🗄️ KuzuDB graph database for books, people, series, and relationships
- 📱 Responsive interface for desktop and mobile devices
- 💾 Backup, restore, and data export tools

### Build from source with Docker Compose

```bash
git clone https://github.com/pickles4evaaaa/mybibliotheca.git
cd mybibliotheca
cp .env.example .env

# Edit .env and set unique SECRET_KEY and SECURITY_PASSWORD_SALT values
docker compose up -d --build
```

The Compose configuration stores application data in `./data` and configures KuzuDB at `/app/data/kuzu`.

> **Important:** KuzuDB requires a single application worker. Keep `WORKERS=1` in production deployments.

## Documentation

Full installation, configuration, administration, backup, and troubleshooting guides are available at [mybibliotheca.org](https://mybibliotheca.org).

## Project structure

```text
mybibliotheca/
├── app/
│   ├── domain/                  # Domain models and business logic
│   ├── infrastructure/          # KuzuDB connections and repositories
│   ├── routes/                  # Application routes
│   ├── services/                # Application services
│   ├── schema/                  # Database schema definitions
│   ├── templates/               # Jinja2 templates
│   └── static/                  # Static assets
├── data/                        # Persistent application data
├── docs/                        # Project documentation
├── scripts/                     # Administration and utility scripts
├── docker-compose.yml           # Docker Compose configuration
├── Dockerfile                   # Application image definition
└── run.py                       # Application entry point
```

## Development

```bash
git clone https://github.com/pickles4evaaaa/mybibliotheca.git
cd mybibliotheca
docker compose -f docker-compose.dev.yml up -d
```

Run the test suite with:

```bash
pytest
```

Bug reports, documentation improvements, and pull requests are welcome on [GitHub](https://github.com/pickles4evaaaa/mybibliotheca).

## Getting help

- Check the [documentation](https://mybibliotheca.org)
- Review container logs with `docker compose logs -f`
- Open an issue on [GitHub](https://github.com/pickles4evaaaa/mybibliotheca/issues)
- Join the [Discord community](https://discord.gg/Hc8C5eRm7Q)

## License

MyBibliotheca is licensed under the [MIT License](LICENSE).
