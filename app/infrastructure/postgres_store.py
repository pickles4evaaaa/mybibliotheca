"""PostgreSQL connection pool and normalized application schema."""

from __future__ import annotations

import os
import threading
import hashlib
import json
from contextlib import contextmanager
from typing import Any, Iterator


SCHEMA_DDL = (
    """
    CREATE TABLE IF NOT EXISTS app_schema_migrations (
        version INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        checksum TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        share_current_reading BOOLEAN NOT NULL DEFAULT TRUE,
        share_reading_activity BOOLEAN NOT NULL DEFAULT TRUE,
        share_library BOOLEAN NOT NULL DEFAULT FALSE,
        is_admin BOOLEAN NOT NULL DEFAULT FALSE,
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        password_must_change BOOLEAN NOT NULL DEFAULT FALSE,
        failed_login_attempts BIGINT NOT NULL DEFAULT 0,
        locked_until TIMESTAMPTZ,
        last_login TIMESTAMPTZ,
        password_changed_at TIMESTAMPTZ,
        reading_streak_offset BIGINT NOT NULL DEFAULT 0,
        timezone TEXT NOT NULL DEFAULT 'UTC',
        display_name TEXT,
        bio TEXT,
        location TEXT,
        website TEXT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS users_username_ci_uq ON users (lower(username))",
    "CREATE UNIQUE INDEX IF NOT EXISTS users_email_ci_uq ON users (lower(email))",
    "CREATE INDEX IF NOT EXISTS users_active_idx ON users (is_active)",
    """
    CREATE TABLE IF NOT EXISTS books (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL DEFAULT '',
        normalized_title TEXT,
        subtitle TEXT,
        isbn13 TEXT,
        isbn10 TEXT,
        asin TEXT,
        description TEXT,
        published_date DATE,
        page_count BIGINT,
        language TEXT,
        cover_url TEXT,
        google_books_id TEXT,
        openlibrary_id TEXT,
        opds_source_id TEXT,
        opds_source_updated_at TEXT,
        opds_source_entry_hash TEXT,
        average_rating DOUBLE PRECISION,
        rating_count BIGINT,
        series TEXT,
        series_volume TEXT,
        series_order BIGINT,
        custom_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        raw_categories JSONB NOT NULL DEFAULT '[]'::jsonb,
        audio_length_minutes BIGINT,
        audio_is_abridged BOOLEAN,
        audio_narrators JSONB NOT NULL DEFAULT '[]'::jsonb,
        media_format TEXT,
        audiobookshelf_id TEXT,
        audio_duration_ms BIGINT,
        media_type TEXT,
        audiobookshelf_updated_at TEXT,
        quantity BIGINT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    "CREATE INDEX IF NOT EXISTS books_isbn13_idx ON books (isbn13) WHERE isbn13 IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS books_isbn10_idx ON books (isbn10) WHERE isbn10 IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS books_title_ci_idx ON books (lower(title))",
    """
    CREATE TABLE IF NOT EXISTS book_relationship_edges (
        source_edge_id BIGINT PRIMARY KEY,
        relationship TEXT NOT NULL,
        source_label TEXT NOT NULL,
        source_id TEXT NOT NULL,
        target_label TEXT NOT NULL,
        target_id TEXT NOT NULL,
        book_id TEXT NOT NULL REFERENCES books(id) ON DELETE RESTRICT,
        properties JSONB NOT NULL,
        content_sha256 TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS book_relationship_edges_book_idx ON book_relationship_edges (book_id, relationship, source_edge_id)",
    "CREATE INDEX IF NOT EXISTS book_relationship_edges_target_idx ON book_relationship_edges (target_label, target_id, relationship)",
    """
    CREATE TABLE IF NOT EXISTS people (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL DEFAULT '',
        normalized_name TEXT,
        birth_date TEXT,
        death_date TEXT,
        birth_year BIGINT,
        death_year BIGINT,
        birth_place TEXT,
        bio TEXT,
        website TEXT,
        openlibrary_id TEXT,
        image_url TEXT,
        wikidata_id TEXT,
        imdb_id TEXT,
        alternate_names JSONB NOT NULL DEFAULT '[]'::jsonb,
        fuller_name TEXT,
        title TEXT,
        official_links JSONB NOT NULL DEFAULT '[]'::jsonb,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    "CREATE INDEX IF NOT EXISTS people_name_ci_idx ON people (lower(name))",
    """
    CREATE TABLE IF NOT EXISTS authors (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL DEFAULT '',
        normalized_name TEXT,
        birth_year BIGINT,
        death_year BIGINT,
        bio TEXT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS publishers (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL DEFAULT '',
        normalized_name TEXT,
        founded_year BIGINT,
        country TEXT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS categories (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL DEFAULT '',
        normalized_name TEXT,
        parent_id TEXT,
        description TEXT,
        level BIGINT NOT NULL DEFAULT 0,
        color TEXT,
        icon TEXT,
        aliases JSONB NOT NULL DEFAULT '[]'::jsonb,
        book_count BIGINT NOT NULL DEFAULT 0,
        user_book_count BIGINT NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb,
        FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS series (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL DEFAULT '',
        normalized_name TEXT,
        description TEXT,
        user_cover TEXT,
        cover_url TEXT,
        custom_cover BOOLEAN,
        generated_placeholder BOOLEAN,
        created_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS libraries (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL DEFAULT '',
        description TEXT,
        is_default BOOLEAN,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS library_shelves (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL DEFAULT '',
        description TEXT,
        position BIGINT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reading_sessions (
        id TEXT PRIMARY KEY,
        user_id TEXT REFERENCES users(id) ON DELETE RESTRICT,
        book_id TEXT REFERENCES books(id) ON DELETE RESTRICT,
        start_time TIMESTAMPTZ,
        end_time TIMESTAMPTZ,
        pages_read BIGINT,
        notes TEXT,
        reading_speed DOUBLE PRECISION,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reading_goals (
        id TEXT PRIMARY KEY,
        user_id TEXT REFERENCES users(id) ON DELETE RESTRICT,
        title TEXT,
        description TEXT,
        target_books BIGINT,
        target_pages BIGINT,
        target_reading_time_minutes BIGINT,
        deadline TIMESTAMPTZ,
        is_completed BOOLEAN,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reviews (
        id TEXT PRIMARY KEY,
        user_id TEXT REFERENCES users(id) ON DELETE RESTRICT,
        book_id TEXT REFERENCES books(id) ON DELETE RESTRICT,
        rating BIGINT,
        review_text TEXT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS locations (
        id TEXT PRIMARY KEY,
        owner_user_id TEXT REFERENCES users(id) ON DELETE RESTRICT,
        name TEXT NOT NULL DEFAULT '',
        description TEXT,
        location_type TEXT,
        address TEXT,
        is_default BOOLEAN,
        is_active BOOLEAN,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    "ALTER TABLE locations ADD COLUMN IF NOT EXISTS owner_user_id TEXT REFERENCES users(id) ON DELETE RESTRICT",
    "ALTER TABLE categories ALTER CONSTRAINT categories_parent_id_fkey DEFERRABLE INITIALLY DEFERRED",
    """
    CREATE TABLE IF NOT EXISTS user_books (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
        book_id TEXT NOT NULL REFERENCES books(id) ON DELETE RESTRICT,
        reading_status TEXT,
        date_added TIMESTAMPTZ,
        start_date TIMESTAMPTZ,
        finish_date TIMESTAMPTZ,
        current_page BIGINT,
        total_pages BIGINT,
        ownership_status TEXT,
        media_type TEXT,
        borrowed_from TEXT,
        borrowed_from_user_id TEXT,
        borrowed_date TIMESTAMPTZ,
        borrowed_due_date TIMESTAMPTZ,
        loaned_to TEXT,
        loaned_to_user_id TEXT,
        loaned_date TIMESTAMPTZ,
        loaned_due_date TIMESTAMPTZ,
        location_id TEXT,
        user_rating DOUBLE PRECISION,
        rating_date TIMESTAMPTZ,
        user_review TEXT,
        review_date TIMESTAMPTZ,
        is_review_spoiler BOOLEAN,
        personal_notes TEXT,
        pace TEXT,
        character_driven BOOLEAN,
        source TEXT,
        custom_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS user_books_user_book_uq ON user_books (user_id, book_id)",
    "CREATE INDEX IF NOT EXISTS user_books_status_idx ON user_books (user_id, reading_status)",
    """
    CREATE TABLE IF NOT EXISTS reading_logs (
        id TEXT PRIMARY KEY,
        user_id TEXT REFERENCES users(id) ON DELETE RESTRICT,
        book_id TEXT REFERENCES books(id) ON DELETE RESTRICT,
        date DATE,
        pages_read BIGINT,
        minutes_read BIGINT,
        notes TEXT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    "ALTER TABLE reading_logs ALTER COLUMN user_id DROP NOT NULL",
    "ALTER TABLE reading_logs ALTER COLUMN book_id DROP NOT NULL",
    "CREATE INDEX IF NOT EXISTS reading_logs_user_date_idx ON reading_logs (user_id, date)",
    """
    CREATE TABLE IF NOT EXISTS book_contributors (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        source_edge_id BIGINT UNIQUE,
        source_relationship TEXT,
        book_id TEXT NOT NULL REFERENCES books(id) ON DELETE RESTRICT,
        person_id TEXT NOT NULL REFERENCES people(id) ON DELETE RESTRICT,
        contribution_type TEXT,
        role TEXT,
        order_index BIGINT,
        created_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    "ALTER TABLE book_contributors ADD COLUMN IF NOT EXISTS source_edge_id BIGINT",
    "ALTER TABLE book_contributors ADD COLUMN IF NOT EXISTS source_relationship TEXT",
    "CREATE UNIQUE INDEX IF NOT EXISTS book_contributors_source_edge_uq ON book_contributors (source_edge_id) WHERE source_edge_id IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS book_contributors_book_idx ON book_contributors (book_id, order_index)",
    """
    CREATE TABLE IF NOT EXISTS book_authors (
        book_id TEXT NOT NULL REFERENCES books(id) ON DELETE RESTRICT,
        author_id TEXT NOT NULL REFERENCES authors(id) ON DELETE RESTRICT,
        PRIMARY KEY (book_id, author_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS book_publishers (
        book_id TEXT NOT NULL REFERENCES books(id) ON DELETE RESTRICT,
        publisher_id TEXT NOT NULL REFERENCES publishers(id) ON DELETE RESTRICT,
        publication_date DATE,
        created_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb,
        PRIMARY KEY (book_id, publisher_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS book_categories (
        book_id TEXT NOT NULL REFERENCES books(id) ON DELETE RESTRICT,
        category_id TEXT NOT NULL REFERENCES categories(id) ON DELETE RESTRICT,
        created_at TIMESTAMPTZ,
        PRIMARY KEY (book_id, category_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS book_series (
        book_id TEXT NOT NULL REFERENCES books(id) ON DELETE RESTRICT,
        series_id TEXT NOT NULL REFERENCES series(id) ON DELETE RESTRICT,
        volume_number BIGINT,
        volume_number_double DOUBLE PRECISION,
        series_order BIGINT,
        created_at TIMESTAMPTZ,
        PRIMARY KEY (book_id, series_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS book_locations (
        book_id TEXT NOT NULL REFERENCES books(id) ON DELETE RESTRICT,
        location_id TEXT NOT NULL REFERENCES locations(id) ON DELETE RESTRICT,
        created_at TIMESTAMPTZ,
        PRIMARY KEY (book_id, location_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS custom_field_definitions (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        display_name TEXT,
        field_type TEXT,
        description TEXT,
        created_by_user_id TEXT,
        is_shareable BOOLEAN,
        is_global BOOLEAN,
        default_value TEXT,
        placeholder_text TEXT,
        help_text TEXT,
        predefined_options JSONB NOT NULL DEFAULT '[]'::jsonb,
        allow_custom_options BOOLEAN,
        rating_min BIGINT,
        rating_max BIGINT,
        rating_labels JSONB NOT NULL DEFAULT '{}'::jsonb,
        usage_count BIGINT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS custom_fields (
        id TEXT PRIMARY KEY,
        user_id TEXT REFERENCES users(id) ON DELETE RESTRICT,
        definition_id TEXT REFERENCES custom_field_definitions(id) ON DELETE RESTRICT,
        book_id TEXT REFERENCES books(id) ON DELETE RESTRICT,
        field_name TEXT,
        field_value JSONB,
        is_required BOOLEAN,
        is_active BOOLEAN,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_custom_fields (
        id TEXT PRIMARY KEY,
        field_name TEXT,
        field_type TEXT,
        is_required BOOLEAN,
        default_value TEXT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS import_mappings (
        id TEXT PRIMARY KEY,
        name TEXT,
        description TEXT,
        user_id TEXT,
        is_system BOOLEAN,
        source_type TEXT,
        field_mappings JSONB NOT NULL DEFAULT '{}'::jsonb,
        sample_headers JSONB NOT NULL DEFAULT '[]'::jsonb,
        is_shareable BOOLEAN,
        usage_count BIGINT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS import_jobs (
        id TEXT PRIMARY KEY,
        task_id TEXT,
        user_id TEXT,
        csv_file_path TEXT,
        field_mappings JSONB NOT NULL DEFAULT '{}'::jsonb,
        default_reading_status TEXT,
        duplicate_handling TEXT,
        custom_fields_enabled BOOLEAN,
        status TEXT,
        processed BIGINT,
        success BIGINT,
        errors BIGINT,
        total BIGINT,
        start_time TIMESTAMPTZ,
        end_time TIMESTAMPTZ,
        current_book TEXT,
        error_messages JSONB NOT NULL DEFAULT '[]'::jsonb,
        recent_activity JSONB NOT NULL DEFAULT '[]'::jsonb,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        expires_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS import_tasks (
        id TEXT PRIMARY KEY,
        user_id TEXT,
        task_type TEXT,
        status TEXT,
        progress BIGINT,
        total_items BIGINT,
        processed_items BIGINT,
        file_path TEXT,
        parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
        results JSONB NOT NULL DEFAULT '{}'::jsonb,
        error_message TEXT,
        created_at TIMESTAMPTZ,
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        extra JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """,
)

# A schema release is immutable once shipped. Any later DDL change must be
# introduced as a new versioned migration instead of silently changing this
# checksum. This prevents an existing installation from accepting an
# unreviewed schema shape during startup.
_SCHEMA_VERSION = 1
_SCHEMA_NAME = "normalized-postgres-bootstrap"
_SCHEMA_MIGRATIONS = (
    (
        2,
        "user-series-notes",
        (
            """
            CREATE TABLE IF NOT EXISTS user_series_notes (
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
                series_id TEXT NOT NULL REFERENCES series(id) ON DELETE RESTRICT,
                notes TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (user_id, series_id)
            )
            """,
        ),
    ),
    (
        3,
        "book-location-overrides",
        (
            """
            CREATE TABLE IF NOT EXISTS book_location_overrides (
                book_id TEXT PRIMARY KEY REFERENCES books(id) ON DELETE RESTRICT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """,
        ),
    ),
    (
        4,
        "import-mapping-templates",
        (
            """
            CREATE TABLE IF NOT EXISTS import_mapping_templates (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL DEFAULT '',
                description TEXT,
                source_type TEXT NOT NULL DEFAULT '',
                sample_headers JSONB NOT NULL DEFAULT '[]'::jsonb,
                field_mappings JSONB NOT NULL DEFAULT '{}'::jsonb,
                times_used BIGINT NOT NULL DEFAULT 0,
                last_used TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                extra JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """,
            "CREATE INDEX IF NOT EXISTS import_mapping_templates_user_created_idx ON import_mapping_templates (user_id, created_at DESC)",
        ),
    ),
    (
        5,
        "global-metadata-records",
        (
            """
            CREATE TABLE IF NOT EXISTS global_metadata_records (
                id TEXT PRIMARY KEY,
                book_id TEXT NOT NULL UNIQUE REFERENCES books(id) ON DELETE RESTRICT,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ,
                extra JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """,
        ),
    ),
    (
        6,
        "legacy-custom-field-definitions",
        (
            """
            CREATE TABLE IF NOT EXISTS legacy_custom_field_definitions (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL DEFAULT '',
                display_name TEXT,
                field_type TEXT,
                description TEXT,
                created_by_user_id TEXT,
                is_shareable BOOLEAN,
                is_global BOOLEAN,
                default_value TEXT,
                placeholder_text TEXT,
                help_text TEXT,
                predefined_options JSONB NOT NULL DEFAULT '[]'::jsonb,
                allow_custom_options BOOLEAN,
                rating_min BIGINT,
                rating_max BIGINT,
                rating_labels JSONB NOT NULL DEFAULT '{}'::jsonb,
                usage_count BIGINT,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ,
                extra JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """,
            "CREATE INDEX IF NOT EXISTS legacy_custom_field_definitions_name_ci_idx ON legacy_custom_field_definitions (lower(name))",
        ),
    ),
)


class PostgresNotConfigured(RuntimeError):
    pass


# A Kuzu upgrade projects into these application tables. Refuse to merge into
# a populated target: IDs may overlap with Kuzu IDs, and an implicit merge
# could overwrite unrelated PostgreSQL users or library records.
POSTGRES_MIGRATION_TARGET_TABLES = (
    "users", "books", "user_books", "reading_logs", "book_contributors",
    "book_relationship_edges", "people", "authors", "publishers", "categories",
    "series", "libraries", "library_shelves", "reading_sessions", "reading_goals",
    "reviews", "locations", "book_authors", "book_publishers", "book_categories",
    "book_series", "book_locations", "custom_field_definitions", "custom_fields",
    "user_custom_fields", "import_mappings", "import_jobs", "import_tasks",
    "user_series_notes", "book_location_overrides", "import_mapping_templates",
    "global_metadata_records", "legacy_custom_field_definitions",
)


class PostgresMigrationTargetNotEmpty(RuntimeError):
    """The configured PostgreSQL target already contains application data."""


class PostgresStore:
    """Psycopg connection pool with explicit transaction boundaries."""

    def __init__(self, dsn: str | None = None, *, min_size: int = 1, max_size: int = 10):
        self.dsn = (dsn or os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL") or "").strip()
        if not self.dsn:
            host = (os.getenv("POSTGRES_HOST") or "").strip()
            user = (os.getenv("POSTGRES_USER") or "").strip()
            database = (os.getenv("POSTGRES_DB") or "").strip()
            password = os.getenv("POSTGRES_PASSWORD") or ""
            if not all((host, user, database, password)):
                raise PostgresNotConfigured(
                    "Set POSTGRES_DSN or configure POSTGRES_HOST, POSTGRES_DB, "
                    "POSTGRES_USER, and a non-empty POSTGRES_PASSWORD"
                )
            try:
                from psycopg.conninfo import make_conninfo
            except ImportError as exc:
                raise RuntimeError("psycopg is required for the PostgreSQL backend") from exc
            self.dsn = make_conninfo(
                host=host,
                port=os.getenv("POSTGRES_PORT", "5432"),
                dbname=database,
                user=user,
                password=password,
                connect_timeout=os.getenv("POSTGRES_CONNECT_TIMEOUT", "10"),
                application_name="mybibliotheca",
            )
        self.min_size = max(0, int(min_size))
        self.max_size = max(self.min_size or 1, int(max_size))
        self._pool: Any = None
        self._lock = threading.Lock()

    def _get_pool(self):
        if self._pool is not None:
            return self._pool
        with self._lock:
            if self._pool is None:
                try:
                    from psycopg_pool import ConnectionPool
                except ImportError as exc:
                    raise RuntimeError("psycopg_pool is required for the PostgreSQL backend") from exc
                pool = ConnectionPool(
                    conninfo=self.dsn,
                    min_size=self.min_size,
                    max_size=self.max_size,
                    open=False,
                    timeout=float(os.getenv("POSTGRES_POOL_TIMEOUT", "10")),
                    kwargs={"application_name": "mybibliotheca"},
                )
                pool.open(wait=True, timeout=float(os.getenv("POSTGRES_CONNECT_TIMEOUT", "10")))
                self._pool = pool
        return self._pool

    @contextmanager
    def connection(self) -> Iterator[Any]:
        """Borrow a pooled connection; caller owns transaction boundaries."""
        with self._get_pool().connection() as connection:
            yield connection

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        """Borrow a connection and commit or roll back one atomic unit."""
        with self.connection() as connection:
            with connection.transaction():
                yield connection

    def initialize_schema(self) -> None:
        """Bootstrap once, then apply only unapplied versioned migrations.

        Running the bootstrap DDL on every call repeatedly takes AccessExclusive
        locks for compatibility ALTERs. Normal requests and cutover audits can
        read those tables concurrently, so rerunning that DDL can deadlock. The
        schema history row is committed atomically with the bootstrap; once it
        exists, the immutable bootstrap is complete and only new migrations
        should execute.
        """
        checksum = hashlib.sha256("\n-- statement --\n".join(SCHEMA_DDL).encode("utf-8")).hexdigest()
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                # This metadata table must exist before we can take the schema
                # lock and inspect the history. Its IF NOT EXISTS check does
                # not lock application tables.
                cursor.execute(SCHEMA_DDL[0])
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("mybibliotheca:postgres:schema",))
                cursor.execute("SELECT version, checksum FROM app_schema_migrations ORDER BY version")
                installed = {int(version): str(installed_checksum) for version, installed_checksum in cursor.fetchall()}
                if not installed:
                    # Fresh PostgreSQL install. Keep the entire bootstrap and
                    # its version record in this transaction so a crash cannot
                    # leave a partially initialized schema marked complete.
                    for statement in SCHEMA_DDL[1:]:
                        cursor.execute(statement)
                    cursor.execute(
                        "INSERT INTO app_schema_migrations (version, name, checksum) VALUES (%s, %s, %s)",
                        (_SCHEMA_VERSION, _SCHEMA_NAME, checksum),
                    )
                    installed[_SCHEMA_VERSION] = checksum
                elif _SCHEMA_VERSION not in installed:
                    raise RuntimeError("PostgreSQL bootstrap schema version record is missing")
                elif installed[_SCHEMA_VERSION] != checksum:
                    raise RuntimeError(
                        f"PostgreSQL schema version {_SCHEMA_VERSION} checksum mismatch; "
                        "refusing to continue with unversioned schema changes"
                    )

                latest_version = _SCHEMA_MIGRATIONS[-1][0] if _SCHEMA_MIGRATIONS else _SCHEMA_VERSION
                if installed and max(installed) > latest_version:
                    raise RuntimeError(
                        f"PostgreSQL schema version {max(installed)} is newer than this application supports "
                        f"({latest_version})"
                    )
                for version, name, statements in _SCHEMA_MIGRATIONS:
                    migration_checksum = hashlib.sha256("\n-- statement --\n".join(statements).encode("utf-8")).hexdigest()
                    if version in installed:
                        if installed[version] != migration_checksum:
                            raise RuntimeError(
                                f"PostgreSQL schema version {version} checksum mismatch; refusing unversioned changes"
                            )
                        continue
                    if version != max(installed, default=0) + 1:
                        raise RuntimeError(f"PostgreSQL schema migration history is missing version {version - 1}")
                    for statement in statements:
                        cursor.execute(statement)
                    cursor.execute(
                        "INSERT INTO app_schema_migrations (version, name, checksum) VALUES (%s, %s, %s)",
                        (version, name, migration_checksum),
                    )
                    installed[version] = migration_checksum

    def health_check(self) -> dict[str, Any]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT current_database(), current_user, now()")
                database, user, server_time = cursor.fetchone()
        return {"ok": True, "database": database, "user": user, "server_time": server_time.isoformat()}

    def create_import_job(self, user_id: str, task_id: str, job_data: dict[str, Any]) -> bool:
        """Persist an import job so every Gunicorn worker can see it."""
        payload = json.dumps(job_data, ensure_ascii=False, default=str)
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO import_jobs (id, task_id, user_id, status, extra, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s::jsonb, now(), now())
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id
                    """,
                    (str(task_id), str(task_id), str(user_id), job_data.get("status", "pending"), payload),
                )
                return cursor.fetchone() is not None

    def update_import_job(self, user_id: str, task_id: str, updates: dict[str, Any]) -> bool:
        """Apply an import job patch atomically, scoped to its owning user."""
        payload = json.dumps(updates, ensure_ascii=False, default=str)
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE import_jobs
                    SET extra = COALESCE(extra, '{}'::jsonb) || %s::jsonb,
                        status = COALESCE(%s, status),
                        updated_at = now()
                    WHERE user_id = %s AND task_id = %s
                    RETURNING id
                    """,
                    (payload, updates.get("status"), str(user_id), str(task_id)),
                )
                return cursor.fetchone() is not None

    def get_import_job(self, user_id: str, task_id: str) -> dict[str, Any] | None:
        """Read a user-owned import job from shared PostgreSQL storage."""
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT extra FROM import_jobs WHERE user_id = %s AND task_id = %s",
                    (str(user_id), str(task_id)),
                )
                row = cursor.fetchone()
        if not row:
            return None
        payload = row[0]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return dict(payload) if isinstance(payload, dict) else None

    def get_user_import_jobs(self, user_id: str) -> dict[str, dict[str, Any]]:
        """List import jobs owned by one user."""
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT task_id, extra FROM import_jobs WHERE user_id = %s ORDER BY created_at DESC",
                    (str(user_id),),
                )
                rows = cursor.fetchall()
        jobs: dict[str, dict[str, Any]] = {}
        for task_id, payload in rows:
            if isinstance(payload, str):
                payload = json.loads(payload)
            if task_id and isinstance(payload, dict):
                jobs[str(task_id)] = dict(payload)
        return jobs

    def delete_import_job(self, user_id: str, task_id: str) -> bool:
        """Delete a user-owned import job."""
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM import_jobs WHERE user_id = %s AND task_id = %s RETURNING id",
                    (str(user_id), str(task_id)),
                )
                return cursor.fetchone() is not None

    def assert_migration_target_empty(self, connection: Any | None = None, *, lock: bool = False) -> None:
        """Refuse Kuzu import into a PostgreSQL database with application rows.

        When called inside the migration transaction, ``lock=True`` holds
        table locks through commit so another writer cannot populate the
        target after the emptiness check and before projection.
        """
        if connection is None:
            with self.connection() as borrowed:
                self.assert_migration_target_empty(borrowed, lock=lock)
            return
        with connection.cursor() as cursor:
            if lock:
                cursor.execute(
                    "LOCK TABLE " + ", ".join(POSTGRES_MIGRATION_TARGET_TABLES) +
                    " IN SHARE ROW EXCLUSIVE MODE"
                )
            populated: list[str] = []
            for table in POSTGRES_MIGRATION_TARGET_TABLES:
                cursor.execute(f"SELECT count(*) FROM {table}")
                row_count = int(cursor.fetchone()[0])
                if row_count:
                    populated.append(f"{table} ({row_count})")
        if populated:
            raise PostgresMigrationTargetNotEmpty(
                "PostgreSQL migration target must be empty before Kuzu cutover; found "
                + ", ".join(populated)
            )

    def close(self) -> None:
        with self._lock:
            if self._pool is not None:
                self._pool.close()
                self._pool = None


_store: PostgresStore | None = None
_store_lock = threading.Lock()


def get_postgres_store() -> PostgresStore:
    """Return a process-wide lazy store using configured DSN settings."""
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = PostgresStore()
    return _store


def reset_postgres_store_for_tests() -> None:
    global _store
    with _store_lock:
        if _store is not None:
            _store.close()
        _store = None
