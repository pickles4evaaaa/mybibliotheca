#!/usr/bin/env python3
"""
Export Legacy Single-User Postgres Database to Legacy V1 SQLite Snapshot
=======================================================================

Purpose:
  Produce a SQLite file that mimics the original single-user (v1) MyBibliotheca schema
  so the existing migration pathway (advanced_migration_system) can import it.

Assumptions:
  - Source is a Postgres database reachable via DATABASE_URL in a .env file OR env var already loaded.
  - Database logically matches the old single-user schema (tables like book, reading_log, etc.)
  - Single user => we'll synthesize a 'user' table with one admin row if it doesn't exist.

Usage:
  python scripts/export_postgres_single_user_to_sqlite.py --output legacy_snapshot.sqlite

Environment:
    DATABASE_URL can be in any of these forms (will be normalized):
        - postgresql://user:pass@host:5432/dbname
        - postgres://user:pass@host:5432/dbname
        - postgresql+psycopg://user:pass@host:5432/dbname (SQLAlchemy style)
    We normalize to a psycopg-compatible "postgresql://" URL.

Safety:
  - Read-only SELECT access required.
  - Script does not modify Postgres.

Output:
  - SQLite file with tables & data.
  - manifest JSON (embedded in sqlite as meta table + printed summary).

Tables Exported (if present):
  book, author, book_author, category, book_category, reading_log, custom_field, custom_field_value,
  plus synthesized user table if absent.

Batching:
  - Streams rows in batches to minimize memory.

"""
import os
import sys
import argparse
import sqlite3
import json
import math
import time
from datetime import datetime, timezone
from contextlib import closing

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None

try:
    import psycopg
except ImportError:
    print("psycopg not installed. Please add psycopg (psycopg[binary]) to requirements if needed.")
    sys.exit(1)

BATCH_SIZE = 1000
LEGACY_TABLES = [
    "book",
    "author",
    "book_author",
    "category",
    "book_category",
    "reading_log",
    "custom_field",
    "custom_field_value"
]

# Minimal legacy v1 schema DDL (adjust as needed based on actual historical schema)
LEGACY_DDL = {
    "book": """CREATE TABLE IF NOT EXISTS book (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        title TEXT,\n        subtitle TEXT,\n        authors TEXT,\n        description TEXT,\n        isbn TEXT,\n        isbn13 TEXT,\n        language TEXT,\n        page_count INTEGER,\n        publisher TEXT,\n        published_year INTEGER,\n        created_at TEXT,\n        updated_at TEXT\n    );""",
    "author": """CREATE TABLE IF NOT EXISTS author (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        name TEXT\n    );""",
    "book_author": """CREATE TABLE IF NOT EXISTS book_author (\n        book_id INTEGER,\n        author_id INTEGER\n    );""",
    "category": """CREATE TABLE IF NOT EXISTS category (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        name TEXT\n    );""",
    "book_category": """CREATE TABLE IF NOT EXISTS book_category (\n        book_id INTEGER,\n        category_id INTEGER\n    );""",
    "reading_log": """CREATE TABLE IF NOT EXISTS reading_log (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        book_id INTEGER,\n        started_at TEXT,\n        finished_at TEXT,\n        notes TEXT,\n        status TEXT\n    );""",
    "custom_field": """CREATE TABLE IF NOT EXISTS custom_field (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        name TEXT,\n        field_type TEXT\n    );""",
    "custom_field_value": """CREATE TABLE IF NOT EXISTS custom_field_value (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        book_id INTEGER,\n        field_id INTEGER,\n        value TEXT\n    );""",
    # Synthesized user table for importer expectations
    "user": """CREATE TABLE IF NOT EXISTS user (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        username TEXT,\n        email TEXT,\n        password_hash TEXT,\n        is_admin INTEGER\n    );""",
    # Meta table
    "_export_meta": """CREATE TABLE IF NOT EXISTS _export_meta (\n        key TEXT PRIMARY KEY,\n        value TEXT\n    );"""
}

# Column selection map: Postgres columns -> legacy columns order (subset friendly)
COLUMN_MAP = {
    "book": ["id","title","subtitle","authors","description","isbn","isbn13","language","page_count","publisher","published_year","created_at","updated_at"],
    "author": ["id","name"],
    "book_author": ["book_id","author_id"],
    "category": ["id","name"],
    "book_category": ["book_id","category_id"],
    "reading_log": ["id","book_id","started_at","finished_at","notes","status"],
    "custom_field": ["id","name","field_type"],
    "custom_field_value": ["id","book_id","field_id","value"],
}

SYNTH_USER = {
    "username": "legacy_admin",
    "email": "legacy@example.com",
    "password_hash": "legacy_migrated",  # placeholder; real admin created during migration
    "is_admin": 1
}

def load_env():
    if load_dotenv:
        load_dotenv()


def get_database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        print("DATABASE_URL not set in environment or .env file")
        sys.exit(1)

    # Normalize SQLAlchemy style driver prefixes to plain postgres
    # e.g. postgresql+psycopg://  -> postgresql://
    #      postgresql+psycopg2:// -> postgresql://
    # Accept "postgres://" (common Heroku style) and leave as-is (psycopg accepts both)
    if url.startswith("postgresql+"):
        plus_idx = url.find("+")
        rest = url.split("://", 1)[1]
        url = "postgresql://" + rest
    return url


def connect_postgres(url: str):
    # psycopg (v3) connect
    return psycopg.connect(url, autocommit=False)


def table_exists(cur, table: str) -> bool:
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name=%s LIMIT 1", (table,))
    return cur.fetchone() is not None


def fetch_count(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    row = cur.fetchone()
    return row[0] if row else 0


def ensure_sqlite_schema(sqlite_conn):
    with sqlite_conn:
        for ddl in LEGACY_DDL.values():
            sqlite_conn.execute(ddl)


def export_table(pg_cur, sqlite_conn, table: str, columns: list):
    """Export a single table, adapting to missing columns (e.g. subtitle)."""
    if not table_exists(pg_cur, table):
        print(f"[SKIP] Table {table} not found in source")
        return 0
    # Introspect actual columns present
    pg_cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s", (table,))
    present_cols = {r[0] for r in pg_cur.fetchall()}
    # Build select list; if a requested column missing, substitute NULL AS col
    select_exprs = []
    for c in columns:
        if c in present_cols:
            select_exprs.append(c)
        else:
            select_exprs.append(f"NULL AS {c}")
            if table == 'book':
                print(f"[WARN] Column '{c}' missing in source 'book' table; filling with NULL")
    select_sql = ",".join(select_exprs)
    total = fetch_count(pg_cur, table)
    if total == 0:
        print(f"[INFO] Table {table} empty")
        return 0
    print(f"[EXPORT] {table} rows={total}")
    offset = 0
    inserted = 0
    placeholder = ",".join(["?"] * len(columns))
    while offset < total:
        pg_cur.execute(f"SELECT {select_sql} FROM {table} ORDER BY {columns[0]} OFFSET %s LIMIT %s", (offset, BATCH_SIZE))
        rows = pg_cur.fetchall()
        if not rows:
            break
        sqlite_conn.executemany(
            f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholder})",
            rows
        )
        inserted += len(rows)
        offset += len(rows)
        if inserted % (BATCH_SIZE * 5) == 0:
            sqlite_conn.commit()
            print(f"  Progress {inserted}/{total}")
    sqlite_conn.commit()
    return inserted


def insert_single_user(sqlite_conn):
    cur = sqlite_conn.execute("SELECT COUNT(*) FROM user")
    if cur.fetchone()[0] == 0:
        sqlite_conn.execute(
            "INSERT INTO user (username,email,password_hash,is_admin) VALUES (?,?,?,?)",
            (SYNTH_USER["username"], SYNTH_USER["email"], SYNTH_USER["password_hash"], SYNTH_USER["is_admin"])
        )
        sqlite_conn.commit()
        print("[INFO] Synthesized single legacy user row")


def write_meta(sqlite_conn, meta: dict):
    with sqlite_conn:
        for k, v in meta.items():
            sqlite_conn.execute("REPLACE INTO _export_meta (key,value) VALUES (?,?)", (k, json.dumps(v)))


def main():
    parser = argparse.ArgumentParser(description="Export legacy single-user Postgres DB to legacy-format SQLite")
    parser.add_argument("--output", default="legacy_snapshot.sqlite", help="Output SQLite filename")
    parser.add_argument("--skip-custom-fields", action="store_true", help="Skip custom field tables even if present")
    args = parser.parse_args()

    load_env()
    db_url = get_database_url()

    start = time.time()
    with closing(connect_postgres(db_url)) as pg_conn, pg_conn.cursor() as pg_cur:
        # Prepare SQLite
        if os.path.exists(args.output):
            os.remove(args.output)
        sqlite_conn = sqlite3.connect(args.output)
        try:
            # Performance pragmas
            sqlite_conn.execute("PRAGMA journal_mode=WAL")
            sqlite_conn.execute("PRAGMA synchronous=OFF")
            sqlite_conn.execute("PRAGMA temp_store=MEMORY")
            ensure_sqlite_schema(sqlite_conn)

            def _iso_utc_now():
                return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

            manifest = {"exported_tables": {}, "started_at": _iso_utc_now(), "source": "postgres_single_user"}

            for table in LEGACY_TABLES:
                if args.skip_custom_fields and table.startswith("custom_field"):
                    continue
                cols = COLUMN_MAP.get(table)
                if not cols:
                    continue
                count = export_table(pg_cur, sqlite_conn, table, cols)
                manifest["exported_tables"][table] = count

            # Ensure user table present & row
            insert_single_user(sqlite_conn)
            manifest["exported_tables"]["user"] = 1

            # Meta
            manifest["completed_at"] = _iso_utc_now()
            manifest["duration_sec"] = round(time.time() - start, 2)
            write_meta(sqlite_conn, manifest)
        finally:
            sqlite_conn.close()

    # After context managers closed
    print("\n[SUMMARY]")
    print(json.dumps(manifest, indent=2))
    print(f"\n[OK] Export complete -> {args.output}")

if __name__ == "__main__":
    main()
