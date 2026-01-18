"""Backfill missing book covers.

Scans for Book nodes with empty cover_url and tries to fetch the best available
cover (Google-first, OpenLibrary fallback). When possible, it downloads and
stores a processed local cover into the covers directory so the UI can serve it
from /covers/<file>.

Usage:
  "/Users/jeremiah/Documents/Python Projects/mybibliotheca/.venv/bin/python" scripts/backfill_missing_covers.py --limit 200

Notes:
- Safe to run without a Flask app context.
- Uses SafeKuzuManager for short-lived, locked DB access.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Optional


# Ensure `import app...` works when running as a script.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _pick_isbn(isbn13: Optional[str], isbn10: Optional[str]) -> Optional[str]:
    for cand in (isbn13, isbn10):
        if isinstance(cand, str) and cand.strip():
            return cand.strip()
    return None


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Backfill missing Book.cover_url values")
    parser.add_argument("--limit", type=int, default=200, help="Max books to process")
    parser.add_argument("--dry-run", action="store_true", help="Do not write changes")
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Do not download/process covers; store remote URL only",
    )
    args = parser.parse_args(argv)

    from app.utils.safe_kuzu_manager import safe_get_connection
    from app.utils.book_utils import get_best_cover_for_book

    # Only import image_processing if caching is enabled.
    process_image_from_url = None
    if not args.no_cache:
        from app.utils.image_processing import process_image_from_url as _process_image_from_url
        process_image_from_url = _process_image_from_url

    processed = 0
    updated = 0
    skipped = 0

    with safe_get_connection(operation="backfill_missing_covers") as conn:
        rows_raw = conn.execute(
            """
            MATCH (b:Book)
            WHERE b.cover_url IS NULL OR b.cover_url = ''
            RETURN b.id AS id, b.title AS title, b.isbn13 AS isbn13, b.isbn10 AS isbn10
            LIMIT $limit
            """,
            {"limit": int(max(1, args.limit))},
        )
        rows: list[tuple[Any, ...]] = [tuple(r) for r in rows_raw]

        if not rows:
            print("No books with missing cover_url found.")
            return 0

        for row in rows:
            processed += 1
            # Kuzu can return mixed/Any typed values; normalize defensively.
            book_id = str(row[0]) if row[0] is not None else ''
            title = str(row[1]) if row[1] is not None else ''
            isbn13_val = row[2]
            isbn10_val = row[3]
            isbn13 = isbn13_val if isinstance(isbn13_val, str) else None
            isbn10 = isbn10_val if isinstance(isbn10_val, str) else None

            isbn = _pick_isbn(isbn13, isbn10)
            if not isbn:
                skipped += 1
                continue

            best = None
            try:
                best = get_best_cover_for_book(isbn=isbn, title=title, author=None)
            except Exception:
                best = None

            cover_url = (best or {}).get("cover_url") if isinstance(best, dict) else None
            if not cover_url:
                skipped += 1
                continue

            final_url = cover_url
            if not args.no_cache and process_image_from_url is not None:
                try:
                    final_url = process_image_from_url(
                        cover_url,
                        headers={"User-Agent": "Mozilla/5.0 (compatible; MyBibliotheca/1.0)"},
                    )
                except Exception:
                    final_url = cover_url

            if args.dry_run:
                print(f"[DRY] {title!r} ({isbn}) -> {final_url}")
                updated += 1
                continue

            conn.execute(
                """
                MATCH (b:Book {id: $id})
                SET b.cover_url = $cover_url
                RETURN b.id
                """,
                {"id": book_id, "cover_url": final_url},
            )
            updated += 1

    print(f"Processed: {processed} | Updated: {updated} | Skipped: {skipped}")
    if args.dry_run:
        print("Dry-run mode: no DB changes were written.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
