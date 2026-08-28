"""Series relationship migration.

Idempotently migrates legacy Book.series / Book.series_volume / Book.series_order
fields into Series nodes + PART_OF_SERIES relationships.

Rules per Series Upgrade spec:
 - Keep legacy fields intact (no deletion / blanking)
 - Deterministic Series ID based on normalized name (preserve original casing in name)
 - Volume parsing supports integers, floats, textual prefixes, and ranges (lower bound);
   stored as DOUBLE in relationship property volume_number_double (adds if missing).
 - No uniqueness enforcement on series_order.
 - Run incrementally: existing relationships are preserved and only unlinked
   legacy books are processed on each run. The marker is informational and
   does not prevent later imports from being repaired.
"""
from __future__ import annotations

import os
import json
import re
import uuid
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

from app.utils.safe_kuzu_manager import get_safe_kuzu_manager

MARKER_FILENAME = "schema_preflight_state.series_entity_migration.json"
logger = logging.getLogger(__name__)

_VOLUME_RANGE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*$")
_VOLUME_FLOAT_RE = re.compile(r"(\d+(?:\.\d+)?)")

def _data_root() -> Path:
    kuzu_path = Path(os.getenv("KUZU_DB_PATH", "data/kuzu")).resolve()
    return kuzu_path.parent if kuzu_path.name == "kuzu" else kuzu_path

def _marker_path() -> Path:
    return _data_root() / MARKER_FILENAME

def marker_exists() -> bool:
    return _marker_path().exists()

def write_marker(payload: dict) -> None:
    try:
        mp = _marker_path()
        mp.write_text(json.dumps(payload, indent=2))
    except Exception:
        pass

def parse_volume(raw: Optional[str]) -> Optional[float]:
    if not raw:
        return None
    txt = raw.strip()
    if not txt:
        return None
    # Range first (lower bound)
    m = _VOLUME_RANGE_RE.match(txt)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            return None
    # Any number inside (handles vol 1, volume 2, bk03, #4, 2.5 etc.)
    m2 = _VOLUME_FLOAT_RE.search(txt.lower())
    if m2:
        try:
            return float(m2.group(1))
        except Exception:
            return None
    return None

def deterministic_series_id(name: str) -> str:
    norm = name.strip().lower()
    h = hashlib.sha256(norm.encode("utf-8")).hexdigest()[:20]
    return f"series_{h}"


def _ensure_series_columns(conn) -> None:
    """Add optional Series properties required by the current Series UI.

    Some beta databases were created from an older SafeKuzu schema that only
    had ``total_books`` on Series.  The relationship migration is already a
    guaranteed startup path, so it is a safe place to make this additive
    upgrade before any Series queries run.
    """
    columns = (
        ("user_cover", "STRING"),
        ("cover_url", "STRING"),
        ("custom_cover", "BOOLEAN"),
        ("generated_placeholder", "BOOLEAN"),
    )
    for column, column_type in columns:
        try:
            conn.execute(f"MATCH (s:Series) RETURN s.{column} LIMIT 1")
        except Exception as probe_error:
            message = str(probe_error)
            if "Cannot find property" not in message or column not in message:
                continue
            try:
                conn.execute(f"ALTER TABLE Series ADD {column} {column_type}")
                logger.info("Added missing Series.%s column", column)
            except Exception as alter_error:
                if "already exists" not in str(alter_error).lower():
                    logger.warning("Could not add Series.%s: %s", column, alter_error)

def run_series_migration(verbose: bool = False) -> dict:
    mgr = get_safe_kuzu_manager()
    result_summary = {
        "skipped": False,
        "series_created": 0,
        "relationships_created": 0,
        "books_processed": 0,
    }
    # NOTE: UI button interactions for series editing/cover upload are separate concerns
    # and not part of this backend migration.
    with mgr.get_connection(operation="series_migration") as conn:
        _ensure_series_columns(conn)
        # Pull only unlinked legacy books. This query is intentionally
        # idempotent, so the migration can safely run at every startup and
        # repair books imported after an earlier marker was written.
        query = """
        MATCH (b:Book)
        WHERE b.series IS NOT NULL AND b.series <> ''
              AND NOT EXISTS { MATCH (b)-[:PART_OF_SERIES]->(:Series) }
        RETURN b.id, b.series, b.series_volume, b.series_order
        """
        try:
            rs_raw = conn.execute(query)
        except Exception:
            return result_summary
        # Normalize list result
        rs_main = rs_raw[0] if isinstance(rs_raw, list) and rs_raw else rs_raw
        rows = []
        if rs_main:
            try:
                while getattr(rs_main, 'has_next', lambda: False)():
                    try:
                        row = rs_main.get_next()  # type: ignore[attr-defined]
                    except Exception:
                        break
                    rows.append(row)
            except Exception:
                pass
        if not rows:
            write_marker({"skipped": True, "reason": "no_legacy_data", "ts": datetime.now(timezone.utc).isoformat()})
            result_summary["skipped"] = True
            return result_summary
        result_summary["books_processed"] = len(rows)
        # Cache for created series
        created_series_norms = {}
        for row in rows:
            try:
                book_id = row[0]
                series_name = row[1]
                volume_raw = row[2]
                series_order = row[3]
            except Exception:
                continue
            if not series_name:
                continue
            norm = series_name.strip().lower()
            # Ensure series node
            if norm not in created_series_norms:
                sid = deterministic_series_id(series_name)
                # Check if exists
                exists_rs_raw = conn.execute("MATCH (s:Series {id:$id}) RETURN s.id LIMIT 1", {"id": sid})
                exists_rs = exists_rs_raw[0] if isinstance(exists_rs_raw, list) and exists_rs_raw else exists_rs_raw
                exists = False
                if exists_rs:
                    try:
                        while getattr(exists_rs, 'has_next', lambda: False)():
                            _ = exists_rs.get_next()  # type: ignore[attr-defined]
                            exists = True
                            break
                    except Exception:
                        exists = False
                if not exists:
                    # Create with original casing
                    conn.execute(
                        "CREATE (s:Series {id:$id, name:$name, normalized_name:$norm, description:NULL, cover_url:NULL, custom_cover:false, generated_placeholder:false, created_at:$ts})",
                        {"id": sid, "name": series_name, "norm": norm, "ts": datetime.now(timezone.utc)}
                    )
                    result_summary["series_created"] += 1
                created_series_norms[norm] = sid
            else:
                sid = created_series_norms[norm]
            # Relationship
            vol_num = parse_volume(volume_raw)
            params = {
                "bid": book_id,
                "sid": sid,
                "vol": int(vol_num) if (vol_num is not None) else None,
                "vol_d": vol_num,
                "ord": series_order if isinstance(series_order, (int, float)) else None,
                "ts": datetime.now(timezone.utc)
            }
            # Create rel only if not exists (defensive)
            conn.execute(
                """
                MATCH (b:Book {id:$bid}), (s:Series {id:$sid})
                MERGE (b)-[r:PART_OF_SERIES]->(s)
                ON CREATE SET r.volume_number = COALESCE($vol, r.volume_number),
                              r.volume_number_double = COALESCE($vol_d, r.volume_number_double),
                              r.series_order = COALESCE($ord, r.series_order),
                              r.created_at = $ts
                ON MATCH SET r.volume_number_double = COALESCE(r.volume_number_double, $vol_d)
                """,
                params
            )
            result_summary["relationships_created"] += 1
        write_marker({"skipped": False, **result_summary, "ts": datetime.now(timezone.utc).isoformat()})
    return result_summary

__all__ = ["run_series_migration", "parse_volume"]
