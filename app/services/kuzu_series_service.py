from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
"""Kuzu Series Service

Provides retrieval and update operations for Series entities and their
associated books using the PART_OF_SERIES relationship migrated from
legacy Book.series fields.

Initial minimal implementation to support upcoming UI pages.
"""

from dataclasses import asdict
from typing import List, Optional, Dict, Any
import logging

from ..infrastructure.kuzu_graph import safe_execute_kuzu_query
from ..domain.models import Series, Book
from .kuzu_async_helper import run_async

logger = logging.getLogger(__name__)


class KuzuSeriesService:
    """Service for Series node operations and related book retrieval."""

    # IMPORTANT: Series cover logic must NEVER mutate individual Book.cover_url values.
    # A custom series cover is purely a series-level visual override. Book covers
    # remain the canonical per-book images. Ensure any future refactors keep this
    # boundary: update_series_cover* may only touch Series.* properties.

    def __init__(self, user_id: Optional[str] = None):
        self.user_id = user_id or "series_service"

    # ---------------------- Retrieval ----------------------
    async def get_all_series_async(self, limit: int = 500) -> List[Series]:
        """Return series with minimal cover logic.

        Precedence: user_cover (custom) else first book cover (earliest published date, NULL dates last, then title).
        We purposely DO NOT mutate any stored cover fields here – purely read/derive.
        """
        query = (
            "MATCH (s:Series) "
            "OPTIONAL MATCH (b:Book)-[:PART_OF_SERIES]->(s) "
            "WITH s, COUNT(b) as bct, MIN(CASE WHEN b.published_date IS NULL THEN date('9999-12-31') ELSE b.published_date END) as firstPub "
            "OPTIONAL MATCH (fb:Book)-[:PART_OF_SERIES]->(s) "
            "WHERE (fb.published_date IS NOT NULL AND fb.published_date = firstPub) OR (firstPub = date('9999-12-31') AND fb.published_date IS NULL) "
            "RETURN s.id, s.name, s.normalized_name, s.description, bct, s.user_cover, fb.cover_url "
            "ORDER BY s.normalized_name LIMIT $limit"
        )
        result = safe_execute_kuzu_query(query, {"limit": limit})
        out: List[Series] = []
        if result and hasattr(result, 'has_next'):
            try:
                while result.has_next():  # type: ignore[attr-defined]
                    row = result.get_next()  # type: ignore[attr-defined]
                    vals = row if isinstance(row, (list, tuple)) else list(row)  # type: ignore
                    try:
                        user_cover = vals[5] if len(vals) > 5 else None
                        first_book_cover = vals[6] if len(vals) > 6 else None
                        effective = user_cover or first_book_cover
                        s_obj = Series(
                            id=vals[0] if len(vals) > 0 else None,
                            name=(vals[1] or "") if len(vals) > 1 else "",
                            normalized_name=(vals[2] or (vals[1] or "").lower()) if len(vals) > 2 else ((vals[1] or "").lower() if len(vals) > 1 else ""),
                            description=vals[3] if len(vals) > 3 else None,
                            user_cover=user_cover,
                            cover_url=effective,
                            custom_cover=bool(user_cover),
                            generated_placeholder=False,
                        )
                        try:
                            s_obj.book_count = int(vals[4]) if len(vals) > 4 and vals[4] is not None else 0  # type: ignore[attr-defined]
                        except Exception:
                            s_obj.book_count = 0  # type: ignore[attr-defined]
                        out.append(s_obj)
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"Error iterating series rows (minimal cover logic): {e}")
        return out

    def get_all_series(self, limit: int = 500) -> List[Series]:
        return run_async(self.get_all_series_async(limit))

    async def get_series_async(self, series_id: str) -> Optional[Series]:
        """Fetch single series with minimal cover precedence (user_cover > first book cover)."""
        query = (
            "MATCH (s:Series) WHERE s.id = $id "
            "OPTIONAL MATCH (b:Book)-[:PART_OF_SERIES]->(s) "
            "WITH s, COUNT(b) as bct, MIN(CASE WHEN b.published_date IS NULL THEN date('9999-12-31') ELSE b.published_date END) as firstPub "
            "OPTIONAL MATCH (fb:Book)-[:PART_OF_SERIES]->(s) "
            "WHERE (fb.published_date IS NOT NULL AND fb.published_date = firstPub) OR (firstPub = date('9999-12-31') AND fb.published_date IS NULL) "
            "RETURN s.id, s.name, s.normalized_name, s.description, bct, s.user_cover, fb.cover_url"
        )
        result = safe_execute_kuzu_query(query, {"id": series_id})
        if result and hasattr(result, 'has_next') and result.has_next():  # type: ignore[attr-defined]
            row = result.get_next()  # type: ignore[attr-defined]
            vals = row if isinstance(row, (list, tuple)) else list(row)  # type: ignore
            try:
                user_cover = vals[5] if len(vals) > 5 else None
                first_book_cover = vals[6] if len(vals) > 6 else None
                effective = user_cover or first_book_cover
                s_obj = Series(
                    id=vals[0] if len(vals) > 0 else None,
                    name=(vals[1] or "") if len(vals) > 1 else "",
                    normalized_name=(vals[2] or (vals[1] or "").lower()) if len(vals) > 2 else ((vals[1] or "").lower() if len(vals) > 1 else ""),
                    description=vals[3] if len(vals) > 3 else None,
                    user_cover=user_cover,
                    cover_url=effective,
                    custom_cover=bool(user_cover),
                    generated_placeholder=False,
                )
                try:
                    s_obj.book_count = int(vals[4]) if len(vals) > 4 and vals[4] is not None else 0  # type: ignore[attr-defined]
                except Exception:
                    s_obj.book_count = 0  # type: ignore[attr-defined]
                return s_obj
            except Exception:
                return None
        return None

    def get_series(self, series_id: str) -> Optional[Series]:
        return run_async(self.get_series_async(series_id))

    async def get_books_for_series_async(self, series_id: str, order: str = "alpha") -> List[Book]:
        """Retrieve books linked to a series. order determines sorting strategy."""
        order_clause = self.build_order_clause(order)
        query = (
            "MATCH (b:Book)-[rel:PART_OF_SERIES]->(s:Series) "
            "WHERE s.id = $id "
            "RETURN b.id, b.title, b.normalized_title, b.cover_url, b.published_date, rel.volume_number, rel.volume_number_double, rel.series_order "
            f"ORDER BY {order_clause}"
        )
        result = safe_execute_kuzu_query(query, {"id": series_id})
        books: List[Book] = []
        if result and hasattr(result, 'has_next'):
            try:
                while result.has_next():  # type: ignore[attr-defined]
                    row = result.get_next()  # type: ignore[attr-defined]
                    try:
                        book = Book(
                            id=row[0],  # type: ignore[index]
                            title=(row[1] or "") if len(row) > 1 else "",  # type: ignore[index]
                            normalized_title=(row[2] or (row[1] or "").lower()) if len(row) > 2 else ((row[1] or "").lower() if len(row) > 1 else ""),  # type: ignore[index]
                            cover_url=row[3] if len(row) > 3 else None,  # type: ignore[index]
                            published_date=row[4] if len(row) > 4 else None,  # type: ignore[index]
                        )
                        book.series_volume = row[5] if len(row) > 5 else None  # type: ignore[index]
                        # Set series_order from the actual database field, not derived from volume
                        if len(row) > 7 and row[7] is not None:  # type: ignore[index]
                            try:
                                book.series_order = int(row[7])  # type: ignore[index]
                            except Exception:
                                pass
                        books.append(book)
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"Error iterating books for series {series_id}: {e}")
        # Fallback: if chosen order relies on data absent across all books -> alphabetical
        if order in ("publication", "volume", "series_order", "suggested"):
            if order == "publication" and all(not b.published_date for b in books):
                books.sort(key=lambda b: b.title.lower())
            elif order in ("volume", "series_order", "suggested") and all(getattr(b, 'series_volume', None) in (None, '') for b in books):
                books.sort(key=lambda b: b.title.lower())
        return books

    # ---------------------- Contributors Augmentation ----------------------
    def _contrib_rel_types(self) -> List[str]:
        return [
            'AUTHORED','NARRATED','EDITED','TRANSLATED','ILLUSTRATED',
            'GAVE_FOREWORD','GAVE_INTRODUCTION','GAVE_AFTERWORD','COMPILED','GHOST_WROTE','CONTRIBUTED'
        ]

    async def add_contributors_async(self, books: List[Book]):
        # Build a UNION ALL style query to gather contributors from each known relationship type.
        # We avoid relying on a generic r.type property (not present in schema) and explicitly tag the role.
        rel_map = [
            ("AUTHORED", "authored"),
            # CO_AUTHORED table not present in schema; co-authors likely modeled via AUTHORED with role property
            ("NARRATED", "narrated"),
            ("EDITED", "edited"),
            ("TRANSLATED", "translated"),
            ("ILLUSTRATED", "illustrated"),
        ]
        for b in books:
            contributors = []
            try:
                for rel_label, role_tag in rel_map:
                    q = (
                        f"MATCH (p:Person)-[r:{rel_label}]->(b:Book {{id:$bid}}) "
                        "RETURN p.id, p.name, r.role"
                    )
                    res = safe_execute_kuzu_query(q, {"bid": b.id})
                    if res and hasattr(res, 'has_next'):
                        while res.has_next():  # type: ignore[attr-defined]
                            row = res.get_next()  # type: ignore[attr-defined]
                            try:
                                # role property may refine (e.g., illustrator vs cover artist); append as secondary info
                                role_prop = ''
                                try:
                                    role_prop = (row[2] or '').strip().lower() if len(row) > 2 else ''  # type: ignore[index]
                                except Exception:
                                    role_prop = ''
                                contributors.append({
                                    'id': row[0],  # type: ignore[index]
                                    'name': row[1],  # type: ignore[index]
                                    'role': role_tag if not role_prop else role_prop
                                })
                            except Exception:
                                pass
                # Sort consolidated list by name
                contributors.sort(key=lambda c: c['name'].lower())
                b.contributors = contributors  # type: ignore[attr-defined]
            except Exception:
                b.contributors = []  # type: ignore[attr-defined]

    def add_contributors(self, books: List[Book]):
        return run_async(self.add_contributors_async(books))

    def get_books_for_series(self, series_id: str, order: str = "alpha") -> List[Book]:
        return run_async(self.get_books_for_series_async(series_id, order))

    # ---------------------- Ordering Helper ----------------------
    def build_order_clause(self, order: str) -> str:
        """Return ORDER BY clause fragment(s) for the given strategy.

        Strategies:
        - alpha: alphabetical by title
        - publication: by published_date then title
        - volume: explicit volume numbers first (double then int), missing last
        - series_order: attempt numeric then fallback title
        - suggested: heuristic reading order: explicit volumes first (double/int), then published_date, then title.
        """
        if order == "publication":
            # Use far-future sentinel date for NULLs to push them last while preserving DATE type
            return "(CASE WHEN b.published_date IS NULL THEN date('9999-12-31') ELSE b.published_date END), LOWER(b.title)"
        if order == "volume":
            return "COALESCE(rel.volume_number_double, rel.volume_number, 1e12), LOWER(b.title)"
        if order == "series_order":
            return "COALESCE(rel.volume_number_double, rel.volume_number, 1e12), LOWER(b.title)"
        if order == "suggested":
            # Explicit volumes first; then by publication (NULLs last via sentinel) then title
            return "COALESCE(rel.volume_number_double, rel.volume_number, 1e12), (CASE WHEN b.published_date IS NULL THEN date('9999-12-31') ELSE b.published_date END), LOWER(b.title)"
        # default alpha
        return "LOWER(b.title)"

    # ---------------------- Mutations ----------------------
    async def update_series_name_async(self, series_id: str, new_name: str) -> bool:
        """Rename a series (and update normalized_name)."""
        try:
            query = (
                "MATCH (s:Series) WHERE s.id = $id "
                "SET s.name = $name, s.normalized_name = LOWER(TRIM($name)) RETURN s.id"
            )
            result = safe_execute_kuzu_query(query, {"id": series_id, "name": new_name})
            return bool(result and hasattr(result, 'has_next') and result.has_next())  # type: ignore[attr-defined]
        except Exception as e:
            logger.error(f"Failed to update series name {series_id}: {e}")
            return False

    def update_series_name(self, series_id: str, new_name: str) -> bool:
        return run_async(self.update_series_name_async(series_id, new_name))

    async def update_series_description_async(self, series_id: str, description: str) -> bool:
        """Update series description."""
        try:
            query = "MATCH (s:Series) WHERE s.id = $id SET s.description = $d RETURN s.id"
            result = safe_execute_kuzu_query(query, {"id": series_id, "d": description})
            return bool(result and hasattr(result, 'has_next') and result.has_next())  # type: ignore[attr-defined]
        except Exception as e:
            logger.error(f"Failed to update series description {series_id}: {e}")
            return False

    def update_series_description(self, series_id: str, description: str) -> bool:
        return run_async(self.update_series_description_async(series_id, description))

    async def update_series_cover_async(self, series_id: str, cover_url: str, custom: bool = True, generated_placeholder: bool | None = None, user_uploaded: bool = True) -> bool:  # signature retained for backward compatibility
        """Store a user uploaded custom cover.

        New minimal semantics: we write to s.user_cover only. We ignore generated_placeholder/user_uploaded flags (kept for signature stability).
        custom_cover flag reflects presence of user_cover (set true when provided).
        """
        try:
            q = "MATCH (s:Series {id:$id}) SET s.user_cover=$c, s.custom_cover=true RETURN s.id"
            res = safe_execute_kuzu_query(q, {"id": series_id, "c": cover_url})
            return bool(res and hasattr(res, 'has_next') and res.has_next())  # type: ignore[attr-defined]
        except Exception as e:
            logger.error(f"Failed setting user_cover for series {series_id}: {e}")
            return False

    def update_series_cover(self, series_id: str, cover_url: str, custom: bool = True, generated_placeholder: bool | None = None, user_uploaded: bool = True) -> bool:
        return run_async(self.update_series_cover_async(series_id, cover_url, custom, generated_placeholder, user_uploaded))

    # ---------------------- Utility ----------------------
    # New: lightweight search + create helpers used by typeahead UI
    async def search_series_async(self, query: str, limit: int = 15) -> List[Dict[str, Any]]:
        """Flexible search for series names (case-insensitive, substring aware).

        Strategy:
        1. Fast prefix query (STARTS WITH) to leverage index-friendly operation.
        2. If result count < limit, broaden: pull a larger window of series and do
           Python-side substring filtering so queries like "wheel" match
           "The Wheel of Time".
        3. Return up to `limit` ordered by (a) position of match in name, then
           (b) normalized name.
        """
        q = (query or '').strip().lower()
        if not q:
            return []

        # Primary prefix query
        prefix_cypher = (
            "MATCH (s:Series) WHERE s.normalized_name STARTS WITH $q OR LOWER(s.name) STARTS WITH $q "
            "OPTIONAL MATCH (b:Book)-[:PART_OF_SERIES]->(s) "
            "WITH s, COUNT(b) as bct "
            "RETURN s.id, s.name, s.normalized_name, bct ORDER BY s.normalized_name LIMIT $limit"
        )
        params = {"q": q, "limit": limit}
        res = safe_execute_kuzu_query(prefix_cypher, params)
        collected: Dict[str, Dict[str, Any]] = {}
        if res and hasattr(res, 'has_next'):
            try:
                while res.has_next():  # type: ignore[attr-defined]
                    row = res.get_next()  # type: ignore[attr-defined]
                    try:
                        collected[row[0]] = {  # type: ignore[index]
                            'id': row[0],  # type: ignore[index]
                            'name': row[1],  # type: ignore[index]
                            'normalized_name': row[2],  # type: ignore[index]
                            'book_count': int(row[3]) if len(row) > 3 and row[3] is not None else 0  # type: ignore[index]
                        }
                    except Exception as e:
                        try:
                            logger.error(
                                "get_all_series_async row decode error (phase1): %s raw_row=%r types=%s",
                                e,
                                row,
                                [type(x).__name__ for x in row] if hasattr(row, '__iter__') else 'n/a'
                            )
                        except Exception:
                            pass
            except Exception as e:
                logger.error(f"search_series_async prefix iteration error: {e}")

        # If we already have enough results OR q is very short, return early
        if len(collected) >= limit:
            return list(collected.values())[:limit]

        # Broader window for substring matching (cap to avoid huge scans)
        broad_cypher = (
            "MATCH (s:Series) OPTIONAL MATCH (b:Book)-[:PART_OF_SERIES]->(s) "
            "WITH s, COUNT(b) as bct RETURN s.id, s.name, s.normalized_name, bct ORDER BY s.normalized_name LIMIT $broad_limit"
        )
        broad_res = safe_execute_kuzu_query(broad_cypher, {"broad_limit": 400})
        if broad_res and hasattr(broad_res, 'has_next'):
            try:
                while broad_res.has_next():  # type: ignore[attr-defined]
                    row = broad_res.get_next()  # type: ignore[attr-defined]
                    try:
                        nid = row[0]  # type: ignore[index]
                        nname = row[2]  # type: ignore[index]
                        if not isinstance(nname, str):
                            continue
                        if q in nname and nid not in collected:
                            collected[nid] = {
                                'id': nid,
                                'name': row[1],  # type: ignore[index]
                                'normalized_name': nname,
                                'book_count': int(row[3]) if len(row) > 3 and row[3] is not None else 0  # type: ignore[index]
                            }
                    except Exception as e:
                        try:
                            logger.error(
                                "get_all_series_async row decode error (phase2): %s raw_row=%r types=%s",
                                e,
                                row,
                                [type(x).__name__ for x in row] if hasattr(row, '__iter__') else 'n/a'
                            )
                        except Exception:
                            pass
            except Exception as e:
                logger.error(f"search_series_async broad iteration error: {e}")

        # Ranking: position of substring match (lower is better), then name
        ranked = []
        for item in collected.values():
            nm = item.get('normalized_name') or ''
            pos = nm.find(q)
            if pos < 0:
                pos = 9999
            ranked.append((pos, nm, item))
        ranked.sort(key=lambda t: (t[0], t[1]))
        return [t[2] for t in ranked[:limit]]

    def search_series(self, query: str, limit: int = 15) -> List[Dict[str, Any]]:
        return run_async(self.search_series_async(query, limit))

    async def create_series_async(self, name: str) -> Optional[Series]:
        """Create a new series if one with the same normalized name doesn't already exist.

        Returns the existing or newly created Series object.
        """
        raw = (name or '').strip()
        if not raw:
            return None
        norm = raw.lower()
        # First check for existing
        existing_q = (
            "MATCH (s:Series) WHERE s.normalized_name = $nn RETURN s.id, s.name, s.normalized_name, s.description, s.cover_url, s.custom_cover, s.generated_placeholder LIMIT 1"
        )
        existing = safe_execute_kuzu_query(existing_q, {"nn": norm})
        if existing and hasattr(existing, 'has_next') and existing.has_next():  # type: ignore[attr-defined]
            row = existing.get_next()  # type: ignore[attr-defined]
            try:
                # returned columns: s.id, s.name, s.normalized_name, s.description, s.cover_url, s.custom_cover, s.generated_placeholder
                return Series(
                    id=row[0],  # type: ignore[index]
                    name=row[1],  # type: ignore[index]
                    normalized_name=row[2],  # type: ignore[index]
                    description=row[3] if len(row) > 3 else None,  # type: ignore[index]
                    cover_url=row[4] if len(row) > 4 else None,  # type: ignore[index]
                    custom_cover=bool(row[5]) if len(row) > 5 else False,  # type: ignore[index]
                    generated_placeholder=bool(row[6]) if len(row) > 6 else False,  # type: ignore[index]
                )
            except Exception:
                pass
        # Create new
        from uuid import uuid4
        sid = f"series_{uuid4().hex}"
        create_q = (
            "CREATE (s:Series {id:$id, name:$name, normalized_name:$nn, created_at:$created}) RETURN s.id, s.name, s.normalized_name"
        )
        created = safe_execute_kuzu_query(create_q, {"id": sid, "name": raw, "nn": norm, "created": datetime.now(timezone.utc)})
        if created and hasattr(created, 'has_next') and created.has_next():  # type: ignore[attr-defined]
            row = created.get_next()  # type: ignore[attr-defined]
            try:
                return Series(id=row[0], name=row[1], normalized_name=row[2], description=None)  # type: ignore[index]
            except Exception:
                return None
        return None

    def create_series(self, name: str) -> Optional[Series]:
        return run_async(self.create_series_async(name))

    async def attach_book_async(self, book_id: str, series_id: str, volume: Optional[str] = None, order_number: Optional[int] = None, volume_number_double: Optional[float] = None) -> bool:
        """Attach a book to a series (idempotent)."""
        try:
            set_bits = []
            params: Dict[str, Any] = {"bid": book_id, "sid": series_id}
            if volume:
                # Store as volume_number (string-ish) AND attempt numeric parse for ordering
                try:
                    vol_clean = volume.strip()
                    if vol_clean:
                        # If purely int
                        if vol_clean.isdigit():
                            params['vol_int'] = int(vol_clean)
                            set_bits.append('r.volume_number = $vol_int')
                            params['vol_d'] = float(params['vol_int'])
                            set_bits.append('r.volume_number_double = $vol_d')
                        else:
                            # Try float
                            try:
                                fval = float(vol_clean)
                                params['vol_d'] = fval
                                set_bits.append('r.volume_number_double = $vol_d')
                                if abs(fval - round(fval)) < 1e-9:
                                    params['vol_int'] = int(round(fval))
                                    set_bits.append('r.volume_number = $vol_int')
                            except Exception:
                                # Non-numeric descriptive string -> store nothing numeric
                                pass
                except Exception:
                    pass
            if order_number is not None:
                params['order_int'] = int(order_number)
                set_bits.append('r.series_order = $order_int')
            if volume_number_double is not None and 'vol_d' not in params:
                try:
                    params['vol_d'] = float(volume_number_double)
                    set_bits.append('r.volume_number_double = $vol_d')
                except Exception:
                    pass
            # Compose SET clause (ensure created_at set once, avoid unsupported datetime())
            now_iso = datetime.now(timezone.utc)
            if set_bits:
                set_clause = 'SET ' + ', '.join(set_bits) + ', r.created_at = COALESCE(r.created_at, $now)'
            else:
                set_clause = 'SET r.created_at = COALESCE(r.created_at, $now)'
            q = (
                "MATCH (b:Book {id:$bid}) MATCH (s:Series {id:$sid}) MERGE (b)-[r:PART_OF_SERIES]->(s) "
                f"{set_clause} RETURN r"
            )
            params['now'] = now_iso
            res = safe_execute_kuzu_query(q, params)
            return bool(res and hasattr(res, 'has_next') and res.has_next())  # type: ignore[attr-defined]
        except Exception as e:
            logger.error(f"attach_book_async error: {e}")
            return False

    def attach_book(self, book_id: str, series_id: str, volume: Optional[str] = None, order_number: Optional[int] = None, volume_number_double: Optional[float] = None) -> bool:
        return run_async(self.attach_book_async(book_id, series_id, volume, order_number, volume_number_double))
    async def count_series_books_async(self, series_id: str) -> int:
        query = "MATCH (:Book)-[:PART_OF_SERIES]->(s:Series) WHERE s.id = $id RETURN COUNT(*)"
        result = safe_execute_kuzu_query(query, {"id": series_id})
        if result and hasattr(result, 'has_next') and result.has_next():  # type: ignore[attr-defined]
            row = result.get_next()  # type: ignore[attr-defined]
            try:
                first_val = None
                try:
                    first_val = row[0]  # type: ignore[index]
                except Exception:
                    try:
                        seq = list(row)
                        if seq:
                            first_val = seq[0]
                    except Exception:
                        pass
                return int(first_val) if first_val is not None else 0
            except Exception:
                return 0
        return 0

    def count_series_books(self, series_id: str) -> int:
        return run_async(self.count_series_books_async(series_id))

    # ---------------------- Per-user Notes ----------------------
    async def get_user_series_notes_async(self, user_id: str, series_id: str) -> Optional[str]:
        q = (
            "MATCH (u:User {id:$uid})-[r:HAS_SERIES_NOTES]->(s:Series {id:$sid}) RETURN r.notes"
        )
        res = safe_execute_kuzu_query(q, {"uid": user_id, "sid": series_id})
        if res and hasattr(res, 'has_next') and res.has_next():  # type: ignore[attr-defined]
            row = res.get_next()  # type: ignore[attr-defined]
            try:
                return row[0]  # type: ignore[index]
            except Exception:
                return None
        return None

    def get_user_series_notes(self, user_id: str, series_id: str) -> Optional[str]:
        return run_async(self.get_user_series_notes_async(user_id, series_id))

    async def upsert_user_series_notes_async(self, user_id: str, series_id: str, notes: str) -> bool:
        q = (
            "MATCH (s:Series {id:$sid}) MATCH (u:User {id:$uid}) MERGE (u)-[r:HAS_SERIES_NOTES]->(s) "
            "SET r.notes=$notes, r.updated_at=$now, r.created_at=COALESCE(r.created_at, $now) RETURN r"
        )
        try:
            res = safe_execute_kuzu_query(q, {"sid": series_id, "uid": user_id, "notes": notes, "now": datetime.now(timezone.utc)})
            return bool(res and hasattr(res, 'has_next') and res.has_next())  # type: ignore[attr-defined]
        except Exception as e:
            logger.error(f"upsert_user_series_notes_async error: {e}")
            return False

    def upsert_user_series_notes(self, user_id: str, series_id: str, notes: str) -> bool:
        return run_async(self.upsert_user_series_notes_async(user_id, series_id, notes))
series_service: Optional[KuzuSeriesService] = None
def get_series_service() -> KuzuSeriesService:
    global series_service
    if series_service is None:
        series_service = KuzuSeriesService()
    return series_service
