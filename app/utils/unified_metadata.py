"""
Unified metadata aggregation for books.

Combines Google Books and OpenLibrary data for a given ISBN, with
date normalization and sensible field merging. Also exposes a title
search passthrough to the enhanced search implementation.
"""

from __future__ import annotations

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests

_META_LOG = logging.getLogger(__name__)
_META_DEBUG = os.getenv("METADATA_DEBUG", "0").lower() in ("1", "true", "yes", "on")

# Configurable timeouts for metadata fetching
# Individual request timeout (default 8 seconds per request)
_REQUEST_TIMEOUT = int(os.getenv("METADATA_REQUEST_TIMEOUT", "8"))
# Overall fetch timeout for parallel provider requests (default 20 seconds total)
_FETCH_TIMEOUT = int(os.getenv("METADATA_FETCH_TIMEOUT", "20"))


def _normalize_isbn_value(val: str | None) -> str:
    """Return an uppercase ISBN string stripped of separators (or empty string)."""
    if not val:
        return ""
    return re.sub(r"[^0-9Xx]", "", str(val)).upper()


def _isbn10_to_13(isbn10: str | None) -> str | None:
    """Convert ISBN-10 to ISBN-13 (prefix 978) including checksum."""
    digits = _normalize_isbn_value(isbn10)
    if len(digits) != 10 or not re.fullmatch(r"[0-9]{9}[0-9X]", digits):
        return None
    core = digits[:9]
    prefix_core = f"978{core}"
    total = 0
    for idx, ch in enumerate(prefix_core):
        weight = 1 if idx % 2 == 0 else 3
        total += int(ch) * weight
    check = (10 - (total % 10)) % 10
    return prefix_core + str(check)


def _isbn13_to_10(isbn13: str | None) -> str | None:
    """Convert ISBN-13 (978/979) to ISBN-10, recalculating checksum."""
    digits = _normalize_isbn_value(isbn13)
    if (
        len(digits) != 13
        or not digits[:12].isdigit()
        or not digits.startswith(("978", "979"))
    ):
        return None
    core = digits[3:12]
    if len(core) != 9 or not core.isdigit():
        return None
    total = 0
    for idx, ch in enumerate(core):
        weight = 10 - idx
        total += weight * int(ch)
    check_val = (11 - (total % 11)) % 11
    check_char = "X" if check_val == 10 else str(check_val)
    return core + check_char


def _collect_isbn_variants(val: str | None) -> set[str]:
    """Return normalized ISBN plus convertible variants suitable for comparisons."""
    norm = _normalize_isbn_value(val)
    if not norm:
        return set()
    variants: set[str] = {norm}
    if len(norm) == 10:
        converted = _isbn10_to_13(norm)
        if converted:
            variants.add(converted)
    elif len(norm) == 13:
        converted = _isbn13_to_10(norm)
        if converted:
            variants.add(converted)
    return variants


def _pick_preferred_identifier(
    values: Any, preferred_norm: str | None
) -> str | None:
    """Select the identifier matching preferred_norm if available."""
    if not values:
        return None
    if not isinstance(values, list):
        values = [values]
    filtered: list[tuple[str, str]] = []
    for raw in values:
        if not raw:
            continue
        norm = _normalize_isbn_value(raw)
        if not norm:
            continue
        filtered.append((str(raw), norm))
    if not filtered:
        return None
    if preferred_norm:
        for raw, norm in filtered:
            if norm == preferred_norm:
                return raw
    return filtered[0][0]


def _extract_series_label(val: Any) -> str | None:
    """Normalize OpenLibrary series metadata to a clean string."""
    if not val:
        return None
    try:
        if isinstance(val, list) and val:
            candidate = str(val[0])
        elif isinstance(val, str):
            candidate = val
        else:
            return None
        candidate = candidate.strip()
        candidate = re.sub(r"^\s*Series\s*:\s*", "", candidate, flags=re.IGNORECASE)
        candidate = re.sub(r"\s*\([^)]*\)\s*$", "", candidate).strip()
        return candidate or None
    except Exception:
        return None


def _load_openlibrary_edition_payload(isbn: str) -> dict[str, Any]:
    """Fetch the edition JSON for an ISBN (best-effort)."""
    try:
        resp = requests.get(
            f"https://openlibrary.org/isbn/{isbn}.json", timeout=_REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        return resp.json() or {}
    except Exception:
        return {}


def _build_payload_from_edition(
    edition_payload: dict[str, Any],
    target_isbn13: str | None,
    target_isbn10: str | None,
) -> dict[str, Any]:
    if not edition_payload:
        return {}

    def _extract_desc(val: Any) -> str | None:
        if isinstance(val, dict):
            return val.get("value")
        if isinstance(val, str):
            return val
        return None

    series_value = _extract_series_label(edition_payload.get("series"))
    description = _extract_desc(edition_payload.get("description"))
    publish_date = edition_payload.get("publish_date") or edition_payload.get(
        "publishDate"
    )
    language_code = None
    languages = edition_payload.get("languages")
    if isinstance(languages, list) and languages:
        entry = languages[0]
        if isinstance(entry, dict):
            lang_key = entry.get("key", "")
            if isinstance(lang_key, str) and lang_key:
                language_code = lang_key.split("/")[-1]
        elif isinstance(entry, str):
            language_code = entry.split("/")[-1] if "/" in entry else entry
    preferred_isbn13 = target_isbn13 if target_isbn13 else None
    preferred_isbn10 = target_isbn10 if target_isbn10 else None
    isbn13 = _pick_preferred_identifier(
        edition_payload.get("isbn_13"), preferred_isbn13
    )
    isbn10 = _pick_preferred_identifier(
        edition_payload.get("isbn_10"), preferred_isbn10
    )
    publishers = edition_payload.get("publishers")
    publisher_value = None
    if isinstance(publishers, list) and publishers:
        publisher_value = str(publishers[0])
    elif isinstance(publishers, str):
        publisher_value = publishers
    elif isinstance(edition_payload.get("publisher"), str):
        publisher_value = edition_payload.get("publisher")
    return {
        "title": edition_payload.get("title"),
        "subtitle": edition_payload.get("subtitle"),
        "publisher": publisher_value,
        "published_date": _normalize_date(publish_date),
        "published_date_raw": publish_date,
        "published_date_specificity": _date_specificity(publish_date),
        "page_count": edition_payload.get("number_of_pages"),
        "language": language_code,
        "description": description,
        "categories": [],
        "authors": [],
        "cover_url": None,
        "openlibrary_id": edition_payload.get("key"),
        "series": series_value,
        "isbn10": isbn10,
        "isbn13": isbn13,
    }


def _normalize_date(val: str | None) -> str | None:
    """Normalize a date string to ISO (YYYY-MM-DD) suitable for HTML date inputs.

    Handles common formats:
    - YYYY
    - YYYY-MM (pads to first day of month)
    - YYYY-MM-DD
    - MM/DD/YYYY or M/D/YYYY
    - Month D, YYYY (e.g., October 6, 2015)
    Returns None if input is falsy or unparseable.
    """
    if not val:
        return None

    s = str(val).strip()
    if not s:
        return None

    # Already ISO YYYY-MM-DD
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s

    # YYYY or YYYY-MM
    m = re.fullmatch(r"(\d{4})(?:-(\d{1,2}))?", s)
    if m:
        year = int(m.group(1))
        month = int(m.group(2)) if m.group(2) else 1
        return f"{year:04d}-{month:02d}-01"

    # MM/DD/YYYY or M/D/YYYY
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        month = int(m.group(1))
        day = int(m.group(2))
        year = int(m.group(3))
        return f"{year:04d}-{month:02d}-{day:02d}"

    # Month D, YYYY
    m = re.fullmatch(r"([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})", s)
    if m:
        month_name = m.group(1).lower()
        day = int(m.group(2))
        year = int(m.group(3))
        months = {
            "january": 1,
            "february": 2,
            "march": 3,
            "april": 4,
            "may": 5,
            "june": 6,
            "july": 7,
            "august": 8,
            "september": 9,
            "october": 10,
            "november": 11,
            "december": 12,
        }
        if month_name in months:
            return f"{year:04d}-{months[month_name]:02d}-{day:02d}"

    # Fallback: if there's a year, return first day of that year
    m = re.search(r"(\d{4})", s)
    if m:
        return f"{int(m.group(1)):04d}-01-01"

    return None


def _date_specificity(date_str: str | None) -> int:
    """Return a rough specificity score for a date string before normalization.

    Scores:
    - 3: full date (YYYY-MM-DD, MM/DD/YYYY, Month D, YYYY, etc.)
    - 2: year-month (YYYY-MM or similar)
    - 1: year only (YYYY)
    - 0: unknown/empty

    Heuristic-based; safe for precedence decisions only.
    """
    if not date_str:
        return 0

    s = str(date_str).strip()
    if not s:
        return 0

    # Full date patterns
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return 3
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", s):
        return 3
    if re.fullmatch(r"[A-Za-z]+\s+\d{1,2},\s*\d{4}", s):
        return 3

    # Year-month
    if re.fullmatch(r"\d{4}-\d{1,2}", s) or re.fullmatch(r"\d{4}/\d{1,2}", s):
        return 2

    # Year-only
    if re.fullmatch(r"\d{4}", s):
        return 1

    # Fallback: try to infer
    m = re.fullmatch(r"(\d{4})(?:[-/](\d{1,2})(?:[-/](\d{1,2}))?)?", s)
    if m:
        return 1 + (1 if m.group(2) else 0) + (1 if m.group(3) else 0)

    return 0


def _fetch_google_by_isbn(isbn: str) -> dict[str, Any]:
    """Fetch Google Books metadata for an ISBN.

    Picks the best item by:
    1) Exact ISBN match (10 or 13) in industryIdentifiers
    2) Highest date specificity of volumeInfo.publishedDate
    Fallback to the first item if list is empty.
    """
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
    try:
        resp = requests.get(
            url,
            timeout=_REQUEST_TIMEOUT,
            headers={
                "User-Agent": "MyBibliotheca/metadata-fetch (+https://example.local)"
            },
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items") or []
        if not items:
            if _META_DEBUG:
                _META_LOG.warning(
                    f"[UNIFIED_METADATA][GOOGLE][EMPTY] isbn={isbn} status={resp.status_code} len=0 url={url}"
                )
            return {}

        # Normalize target ISBN (strip non-digits/X)
        import re as _re

        target = _re.sub(r"[^0-9Xx]", "", str(isbn))

        def _extract_isbns(it):
            vi = it.get("volumeInfo", {})
            ids = vi.get("industryIdentifiers", []) or []
            i10 = None
            i13 = None
            for ident in ids:
                t = ident.get("type")
                val = _re.sub(r"[^0-9Xx]", "", str(ident.get("identifier") or ""))
                if t == "ISBN_10" and val:
                    i10 = val
                elif t == "ISBN_13" and val:
                    i13 = val
            return i10, i13

        def _score(it):
            vi = it.get("volumeInfo", {})
            i10, i13 = _extract_isbns(it)
            raw_date = vi.get("publishedDate")
            spec = _date_specificity(raw_date)
            match = (
                1
                if (target and (target == (i13 or "") or target == (i10 or "")))
                else 0
            )
            return (match, spec)

        # Choose item with best (match, specificity)
        item = max(items, key=_score)
        vi = item.get("volumeInfo", {})
        ids = vi.get("industryIdentifiers", [])
        isbn10 = None
        isbn13 = None
        for ident in ids:
            t = ident.get("type")
            if t == "ISBN_10":
                isbn10 = ident.get("identifier")
            elif t == "ISBN_13":
                isbn13 = ident.get("identifier")
        # Guard against Google returning a different ISBN entirely.
        # If neither ISBN matches the requested value, treat the Google payload as empty
        # so we don't overwrite correct metadata (e.g., different manga volumes).
        norm_isbn10 = _re.sub(r"[^0-9Xx]", "", isbn10 or "")
        norm_isbn13 = _re.sub(r"[^0-9Xx]", "", isbn13 or "")
        provided_variants: set[str] = set()
        provided_variants |= _collect_isbn_variants(norm_isbn10)
        provided_variants |= _collect_isbn_variants(norm_isbn13)
        req_variants = _collect_isbn_variants(target)
        if (
            req_variants
            and provided_variants
            and not (req_variants & provided_variants)
        ):
            if _META_DEBUG:
                _META_LOG.warning(
                    f"[UNIFIED_METADATA][GOOGLE][ISBN_MISMATCH_DROP] requested={target} got10={norm_isbn10 or 'None'} got13={norm_isbn13 or 'None'} variants={sorted(provided_variants)}"
                )
            return {}

        # Cover
        cover_url = None
        image_links = vi.get("imageLinks") or {}
        for size in ["extraLarge", "large", "medium", "small", "thumbnail"]:
            if image_links.get(size):
                cover_url = image_links[size].replace("http://", "https://")
                break

        raw_date = vi.get("publishedDate")
        published_date = _normalize_date(raw_date)
        specificity = _date_specificity(raw_date)

        # Prepare to enhance with full volume fetch
        description = vi.get("description")
        printed_page = vi.get("printedPageCount")
        page_count_val = vi.get("pageCount")
        # Secondary fetch: prefer longer description, printed page count, and real ISBNs
        try:
            vol_id = item.get("id")
            if vol_id:
                full_resp = requests.get(
                    f"https://www.googleapis.com/books/v1/volumes/{vol_id}?projection=full",
                    timeout=_REQUEST_TIMEOUT,
                )
                full_resp.raise_for_status()
                full_data = full_resp.json() or {}
                fvi = full_data.get("volumeInfo") or {}
                # Prefer longer description if available
                full_desc = fvi.get("description")
                if full_desc and (
                    not description or len(str(full_desc)) > len(str(description))
                ):
                    description = full_desc
                # Pull printed/page counts from full
                if fvi.get("printedPageCount"):
                    printed_page = fvi.get("printedPageCount")
                if fvi.get("pageCount"):
                    page_count_val = fvi.get("pageCount")
                # Pull ISBNs if missing
                if not (isbn10 and isbn13):
                    fids = fvi.get("industryIdentifiers", []) or []
                    for ident in fids:
                        if ident.get("type") == "ISBN_10" and not isbn10:
                            isbn10 = ident.get("identifier")
                        elif ident.get("type") == "ISBN_13" and not isbn13:
                            isbn13 = ident.get("identifier")
        except Exception:
            pass
        # Fallback to text snippet if still none
        if not description:
            description = (item.get("searchInfo") or {}).get("textSnippet")

        # Categories handling: initial shallow categories vs. richer full volume categories.
        # We prefer any full categories list that either:
        # 1) Has more entries OR
        # 2) Contains at least one hierarchical delimiter ('/' or '>') not present in the initial list.
        # This guards against cases where the initial search only returns a top-level umbrella (e.g. ['Fiction'])
        # while the full volume reveals hierarchical genre paths (e.g. ['Fiction / Science Fiction / Action & Adventure', ...]).
        base_categories = vi.get("categories") or []
        full_cats = []
        try:
            fvi_obj = locals().get("fvi")
            if fvi_obj:
                full_cats = fvi_obj.get("categories") or []

                def _has_hierarchy(cats):
                    return any(
                        isinstance(c, str) and ("/" in c or ">" in c) for c in cats
                    )

                if full_cats and (
                    len(full_cats) > len(base_categories)
                    or _has_hierarchy(full_cats)
                    and not _has_hierarchy(base_categories)
                ):
                    base_categories = full_cats
        except Exception:
            pass

        # Peel a 'Series:' category if present from Google categories
        series_value = None
        _cleaned = []
        for c in base_categories:
            if isinstance(c, str):
                s = c.strip()
                if s.lower().startswith("series:") and not series_value:
                    series_value = s.split(":", 1)[1].strip() or None
                else:
                    _cleaned.append(s)
        base_categories = _cleaned

        # Preserve raw hierarchical category path strings; frontend can expand
        raw_category_paths = list(base_categories)

        return {
            "title": vi.get("title") or "",
            "subtitle": vi.get("subtitle") or None,
            "authors": vi.get("authors") or [],
            "publisher": vi.get("publisher") or None,
            "published_date": published_date,
            "published_date_raw": raw_date,
            "published_date_specificity": specificity,
            # Prefer printedPageCount if present (from full or initial response)
            "page_count": printed_page or page_count_val,
            "language": vi.get("language") or "en",
            "description": description,
            "categories": base_categories,
            "raw_category_paths": raw_category_paths,
            "average_rating": vi.get("averageRating"),
            "rating_count": vi.get("ratingsCount"),
            "cover_url": cover_url,
            "isbn10": isbn10,
            "isbn13": isbn13,
            "google_books_id": item.get("id"),
            "series": series_value,
        }
    except Exception as e:
        # Suppress but record when debugging; callers still treat empty dict as failure.
        if _META_DEBUG:
            _META_LOG.warning(f"[UNIFIED_METADATA][GOOGLE][EXC] isbn={isbn} err={e}")
        return {}


def _fetch_openlibrary_by_isbn(isbn: str) -> dict[str, Any]:
    """Fetch OpenLibrary metadata for an ISBN using the lightweight data API."""
    bibkey = f"ISBN:{isbn}"
    url = f"https://openlibrary.org/api/books?bibkeys={bibkey}&format=json&jscmd=data"
    target_norm = _normalize_isbn_value(isbn)
    target_isbn13 = None
    target_isbn10 = None
    if target_norm:
        if len(target_norm) == 13:
            target_isbn13 = target_norm
            target_isbn10 = _isbn13_to_10(target_norm)
        elif len(target_norm) == 10:
            target_isbn10 = target_norm
            target_isbn13 = _isbn10_to_13(target_norm)
    edition_payload = _load_openlibrary_edition_payload(isbn)
    try:
        resp = requests.get(
            url,
            timeout=_REQUEST_TIMEOUT,
            headers={
                "User-Agent": "MyBibliotheca/metadata-fetch (+https://example.local)"
            },
        )
        resp.raise_for_status()
        data = resp.json() or {}
        ol = data.get(bibkey) or {}
        if not ol:
            return _build_payload_from_edition(
                edition_payload, target_isbn13, target_isbn10
            )

        # jscmd=data shape
        authors = [
            a.get("name") for a in (ol.get("authors") or []) if isinstance(a, dict)
        ]
        publishers = [
            p.get("name") if isinstance(p, dict) else str(p)
            for p in (ol.get("publishers") or [])
        ]
        raw_date = ol.get("publish_date")
        published_date = _normalize_date(raw_date)
        number_of_pages = ol.get("number_of_pages")
        cover = ol.get("cover") or {}
        cover_url = cover.get("large") or cover.get("medium") or cover.get("small")
        identifiers = ol.get("identifiers") or {}
        # Description normalization (string or dict)
        desc = ol.get("description")
        if isinstance(desc, dict):
            desc = desc.get("value")
        if not desc:
            # Sometimes only 'notes' exists
            notes = ol.get("notes")
            if isinstance(notes, dict):
                desc = notes.get("value")
            elif isinstance(notes, str):
                desc = notes
        # Extract ISBNs from identifiers if available (prefer the requested one)
        isbn10 = _pick_preferred_identifier(identifiers.get("isbn_10"), target_isbn10)
        isbn13 = _pick_preferred_identifier(identifiers.get("isbn_13"), target_isbn13)
        # Try to get a usable OpenLibrary link path
        ol_key = ol.get("key")  # often '/books/OL...M'
        if not ol_key:
            openlibrary = identifiers.get("openlibrary") or []
            if openlibrary:
                # edition id like 'OL12345M' -> build books path
                ol_key = f"/books/{openlibrary[0]}"

        # Normalize categories (subjects) to names
        def _norm_subjects(subjs):
            out = []
            for s in subjs or []:
                name = (
                    s.get("name")
                    if isinstance(s, dict)
                    else (str(s) if s is not None else None)
                )
                if name and name not in out:
                    out.append(name)
            return out

        # Series may appear explicitly in OL 'series' field (string or list)
        def _extract_series(val):
            if not val:
                return None
            try:
                if isinstance(val, list) and val:
                    candidate = str(val[0])
                elif isinstance(val, str):
                    candidate = val
                else:
                    return None
                candidate = candidate.strip()
                # Strip a leading 'Series:' if present and any trailing parenthetical like '(1)'
                import re as _re

                candidate = _re.sub(
                    r"^\s*Series\s*:\s*", "", candidate, flags=_re.IGNORECASE
                )
                candidate = _re.sub(r"\s*\([^)]*\)\s*$", "", candidate).strip()
                return candidate or None
            except Exception:
                return None

        series_value = _extract_series_label(ol.get("series"))

        base_categories = _norm_subjects(ol.get("subjects"))

        payload = {
            "title": ol.get("title"),
            "subtitle": ol.get("subtitle"),
            "authors": authors,
            "publisher": publishers[0] if publishers else None,
            "published_date": published_date,
            "published_date_raw": raw_date,
            "published_date_specificity": _date_specificity(raw_date),
            "page_count": number_of_pages,
            "language": None,
            "description": desc,
            "categories": base_categories,
            "cover_url": cover_url,
            "openlibrary_id": ol_key,
            "series": series_value,
            "isbn10": isbn10,
            "isbn13": isbn13,
        }
        # Overlay edition-specific fields to ensure correct volume metadata
        if edition_payload:
            edition_overlay = _build_payload_from_edition(
                edition_payload, target_isbn13, target_isbn10
            )
            for key in [
                "title",
                "subtitle",
                "publisher",
                "published_date",
                "published_date_raw",
                "published_date_specificity",
                "page_count",
                "language",
                "description",
                "series",
                "isbn10",
                "isbn13",
                "openlibrary_id",
            ]:
                val = edition_overlay.get(key)
                if val:
                    payload[key] = val
        return payload
    except Exception as e:
        if _META_DEBUG:
            _META_LOG.warning(f"[UNIFIED_METADATA][OPENLIB][EXC] isbn={isbn} err={e}")
        return {}


def _choose_longer_text(a: str | None, b: str | None) -> str | None:
    if a and b:
        return a if len(a) >= len(b) else b
    return a or b or None


def _merge_dicts(google: dict[str, Any], openlib: dict[str, Any]) -> dict[str, Any]:
    """Merge two metadata dicts with sensible precedence rules."""
    merged: dict[str, Any] = {}

    # Lazy import for policies to avoid import cycles when app not fully initialized.
    # Provide a consistent function signature to keep type checkers happy.
    try:  # pragma: no cover - simple import guard
        from app.utils.metadata_settings import (
            apply_field_policy as _real_apply_field_policy,
        )  # type: ignore
    except Exception:  # pragma: no cover

        def _real_apply_field_policy(
            entity: str, field: str, google_val, openlib_val, merged_val
        ):  # type: ignore
            return merged_val

    def apply_field_policy(
        entity: str, field: str, google_val, openlib_val, merged_val
    ):  # type: ignore
        return _real_apply_field_policy(
            entity, field, google_val, openlib_val, merged_val
        )

    # Simple text fields: choose the more complete (longer) text when both present; prefer Google on exact ties
    for key in ["title", "subtitle", "publisher"]:
        g_val = google.get(key)
        o_val = openlib.get(key)
        if g_val and o_val:
            merged[key] = _choose_longer_text(g_val, o_val)
        else:
            merged[key] = g_val if g_val is not None else o_val

    # Language and ratings: prefer Google, fallback to OpenLibrary
    for key in ["language", "average_rating", "rating_count"]:
        merged[key] = (
            google.get(key) if google.get(key) is not None else openlib.get(key)
        )

    # Dates: prefer higher specificity, and prefer Google on ties
    g_date = google.get("published_date")
    o_date = openlib.get("published_date")
    g_spec = google.get("published_date_specificity", 0)
    o_spec = openlib.get("published_date_specificity", 0)
    if g_date and o_date:
        if g_spec > o_spec:
            merged["published_date"] = g_date
        elif o_spec > g_spec:
            merged["published_date"] = o_date
        else:
            # Tie: prefer Google
            merged["published_date"] = g_date
    else:
        merged["published_date"] = g_date or o_date

    # Page count: take the max if both present
    g_pages = google.get("page_count")
    o_pages = openlib.get("page_count")
    try:
        merged["page_count"] = max(x for x in [g_pages, o_pages] if x is not None)
    except ValueError:
        merged["page_count"] = g_pages or o_pages

    # Description: take the longer text
    merged["description"] = _choose_longer_text(
        google.get("description"), openlib.get("description")
    )

    # Authors & categories: union preserving order (Google first)
    authors: list[str] = []
    seen_authors = set()
    for src in [google.get("authors") or [], openlib.get("authors") or []]:
        for name in src:
            if isinstance(name, str):
                key = name.strip().casefold()
                if key and key not in seen_authors:
                    authors.append(name.strip())
                    seen_authors.add(key)
    merged["authors"] = authors

    # Extract 'Series:' indicators from categories before merging
    def _peel_series(src: dict[str, Any]) -> tuple[str | None, list[str]]:
        series_name = None
        cats: list[str] = []
        for c in src.get("categories") or []:
            if isinstance(c, str):
                s = c.strip()
                if s.lower().startswith("series:") and not series_name:
                    series_name = s.split(":", 1)[1].strip() or None
                else:
                    cats.append(s)
        return series_name, cats

    g_series, g_cats = _peel_series(google)
    o_series, o_cats = _peel_series(openlib)

    # Light normalization before dedupe: trim, collapse spaces, strip trailing errant punctuation
    import re as _re

    def _norm_cat(val: str) -> str | None:
        if not isinstance(val, str):
            return None
        s = val.strip()
        if not s:
            return None
        s = _re.sub(r"\s+", " ", s)  # collapse whitespace
        s = _re.sub(r"[\s\.,;:]+$", "", s).strip()  # strip trailing punctuation/spaces
        return s or None

    # Apply normalization to provider lists
    g_cats = [c for c in (_norm_cat(c) for c in g_cats) if c]
    o_cats = [c for c in (_norm_cat(c) for c in o_cats) if c]

    categories: list[str] = []
    seen_cats = set()
    for src in [g_cats, o_cats]:
        for c in src:
            key = c.casefold()
            if key and key not in seen_cats:
                categories.append(c)
                seen_cats.add(key)
    merged["categories"] = categories

    # Series: prefer explicit Google series, else OpenLibrary series; fallback: None
    merged["series"] = (
        g_series or o_series or google.get("series") or openlib.get("series")
    )

    # Cover: use ONLY Google-provided URL (no processing here). OpenLibrary fallback only if Google missing.
    from app.utils.book_utils import (
        select_highest_google_image,
        upgrade_google_cover_url,
    )

    raw_google_cover = google.get("cover_url") or select_highest_google_image(
        google.get("image_links_all")
    )
    if raw_google_cover:
        merged["cover_url"] = upgrade_google_cover_url(raw_google_cover)
        merged["cover_source"] = "Google Books"
    else:
        fallback_ol = openlib.get("cover_url")
        if fallback_ol:
            merged["cover_url"] = fallback_ol
            merged["cover_source"] = "OpenLibrary"
        else:
            merged["cover_url"] = None

    # IDs & ISBNs
    merged["google_books_id"] = google.get("google_books_id")
    merged["openlibrary_id"] = openlib.get("openlibrary_id")

    # Prefer Google ISBNs, fallback to OpenLibrary when missing
    def _norm_isbn(v: str | None) -> str | None:
        if not v:
            return None
        import re as _re

        return _re.sub(r"[^0-9Xx]", "", str(v)) or None

    merged["isbn10"] = _norm_isbn(google.get("isbn10")) or _norm_isbn(
        openlib.get("isbn10")
    )
    merged["isbn13"] = _norm_isbn(google.get("isbn13")) or _norm_isbn(
        openlib.get("isbn13")
    )

    # Apply field policies (books entity)
    for field, value in list(merged.items()):
        try:
            merged[field] = apply_field_policy(
                "books", field, google.get(field), openlib.get(field), value
            )
        except Exception:
            pass
    # Cover source fix if cover removed
    if not merged.get("cover_url"):
        merged.pop("cover_source", None)
    return merged


def _unified_fetch_pair(isbn: str):
    """Internal: concurrently fetch provider raw dicts and gather error states.

    Returns (google_dict, openlib_dict, errors_dict).
    Errors dict entries are provider -> description ('empty' if empty successful response).
    """
    import re as _re_norm

    isbn_clean = (isbn or "").strip()
    isbn_clean = _re_norm.sub(r"[^0-9Xx]", "", isbn_clean)

    # Early structural + checksum validation to avoid wasting provider calls
    def _valid_isbn10(val: str) -> bool:
        if len(val) != 10:
            return False
        if not _re_norm.match(r"^[0-9]{9}[0-9Xx]$", val):
            return False
        s = 0
        for i, ch in enumerate(val[:9]):
            if not ch.isdigit():
                return False
            s += (10 - i) * int(ch)
        check = val[9]
        if check in "Xx":
            s += 10
        else:
            if not check.isdigit():
                return False
            s += int(check)
        return s % 11 == 0

    def _valid_isbn13(val: str) -> bool:
        if len(val) != 13 or not val.isdigit():
            return False
        if not (val.startswith("978") or val.startswith("979")):
            return False
        tot = 0
        for i, ch in enumerate(val[:12]):
            w = 1 if i % 2 == 0 else 3
            tot += int(ch) * w
        calc = (10 - (tot % 10)) % 10
        return calc == int(val[12])

    def _isbn_valid(val: str) -> bool:
        return _valid_isbn10(val) or _valid_isbn13(val)

    if not isbn_clean:
        return {}, {}, {"input": "empty"}
    if not _isbn_valid(isbn_clean):
        return {}, {}, {"input": "invalid_format"}
    google: dict[str, Any] = {}
    openlib: dict[str, Any] = {}
    _errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=2) as ex:
        future_map = {
            ex.submit(_fetch_google_by_isbn, isbn_clean): "google",
            ex.submit(_fetch_openlibrary_by_isbn, isbn_clean): "openlib",
        }
        # Add timeout to as_completed to prevent indefinite blocking
        # This ensures the entire fetch operation doesn't exceed _FETCH_TIMEOUT
        try:
            for fut in as_completed(future_map, timeout=_FETCH_TIMEOUT):
                kind = future_map[fut]
                try:
                    # Add timeout to result() as well for additional safety
                    data = fut.result(timeout=_FETCH_TIMEOUT) or {}
                except Exception as ex_var:  # defensive; provider funcs should swallow
                    data = {}
                    _errors[kind] = f"exception:{ex_var}"
                if kind == "google":
                    google = data
                else:
                    openlib = data
        except TimeoutError:
            # If as_completed times out, mark all pending futures as timed out
            for fut, kind in future_map.items():
                if not fut.done():
                    _errors[kind] = "timeout"
                    # Cancel any still-running futures
                    fut.cancel()
            if _META_DEBUG:
                _META_LOG.warning(
                    f"[UNIFIED_METADATA][TIMEOUT] isbn={isbn} timeout={_FETCH_TIMEOUT}s"
                )

    # Record empty provider results explicitly for diagnostics
    if not google and "google" not in _errors:
        _errors["google"] = "empty"
    if not openlib and "openlib" not in _errors:
        _errors["openlib"] = "empty"
    return google, openlib, _errors


def fetch_unified_by_isbn_detailed(isbn: str) -> tuple[dict[str, Any], dict[str, str]]:
    """Public detailed fetch returning (merged_metadata, provider_errors).

    If both providers empty, merged_metadata is {} and errors indicate causes.
    Now includes ISBN mismatch detection to warn when metadata sources return
    information for a different ISBN than requested.
    """
    google, openlib, _errors = _unified_fetch_pair(isbn)
    req_variants = _collect_isbn_variants(isbn)
    req_isbn = _normalize_isbn_value(isbn)
    dropped_providers: list[str] = []

    def _filter_provider(name: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not payload or not req_variants:
            return payload
        provider_variants: set[str] = set()
        provider_variants |= _collect_isbn_variants(payload.get("isbn13"))
        provider_variants |= _collect_isbn_variants(payload.get("isbn10"))
        if provider_variants and not (provider_variants & req_variants):
            friendly = "Google Books" if name == "google" else "OpenLibrary"
            dropped_providers.append(name)
            _errors[name] = "isbn_mismatch"
            _META_LOG.warning(
                f"[UNIFIED_METADATA][{friendly.upper()}][ISBN_MISMATCH_DROP] requested={req_isbn or isbn} provider_isbns={sorted(provider_variants)}"
            )
            return {}
        return payload

    google = _filter_provider("google", google)
    openlib = _filter_provider("openlib", openlib)

    if not google and not openlib:
        level = (
            _META_LOG.error
            if any(v != "empty" for v in _errors.values())
            else _META_LOG.warning
        )
        level(f"[UNIFIED_METADATA][EMPTY] isbn={isbn} errors={_errors}")
        return {}, _errors

    # Ensure dates normalization
    if google.get("published_date"):
        google["published_date"] = _normalize_date(google["published_date"])
    if openlib.get("published_date"):
        openlib["published_date"] = _normalize_date(openlib["published_date"])

    merged = _merge_dicts(google, openlib)

    if "raw_category_paths" not in merged:
        cats = merged.get("categories") or []
        merged["raw_category_paths"] = list(cats)

    requested_isbn = req_isbn

    # Ensure requested ISBN is preserved in merged result if missing
    try:
        if requested_isbn:
            if not merged.get("isbn13") and len(requested_isbn) == 13:
                merged["isbn13"] = requested_isbn
            if not merged.get("isbn10") and len(requested_isbn) == 10:
                merged["isbn10"] = requested_isbn
    except Exception:
        pass

    # ISBN mismatch detection
    warnings: list[str] = []
    if dropped_providers:
        labels = {"google": "Google Books", "openlib": "OpenLibrary"}
        friendly = ", ".join(labels.get(name, name) for name in dropped_providers)
        warnings.append(f"Ignored metadata from {friendly} due to ISBN mismatch")
    isbn_mismatch = False

    if requested_isbn:
        returned_variants: set[str] = set()
        returned_variants |= _collect_isbn_variants(merged.get("isbn13"))
        returned_variants |= _collect_isbn_variants(merged.get("isbn10"))

        if (
            returned_variants
            and req_variants
            and not (returned_variants & req_variants)
        ):
            isbn_mismatch = True
            returned_isbns = []
            if merged.get("isbn13"):
                returned_isbns.append(
                    f"ISBN-13: {_normalize_isbn_value(merged.get('isbn13'))}"
                )
            if merged.get("isbn10"):
                returned_isbns.append(
                    f"ISBN-10: {_normalize_isbn_value(merged.get('isbn10'))}"
                )
            returned_str = ", ".join(returned_isbns) if returned_isbns else "unknown"
            warnings.append(
                f"ISBN mismatch: Requested {requested_isbn}, but metadata returned for {returned_str}"
            )
            _META_LOG.warning(
                f"[UNIFIED_METADATA][ISBN_MISMATCH] requested={requested_isbn} returned_variants={sorted(returned_variants)}"
            )
            title = merged.get("title", "")
            if title:
                vol_match = re.search(
                    r"\b(?:vol\.?|volume|book|v\.?)\s*:?\s*(\d+)", title, re.IGNORECASE
                )
                if vol_match:
                    warnings.append(
                        f"Metadata shows volume {vol_match.group(1)} - verify this matches your book"
                    )

    # Add metadata quality indicators
    merged["_metadata_warnings"] = warnings
    merged["_isbn_mismatch"] = isbn_mismatch
    merged["_requested_isbn"] = requested_isbn

    return merged, _errors


def fetch_unified_by_isbn(isbn: str) -> dict[str, Any]:
    """Fetch and merge Google Books and OpenLibrary metadata for an ISBN (parallel IO).

    Now aggressively normalizes the ISBN (strip non-digit/X) so that upstream
    sources receive canonical forms regardless of CSV formatting or hyphens.
    """
    data, _ = fetch_unified_by_isbn_detailed(isbn)
    return data


def fetch_unified_by_title(
    title: str, max_results: int = 10, author: str | None = None
) -> list[dict[str, Any]]:
    """Passthrough to enhanced title search across Google Books and OpenLibrary, with optional author filter."""
    from app.utils.book_search import search_books_by_title

    return search_books_by_title(title, max_results, author)
