"""Focused data-normalization helpers for book routes.

The functions in this module are intentionally side-effect free except for
the legacy logging behavior in date parsing.  book_routes re-exports them
through module-level imports for compatibility.
"""

from __future__ import annotations

from collections import OrderedDict
from datetime import date, datetime, timezone
import re
from typing import Any, List, Optional

from app.domain.models import ReadingStatus
from flask import current_app


def _normalize_personal_datetime(value):
    """Convert user-provided date inputs into timezone-aware datetimes."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        candidate = s.replace('Z', '+00:00')
        try:
            parsed = datetime.fromisoformat(candidate)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
        for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d'):
            try:
                parsed = datetime.strptime(s, fmt)
                return parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        try:
            epoch = float(s)
            if epoch > 10_000_000_000:
                epoch /= 1000.0
            return datetime.fromtimestamp(epoch, tz=timezone.utc)
        except (ValueError, OSError):
            return None
    return None


def _datetimes_equal(a, b):
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return a == b


def _humanize_status(status: str) -> str:
    mapping = {
        'plan_to_read': 'Plan to Read',
        'reading': 'Reading',
        'currently_reading': 'Currently Reading',
        'read': 'Read',
        'on_hold': 'On Hold',
        'did_not_finish': 'Did Not Finish',
        'library_only': 'Library Only'
    }
    base = mapping.get(status, status.replace('_', ' ').replace('-', ' ').title())
    return base


def _normalize_reading_status(raw_status: str) -> Optional[str]:
    if not raw_status:
        return None
    normalized = raw_status.strip().lower().replace('-', '_').replace(' ', '_')
    alias_map = {
        'currently_reading': 'reading',
        'in_progress': 'reading',
        'current': 'reading',
        'want_to_read': 'plan_to_read',
        'wishlist_reading': 'plan_to_read',
        'has_read': 'read',
        'completed': 'read',
        'complete': 'read',
        'finished': 'read',
        'paused': 'on_hold',
        'hold': 'on_hold',
        'dnf': 'did_not_finish',
        'dropped': 'did_not_finish'
    }
    mapped = alias_map.get(normalized, normalized)
    allowed_statuses = {status.value for status in ReadingStatus}
    if mapped in allowed_statuses:
        return mapped
    return None


def _dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    deduped: List[str] = []
    for value in values:
        if not value:
            continue
        cleaned = value.strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(cleaned)
    return deduped


def _parse_additional_categories(raw_value: str) -> List[str]:
    if not raw_value:
        return []
    tokens = [token.strip() for token in re.split(r'[\n,]+', raw_value) if token.strip()]
    return tokens


def _category_name_from_record(record: Any) -> Optional[str]:
    if record is None:
        return None
    if isinstance(record, dict):
        for key in ('name', 'label', 'value', 'normalized_name', 'result'):
            value = record.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        # Fallback: inspect remaining values
        for value in record.values():
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None
    if isinstance(record, str):
        cleaned = record.strip()
        return cleaned or None
    name_attr = getattr(record, 'name', None)
    if isinstance(name_attr, str) and name_attr.strip():
        return name_attr.strip()
    cleaned = str(record).strip()
    return cleaned or None


def _extract_existing_categories_from_book(book: Any) -> List[str]:
    ordered: OrderedDict[str, str] = OrderedDict()

    def _record(value: Any) -> None:
        if value is None:
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                _record(item)
            return
        if isinstance(value, dict):
            candidate = _category_name_from_record(value)
            if candidate:
                _record(candidate)
            return
        # Handle objects with name/label attributes before falling back to repr strings
        name_attr = getattr(value, 'name', None)
        if isinstance(name_attr, str) and name_attr.strip():
            normalized = name_attr.strip()
            key = normalized.lower()
            if key not in ordered:
                ordered[key] = normalized
            return
        label_attr = getattr(value, 'label', None)
        if isinstance(label_attr, str) and label_attr.strip():
            normalized = label_attr.strip()
            key = normalized.lower()
            if key not in ordered:
                ordered[key] = normalized
            return
        if isinstance(value, str):
            normalized = value.strip()
            if not normalized:
                return
            # Attempt to split composite strings (comma / newline separated)
            segments = _parse_additional_categories(value)
            if len(segments) > 1:
                for segment in segments:
                    _record(segment)
                return
            key = normalized.lower()
            if key not in ordered:
                ordered[key] = normalized
            return
        cleaned = str(value).strip()
        if cleaned:
            key = cleaned.lower()
            if key not in ordered:
                ordered[key] = cleaned

    if isinstance(book, dict):
        for key in ('raw_categories', 'categories', 'category_names', 'tags', 'genres', 'audiobookshelf_categories'):
            if key in book:
                _record(book[key])
        import_metadata = book.get('import_metadata')
        if isinstance(import_metadata, dict):
            for key in ('categories', 'tags', 'genres'):
                if key in import_metadata:
                    _record(import_metadata[key])
    else:
        for attr in ('raw_categories', 'categories', 'category_names', 'tags', 'genres'):
            if hasattr(book, attr):
                _record(getattr(book, attr))

    return list(ordered.values())


def _convert_published_date_to_date(published_date_str):
    """Convert published_date string to date object using enhanced date parser."""
    if not published_date_str or not isinstance(published_date_str, str):
        return None
    
    try:
        # Simple date parsing - handle common formats
        from datetime import datetime
        # Try common date formats
        formats = ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y', '%Y']
        for fmt in formats:
            try:
                return datetime.strptime(published_date_str.strip(), fmt).date()
            except ValueError:
                continue
        return None
    except Exception as e:
        current_app.logger.warning(f"Failed to parse published date '{published_date_str}': {e}")
        return None


def _safe_date_to_isoformat(date_obj):
    """Safely convert date object to ISO format string."""
    if date_obj and hasattr(date_obj, 'isoformat'):
        return date_obj.isoformat()
    return None


def _format_published_date_for_input(published_date_str):
    """Format published_date for HTML5 date input (YYYY-MM-DD)."""
    if not published_date_str or not isinstance(published_date_str, str):
        return None
    
    try:
        from datetime import datetime
        date_str = published_date_str.strip()
        
        # Try various date formats that APIs might return
        formats = [
            '%Y-%m-%d',        # 2023-12-25
            '%Y/%m/%d',        # 2023/12/25  
            '%m/%d/%Y',        # 12/25/2023
            '%d/%m/%Y',        # 25/12/2023
            '%Y-%m',           # 2023-12
            '%Y/%m',           # 2023/12
            '%m/%Y',           # 12/2023
            '%Y',              # 2023
            '%B %d, %Y',       # December 25, 2023
            '%b %d, %Y',       # Dec 25, 2023
            '%d %B %Y',        # 25 December 2023
            '%d %b %Y',        # 25 Dec 2023
            '%B %Y',           # December 2023
            '%b %Y',           # Dec 2023
        ]
        
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                # Return YYYY-MM-DD format for HTML5 date input
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # If no format matches, try to extract just the year
        import re
        year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
        if year_match:
            year = year_match.group()
            # Return January 1st of that year as default
            return f"{year}-01-01"
        
        return None
    except Exception as e:
        return None

