"""
User settings utilities

Persist lightweight per-user settings (like reading defaults) in JSON files under data/user_settings/.
This avoids relying on incomplete update support in the Kuzu user repository while remaining simple
and portable. If a future DB-backed implementation is added, these helpers can be adapted to read
from the database first and fall back to JSON.
"""
from __future__ import annotations

import os
import json
from typing import Any, Dict, List, Optional, Tuple
from flask import current_app

from app.domain.models import MediaType, ReadingStatus


_MEDIA_TYPE_VALUES = {mt.value for mt in MediaType}

# Valid reading status values for default selection (derived from enum)
# Includes empty string to represent "Not Set" option
# Use a set comprehension to get unique values (handles enum aliases)
_READING_STATUS_VALUES = {rs.value for rs in ReadingStatus} | {''}

# Display labels for reading status values
_READING_STATUS_LABELS = {
    '': 'Not Set',
    'plan_to_read': 'Plan to Read',
    'reading': 'Currently Reading',
    'read': 'Read',
    'on_hold': 'On Hold',
    'did_not_finish': 'Did Not Finish',
    'library_only': 'Library Only',
}

# Reading status choices for the default reading status dropdown
# Ordered list of (value, label) pairs for display
_DEFAULT_READING_STATUS_ORDER: Tuple[Tuple[str, str], ...] = (
    ('', _READING_STATUS_LABELS['']),
    (ReadingStatus.PLAN_TO_READ.value, _READING_STATUS_LABELS[ReadingStatus.PLAN_TO_READ.value]),
    (ReadingStatus.READING.value, _READING_STATUS_LABELS[ReadingStatus.READING.value]),
    (ReadingStatus.READ.value, _READING_STATUS_LABELS[ReadingStatus.READ.value]),
    (ReadingStatus.ON_HOLD.value, _READING_STATUS_LABELS[ReadingStatus.ON_HOLD.value]),
    (ReadingStatus.DNF.value, _READING_STATUS_LABELS[ReadingStatus.DNF.value]),
    (ReadingStatus.LIBRARY_ONLY.value, _READING_STATUS_LABELS[ReadingStatus.LIBRARY_ONLY.value]),
)

_LIBRARY_SORT_ORDER: Tuple[Tuple[str, str], ...] = (
    ('title_asc', 'Title A-Z'),
    ('title_desc', 'Title Z-A'),
    ('author_first_asc', 'Author First, Last A-Z'),
    ('author_first_desc', 'Author First, Last Z-A'),
    ('author_last_asc', 'Author Last, First A-Z'),
    ('author_last_desc', 'Author Last, First Z-A'),
    ('date_added_desc', 'Date Added (Newest First)'),
    ('date_added_asc', 'Date Added (Oldest First)'),
    ('publication_date_desc', 'Publication Date (Newest First)'),
    ('publication_date_asc', 'Publication Date (Oldest First)')
)

_LIBRARY_STATUS_ORDER: Tuple[Tuple[str, str], ...] = (
    ('reading', 'Currently Reading'),
    ('read', 'Books Read'),
    ('plan_to_read', 'Plan to Read'),
    ('wishlist', 'Wishlist'),
    ('on_hold', 'On Hold'),
    ('all', 'All Books')
)

_LIBRARY_SORT_OPTIONS = {key for key, _ in _LIBRARY_SORT_ORDER}
_LIBRARY_STATUS_OPTIONS = {key for key, _ in _LIBRARY_STATUS_ORDER}.union({'currently_reading'})


def normalize_library_sort_option(raw: Optional[str]) -> Optional[str]:
    """Normalize a library sort option to a known value."""
    if raw is None:
        return None
    candidate = str(raw).strip().lower()
    return candidate if candidate in _LIBRARY_SORT_OPTIONS else None


def normalize_library_status_filter(raw: Optional[str]) -> Optional[str]:
    """Normalize a library status filter to a supported token."""
    if raw is None:
        return None
    candidate = str(raw).strip().lower()
    aliases = {
        'currently_reading': 'reading',
        'currently reading': 'reading',
        'want_to_read': 'plan_to_read',
        'wishlist_reading': 'plan_to_read',
        '': 'all',
    }
    candidate = aliases.get(candidate, candidate)
    return candidate if candidate in _LIBRARY_STATUS_OPTIONS else None


def get_library_sort_choices() -> List[Tuple[str, str]]:
    """Return available library sort options as (value, label) pairs."""
    return list(_LIBRARY_SORT_ORDER)


def get_library_status_choices() -> List[Tuple[str, str]]:
    """Return available library status filter options as (value, label) pairs."""
    return list(_LIBRARY_STATUS_ORDER)


def normalize_default_reading_status(raw: Optional[str]) -> str:
    """Normalize a default reading status to a known value.
    
    Returns empty string if not a valid reading status value.
    """
    if raw is None:
        return ''
    candidate = str(raw).strip().lower()
    # Handle common aliases
    aliases = {
        'currently_reading': 'reading',
        'currently reading': 'reading',
        'want_to_read': 'plan_to_read',
        'dnf': 'did_not_finish',
    }
    candidate = aliases.get(candidate, candidate)
    return candidate if candidate in _READING_STATUS_VALUES else ''


def get_default_reading_status_choices() -> List[Tuple[str, str]]:
    """Return available default reading status options as (value, label) pairs."""
    return list(_DEFAULT_READING_STATUS_ORDER)


def _data_dir() -> str:
    try:
        return current_app.config.get('DATA_DIR', 'data')
    except Exception:
        return 'data'


def _user_settings_path(user_id: str) -> str:
    base = os.path.join(_data_dir(), 'user_settings')
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, f"{user_id}.json")


def load_user_settings(user_id: Optional[str]) -> Dict[str, Any]:
    """Load per-user settings JSON. Returns an empty dict if not found or invalid."""
    if not user_id:
        return {}
    path = _user_settings_path(str(user_id))
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        # Ensure known ABS fields exist with defaults
        if isinstance(data, dict):
            data.setdefault('abs_username', '')
            data.setdefault('abs_api_key', '')
            data.setdefault('abs_sync_books', False)
            data.setdefault('abs_sync_listening', False)
            # Library defaults (sort/status)
            sort_token = normalize_library_sort_option(data.get('library_default_sort'))
            status_token = normalize_library_status_filter(data.get('library_default_status'))
            data['library_default_sort'] = sort_token or 'title_asc'
            data['library_default_status'] = status_token or 'all'
            # Default reading status for new books
            data['default_reading_status'] = normalize_default_reading_status(
                data.get('default_reading_status')
            )
            return data
        return {}
    except Exception:
        return {}


def save_user_settings(user_id: Optional[str], updates: Dict[str, Any]) -> bool:
    """Persist per-user settings, merging with existing if present."""
    if not user_id:
        return False
    existing = load_user_settings(user_id)
    normalized_updates: Dict[str, Any] = {}
    for key, value in updates.items():
        if key == 'library_default_sort':
            norm = normalize_library_sort_option(value)
            normalized_updates[key] = norm or 'title_asc'
        elif key == 'library_default_status':
            norm = normalize_library_status_filter(value)
            normalized_updates[key] = norm or 'all'
        elif key == 'default_reading_status':
            normalized_updates[key] = normalize_default_reading_status(value)
        else:
            if value is not None:
                normalized_updates[key] = value

    existing.update(normalized_updates)
    try:
        path = _user_settings_path(str(user_id))
        with open(path, 'w') as f:
            json.dump(existing, f, indent=2)
        return True
    except Exception as e:
        try:
            current_app.logger.error(f"Failed to save user settings for {user_id}: {e}")
        except Exception:
            pass
        return False


def get_effective_reading_defaults(user_id: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    """
    Return (pages, minutes) defaults for reading logs for the given user.
    Precedence: per-user JSON overrides > admin system_config defaults > (None, None)
    """
    # Per-user overrides
    u = load_user_settings(user_id)
    up = u.get('default_pages_per_log')
    um = u.get('default_minutes_per_log')
    try:
        up_i = int(up) if up not in (None, "",) else None
    except Exception:
        up_i = None
    try:
        um_i = int(um) if um not in (None, "",) else None
    except Exception:
        um_i = None
    if up_i is not None or um_i is not None:
        return up_i, um_i

    # Fall back to admin/system defaults
    try:
        from app.admin import load_system_config
        cfg = load_system_config() or {}
        rld = cfg.get('reading_log_defaults') or {}
        ap = rld.get('default_pages_per_log')
        am = rld.get('default_minutes_per_log')
        ap_i = int(ap) if ap not in (None, "") else None
        am_i = int(am) if am not in (None, "") else None
        return ap_i, am_i
    except Exception:
        return None, None


def get_effective_rows_per_page(user_id: Optional[str]) -> Optional[int]:
    """
    Return the effective default rows-per-page for the library grid for the given user.
    Precedence: per-user JSON override > admin system_config default > None
    """
    try:
        # Per-user override
        u = load_user_settings(user_id)
        val = u.get('library_rows_per_page')
        try:
            v_i = int(val) if val not in (None, "") else None
        except Exception:
            v_i = None
        if v_i is not None and v_i >= 1:
            return v_i
    except Exception:
        pass

    # Fallback to admin/system defaults
    try:
        from app.admin import load_system_config
        cfg = load_system_config() or {}
        lib = cfg.get('library_defaults') or {}
        dv = lib.get('default_rows_per_page')
        try:
            dv_i = int(dv) if dv not in (None, "") else None
        except Exception:
            dv_i = None
        return dv_i if (dv_i is None or dv_i >= 1) else None
    except Exception:
        return None


def get_library_view_defaults(user_id: Optional[str]) -> Tuple[str, str]:
    """Return (status_filter, sort_option) defaults for the library view."""
    settings = load_user_settings(user_id)
    status = normalize_library_status_filter(settings.get('library_default_status')) or 'all'
    sort = normalize_library_sort_option(settings.get('library_default_sort')) or 'title_asc'
    return status, sort


def get_default_reading_status(user_id: Optional[str]) -> str:
    """Return the user's default reading status for newly added books.
    
    Returns empty string if not set (meaning "Not Set" in the UI).
    """
    settings = load_user_settings(user_id)
    return normalize_default_reading_status(settings.get('default_reading_status'))


def get_default_book_format() -> str:
    """Return the admin-configured default book format with a safe fallback."""
    try:
        from app.admin import load_system_config

        cfg = load_system_config() or {}
        lib = cfg.get('library_defaults') or {}
        raw = lib.get('default_book_format')
        if isinstance(raw, str):
            candidate = raw.strip().lower()
            if candidate in _MEDIA_TYPE_VALUES:
                return candidate
    except Exception:
        pass
    return MediaType.PHYSICAL.value
