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
from typing import Any, Dict, Optional, Tuple
from flask import current_app

from app.domain.models import MediaType


_MEDIA_TYPE_VALUES = {mt.value for mt in MediaType}


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
            return data
        return {}
    except Exception:
        return {}


def save_user_settings(user_id: Optional[str], updates: Dict[str, Any]) -> bool:
    """Persist per-user settings, merging with existing if present."""
    if not user_id:
        return False
    existing = load_user_settings(user_id)
    existing.update({k: v for k, v in updates.items() if v is not None})
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
