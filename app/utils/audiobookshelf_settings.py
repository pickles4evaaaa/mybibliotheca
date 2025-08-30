"""
Helpers for reading and writing Audiobookshelf settings in data/audiobookshelf_settings.json
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict

from flask import current_app


DEFAULTS: Dict[str, Any] = {
    'enabled': False,
    'base_url': '',
    'api_key': '',
    'library_ids': [],
    'last_library_sync': None,
    # Per-library last sync cutoffs keyed by library_id -> ISO string or epoch seconds
    'last_library_sync_map': {},
    # Optional ABS user id to scope listening/session syncs; if omitted, use 'me' endpoints
    'abs_user_id': None,
    # Scheduler preferences (UI only; backend scheduler may consume later)
    'auto_sync_enabled': False,
    'library_sync_every_hours': 24,
    'listening_sync_every_hours': 12,
    'last_listening_sync': None,
    # Debug flag to enable verbose listening sync logs
    'debug_listening_sync': False,
    # Enforce job order: books first, then listening
    'enforce_book_first': True,
}


def _settings_path() -> str:
    try:
        data_dir = current_app.config.get('DATA_DIR', 'data')  # type: ignore[attr-defined]
    except Exception:
        data_dir = 'data'
    return os.path.join(data_dir, 'audiobookshelf_settings.json')


def load_abs_settings() -> Dict[str, Any]:
    path = _settings_path()
    try:
        if os.path.exists(path):
            with open(path, 'r') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    merged = DEFAULTS.copy()
                    merged.update({k: v for k, v in data.items() if k in DEFAULTS})
                    # Normalize booleans stored as strings
                    def _to_bool(val: Any) -> bool:
                        if isinstance(val, bool):
                            return val
                        if isinstance(val, (int, float)):
                            try:
                                return bool(int(val))
                            except Exception:
                                return bool(val)
                        if isinstance(val, str):
                            return val.strip().lower() in ('1', 'true', 'yes', 'on')
                        return False
                    # Apply to known boolean keys
                    for key in ('enabled','auto_sync_enabled', 'debug_listening_sync','enforce_book_first'):
                        if key in merged:
                            merged[key] = _to_bool(merged.get(key))
                    return merged
    except Exception:
        pass
    return DEFAULTS.copy()


def save_abs_settings(update: Dict[str, Any]) -> bool:
    path = _settings_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        current = load_abs_settings()
        # Only allow known keys
        for k in list(update.keys()):
            if k not in DEFAULTS:
                update.pop(k, None)
        # Normalize library_ids to list[str]
        libs = update.get('library_ids')
        if libs is not None:
            if isinstance(libs, str):
                libs = [s.strip() for s in libs.split(',') if s.strip()]
            elif isinstance(libs, (tuple, list)):
                libs = [str(s).strip() for s in libs if str(s).strip()]
            else:
                libs = []
            update['library_ids'] = libs
        # Normalize boolean flags
        def _to_bool(val: Any) -> bool:
            if isinstance(val, bool):
                return val
            if isinstance(val, (int, float)):
                try:
                    return bool(int(val))
                except Exception:
                    return bool(val)
            if isinstance(val, str):
                return val.strip().lower() in ('1', 'true', 'yes', 'on')
            return False
        for key in ('enabled','auto_sync_enabled', 'debug_listening_sync','enforce_book_first'):
            if key in update:
                update[key] = _to_bool(update.get(key))
        current.update(update or {})
        with open(path, 'w') as f:
            json.dump(current, f, indent=2)
        return True
    except Exception:
        return False

