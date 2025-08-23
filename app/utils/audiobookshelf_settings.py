"""
Helpers for reading and writing Audiobookshelf settings in data/audiobookshelf_settings.json
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict

from flask import current_app


DEFAULTS: Dict[str, Any] = {
    'base_url': '',
    'api_key': '',
    'library_ids': [],
    'last_library_sync': None,
    # Scheduler preferences (UI only; backend scheduler may consume later)
    'auto_sync_enabled': False,
    'library_sync_every_hours': 24,
    'listening_sync_every_hours': 12,
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
        current.update(update or {})
        with open(path, 'w') as f:
            json.dump(current, f, indent=2)
        return True
    except Exception:
        return False

