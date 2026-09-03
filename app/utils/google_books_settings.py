"""Helpers for reading and writing Google Books settings in
data/google_books_settings.json

Only one setting for now: an optional API key. Without one Google applies a
shared, anonymous per-IP quota that self-hosted instances hit easily during
bulk imports. A key saved in the settings file wins; the GOOGLE_API_KEY
environment variable is the zero-config fallback.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

from flask import current_app


ENV_VAR = 'GOOGLE_API_KEY'

DEFAULTS: Dict[str, Any] = {
    'api_key': '',
}


def _settings_path() -> str:
    try:
        data_dir = current_app.config.get('DATA_DIR', 'data')  # type: ignore[attr-defined]
    except Exception:
        data_dir = 'data'
    return os.path.join(data_dir, 'google_books_settings.json')


def load_google_books_settings() -> Dict[str, Any]:
    """Return settings merged over DEFAULTS."""
    merged = DEFAULTS.copy()
    path = _settings_path()
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                merged.update({k: v for k, v in data.items() if k in DEFAULTS})
    except Exception:
        merged = DEFAULTS.copy()
    merged['api_key'] = str(merged.get('api_key') or '').strip()
    return merged


def save_google_books_settings(update: Dict[str, Any]) -> bool:
    """Persist known keys from `update`. Returns True on success."""
    path = _settings_path()
    try:
        current = load_google_books_settings()
        for key, value in (update or {}).items():
            if key == 'api_key':
                current[key] = str(value or '').strip()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(current, f, indent=2)
        return True
    except Exception:
        return False


def get_google_api_key() -> Optional[str]:
    """Effective Google API key: settings file, then GOOGLE_API_KEY env var."""
    key = load_google_books_settings().get('api_key') or (os.environ.get(ENV_VAR) or '').strip()
    return key or None


__all__ = ['load_google_books_settings', 'save_google_books_settings', 'get_google_api_key']
