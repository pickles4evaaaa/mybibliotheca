"""Language catalog utilities.

This provides a dynamic, JSON-backed list of language tokens for book metadata.

- Source includes:
  - Built-in seed (common languages)
  - User-entered languages (persisted)
  - Languages discovered from external metadata (persisted)

Persistence is intentionally JSON-based for now to keep migration/simple portability.

TODO(chore): Move this catalog into KuzuDB with a clear normalization strategy
(e.g., mapping ISO-639 language codes vs. locale-like tags such as "nl_NL",
plus supporting non-standard languages like "Klingon").
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


_SEED_LABELS: Dict[str, str] = {
    "en": "English",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "it": "Italian",
    "pt": "Portuguese",
    "ru": "Russian",
    "ja": "Japanese",
    "ko": "Korean",
    "zh": "Chinese",
    "ar": "Arabic",
    "hi": "Hindi",
}


@dataclass(frozen=True)
class LanguageCatalog:
    labels: Dict[str, str]
    languages: List[str]


_cache: Optional[LanguageCatalog] = None
_cache_mtime: Optional[float] = None


def _resolve_data_dir() -> Path:
    """Best-effort resolution of the application's data directory."""
    env_dir = os.getenv("MYBIBLIOTHECA_DATA_DIR") or os.getenv("DATA_DIR")
    if env_dir:
        return Path(env_dir)

    try:
        from flask import current_app

        data_dir = current_app.config.get("DATA_DIR")  # type: ignore[attr-defined]
        if data_dir:
            return Path(str(data_dir))
    except Exception:
        pass

    try:
        return Path(__file__).resolve().parents[2] / "data"
    except Exception:
        return Path.cwd() / "data"


def _catalog_path() -> Path:
    return _resolve_data_dir() / "language_catalog.json"


def _seed_catalog() -> Dict[str, Any]:
    # Keep order stable for UX.
    seed_list = list(_SEED_LABELS.keys())
    return {
        "version": 1,
        "labels": dict(_SEED_LABELS),
        "languages": seed_list,
        "custom": [],
        "last_updated": datetime.utcnow().isoformat() + "Z",
    }


def _normalize_token(value: Optional[str]) -> str:
    if not value:
        return ""
    return str(value).strip()


def _load_raw() -> Dict[str, Any]:
    path = _catalog_path()
    if not path.exists():
        return _seed_catalog()

    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else _seed_catalog()
    except Exception:
        return _seed_catalog()


def _write_raw(data: Dict[str, Any]) -> bool:
    path = _catalog_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        data["last_updated"] = datetime.utcnow().isoformat() + "Z"
        with path.open("w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, sort_keys=False)
        return True
    except Exception:
        return False


def _build_catalog(data: Dict[str, Any]) -> LanguageCatalog:
    labels_obj = data.get("labels")
    labels_src: Dict[str, Any] = labels_obj if isinstance(labels_obj, dict) else {}
    labels: Dict[str, str] = {str(k): str(v) for k, v in labels_src.items() if k and v}

    languages_obj = data.get("languages")
    languages_raw: List[Any] = languages_obj if isinstance(languages_obj, list) else []
    custom_obj = data.get("custom")
    custom_raw: List[Any] = custom_obj if isinstance(custom_obj, list) else []

    ordered: List[str] = []
    seen = set()

    for item in languages_raw:
        token = _normalize_token(item)
        if token and token not in seen:
            ordered.append(token)
            seen.add(token)

    for item in custom_raw:
        token = _normalize_token(item)
        if token and token not in seen:
            ordered.append(token)
            seen.add(token)

    # Ensure seeds exist even if file is partial.
    for token in _SEED_LABELS.keys():
        if token not in seen:
            ordered.insert(0, token)
            seen.add(token)

    return LanguageCatalog(labels=labels, languages=ordered)


def load_language_catalog(force_reload: bool = False) -> LanguageCatalog:
    global _cache, _cache_mtime

    path = _catalog_path()
    mtime = None
    try:
        mtime = path.stat().st_mtime
    except Exception:
        mtime = None

    if not force_reload and _cache is not None and _cache_mtime == mtime:
        return _cache

    raw = _load_raw()
    # If the file doesn't exist, persist seeds so it becomes user-editable.
    if not path.exists():
        _write_raw(raw)
        try:
            mtime = path.stat().st_mtime
        except Exception:
            mtime = None

    _cache = _build_catalog(raw)
    _cache_mtime = mtime
    return _cache


def get_language_choices() -> List[Tuple[str, str]]:
    """Return (token,label) tuples for select dropdowns."""
    catalog = load_language_catalog()
    out: List[Tuple[str, str]] = []
    for token in catalog.languages:
        label = catalog.labels.get(token) or token
        out.append((token, label))
    return out


def language_label(token: Optional[str]) -> str:
    token_n = _normalize_token(token)
    if not token_n:
        return ""
    catalog = load_language_catalog()
    return catalog.labels.get(token_n) or token_n


def remember_language(token: Optional[str]) -> bool:
    """Persist a language token into the catalog if it's new."""
    token_n = _normalize_token(token)
    if not token_n:
        return False

    data = _load_raw()

    languages_obj = data.get("languages")
    languages: List[str] = [str(x).strip() for x in languages_obj] if isinstance(languages_obj, list) else []
    custom_obj = data.get("custom")
    custom: List[str] = [str(x).strip() for x in custom_obj] if isinstance(custom_obj, list) else []

    existing = {x for x in (languages + custom) if x}
    if token_n in existing:
        return False

    # Prefer putting seed codes into languages list, everything else in custom.
    if token_n in _SEED_LABELS:
        languages.append(token_n)
        data["languages"] = languages
    else:
        custom.append(token_n)
        data["custom"] = custom

    ok = _write_raw(data)
    if ok:
        load_language_catalog(force_reload=True)
    return ok


def remember_languages(tokens: Iterable[Optional[str]]) -> int:
    count = 0
    for token in tokens:
        if remember_language(token):
            count += 1
    return count
