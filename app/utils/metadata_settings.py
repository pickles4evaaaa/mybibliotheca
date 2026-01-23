"""Field-level metadata provider settings management.

Allows admin to configure for each metadata field whether Google, OpenLibrary,
both (with a preferred default), or none should supply data. Applies to books
and people enrichment pipelines.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DEFAULT_BOOK_FIELDS = [
    "title",
    "subtitle",
    "authors",
    "publisher",
    "published_date",
    "page_count",
    "language",
    "description",
    "categories",
    "average_rating",
    "rating_count",
    "cover_url",
    "series",
    "google_books_id",
    "openlibrary_id",
    "isbn10",
    "isbn13",
]
DEFAULT_PERSON_FIELDS = [
    "name",
    "birth_date",
    "death_date",
    "birth_year",
    "death_year",
    "birth_place",
    "bio",
    "website",
    "image_url",
    "openlibrary_id",
]


def _defaults_for(entity: str) -> dict[str, dict[str, str]]:
    fields = DEFAULT_BOOK_FIELDS if entity == "books" else DEFAULT_PERSON_FIELDS
    data: dict[str, dict[str, str]] = {}
    for f in fields:
        data[f] = {"mode": "both", "default": "google"}
    if entity == "people":
        for f in [
            "bio",
            "birth_date",
            "death_date",
            "birth_year",
            "death_year",
            "birth_place",
            "image_url",
            "openlibrary_id",
        ]:
            if f in data:
                data[f]["default"] = "openlibrary"
    return data


# Provider availability map: which providers can supply each field.
# Used by UI to limit choices and by save() to coerce invalid modes.
BOOK_FIELD_PROVIDERS: dict[str, set] = {
    "google_books_id": {"google"},
    "openlibrary_id": {"openlibrary"},
    "average_rating": {"google"},
    "rating_count": {"google"},
    "language": {"google"},
    "cover_url": {"google", "openlibrary"},  # google preferred, OL fallback
    "isbn10": {"google", "openlibrary"},
    "isbn13": {"google", "openlibrary"},
    "title": {"google", "openlibrary"},
    "subtitle": {"google", "openlibrary"},
    "authors": {"google", "openlibrary"},
    "publisher": {"google", "openlibrary"},
    "published_date": {"google", "openlibrary"},
    "page_count": {"google", "openlibrary"},
    "description": {"google", "openlibrary"},
    "categories": {"google", "openlibrary"},
    "series": {"google", "openlibrary"},
}

PERSON_FIELD_PROVIDERS: dict[str, set] = {
    # Currently only OpenLibrary supplies people data
    "name": {"openlibrary"},
    "birth_date": {"openlibrary"},
    "death_date": {"openlibrary"},
    "birth_year": {"openlibrary"},
    "death_year": {"openlibrary"},
    "birth_place": {"openlibrary"},
    "bio": {"openlibrary"},
    "website": {"openlibrary"},
    "image_url": {"openlibrary"},
    "openlibrary_id": {"openlibrary"},
}


class MetadataSettingsCache:
    def __init__(self, data_dir: str):
        self.path = Path(data_dir) / "metadata_settings.json"
        self._cache: dict[str, Any] | None = None

    def load(self) -> dict[str, Any]:
        if self._cache is not None:
            return self._cache
        if self.path.exists():
            try:
                with open(self.path) as f:
                    data = json.load(f)
            except Exception:
                data = {}
        else:
            data = {}
        base_books_raw = data.get("books")
        base_people_raw = data.get("people")
        base_books = base_books_raw if isinstance(base_books_raw, dict) else {}
        base_people = base_people_raw if isinstance(base_people_raw, dict) else {}
        def_books = _defaults_for("books")
        def_people = _defaults_for("people")
        for k, v in def_books.items():
            base_books.setdefault(k, v)
        for k, v in def_people.items():
            base_people.setdefault(k, v)
        self._cache = {"books": base_books, "people": base_people}
        return self._cache

    def save(self, incoming: dict[str, Any]) -> bool:
        try:
            current = self.load()
            for entity in ["books", "people"]:
                if entity not in incoming or not isinstance(incoming[entity], dict):
                    continue
                for field, cfg in incoming[entity].items():
                    if field not in current[entity]:
                        continue
                    if not isinstance(cfg, dict):
                        continue
                    mode = str(cfg.get("mode", "both")).lower()
                    allowed_providers = (
                        BOOK_FIELD_PROVIDERS.get(field, {"google", "openlibrary"})
                        if entity == "books"
                        else PERSON_FIELD_PROVIDERS.get(field, {"openlibrary"})
                    )
                    # Determine allowed modes for this field
                    if allowed_providers == {"google"}:
                        valid_modes = {"google", "none"}
                    elif allowed_providers == {"openlibrary"}:
                        valid_modes = {"openlibrary", "none"}
                    else:
                        valid_modes = {"google", "openlibrary", "both", "none"}
                    if mode not in valid_modes:
                        # Coerce invalid submissions to sensible default
                        mode = (
                            "google"
                            if "google" in allowed_providers
                            else next(iter(allowed_providers))
                        )
                    entry: dict[str, Any] = {"mode": mode}
                    if mode == "both":
                        default = str(cfg.get("default", "google")).lower()
                        if default not in ("google", "openlibrary"):
                            default = "google"
                        entry["default"] = default
                    current[entity][field] = entry
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "w") as f:
                json.dump(current, f, indent=2)
            self._cache = current
            return True
        except Exception:
            return False


_global_cache: MetadataSettingsCache | None = None


def _get_cache() -> MetadataSettingsCache:
    from flask import current_app

    global _global_cache
    if _global_cache is None:
        try:
            data_dir = current_app.config.get("DATA_DIR", "data")
        except Exception:
            data_dir = "data"
        _global_cache = MetadataSettingsCache(data_dir)
    return _global_cache


def get_metadata_settings() -> dict[str, Any]:
    return _get_cache().load()


def save_metadata_settings(data: dict[str, Any]) -> bool:
    return _get_cache().save(data)


def get_field_policy(entity: str, field: str) -> dict[str, str]:
    settings = get_metadata_settings()
    entity_key = "books" if entity == "books" else "people"
    return settings.get(entity_key, {}).get(
        field, {"mode": "both", "default": "google"}
    )


def apply_field_policy(entity: str, field: str, google_val, openlib_val, merged_val):
    pol = get_field_policy(entity, field)
    mode = pol.get("mode", "both")
    if mode == "none":
        return None
    if mode == "google":
        return google_val
    if mode == "openlibrary":
        return openlib_val
    default = pol.get("default", "google")
    if google_val is not None and openlib_val is not None:
        return google_val if default == "google" else openlib_val
    return merged_val
