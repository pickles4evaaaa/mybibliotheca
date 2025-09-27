"""OPDS mapping helpers.

This module centralizes the mapping whitelist and helpers used by both the
settings UI and the OPDS sync service. Keeping the rules here makes it easy to
share between Flask views and service-layer code as well as the unit tests.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Set

# Whitelist of MyBibliotheca fields that can be mapped from OPDS sources.
MB_FIELD_WHITELIST: List[str] = [
    "title",
    "subtitle",
    "description",
    "language",
    "categories",
    "raw_categories",
    "cover_url",
    "opds_source_id",
    "series",
    "series_order",
    "page_count",
    "publisher",
    "published_date",
    "average_rating",
    "tags",
    "contributors.AUTHORED",
    "contributors.NARRATED",
]

# Human-readable labels for UI usage (optional).
MB_FIELD_LABELS: Dict[str, str] = {
    "contributors.AUTHORED": "Contributors · Authors",
    "contributors.NARRATED": "Contributors · Narrators",
    "opds_source_id": "Stable OPDS Identifier",
    "raw_categories": "Raw Categories (JSON)",
    "series_order": "Series Order (Number)",
    "published_date": "Published Date",
    "subtitle": "Subtitle",
    "average_rating": "Average Rating",
    "tags": "Tags",
}

# Baseline set of source expressions that are commonly available across OPDS 1 feeds.
_DEFAULT_SOURCE_BASE: Set[str] = {
    "entry.title",
    "entry.subtitle",
    "entry.id",
    "entry.summary",
    "entry.content",
    "entry.rating",
    "entry.tags",
    "entry.updated",
    "entry.published",
    "entry.authors",
    "entry.categories",
    "entry.language",
    "entry.dc:language",
    "entry.dc:title",
    "entry.dc:identifier",
    "entry.dc:publisher",
    "entry.dc:issued",
    "entry.dcterms:identifier",
    "entry.dcterms:issued",
    "entry.dcterms:publisher",
    "entry.dcterms:language",
}


def _normalize_inventory_list(values: Optional[Iterable[Any]]) -> List[str]:
    if not values:
        return []
    seen: Set[str] = set()
    normalized: List[str] = []
    for item in values:
        if item is None:
            continue
        value = str(item).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def build_source_options(inventory: Optional[Dict[str, Any]] = None) -> List[str]:
    """Return a sorted list of valid OPDS source expressions.

    Args:
        inventory: Dict describing fields observed during the last probe. Keys we
            pay attention to are ``entry`` (list of element names) and
            ``link_rels`` (list of rel values).
    """
    options: Set[str] = set(_DEFAULT_SOURCE_BASE)
    inv = inventory or {}

    # Entry element names observed in the feed (e.g., title, summary, id, dcterms:issued).
    for field in _normalize_inventory_list(inv.get("entry")):
        options.add(f"entry.{field}")

    # Attribute-driven fields (link rel) to access href targets.
    for rel in _normalize_inventory_list(inv.get("link_rels")):
        options.add(f"link[rel={rel}].href")

    # Some feeds include useful <link type="..."> entries (e.g., MIME-specific enclosures).
    for link_type in _normalize_inventory_list(inv.get("link_types")):
        options.add(f"link[type={link_type}].href")

    # Provide a predictable order for UI drop-downs.
    return sorted(options)


def clean_mapping(
    mapping: Optional[Dict[str, Any]],
    inventory: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """Validate and sanitize a user-provided mapping configuration.

    Unknown MyBibliotheca fields or source expressions that are not present in
    the last probe inventory are discarded.
    """
    if not mapping:
        return {}

    allowed_fields = set(MB_FIELD_WHITELIST)
    valid_sources = set(build_source_options(inventory))

    cleaned: Dict[str, str] = {}
    for raw_key, raw_value in mapping.items():
        key = str(raw_key).strip()
        if key not in allowed_fields:
            continue
        value = str(raw_value).strip()
        if not value or value not in valid_sources:
            continue
        cleaned[key] = value
    return cleaned


__all__ = [
    "MB_FIELD_WHITELIST",
    "MB_FIELD_LABELS",
    "build_source_options",
    "clean_mapping",
]
