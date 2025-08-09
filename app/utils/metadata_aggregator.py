"""
Metadata Aggregator facade.

This module provides a stable import path for unified book metadata
aggregation. It delegates to app.utils.unified_metadata which merges
Google Books and OpenLibrary data and normalizes dates.

Use cases:
- fetch_unified_by_isbn(isbn): merged metadata for a single ISBN
- fetch_unified_by_title(title, max_results=10, author=None): ranked search results, author-aware

Note: Prefer importing from this module in new code to make future
internal changes transparent to callers.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

# Re-export from the canonical implementation
from .unified_metadata import (
	fetch_unified_by_isbn,
	fetch_unified_by_title,
)

__all__ = [
	'fetch_unified_by_isbn',
	'fetch_unified_by_title',
]

