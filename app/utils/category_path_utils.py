from __future__ import annotations

import re
from typing import Iterable, List, Optional


def _normalize_category_segment(value: str) -> str:
    return (value or "").strip().lower()


def _split_category_path(value: str) -> List[str]:
    if not isinstance(value, str):
        return []
    return [part.strip() for part in re.split(r"[>/]", value) if part.strip()]


def filter_raw_category_paths_by_visible_segments(
    raw_category_paths: Optional[Iterable[str]],
    visible_category_segments: Iterable[str],
) -> List[str]:
    """Filter hierarchical raw category paths based on visible UI segment chips.

    The Add Book UI renders hierarchical paths like "Fiction > Mystery" as separate
    chips ("Fiction", "Mystery"). If a user removes a chip, any raw path requiring
    that segment should be dropped.

    This is a defensive server-side guard in case client-side state becomes stale.
    """

    if not raw_category_paths:
        return []

    visible = {_normalize_category_segment(v) for v in visible_category_segments if _normalize_category_segment(v)}
    if not visible:
        return []

    filtered: List[str] = []
    for raw in raw_category_paths:
        raw_str = (raw or "").strip()
        if not raw_str:
            continue
        parts = _split_category_path(raw_str)
        if not parts:
            continue
        if all(_normalize_category_segment(part) in visible for part in parts):
            filtered.append(raw_str)

    return filtered
