from __future__ import annotations

from typing import Any, Optional, Tuple


_AUTHOR_CONTRIBUTION_TYPES = {
    "authored",
    "co_authored",
}


def _normalize_first_last(name: str) -> str:
    """Return a normalized 'first last' string suitable for sorting.

    Prefers behavior aligned with Person._normalize_name:
    - "Last, First" -> "First Last"
    - trims whitespace

    This does not attempt language-specific surname particles; it follows existing
    project expectations (simple, predictable behavior).
    """
    raw = (name or "").strip()
    if not raw:
        return ""

    if "," in raw:
        last, rest = [part.strip() for part in raw.split(",", 1)]
        if last and rest:
            raw = f"{rest} {last}".strip()

    # Collapse whitespace
    raw = " ".join(raw.split())
    return raw


def _split_last_first(first_last: str) -> Tuple[str, str]:
    """Split a normalized 'first last' string into (last, first).

    Uses the last whitespace-delimited token as the surname.
    """
    s = (first_last or "").strip()
    if not s:
        return ("", "")

    parts = s.split()
    if len(parts) == 1:
        return (parts[0], "")

    last = parts[-1]
    first = " ".join(parts[:-1])
    return (last, first)


def _safe_title_key(book: Any) -> str:
    title = ""
    if isinstance(book, dict):
        title = book.get("title", "") or ""
    else:
        title = getattr(book, "title", "") or ""
    return str(title).casefold()


def _iter_contributors(book: Any):
    if isinstance(book, dict):
        contributors = book.get("contributors")
    else:
        contributors = getattr(book, "contributors", None)

    if not contributors:
        return []

    if isinstance(contributors, list):
        return contributors

    # Defensive: allow single contributor object
    return [contributors]


def _contribution_type_value(contributor: Any) -> str:
    if isinstance(contributor, dict):
        ct = contributor.get("contribution_type")
    else:
        ct = getattr(contributor, "contribution_type", None)

    # Enum or object with .value
    if hasattr(ct, "value"):
        ct = getattr(ct, "value")

    return str(ct or "").strip().lower()


def _person_from_contributor(contributor: Any) -> Any:
    if isinstance(contributor, dict):
        return contributor.get("person")
    return getattr(contributor, "person", None)


def _person_name(person: Any) -> str:
    if not person:
        return ""
    if isinstance(person, dict):
        return str(person.get("name") or "")
    return str(getattr(person, "name", "") or "")


def _person_normalized_name(person: Any) -> str:
    if not person:
        return ""
    if isinstance(person, dict):
        return str(person.get("normalized_name") or "")
    return str(getattr(person, "normalized_name", "") or "")


def _primary_author_person(book: Any) -> Optional[Any]:
    """Best-effort extraction of the primary author Person from a book.

    Prefers Person table data via contributors.
    """
    for contributor in _iter_contributors(book):
        ct = _contribution_type_value(contributor)
        if ct in _AUTHOR_CONTRIBUTION_TYPES:
            person = _person_from_contributor(contributor)
            if person:
                return person
    return None


def author_first_sort_key_for_book(book: Any) -> Tuple[str, str]:
    """Sort key for 'Author First, Last' ordering.

    Uses Person.normalized_name when available to normalize "Last, First" to
    "First Last".

    Returns (author_key, title_key) for stable deterministic ordering.
    """
    person = _primary_author_person(book)
    if person:
        # Prefer the Person table's canonical name and normalize it consistently.
        # Do not trust stored normalized_name for ordering; legacy rows may have
        # been normalized as "last first".
        name = _person_name(person)
        if name.strip():
            return (_normalize_first_last(name).casefold(), _safe_title_key(book))

        normalized = _person_normalized_name(person)
        if normalized.strip():
            return (_normalize_first_last(normalized).casefold(), _safe_title_key(book))

    # Fallback to legacy fields
    if isinstance(book, dict):
        raw = book.get("author") or ""
        if not raw and isinstance(book.get("authors"), list) and book["authors"]:
            first = book["authors"][0]
            if isinstance(first, dict):
                raw = first.get("name") or ""
            else:
                raw = getattr(first, "name", "") or str(first)
    else:
        raw = getattr(book, "author", "") or ""
    return (_normalize_first_last(str(raw)).casefold(), _safe_title_key(book))


def author_last_sort_key_for_book(book: Any) -> Tuple[str, str, str]:
    """Sort key for 'Author Last, First' ordering.

    Returns (last, first, title) where last/first are derived from a normalized
    "first last" representation.
    """
    person = _primary_author_person(book)
    if person:
        # Prefer Person.name; derive consistent first/last ordering from it.
        name = _person_name(person)
        if name.strip():
            first_last = _normalize_first_last(name)
        else:
            normalized = _person_normalized_name(person)
            first_last = _normalize_first_last(normalized)
    else:
        if isinstance(book, dict):
            raw = book.get("author") or ""
            if not raw and isinstance(book.get("authors"), list) and book["authors"]:
                first = book["authors"][0]
                if isinstance(first, dict):
                    raw = first.get("name") or ""
                else:
                    raw = getattr(first, "name", "") or str(first)
        else:
            raw = getattr(book, "author", "") or ""
        first_last = _normalize_first_last(str(raw))

    last, first = _split_last_first(first_last)
    return (last.casefold(), first.casefold(), _safe_title_key(book))
