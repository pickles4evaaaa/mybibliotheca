"""Helpers that keep secrets out of logs.

``redact_sensitive`` masks values of mapping keys that look sensitive;
``RedactingFilter`` is a defence-in-depth logging filter that scrubs
``'password': 'value'`` style fragments from already-formatted messages.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from typing import Any

REDACTED = '***REDACTED***'

SENSITIVE_KEY_RE = re.compile(r'password|passwd|secret|token|hash|salt|key|csrf', re.IGNORECASE)


def is_sensitive_key(key: Any) -> bool:
    return bool(SENSITIVE_KEY_RE.search(str(key)))


def redact_sensitive(obj: Any) -> Any:
    """Return a copy of ``obj`` with sensitive values masked (recursively).

    Handles dicts, Flask sessions, werkzeug MultiDicts, lists/tuples/sets.
    Never mutates the input.
    """
    if isinstance(obj, Mapping) or hasattr(obj, 'items') and hasattr(obj, 'keys'):
        try:
            items = list(obj.items())  # type: ignore[union-attr]
        except Exception:
            return REDACTED
        return {k: (REDACTED if is_sensitive_key(k) and v not in (None, '') else redact_sensitive(v)) for k, v in items}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return type(obj)(redact_sensitive(v) for v in obj) if not isinstance(obj, (set, frozenset)) else [redact_sensitive(v) for v in obj]
    return obj


_FRAGMENT_RE = re.compile(
    r"""(?P<k>['"]?[\w\-]*(?:password|passwd|secret|token|hash|salt|key|csrf)[\w\-]*['"]?\s*[:=]\s*)"""
    r"""(?P<v>'[^']*'|"[^"]*"|[^\s,}\]\)]+)""",
    re.IGNORECASE,
)


def scrub_text(text: str) -> str:
    """Mask ``key: value`` / ``key=value`` fragments with sensitive keys in a string."""
    return _FRAGMENT_RE.sub(lambda m: m.group('k') + "'" + REDACTED + "'", text)


class RedactingFilter(logging.Filter):
    """Scrub sensitive key/value fragments from every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            return True
        scrubbed = scrub_text(message)
        if scrubbed != message:
            record.msg = scrubbed
            record.args = ()
        return True


_installed = False


def install_redacting_filter() -> None:
    """Scrub every log record process-wide via the record factory (idempotent)."""
    global _installed
    if _installed:
        return
    _installed = True
    previous = logging.getLogRecordFactory()
    scrubber = RedactingFilter()

    def factory(*args, **kwargs):
        record = previous(*args, **kwargs)
        scrubber.filter(record)
        return record

    logging.setLogRecordFactory(factory)
