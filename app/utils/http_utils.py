"""Generic helpers for outbound HTTP that do not depend on any one provider."""

from __future__ import annotations

import re

__all__ = ['redact_url']

_SECRET_PARAM_RE = re.compile(
    r"([?&](?:key|api_key|apikey|access_token|token)=)[^&#\s'\")\]]*",
    re.IGNORECASE,
)


def redact_url(url: str) -> str:
    """Mask credential query params in a URL (or any text containing one)."""
    try:
        return _SECRET_PARAM_RE.sub(r'\1***', str(url))
    except Exception:
        return str(url)