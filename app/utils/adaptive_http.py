"""Adaptive HTTP helpers (rate limiting + backoff).

This module provides a lightweight, threadsafe, per-provider adaptive rate limiter.
It is intentionally Flask-agnostic so it can be used from background import threads.

Behavior:
- On HTTP 429 (or Google Books 403 quota-style responses), it backs off quickly.
- After a cooldown, it gradually speeds back up on successful responses.

Tuning via env vars:
- HTTP_MIN_DELAY_SECONDS (default 0)
- HTTP_MAX_DELAY_SECONDS (default 10)
- HTTP_BACKOFF_MULTIPLIER (default 2.0)
- HTTP_RECOVERY_MULTIPLIER (default 0.9)
- HTTP_MAX_RETRIES (default 3)
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
import random
import threading
import time
from typing import Dict, Optional, Mapping

import requests

logger = logging.getLogger(__name__)


def _read_float_env(name: str, default: float) -> float:
    try:
        raw = os.getenv(name)
        if raw in (None, ''):
            return default
        return float(raw)
    except Exception:
        return default


def _read_int_env(name: str, default: int) -> int:
    try:
        raw = os.getenv(name)
        if raw in (None, ''):
            return default
        return int(raw)
    except Exception:
        return default


def _parse_retry_after_seconds(headers: Mapping[str, str]) -> Optional[float]:
    try:
        ra = headers.get('Retry-After')
        if not ra:
            return None
        # Retry-After can be seconds or HTTP date; handle seconds only.
        return float(ra)
    except Exception:
        return None


@dataclass
class _LimiterState:
    delay: float
    next_allowed: float
    success_streak: int


class AdaptiveRateLimiter:
    def __init__(self, key: str):
        self.key = key
        self._lock = threading.RLock()
        self._min_delay = max(0.0, _read_float_env('HTTP_MIN_DELAY_SECONDS', 0.0))
        self._max_delay = max(self._min_delay, _read_float_env('HTTP_MAX_DELAY_SECONDS', 10.0))
        self._backoff_mult = max(1.1, _read_float_env('HTTP_BACKOFF_MULTIPLIER', 2.0))
        self._recovery_mult = min(0.999, max(0.5, _read_float_env('HTTP_RECOVERY_MULTIPLIER', 0.9)))

        # Optional jitter to spread bursts across threads.
        self._jitter_min = max(0.0, _read_float_env('HTTP_JITTER_MIN_SECONDS', 0.0))
        self._jitter_max = max(self._jitter_min, _read_float_env('HTTP_JITTER_MAX_SECONDS', 0.25))
        now = time.monotonic()
        self._state = _LimiterState(delay=self._min_delay, next_allowed=now, success_streak=0)

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait_for = max(0.0, self._state.next_allowed - now)
            if wait_for > 0:
                time.sleep(wait_for)

            # Add a small jitter even when not rate-limited to reduce stampedes.
            if self._jitter_max > 0 or self._jitter_min > 0:
                jitter = (
                    random.uniform(self._jitter_min, self._jitter_max)
                    if self._jitter_max > self._jitter_min
                    else self._jitter_min
                )
                if jitter > 0:
                    time.sleep(jitter)
            # Reserve the next slot.
            now2 = time.monotonic()
            self._state.next_allowed = now2 + self._state.delay

    def on_success(self) -> None:
        with self._lock:
            self._state.success_streak += 1
            # Recover slowly (only after a few successes to avoid flapping).
            if self._state.success_streak >= 3 and self._state.delay > self._min_delay:
                self._state.delay = max(self._min_delay, self._state.delay * self._recovery_mult)
                self._state.success_streak = 0

    def on_rate_limited(self, retry_after_seconds: Optional[float] = None) -> None:
        with self._lock:
            self._state.success_streak = 0
            new_delay = self._state.delay * self._backoff_mult
            # If we had no delay yet, start with a small bump.
            if new_delay <= 0:
                new_delay = 0.5
            if retry_after_seconds is not None:
                new_delay = max(new_delay, float(retry_after_seconds))
            # Add a little jitter so parallel threads don't stampede.
            jitter = random.uniform(0.0, min(0.25, new_delay * 0.1))
            new_delay = min(self._max_delay, new_delay + jitter)
            self._state.delay = new_delay
            self._state.next_allowed = time.monotonic() + new_delay

    def on_transient_error(self) -> None:
        # For 5xx/timeouts: modest backoff to reduce pressure.
        with self._lock:
            self._state.success_streak = 0
            bumped = self._state.delay
            if bumped <= 0:
                bumped = 0.2
            else:
                bumped = bumped * 1.25
            self._state.delay = min(self._max_delay, max(self._min_delay, bumped))


_limiters: Dict[str, AdaptiveRateLimiter] = {}
_limiters_lock = threading.RLock()


def get_limiter(key: str) -> AdaptiveRateLimiter:
    with _limiters_lock:
        limiter = _limiters.get(key)
        if limiter is None:
            limiter = AdaptiveRateLimiter(key)
            _limiters[key] = limiter
        return limiter


def _looks_like_google_quota(resp: requests.Response) -> bool:
    if resp.status_code != 403:
        return False
    try:
        text = (resp.text or '')[:4000]
        # Common Google Books quota strings.
        return (
            'rateLimitExceeded' in text
            or 'userRateLimitExceeded' in text
            or 'quotaExceeded' in text
            or 'Daily Limit Exceeded' in text
            or 'Quota exceeded' in text
        )
    except Exception:
        return False


def adaptive_get(
    limiter_key: str,
    url: str,
    *,
    timeout: float | tuple[float, float] | None = None,
    max_retries: Optional[int] = None,
    **kwargs,
) -> requests.Response:
    """requests.get with adaptive pacing and retry-on-rate-limit.

    Returns the final response (even if unsuccessful) after retries.
    """
    limiter = get_limiter(limiter_key)
    retries = max_retries if max_retries is not None else _read_int_env('HTTP_MAX_RETRIES', 3)
    retries = max(1, int(retries))

    last_resp: Optional[requests.Response] = None
    for attempt in range(retries):
        limiter.wait()
        try:
            resp = requests.get(url, timeout=timeout, **kwargs)
            last_resp = resp

            # Rate limit / quota signals.
            if resp.status_code == 429 or _looks_like_google_quota(resp):
                ra = _parse_retry_after_seconds(resp.headers) or None
                limiter.on_rate_limited(ra)
                logger.warning(f"[HTTP][RATE_LIMIT] key={limiter_key} url={url} status={resp.status_code} retry_after={ra}")
                if attempt < retries - 1:
                    continue
                return resp

            # Transient server errors.
            if 500 <= resp.status_code <= 599:
                limiter.on_transient_error()
                if attempt < retries - 1:
                    continue
                return resp

            limiter.on_success()
            return resp

        except requests.exceptions.RequestException as exc:
            limiter.on_transient_error()
            logger.warning(f"[HTTP][ERROR] key={limiter_key} url={url} err={exc}")
            if attempt < retries - 1:
                continue
            raise

    # Should not reach, but return last response if it exists.
    if last_resp is not None:
        return last_resp
    raise RuntimeError('adaptive_get failed without producing a response')
