"""Centralized cover fetching & processing service (temporary instrumentation)."""
from __future__ import annotations
import os
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import uuid
from flask import current_app, request, has_request_context

from app.utils.book_utils import get_best_cover_for_book, get_cover_candidates
from app.utils.image_processing import process_image_from_url
import requests


@dataclass
class CoverResult:
    source: str
    quality: str
    original_url: Optional[str]
    cached_url: Optional[str]
    elapsed: float
    steps: str

_PROCESSED_CACHE: dict[str, str] = {}
_COVER_JOBS: dict[str, dict[str, Any]] = {}
_EXECUTOR = ThreadPoolExecutor(max_workers=4)

class CoverService:
    """Unified facade for selecting & caching book covers.

    New flow: collect candidates first (no download), choose preferred, then download if not cached.
    """

    def fetch_and_cache(self, isbn: Optional[str] = None, title: Optional[str] = None, author: Optional[str] = None, prefer_provider: Optional[str] = None) -> CoverResult:
        t0 = time.perf_counter()
        steps: list[str] = []
        sel = None
        # Global processing deadline (seconds) after which we short‑circuit and return original URL unprocessed
        try:
            deadline = float(os.getenv('COVER_PROCESS_DEADLINE', '2.5'))
        except Exception:
            deadline = 2.5
        try:
            steps.append('candidates:start')
            # Google-only fast path: if ISBN present, avoid OpenLibrary lookups inside get_cover_candidates
            candidates = get_cover_candidates(isbn=isbn, title=None, author=None) if isbn else get_cover_candidates(isbn=isbn, title=title, author=author)
            steps.append(f"candidates={len(candidates)}")
            chosen = None
            if prefer_provider:
                for c in candidates:
                    if c['provider'] == prefer_provider:
                        chosen = c
                        break
            if not chosen and candidates:
                # Prefer explicit Google size labels (extraLarge > large > medium > small > thumbnail > smallThumbnail)
                size_priority = {
                    'extraLarge': 0,
                    'zoom0': 1,  # often largest delivered asset
                    'large': 2,
                    'zoom1': 3,
                    'medium': 4,
                    'zoom2': 5,
                    'small': 6,
                    'zoom3': 7,
                    'thumbnail': 8,
                    'zoom4': 9,
                    'smallThumbnail': 10,
                    'zoom5': 11,
                }
                def _rank(c):
                    if c['provider'] == 'google':
                        size = c.get('size','')
                        if size in size_priority:
                            return size_priority[size]
                        if size.startswith('zoom'):
                            try:
                                z = int(size.replace('zoom',''))
                            except ValueError:
                                z = 50
                            # zoom variants after explicit sizes
                            return 10 + z
                        # original or unknown google size after zooms
                        return 30
                    # OpenLibrary after all google variants
                    return 100
                chosen = min(candidates, key=_rank)
            cover_url = chosen.get('url') if chosen else None
            if cover_url:
                try:
                    from app.utils.book_utils import normalize_cover_url
                    cover_url = normalize_cover_url(cover_url)
                except Exception:
                    pass
            if not cover_url:
                steps.append('fallback:legacy_selector')
                sel = get_best_cover_for_book(isbn=isbn, title=title, author=author)
                cover_url = sel.get('cover_url')
            # Secondary fallback: if still no cover and we have title/author, attempt title/author driven candidates
            if not cover_url and (title or author):
                try:
                    steps.append('fallback:title_author_candidates')
                    ta_candidates = get_cover_candidates(isbn=None, title=title, author=author)
                    if ta_candidates:
                        cover_url = ta_candidates[0].get('url')
                        if cover_url:
                            from app.utils.book_utils import normalize_cover_url
                            try:
                                cover_url = normalize_cover_url(cover_url)
                            except Exception:
                                pass
                except Exception:
                    steps.append('fallback:title_author_fail')
            cached_url = None
            if cover_url:
                try:
                    if cover_url in _PROCESSED_CACHE:
                        cached_url = _PROCESSED_CACHE[cover_url]
                        steps.append('cache:hit')
                    else:
                        # If we've already burned most of the deadline, pass through original to avoid user-visible lag
                        if time.perf_counter() - t0 > deadline:
                            steps.append('deadline:pass_through')
                            cached_url = cover_url  # Return remote URL directly (UI can still display it)
                        else:
                        # Optional HEAD probe to validate asset size (skip if non-http)
                            if cover_url.startswith('http'):
                                try:
                                    h = requests.head(cover_url, timeout=1.5, allow_redirects=True)
                                    cl = int(h.headers.get('Content-Length','0') or 0)
                                    if cl < 15_000:
                                        steps.append(f'probe:tiny={cl}')
                                    else:
                                        steps.append(f'probe:bytes={cl}')
                                except Exception:
                                    steps.append('probe:fail')
                            steps.append('download:start')
                            rel = process_image_from_url(cover_url)
                            steps.append('download:ok')
                            if rel and rel.startswith('/'):
                                if has_request_context():
                                    try:
                                        cached_url = request.host_url.rstrip('/') + rel
                                    except Exception:
                                        cached_url = rel
                                else:
                                    cached_url = rel
                            else:
                                cached_url = rel
                            if cached_url:
                                _PROCESSED_CACHE[cover_url] = cached_url
                except Exception as e:
                    steps.append(f'download:fail={e.__class__.__name__}')
                    current_app.logger.warning(f"[COVER][SERVICE] Download/process failed url={cover_url} err={e}")
            elapsed = time.perf_counter() - t0
            # Determine source & quality
            source = 'none'
            quality = 'none'
            if cover_url:
                if chosen:
                    source = chosen.get('provider')
                    # Heuristic quality tags
                    quality = 'high' if ('zoom=1' in cover_url or 'extraLarge' in cover_url or 'edge=c' in cover_url) else 'medium'
                elif sel:
                    source = sel.get('source')
                    quality = sel.get('quality')
            result = CoverResult(source=source, quality=quality, original_url=cover_url, cached_url=cached_url, elapsed=elapsed, steps='|'.join(steps))
            log_fn = current_app.logger.info if cached_url else current_app.logger.warning
            log_fn(f"[COVER][SERVICE] isbn={isbn} title='{(title or '')[:40]}' elapsed={elapsed:.3f}s steps={result.steps} cached={bool(cached_url)}")
            return result
        except Exception as e:
            elapsed = time.perf_counter() - t0
            current_app.logger.error(f"[COVER][SERVICE] FATAL isbn={isbn} title='{(title or '')[:40]}' err={e} elapsed={elapsed:.3f}s")
            return CoverResult(source='error', quality='none', original_url=None, cached_url=None, elapsed=elapsed, steps='fatal')

    # --- New async / staged selection API ---
    def select_candidate(self, isbn: Optional[str] = None, title: Optional[str] = None, author: Optional[str] = None, prefer_provider: Optional[str] = None) -> Optional[dict]:
        """Return the chosen candidate dict without downloading (fast path for UI)."""
        candidates = get_cover_candidates(isbn=isbn, title=title, author=author)
        if not candidates:
            return None
        if prefer_provider:
            for c in candidates:
                if c['provider'] == prefer_provider:
                    return c
        # Reuse ranking logic by mimicking portion of fetch
        def _rank(c):
            size_priority = {
                'extraLarge': 0,
                'zoom0': 1,
                'large': 2,
                'zoom1': 3,
                'medium': 4,
                'zoom2': 5,
                'small': 6,
                'zoom3': 7,
                'thumbnail': 8,
                'zoom4': 9,
                'smallThumbnail': 10,
                'zoom5': 11,
            }
            if c['provider'] == 'google':
                s = c.get('size', '')
                if s in size_priority:
                    return size_priority[s]
                if s.startswith('zoom'):
                    try:
                        z = int(s.replace('zoom', ''))
                    except ValueError:
                        z = 99
                    return 20 + z
                return 30
            return 100
        return min(candidates, key=_rank)

    def schedule_async_processing(self, isbn: Optional[str] = None, title: Optional[str] = None, author: Optional[str] = None, prefer_provider: Optional[str] = None) -> dict:
        """Schedule background processing of best cover. Returns job info with immediate candidate URL.

        The UI can display the remote candidate immediately; poll status endpoint to swap to processed local URL.
        """
        cand = self.select_candidate(isbn=isbn, title=title, author=author, prefer_provider=prefer_provider)
        if cand and cand.get('url'):
            try:
                from app.utils.book_utils import normalize_cover_url
                cand['url'] = normalize_cover_url(cand['url'])
            except Exception:
                pass
        job_id = str(uuid.uuid4())
        job_record = {
            'id': job_id,
            'status': 'pending',
            'original_url': cand.get('url') if cand else None,
            'processed_url': None,
            'error': None,
            'isbn': isbn,
            'title': title,
            'provider': cand.get('provider') if cand else None,
            'size': cand.get('size') if cand else None,
            'started': time.time(),
            'completed': None
        }
        _COVER_JOBS[job_id] = job_record

        # Capture app context object so background thread can log safely
        try:
            # current_app is a proxy; capturing it directly is acceptable
            app_obj = current_app
        except Exception:
            app_obj = None

        def _worker(app_obj=app_obj):
            ctx = app_obj.app_context() if app_obj else None
            if ctx:
                ctx.push()
            try:
                try:
                    if not cand or not cand.get('url'):
                        job_record['status'] = 'no_candidate'
                        job_record['completed'] = time.time()
                        return
                    cr = self.fetch_and_cache(isbn=isbn, title=title, author=author, prefer_provider=prefer_provider)
                    if cr and cr.cached_url:
                        job_record['processed_url'] = cr.cached_url
                        job_record['status'] = 'done'
                    else:
                        job_record['status'] = 'failed'
                    job_record['completed'] = time.time()
                except Exception as e:  # pragma: no cover
                    job_record['status'] = 'error'
                    job_record['error'] = str(e)
                    job_record['completed'] = time.time()
                    try:
                        current_app.logger.error(f"[COVER][ASYNC] Job {job_id} failed: {e}")
                    except Exception:
                        pass
            finally:
                if ctx:
                    ctx.pop()
        _EXECUTOR.submit(_worker)
        return job_record

    def get_job(self, job_id: str) -> Optional[dict]:
        return _COVER_JOBS.get(job_id)


cover_service = CoverService()

__all__ = ['cover_service', 'CoverService', 'CoverResult']
