"""Safe, best-effort repairs for books imported without bibliographic metadata.

The repair operations in this module deliberately only fill missing fields.  A
bad automatic ISBN is harder to notice than a missing one, so matching has a
fairly high threshold and also requires a useful title match (and an author
match when an author is available).
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from difflib import SequenceMatcher
import json
import logging
import re
import threading
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.infrastructure.kuzu_graph import safe_execute_kuzu_query
from app.services.kuzu_service_facade import _convert_query_result_to_list
from app.utils.book_search import search_openlibrary
from app.utils.adaptive_http import adaptive_get
from app.services.cover_service import cover_service


logger = logging.getLogger(__name__)

_OPENLIBRARY_EDITION_CACHE: Dict[str, List[Dict[str, Any]]] = {}
_OPENLIBRARY_EDITION_CACHE_LOCK = threading.RLock()


def _bulk_title_search(title: str, max_results: int = 8, author: Optional[str] = None) -> List[Dict[str, Any]]:
    """Find OpenLibrary work candidates for a repair candidate.

    Public Google Books queries can respond with HTTP 429 during a bulk run.
    OpenLibrary's work search is stable, but its search response often omits
    ISBNs; ``_openlibrary_edition_candidates`` follows a high-confidence work
    match to its edition records, where identifiers are present.
    """
    try:
        return search_openlibrary(title, max_results * 2, author) or []
    except Exception:
        return []


RepairProgressCallback = Callable[[int, int, int, int, int, str], None]


@dataclass
class RepairSummary:
    """Outcome of one bulk repair operation."""

    scanned: int = 0
    updated: int = 0
    skipped: int = 0
    failed: int = 0
    details: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def processed(self) -> int:
        return self.updated + self.skipped + self.failed


def _value(row: Dict[str, Any], index: int, name: str) -> Any:
    """Read both the application's col_N query format and named rows."""
    if name in row:
        return row.get(name)
    return row.get(f"col_{index}")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _normalized_isbn(value: Any) -> str:
    return re.sub(r"[^0-9Xx]", "", _text(value)).upper()


def _isbn13_to_10(isbn13: str) -> Optional[str]:
    isbn13 = _normalized_isbn(isbn13)
    if len(isbn13) != 13 or not isbn13.startswith("978") or not isbn13[:12].isdigit():
        return None
    core = isbn13[3:12]
    total = sum((10 - i) * int(ch) for i, ch in enumerate(core))
    check = (11 - (total % 11)) % 11
    return core + ("X" if check == 10 else str(check))


def _isbn10_to_13(isbn10: str) -> Optional[str]:
    isbn10 = _normalized_isbn(isbn10)
    if len(isbn10) != 10 or not re.fullmatch(r"[0-9]{9}[0-9X]", isbn10):
        return None
    core = "978" + isbn10[:9]
    total = sum((1 if i % 2 == 0 else 3) * int(ch) for i, ch in enumerate(core))
    return core + str((10 - (total % 10)) % 10)


def _valid_isbn(value: Any) -> bool:
    isbn = _normalized_isbn(value)
    if len(isbn) == 10 and re.fullmatch(r"[0-9]{9}[0-9X]", isbn):
        return sum((10 - i) * (10 if ch == "X" else int(ch)) for i, ch in enumerate(isbn)) % 11 == 0
    if len(isbn) == 13 and isbn.isdigit():
        return sum((1 if i % 2 == 0 else 3) * int(ch) for i, ch in enumerate(isbn)) % 10 == 0
    return False


def _normalize_title(value: Any) -> str:
    value = _text(value).lower()
    value = re.sub(r"\([^)]*\)|\[[^]]*\]", " ", value)
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    words = value.split()
    if words and words[0] in {"a", "an", "the"}:
        words = words[1:]
    return " ".join(words)


def _title_score(left: Any, right: Any) -> float:
    a, b = _normalize_title(left), _normalize_title(right)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    sequence = SequenceMatcher(None, a, b).ratio()
    a_words, b_words = set(a.split()), set(b.split())
    overlap = len(a_words & b_words) / max(len(a_words | b_words), 1)
    return max(sequence, overlap)


def _normalize_person(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", _text(value).lower())


def _author_score(book_author: str, candidate: Dict[str, Any]) -> float:
    wanted = _normalize_person(book_author)
    if not wanted:
        return 0.0
    authors = candidate.get("authors") or []
    if isinstance(authors, str):
        authors = [authors]
    normalized = [_normalize_person(author) for author in authors if author]
    if not normalized and candidate.get("author"):
        normalized = [_normalize_person(candidate.get("author"))]
    if wanted in normalized:
        return 1.0
    if any(wanted in name or name in wanted for name in normalized if name):
        return 0.82
    wanted_parts = _text(book_author).lower().split()
    wanted_last = _normalize_person(wanted_parts[-1]) if wanted_parts else ""
    if wanted_last and any(wanted_last == name[-len(wanted_last):] for name in normalized if name):
        return 0.65
    return 0.0


def _candidate_isbns(candidate: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    isbn10 = _normalized_isbn(candidate.get("isbn_10") or candidate.get("isbn10"))
    isbn13 = _normalized_isbn(candidate.get("isbn_13") or candidate.get("isbn13"))
    if isbn10 and not _valid_isbn(isbn10):
        isbn10 = ""
    if isbn13 and not _valid_isbn(isbn13):
        isbn13 = ""
    if isbn13:
        # Do not persist two identifiers that describe different editions.
        derived_isbn10 = _isbn13_to_10(isbn13)
        if derived_isbn10:
            isbn10 = derived_isbn10
    if isbn10 and not isbn13:
        isbn13 = _isbn10_to_13(isbn10) or ""
    return isbn10 or None, isbn13 or None


def _book_author_from_row(row: Dict[str, Any]) -> str:
    authors = row.get("authors")
    if isinstance(authors, list):
        return ", ".join(_text(author) for author in authors if _text(author))
    return _text(authors)


def _year(value: Any) -> Optional[str]:
    match = re.search(r"\b(1[5-9]\d{2}|20\d{2})\b", _text(value))
    return match.group(1) if match else None


def _first_valid_isbn(value: Any) -> Optional[str]:
    values = value if isinstance(value, list) else [value]
    for item in values:
        isbn = _normalized_isbn(item)
        if _valid_isbn(isbn):
            return isbn
    return None


class BookRepairService:
    """Run conservative metadata repairs against the shared Book nodes."""

    def __init__(self, *, max_workers: int = 6, search: Callable[..., List[Dict[str, Any]]] = _bulk_title_search):
        self.max_workers = max(1, int(max_workers))
        self.search = search

    def _query_books(self, kind: str) -> List[Dict[str, Any]]:
        if kind == "isbn":
            missing = "(b.isbn13 IS NULL OR b.isbn13 = '') AND (b.isbn10 IS NULL OR b.isbn10 = '')"
            operation = "repairs_find_missing_isbn"
        elif kind == "cover":
            missing = "b.cover_url IS NULL OR b.cover_url = ''"
            operation = "repairs_find_missing_covers"
        else:
            raise ValueError(f"Unknown repair kind: {kind}")

        query = f"""
        MATCH (b:Book)
        WHERE {missing}
        OPTIONAL MATCH (p:Person)-[:AUTHORED]->(b)
        RETURN b.id, b.title, b.isbn13, b.isbn10, b.cover_url,
               b.published_date, p.name
        ORDER BY b.title
        """
        rows = _convert_query_result_to_list(
            safe_execute_kuzu_query(query, {}, operation=operation)
        )
        books: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            book_id = _text(_value(row, 0, "id"))
            if not book_id:
                continue
            book = books.setdefault(book_id, {
                "id": book_id,
                "title": _value(row, 1, "title"),
                "isbn13": _value(row, 2, "isbn13"),
                "isbn10": _value(row, 3, "isbn10"),
                "cover_url": _value(row, 4, "cover_url"),
                "published_date": _value(row, 5, "published_date"),
                "authors": [],
            })
            author = _text(_value(row, 6, "name"))
            if author and author not in book["authors"]:
                book["authors"].append(author)
        return list(books.values())

    def _update_book(self, book_id: str, updates: Dict[str, Any]) -> bool:
        allowed = {"isbn10", "isbn13", "cover_url", "updated_at"}
        updates = {key: value for key, value in updates.items() if key in allowed and value not in (None, "")}
        if not updates:
            return False
        updates["updated_at"] = datetime.now(timezone.utc)
        set_clause = ", ".join(f"b.{key} = ${key}" for key in updates)
        result = safe_execute_kuzu_query(
            f"MATCH (b:Book {{id: $book_id}}) SET {set_clause} RETURN b.id",
            {"book_id": book_id, **updates},
            operation="repairs_update_book_metadata",
        )
        return bool(_convert_query_result_to_list(result))

    def _invalidate_user_library_caches(self, book_id: str) -> None:
        """Make global metadata changes visible in cached user library views."""
        try:
            from app.utils.simple_cache import bump_user_library_version

            result = safe_execute_kuzu_query(
                """
                MATCH (u:User)-[:HAS_PERSONAL_METADATA]->(b:Book {id: $book_id})
                RETURN DISTINCT u.id
                """,
                {"book_id": book_id},
                operation="repairs_invalidate_book_caches",
            )
            for row in _convert_query_result_to_list(result):
                user_id = _text(_value(row, 0, "user_id"))
                if user_id:
                    bump_user_library_version(user_id)
        except Exception:
            return

    def _clear_needs_review(self, book_id: str) -> None:
        """Clear the import flag for users whose personal metadata has it.

        ``needs_review`` is intentionally user-specific, while ISBN is shared
        book metadata.  Once a book receives a high-confidence ISBN, clearing
        the flag for every user attached to that book keeps the library view
        consistent without touching any other personal fields.
        """
        try:
            from app.services.personal_metadata_service import personal_metadata_service

            result = safe_execute_kuzu_query(
                """
                MATCH (u:User)-[r:HAS_PERSONAL_METADATA]->(b:Book {id: $book_id})
                WHERE r.personal_custom_fields IS NOT NULL
                  AND r.personal_custom_fields <> ''
                RETURN u.id, r.personal_custom_fields
                """,
                {"book_id": book_id},
                operation="repairs_find_needs_review_flags",
            )
            for row in _convert_query_result_to_list(result):
                user_id = _text(_value(row, 0, "user_id"))
                raw_metadata = _value(row, 1, "personal_custom_fields")
                if not user_id or not raw_metadata:
                    continue
                try:
                    metadata = json.loads(raw_metadata) if isinstance(raw_metadata, str) else raw_metadata
                except Exception:
                    metadata = {}
                if isinstance(metadata, dict) and metadata.get("needs_review"):
                    personal_metadata_service.update_personal_metadata(
                        user_id, book_id, custom_updates={"needs_review": None}, merge=True
                    )
        except Exception:
            # ISBN repair must not be reported as failed because an optional
            # per-user flag could not be cleared.
            return

    def _openlibrary_edition_candidates(
        self, book: Dict[str, Any], work_candidates: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Resolve exact OpenLibrary work matches to one best ISBN edition.

        A work can have many editions.  We only use this fallback when the
        imported title is an exact/near-exact work match and the author agrees
        (when present).  If a publication year was imported, it is used to
        prefer an edition from that year.
        """
        title = _text(book.get("title"))
        author = _book_author_from_row(book)
        imported_year = _year(book.get("published_date"))
        resolved: List[Dict[str, Any]] = []
        seen_works: set[str] = set()

        for work in work_candidates:
            work_id = _text(work.get("openlibrary_id"))
            if not work_id or work_id in seen_works:
                continue
            work_title_score = _title_score(title, work.get("title"))
            work_author_score = _author_score(author, work)
            if author:
                if work_title_score < 0.90 or work_author_score < 0.65:
                    continue
            elif work_title_score < 0.96:
                continue
            seen_works.add(work_id)

            with _OPENLIBRARY_EDITION_CACHE_LOCK:
                editions = _OPENLIBRARY_EDITION_CACHE.get(work_id)
            if editions is None:
                try:
                    response = adaptive_get(
                        "openlibrary",
                        f"https://openlibrary.org/works/{work_id}/editions.json?limit=25",
                        timeout=(2.5, 5.0),
                        max_retries=1,
                    )
                    response.raise_for_status()
                    payload = response.json() or {}
                    editions = payload.get("entries") or []
                    if not isinstance(editions, list):
                        editions = []
                except Exception:
                    editions = []
                with _OPENLIBRARY_EDITION_CACHE_LOCK:
                    _OPENLIBRARY_EDITION_CACHE[work_id] = editions

            ranked_editions: List[Tuple[float, Dict[str, Any]]] = []
            for edition in editions:
                if not isinstance(edition, dict):
                    continue
                isbn10 = _first_valid_isbn(edition.get("isbn_10"))
                isbn13 = _first_valid_isbn(edition.get("isbn_13"))
                if not isbn10 and not isbn13:
                    continue
                edition_title_score = _title_score(title, edition.get("title"))
                if edition_title_score < 0.96:
                    continue
                score = edition_title_score * 0.75 + work_author_score * 0.25 if author else edition_title_score
                edition_year = _year(edition.get("publish_date"))
                if imported_year and edition_year:
                    score += 0.08 if imported_year == edition_year else -0.08
                ranked_editions.append((score, {
                    **work,
                    "title": edition.get("title") or work.get("title"),
                    "isbn_10": isbn10,
                    "isbn_13": isbn13,
                    "published_date": edition.get("publish_date"),
                    "edition_match": True,
                }))
            if ranked_editions:
                ranked_editions.sort(key=lambda item: item[0], reverse=True)
                resolved.append(ranked_editions[0][1])
        return resolved

    def _best_isbn_match(self, book: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], float]:
        title = _text(book.get("title"))
        author = _book_author_from_row(book)
        if not title:
            return None, 0.0
        candidates = self.search(title, max_results=8, author=author or None) or []
        if not any(_candidate_isbns(candidate) != (None, None) for candidate in candidates):
            candidates.extend(self._openlibrary_edition_candidates(book, candidates))
        ranked: List[Tuple[float, Dict[str, Any], float, float]] = []
        for candidate in candidates:
            isbn10, isbn13 = _candidate_isbns(candidate)
            if not isbn10 and not isbn13:
                continue
            title_similarity = _title_score(title, candidate.get("title"))
            author_similarity = _author_score(author, candidate)
            if author:
                score = title_similarity * 0.72 + author_similarity * 0.28
                eligible = title_similarity >= 0.72 and author_similarity >= 0.65 and score >= 0.78
            else:
                score = title_similarity
                eligible = title_similarity >= 0.90
            if eligible:
                ranked.append((score, candidate, title_similarity, author_similarity))
        ranked.sort(key=lambda item: item[0], reverse=True)
        if not ranked:
            return None, 0.0
        # Require a small lead when two different editions/titles are similarly ranked.
        if len(ranked) > 1 and ranked[0][0] - ranked[1][0] < 0.025:
            first_isbn = _candidate_isbns(ranked[0][1])
            second_isbn = _candidate_isbns(ranked[1][1])
            if first_isbn != second_isbn and not ranked[0][1].get("edition_match"):
                return None, ranked[0][0]
        return ranked[0][1], ranked[0][0]

    def assign_missing_isbns(self, progress_callback: Optional[RepairProgressCallback] = None) -> RepairSummary:
        books = self._query_books("isbn")
        summary = RepairSummary(scanned=len(books))
        logger.info("[BOOK_REPAIR][ISBN][START] scanned=%d", summary.scanned)
        if progress_callback:
            progress_callback(0, summary.scanned, 0, 0, 0, "Searching for ISBN matches")

        def match(book: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], float, Optional[Exception]]:
            try:
                candidate, score = self._best_isbn_match(book)
                return book, candidate, score, None
            except Exception as exc:  # one bad API response must not stop the batch
                return book, None, 0.0, exc

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="isbn-repair") as executor:
            futures = [executor.submit(match, book) for book in books]
            for future in as_completed(futures):
                book, candidate, score, error = future.result()
                if error:
                    summary.failed += 1
                    summary.details.append({"title": _text(book.get("title")), "status": "failed"})
                    if progress_callback:
                        progress_callback(summary.processed, summary.scanned, summary.updated, summary.skipped, summary.failed, "Searching for ISBN matches")
                    continue
                if not candidate:
                    summary.skipped += 1
                    summary.details.append({"title": _text(book.get("title")), "status": "no_match"})
                    if progress_callback:
                        progress_callback(summary.processed, summary.scanned, summary.updated, summary.skipped, summary.failed, "Searching for ISBN matches")
                    continue
                isbn10, isbn13 = _candidate_isbns(candidate)
                try:
                    if not self._update_book(book["id"], {"isbn10": isbn10, "isbn13": isbn13}):
                        raise RuntimeError("database update returned no rows")
                    self._clear_needs_review(book["id"])
                    self._invalidate_user_library_caches(book["id"])
                    summary.updated += 1
                    summary.details.append({
                        "title": _text(book.get("title")),
                        "status": "updated",
                        "isbn": isbn13 or isbn10,
                        "score": round(score, 3),
                    })
                except Exception as exc:
                    summary.failed += 1
                    summary.details.append({"title": _text(book.get("title")), "status": "failed"})
                    logger.error(
                        "[BOOK_REPAIR][ISBN_UPDATE_FAILED] book_id=%s title=%r error=%s",
                        book.get("id"), _text(book.get("title")), exc,
                    )
                if progress_callback:
                    progress_callback(summary.processed, summary.scanned, summary.updated, summary.skipped, summary.failed, "Assigning ISBN matches")
        logger.info(
            "[BOOK_REPAIR][ISBN][DONE] scanned=%d updated=%d skipped=%d failed=%d",
            summary.scanned, summary.updated, summary.skipped, summary.failed,
        )
        return summary

    def fetch_missing_covers(self, progress_callback: Optional[RepairProgressCallback] = None) -> RepairSummary:
        books = self._query_books("cover")
        summary = RepairSummary(scanned=len(books))
        logger.info("[BOOK_REPAIR][COVERS][START] scanned=%d", summary.scanned)
        if progress_callback:
            progress_callback(0, summary.scanned, 0, 0, 0, "Fetching book covers")

        def fetch(book: Dict[str, Any]) -> Tuple[Dict[str, Any], Any, Optional[Exception]]:
            try:
                author = _book_author_from_row(book)
                isbn = _text(book.get("isbn13")) or _text(book.get("isbn10")) or None
                result = cover_service.fetch_and_cache(
                    isbn=isbn,
                    title=_text(book.get("title")) or None,
                    author=author or None,
                )
                return book, result, None
            except Exception as exc:
                return book, None, exc

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="cover-repair") as executor:
            futures = [executor.submit(fetch, book) for book in books]
            for future in as_completed(futures):
                book, result, error = future.result()
                if error or not result:
                    summary.failed += 1
                    summary.details.append({"title": _text(book.get("title")), "status": "failed"})
                    if progress_callback:
                        progress_callback(summary.processed, summary.scanned, summary.updated, summary.skipped, summary.failed, "Fetching book covers")
                    continue
                cover_url = getattr(result, "cached_url", None) or getattr(result, "original_url", None)
                if not cover_url:
                    summary.skipped += 1
                    summary.details.append({"title": _text(book.get("title")), "status": "no_cover"})
                    if progress_callback:
                        progress_callback(summary.processed, summary.scanned, summary.updated, summary.skipped, summary.failed, "Fetching book covers")
                    continue
                try:
                    if not self._update_book(book["id"], {"cover_url": cover_url}):
                        raise RuntimeError("database update returned no rows")
                    self._invalidate_user_library_caches(book["id"])
                    summary.updated += 1
                    summary.details.append({"title": _text(book.get("title")), "status": "updated"})
                except Exception as exc:
                    summary.failed += 1
                    summary.details.append({"title": _text(book.get("title")), "status": "failed"})
                    logger.error(
                        "[BOOK_REPAIR][COVER_UPDATE_FAILED] book_id=%s title=%r error=%s",
                        book.get("id"), _text(book.get("title")), exc,
                    )
                if progress_callback:
                    progress_callback(summary.processed, summary.scanned, summary.updated, summary.skipped, summary.failed, "Fetching book covers")
        logger.info(
            "[BOOK_REPAIR][COVERS][DONE] scanned=%d updated=%d skipped=%d failed=%d",
            summary.scanned, summary.updated, summary.skipped, summary.failed,
        )
        return summary


book_repair_service = BookRepairService()


# Repair jobs are intentionally kept in-process: the work is I/O-heavy and
# this avoids holding the settings request open.  Only one bulk repair runs at
# a time because both operations write to the shared Kuzu database.
_JOB_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="book-repair-job")
_JOB_LOCK = threading.RLock()
_REPAIR_JOBS: Dict[str, Dict[str, Any]] = {}
_ACTIVE_JOB_ID: Optional[str] = None


def _job_copy(job: Dict[str, Any]) -> Dict[str, Any]:
    with _JOB_LOCK:
        return dict(job)


def get_repair_job(job_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not job_id:
        return None
    with _JOB_LOCK:
        job = _REPAIR_JOBS.get(str(job_id))
        return _job_copy(job) if job else None


def get_active_repair_job() -> Optional[Dict[str, Any]]:
    with _JOB_LOCK:
        return get_repair_job(_ACTIVE_JOB_ID) if _ACTIVE_JOB_ID else None


def _update_repair_job(job_id: str, **updates: Any) -> None:
    with _JOB_LOCK:
        job = _REPAIR_JOBS.get(job_id)
        if job:
            job.update(updates)
            job["updated_at"] = datetime.now(timezone.utc).isoformat()


def _run_repair_job(job_id: str, action: str) -> None:
    global _ACTIVE_JOB_ID
    _update_repair_job(job_id, status="running", phase="Preparing repair")
    logger.error("[BOOK_REPAIR][JOB_START] id=%s action=%s", job_id, action)

    last_logged_progress = -1

    def progress(processed: int, total: int, updated: int, skipped: int, failed: int, phase: str) -> None:
        nonlocal last_logged_progress
        _update_repair_job(
            job_id,
            processed=processed,
            total=total,
            updated=updated,
            skipped=skipped,
            failed=failed,
            phase=phase,
        )
        if processed == 0 or processed == total or processed - last_logged_progress >= 10:
            logger.error(
                "[BOOK_REPAIR][JOB_PROGRESS] id=%s action=%s processed=%d/%d updated=%d skipped=%d failed=%d",
                job_id, action, processed, total, updated, skipped, failed,
            )
            last_logged_progress = processed

    try:
        if action == "assign_missing_isbns":
            summary = book_repair_service.assign_missing_isbns(progress_callback=progress)
        else:
            summary = book_repair_service.fetch_missing_covers(progress_callback=progress)
        _update_repair_job(
            job_id,
            status="completed",
            phase="Complete",
            processed=summary.processed,
            total=summary.scanned,
            updated=summary.updated,
            skipped=summary.skipped,
            failed=summary.failed,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
        logger.error(
            "[BOOK_REPAIR][JOB_DONE] id=%s action=%s scanned=%d updated=%d skipped=%d failed=%d",
            job_id, action, summary.scanned, summary.updated, summary.skipped, summary.failed,
        )
    except Exception as exc:
        logger.error("[BOOK_REPAIR][JOB_FAILED] id=%s action=%s error=%s", job_id, action, exc, exc_info=True)
        _update_repair_job(
            job_id,
            status="failed",
            phase="Failed",
            error="The repair stopped unexpectedly. Check the application logs.",
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
    finally:
        with _JOB_LOCK:
            if _ACTIVE_JOB_ID == job_id:
                _ACTIVE_JOB_ID = None


def start_repair_job(action: str) -> Dict[str, Any]:
    """Queue a repair and return immediately with a pollable job record."""
    global _ACTIVE_JOB_ID
    if action not in {"assign_missing_isbns", "fetch_missing_covers"}:
        raise ValueError(f"Unknown repair action: {action}")
    with _JOB_LOCK:
        if _ACTIVE_JOB_ID:
            active = _REPAIR_JOBS.get(_ACTIVE_JOB_ID)
            if active and active.get("status") in {"queued", "running"}:
                return _job_copy(active)
        job_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc).isoformat()
        job = {
            "job_id": job_id,
            "action": action,
            "status": "queued",
            "phase": "Queued",
            "processed": 0,
            "total": 0,
            "updated": 0,
            "skipped": 0,
            "failed": 0,
            "created_at": now,
            "updated_at": now,
        }
        _REPAIR_JOBS[job_id] = job
        _ACTIVE_JOB_ID = job_id
        _JOB_EXECUTOR.submit(_run_repair_job, job_id, action)
        logger.error("[BOOK_REPAIR][JOB_QUEUED] id=%s action=%s", job_id, action)
        return _job_copy(job)
