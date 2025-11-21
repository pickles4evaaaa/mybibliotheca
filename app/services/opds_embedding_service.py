"""Background runner that turns OPDS acquisitions into RAG embeddings."""
from __future__ import annotations

import logging
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

import requests
from flask import current_app
from requests import Response
from requests.auth import HTTPBasicAuth, HTTPDigestAuth

from app.rag_config import rag_config_dataclass
from app.utils.opds_settings import load_opds_settings, save_opds_settings
from app.utils.safe_kuzu_manager import safe_execute_query

from .rag_vector_service import RAGVectorService

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)  # Force DEBUG level for RAG
_MAX_DEFAULT_FILE_MB = 80
_DEFAULT_ALLOWED_FORMATS = ("epub", "pdf", "text")
_DOWNLOAD_TIMEOUT = 45


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class EmbeddingJob:
    book_id: str
    entry_hash: Optional[str]
    opds_id: Optional[str]
    title: Optional[str]
    description: Optional[str]
    raw_links: List[Dict[str, Any]] = field(default_factory=list)
    media_type: Optional[str] = None
    user_id: Optional[str] = None
    auth: Optional[Tuple[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    force: bool = False


class _OPDSEmbeddingRunner:
    """Single background worker that downloads OPDS assets + feeds the vector store."""

    def __init__(self) -> None:
        self._queue: Deque[EmbeddingJob] = deque()
        self._lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._app = None
        self._rag_service: Optional[RAGVectorService] = None
        self._cache_dir: Optional[Path] = None
        self._current_job: Optional[EmbeddingJob] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "queue_size": len(self._queue),
                "running": self._running,
                "is_alive": self._thread.is_alive() if self._thread else False,
                "current_job": {
                    "book_id": self._current_job.book_id,
                    "title": self._current_job.title
                } if self._current_job else None
            }

    def ensure_started(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._running = True
            try:
                self._app = current_app._get_current_object()  # type: ignore[attr-defined]
            except Exception:
                self._app = None
            thread = threading.Thread(target=self._run_loop, name="opds-embedding-runner", daemon=True)
            self._thread = thread
            thread.start()

    def enqueue_entry(
        self,
        *,
        book_id: str,
        entry_hash: Optional[str],
        opds_id: Optional[str],
        title: Optional[str],
        description: Optional[str],
        raw_links: Optional[List[Dict[str, Any]]],
        media_type: Optional[str],
        auth: Optional[Tuple[str, str]],
        headers: Optional[Dict[str, str]],
        user_id: Optional[str],
        force: bool = False,
    ) -> bool:
        LOGGER.info(f"RAG: Enqueue request for book_id={book_id} title='{title}' force={force}")
        if not force and not self._auto_embedding_enabled():
            LOGGER.info(f"RAG: Auto-embedding disabled and not forced. Skipping {book_id}.")
            return False
        job = EmbeddingJob(
            book_id=book_id,
            entry_hash=entry_hash,
            opds_id=opds_id,
            title=title,
            description=description,
            raw_links=list(raw_links or []),
            media_type=media_type,
            user_id=user_id,
            auth=auth,
            headers=headers,
            force=force,
        )
        self.ensure_started()
        with self._lock:
            self._queue.append(job)
        LOGGER.info(f"RAG: Job enqueued for {book_id}. Queue size: {len(self._queue)}")
        return True

    # ------------------------------------------------------------------
    # Runner internals
    # ------------------------------------------------------------------
    def _run_loop(self) -> None:
        app = self._app
        if app is None:
            LOGGER.warning("OPDS embedding runner exiting – no Flask app context available")
            return
        LOGGER.info("RAG: Runner loop started.")
        ctx = app.app_context()
        ctx.push()
        try:
            while self._running:
                job = None
                with self._lock:
                    if self._queue:
                        job = self._queue.popleft()
                        self._current_job = job
                if job is None:
                    time.sleep(0.5)
                    continue
                try:
                    LOGGER.info(f"RAG: Processing job for book_id={job.book_id}")
                    self._process_job(job)
                    LOGGER.info(f"RAG: Finished job for book_id={job.book_id}")
                except Exception as exc:
                    LOGGER.exception("Embedding job failed for %s: %s", job.book_id, exc)
                finally:
                    with self._lock:
                        self._current_job = None
        finally:
            LOGGER.info("RAG: Runner loop exiting.")
            try:
                ctx.pop()
            except Exception:
                pass

    def process_job_sync(self, job: EmbeddingJob) -> Dict[str, Any]:
        """Synchronous processing logic (download -> extract -> chunk -> upsert)."""
        LOGGER.debug(f"RAG: Starting sync processing for {job.book_id}")
        if not self._ensure_columns_ready():
            LOGGER.error("RAG: Schema columns not ready")
            return {"status": "failed", "reason": "schema error"}

        if not job.force and self._book_already_current(job.book_id, job.entry_hash):
            LOGGER.info(f"RAG: Book {job.book_id} already current (hash match). Skipping.")
            return {"status": "skipped", "reason": "already current"}

        settings = load_opds_settings()
        allowed_formats = self._allowed_formats(settings)
        max_mb = self._max_file_mb(settings)
        
        LOGGER.debug(f"RAG: Selecting download for {job.book_id}. Allowed: {allowed_formats}")
        download_info = self._select_download(job.raw_links, allowed_formats)
        
        text_payload = ""
        source_url = None
        source_format = None

        if download_info:
            source_url, source_format = download_info
            LOGGER.info(f"RAG: Downloading {source_format} from {source_url} for {job.book_id}")
            try:
                asset_path = self._download_asset(
                    source_url,
                    source_format,
                    job.auth,
                    job.headers,
                    max_mb
                )
                LOGGER.debug(f"RAG: Downloaded to {asset_path}")
                text_payload = self._extract_text(asset_path, download_info[1])
                LOGGER.info(f"RAG: Extracted {len(text_payload) if text_payload else 0} chars")
            except Exception as e:
                LOGGER.error(f"RAG: Download/Extraction failed for {job.book_id}: {e}")
                # Fallthrough to description
            finally:
                try:
                    if 'asset_path' in locals():
                        asset_path.unlink(missing_ok=True)  # type: ignore[arg-type]
                except Exception:
                    pass
        else:
            LOGGER.warning(f"RAG: No suitable download link found for {job.book_id}")

        if not text_payload and job.description:
            LOGGER.info(f"RAG: No text payload, using description for {job.book_id}")
            text_payload = str(job.description)
            source_format = source_format or "metadata"
            source_url = source_url or "__description__"
            
        if not text_payload:
            LOGGER.error(f"RAG: No text available for {job.book_id}")
            self._mark_book_status(job.book_id, status="failed: no text", error="No acquisition link or description provided")
            return {"status": "failed", "reason": "no text available"}
            
        rag_service = self._get_rag_service()
        try:
            LOGGER.info(f"RAG: Upserting book {job.book_id} to vector store...")
            chunk_count = rag_service.upsert_book(
                book_id=job.book_id,
                text=text_payload,
                metadata={
                    "opds_source_id": job.opds_id,
                    "media_type": job.media_type,
                    "source_url": source_url,
                    "source_format": source_format,
                    "title": job.title or "Unknown Title",
                },
            )
            LOGGER.info(f"RAG: Upsert complete for {job.book_id}. Chunks: {chunk_count}")
            self._mark_book_status(
                job.book_id,
                status="complete",
                chunk_count=chunk_count,
                source_url=source_url,
                source_format=source_format,
                entry_hash=job.entry_hash,
            )
            return {
                "status": "complete",
                "chunk_count": chunk_count,
                "source_url": source_url,
                "source_format": source_format,
            }
        except Exception as exc:
            LOGGER.exception(f"RAG: Upsert failed for {job.book_id}")
            self._mark_book_status(job.book_id, status=f"failed: {exc}", error=str(exc))
            return {"status": "failed", "reason": f"upsert failed: {exc}"}

    def _process_job(self, job: EmbeddingJob) -> None:
        LOGGER.debug(f"RAG: Processing job for book {job.book_id} (title: {job.title})")
        self.process_job_sync(job)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _auto_embedding_enabled(self) -> bool:
        try:
            settings = load_opds_settings()
        except Exception:
            return False
        if not settings.get("auto_embed_enabled"):
            return False
        return self._rag_enabled()

    def _rag_enabled(self) -> bool:
        try:
            return rag_config_dataclass().enabled
        except Exception:
            return False

    def _allowed_formats(self, settings: Dict[str, Any]) -> Tuple[str, ...]:
        raw = settings.get("auto_embed_formats")
        if isinstance(raw, list) and raw:
            cleaned = tuple({str(item).strip().lower() for item in raw if str(item).strip()})
            return cleaned or _DEFAULT_ALLOWED_FORMATS
        return _DEFAULT_ALLOWED_FORMATS

    def _max_file_mb(self, settings: Dict[str, Any]) -> int:
        try:
            value = int(settings.get("auto_embed_max_file_mb") or _MAX_DEFAULT_FILE_MB)
            if value <= 0:
                return _MAX_DEFAULT_FILE_MB
            return value
        except Exception:
            return _MAX_DEFAULT_FILE_MB

    def _cache_base_dir(self) -> Path:
        if self._cache_dir is not None:
            return self._cache_dir
        try:
            data_dir = current_app.config.get("DATA_DIR", "data")  # type: ignore[attr-defined]
        except Exception:
            data_dir = "data"
        cache_dir = Path(data_dir) / "opds_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        self._cache_dir = cache_dir
        return cache_dir

    def _get_rag_service(self) -> RAGVectorService:
        if self._rag_service is None:
            self._rag_service = RAGVectorService()
        return self._rag_service

    def _ensure_columns_ready(self) -> bool:
        try:
            safe_execute_query(
                """
                MATCH (b:Book) RETURN b.rag_ingest_status LIMIT 1
                """,
                operation="opds_embedding_column_check",
            )
            return True
        except Exception:
            try:
                safe_execute_query("ALTER TABLE Book ADD rag_ingest_status STRING")
                safe_execute_query("ALTER TABLE Book ADD rag_ingest_error STRING")
                safe_execute_query("ALTER TABLE Book ADD rag_ingested_at TIMESTAMP")
                safe_execute_query("ALTER TABLE Book ADD rag_chunk_count INT64")
                safe_execute_query("ALTER TABLE Book ADD rag_source_url STRING")
                safe_execute_query("ALTER TABLE Book ADD rag_source_format STRING")
                return True
            except Exception as exc:
                LOGGER.error("Failed ensuring rag ingestion columns: %s", exc)
                return False

    def _book_already_current(self, book_id: str, entry_hash: Optional[str]) -> bool:
        if not entry_hash:
            return False
        try:
            result = safe_execute_query(
                """
                MATCH (b:Book {id: $book_id})
                RETURN b.opds_source_entry_hash AS entry_hash, b.rag_ingest_status AS status
                """,
                {"book_id": book_id},
                operation="opds_embedding_existing",
            )
            if result and hasattr(result, "has_next") and result.has_next():
                row = result.get_next()
                stored_hash = row[0]
                status = row[1]
                return stored_hash == entry_hash and str(status or "").startswith("complete")
        except Exception:
            return False
        return False

    def _select_download(self, links: List[Dict[str, Any]], allowed_formats: Tuple[str, ...]) -> Optional[Tuple[str, str]]:
        if not links:
            return None
        candidates: List[Tuple[str, str]] = []
        for link in links:
            href = link.get("href")
            if not isinstance(href, str) or not href.strip():
                continue
            rel = str(link.get("rel") or "").lower()
            link_type = str(link.get("type") or "").lower()
            fmt = self._infer_format(href, link_type)
            if fmt is None:
                continue
            if not self._is_acquisition(rel, link_type) and fmt != "text":
                continue
            candidates.append((href, fmt))
        if not candidates:
            return None
        for preferred in allowed_formats:
            for href, fmt in candidates:
                if fmt == preferred:
                    return (href, fmt)
        return candidates[0]

    def _infer_format(self, href: str, link_type: str) -> Optional[str]:
        href_lower = href.lower()
        if "epub" in link_type or href_lower.endswith(".epub"):
            return "epub"
        if "pdf" in link_type or href_lower.endswith(".pdf"):
            return "pdf"
        if "text" in link_type or href_lower.endswith(('.txt', '.text')):
            return "text"
        if "plain" in link_type:
            return "text"
        return None

    def _is_acquisition(self, rel: str, link_type: str) -> bool:
        rel_lower = rel or ""
        type_lower = link_type or ""
        rel_lower = rel_lower.lower()
        type_lower = type_lower.lower()
        if rel_lower.startswith("http://opds-spec.org/acquisition"):
            return True
        if "opds-acquisition" in type_lower:
            return True
        if "acquisition" in type_lower:
            return True
        return False

    def _download_asset(
        self,
        url: str,
        fmt: str,
        auth: Optional[Tuple[str, str]],
        headers: Optional[Dict[str, str]],
        max_file_mb: int,
    ) -> Path:
        request_kwargs: Dict[str, Any] = {"stream": True, "timeout": _DOWNLOAD_TIMEOUT}
        if headers:
            request_kwargs["headers"] = headers
        resp = self._issue_request(url, auth, request_kwargs)
        max_bytes = max_file_mb * 1024 * 1024
        cache_dir = self._cache_base_dir()
        ext = self._extension_for_format(fmt)
        file_name = f"{uuid.uuid4().hex}{ext}"
        file_path = cache_dir / file_name
        total = 0
        with open(file_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=65536):
                if not chunk:
                    continue
                fh.write(chunk)
                total += len(chunk)
                if total > max_bytes:
                    resp.close()
                    fh.flush()
                    file_path.unlink(missing_ok=True)  # type: ignore[arg-type]
                    raise ValueError("Download exceeded allowed file size")
        resp.close()
        return file_path

    def _issue_request(self, url: str, auth: Optional[Tuple[str, str]], request_kwargs: Dict[str, Any]) -> Response:
        response = requests.get(url, **request_kwargs, auth=self._build_basic_auth(auth))
        if response.status_code == 401 and auth:
            response.close()
            response = requests.get(url, **request_kwargs, auth=self._build_digest_auth(auth))
        response.raise_for_status()
        return response

    @staticmethod
    def _build_basic_auth(auth_tuple: Optional[Tuple[str, str]]) -> Optional[HTTPBasicAuth]:
        if not auth_tuple:
            return None
        return HTTPBasicAuth(auth_tuple[0], auth_tuple[1])

    @staticmethod
    def _build_digest_auth(auth_tuple: Optional[Tuple[str, str]]) -> Optional[HTTPDigestAuth]:
        if not auth_tuple:
            return None
        return HTTPDigestAuth(auth_tuple[0], auth_tuple[1])

    @staticmethod
    def _extension_for_format(fmt: str) -> str:
        lookup = {
            "epub": ".epub",
            "pdf": ".pdf",
            "text": ".txt",
        }
        return lookup.get(fmt, ".bin")

    def _extract_text(self, asset_path: Path, fmt: str) -> str:
        if fmt == "epub":
            return self._extract_epub(asset_path)
        if fmt == "pdf":
            return self._extract_pdf(asset_path)
        return asset_path.read_text(encoding="utf-8", errors="ignore")

    def _extract_epub(self, asset_path: Path) -> str:
        try:
            import ebooklib
            from ebooklib import epub  # type: ignore
            from bs4 import BeautifulSoup  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("ebooklib and beautifulsoup4 are required for EPUB extraction") from exc
        book = epub.read_epub(str(asset_path))
        texts: List[str] = []
        for item in book.get_items():
            if item.get_type() != ebooklib.ITEM_DOCUMENT:
                continue
            soup = BeautifulSoup(item.get_body_content(), "html.parser")
            text = soup.get_text(" ", strip=True)
            if text:
                texts.append(text)
        return "\n\n".join(texts)

    def _extract_pdf(self, asset_path: Path) -> str:
        try:
            from pypdf import PdfReader  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("pypdf is required for PDF extraction") from exc
        reader = PdfReader(str(asset_path))
        texts: List[str] = []
        for page in reader.pages:
            text = page.extract_text() or ""
            text = text.strip()
            if text:
                texts.append(text)
        return "\n\n".join(texts)

    def _mark_book_status(
        self,
        book_id: str,
        *,
        status: str,
        chunk_count: Optional[int] = None,
        source_url: Optional[str] = None,
        source_format: Optional[str] = None,
        entry_hash: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        params: Dict[str, Any] = {
            "book_id": book_id,
            "status": status,
            "ingested_at": datetime.now(timezone.utc),
            "chunk_count": chunk_count,
            "source_url": source_url,
            "source_format": source_format,
            "error": error,
            "entry_hash": entry_hash,
        }
        safe_execute_query(
            """
            MATCH (b:Book {id: $book_id})
            SET b.rag_ingest_status = $status,
                b.rag_ingested_at = $ingested_at,
                b.rag_chunk_count = $chunk_count,
                b.rag_source_url = $source_url,
                b.rag_source_format = $source_format,
                b.rag_ingest_error = $error,
                b.opds_source_entry_hash = COALESCE($entry_hash, b.opds_source_entry_hash)
            """,
            params,
            operation="opds_embedding_update",
        )


_runner_singleton = _OPDSEmbeddingRunner()


def get_opds_embedding_runner() -> _OPDSEmbeddingRunner:
    return _runner_singleton


def ensure_opds_embedding_runner() -> None:
    try:
        _runner_singleton.ensure_started()
    except Exception:
        pass


__all__ = [
    "get_opds_embedding_runner",
    "ensure_opds_embedding_runner",
    "_OPDSEmbeddingRunner",
    "EmbeddingJob",
]
