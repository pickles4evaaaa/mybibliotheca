"""OPDS sync orchestration helpers."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import uuid
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .kuzu_async_helper import run_async
from .opds_probe_service import opds_probe_service, OPDSProbeService
from ..infrastructure.kuzu_graph import safe_execute_kuzu_query
from ..infrastructure.kuzu_repositories import KuzuBookRepository
from ..domain.models import BookContribution, ContributionType, Person
from ..location_service import LocationService
from ..utils.image_processing import process_image_from_url
from ..utils.safe_kuzu_manager import safe_get_connection
from flask import current_app, has_app_context

AUDIO_HINTS = {"audio", "mp3", "m4b", "flac", "ogg", "wav"}
KINDLE_HINTS = {"mobi", "azw", "azw3", "azw4", "azw8", "kf8", "kfx", "kindle"}
EBOOK_HINTS = {"epub", "pdf", "ebook", "html", "txt", "text"}


logger = logging.getLogger(__name__)


def _normalize_title(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _serialize_for_hash(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, list):
        return [_serialize_for_hash(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_serialize_for_hash(item) for item in value)
    if isinstance(value, dict):
        return {str(k): _serialize_for_hash(v) for k, v in value.items()}
    if isinstance(value, set):
        return sorted(_serialize_for_hash(item) for item in value)
    return value


_HASH_EXCLUDED_KEYS = {"raw_links", "entry"}


def _compute_entry_hash(entry: Dict[str, Any]) -> str:
    payload: Dict[str, Any] = {}
    for key in sorted(entry.keys()):
        if key in _HASH_EXCLUDED_KEYS:
            continue
        payload[key] = _serialize_for_hash(entry[key])
    marshalled = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(marshalled.encode("utf-8")).hexdigest()


def _normalize_timestamp(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        reference = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return reference.astimezone(timezone.utc).isoformat()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        candidate = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            return text
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    return str(value)


def _to_kuzu_date(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        candidate = text.replace("Z", "+00:00")
        try:
            parsed_dt = datetime.fromisoformat(candidate)
            return parsed_dt.date()
        except ValueError:
            try:
                return date.fromisoformat(text[:10])
            except ValueError:
                return None
    return None


def _build_set_clause(alias: str, properties: Dict[str, Any], *, prefix: str) -> Tuple[str, Dict[str, Any]]:
    assignments: List[str] = []
    params: Dict[str, Any] = {}
    for key, value in properties.items():
        param_key = f"{prefix}_{key}"
        assignments.append(f"{alias}.{key} = ${param_key}")
        params[param_key] = value
    return ", ".join(assignments), params


def _infer_media_type(detected_formats: Iterable[str]) -> str:
    tokens = {str(value).lower() for value in detected_formats if value}
    if any(any(hint in token for hint in AUDIO_HINTS) for token in tokens):
        return "audiobook"
    if tokens & KINDLE_HINTS:
        return "kindle"
    if tokens & EBOOK_HINTS:
        return "ebook"
    if any("mobi" in token or "azw" in token or "kindle" in token for token in tokens):
        return "kindle"
    return "ebook"


def _strip_urn(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    normalized = str(value).strip()
    if normalized.lower().startswith("urn:"):
        normalized = normalized.split(":", 2)[-1]
    if normalized.lower().startswith("uuid:"):
        normalized = normalized.split(":", 1)[-1]
    return normalized or None


def _normalize_oid(entry: Dict[str, Any]) -> Optional[str]:
    identifiers = entry.get("identifiers") or []
    for candidate in identifiers:
        normalized = _strip_urn(candidate)
        if normalized:
            return normalized
    for key in ("opds_source_id", "id", "entry_id"):
        normalized = _strip_urn(entry.get(key))
        if normalized:
            return normalized
    return None


def _detect_formats(sample: Dict[str, Any]) -> List[str]:
    formats: List[str] = []
    for link in sample.get("raw_links", []) or []:
        tokens = {
            str(link.get("type", "")).lower(),
            str(link.get("rel", "")).lower(),
            str(link.get("href", "")).lower(),
        }
        for token in tokens:
            if not token:
                continue
            if any(hint in token for hint in AUDIO_HINTS):
                formats.append("audio")
            if "epub" in token:
                formats.append("epub")
            if "pdf" in token:
                formats.append("pdf")
            if "mobi" in token or "azw" in token:
                formats.append("mobi")
    return sorted({fmt for fmt in formats})


def _ensure_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, (tuple, set)):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        items: List[str] = []
        for token in value.replace(";", ",").split(","):
            token = token.strip()
            if token:
                items.append(token)
        return items
    return [str(value).strip()] if str(value).strip() else []


def _to_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            return float(int(value))
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def _to_date_str(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        # Normalize trailing Z to ISO compatible offset
        candidate = text.replace("Z", "+00:00")
        try:
            dt_obj = datetime.fromisoformat(candidate)
            return dt_obj.date().isoformat()
        except ValueError:
            if len(text) >= 10 and text[4] == '-' and text[7] == '-':
                return text[:10]
    return None


def _resolve_source(sample: Dict[str, Any], expression: str) -> Any:
    if not expression:
        return None
    expr = expression.strip()
    if expr.startswith("entry."):
        key = expr[len("entry.") :]
        return sample.get(key)
    if expr.startswith("link[") and "].href" in expr:
        inner = expr[len("link[") : expr.index("]")]
        attribute, _, value = inner.partition("=")
        attribute = attribute.strip()
        value = value.strip().strip('"\'')
        for link in sample.get("raw_links", []) or []:
            if str(link.get(attribute)) == value:
                return link.get("href")
        return None
    return sample.get(expr)


def _ensure_contributors(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    contributors = entry.setdefault("contributors", [])
    return contributors


def _assign_contributors(entry: Dict[str, Any], role: str, value: Any) -> None:
    names = _ensure_list(value)
    if not names:
        return
    role_upper = role.upper()
    contributors = _ensure_contributors(entry)
    start_index = len(contributors)
    for offset, name in enumerate(names):
        contributors.append({
            "id": str(uuid.uuid4()),
            "name": name,
            "role": role_upper,
            "order": start_index + offset,
        })
    if role_upper == "AUTHORED":
        entry["authors"] = names


def apply_mapping_to_samples(
    samples: Iterable[Dict[str, Any]],
    mapping: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    normalized_entries: List[Dict[str, Any]] = []
    mapping = mapping or {}
    for sample in samples:
        entry: Dict[str, Any] = {
            "title": sample.get("title"),
            "subtitle": sample.get("subtitle"),
            "description": sample.get("summary") or sample.get("content"),
            "language": sample.get("language"),
            "categories": _ensure_list(sample.get("categories")),
            "raw_categories": sample.get("raw_categories") or sample.get("categories"),
            "cover_url": None,
            "entry_id": sample.get("entry_id") or sample.get("id"),
            "identifiers": sample.get("identifiers") or [],
            "authors": sample.get("authors") or [],
            "contributors": [],
            "raw_links": sample.get("raw_links") or [],
            "published": sample.get("published"),
            "updated": sample.get("updated"),
            "published_date": _to_date_str(sample.get("published_date") or sample.get("published")),
            "detected_formats": _detect_formats(sample),
            "page_count": _to_int(sample.get("page_count")),
            "series": sample.get("series"),
            "series_order": _to_int(sample.get("series_order")),
            "publisher": sample.get("publisher"),
            "rating": sample.get("rating"),
            "average_rating": _to_float(sample.get("average_rating") or sample.get("rating")),
            "tags": _ensure_list(sample.get("tags")),
        }

        for target_field, source_expr in mapping.items():
            value = _resolve_source(sample, source_expr)
            if value in (None, ""):
                continue
            if target_field.startswith("contributors."):
                role = target_field.split(".", 1)[1]
                _assign_contributors(entry, role, value)
                continue
            if target_field == "categories":
                entry["categories"] = _ensure_list(value)
                continue
            if target_field == "raw_categories":
                entry["raw_categories"] = value
                continue
            if target_field == "series_order":
                entry[target_field] = _to_int(value)
                continue
            if target_field == "page_count":
                entry[target_field] = _to_int(value)
                continue
            if target_field == "average_rating":
                entry[target_field] = _to_float(value)
                continue
            if target_field == "published_date":
                normalized_value = _to_date_str(value)
                if normalized_value is not None:
                    entry[target_field] = normalized_value
                else:
                    entry[target_field] = str(value).strip() or None
                continue
            entry[target_field] = value

        entry["tags"] = _ensure_list(entry.get("tags"))
        if not entry.get("categories") and entry["tags"]:
            entry["categories"] = list(entry["tags"])

        if not entry.get("cover_url"):
            for link in entry.get("raw_links", []) or []:
                rel_val = (link.get("rel") or "").lower()
                type_val = (link.get("type") or "").lower()
                href_val = link.get("href")
                if not href_val:
                    continue
                if rel_val in {"http://opds-spec.org/image", "http://opds-spec.org/cover"} or ("image" in type_val and "thumbnail" not in type_val):
                    entry["cover_url"] = href_val
                    break

        if not entry.get("cover_thumbnail"):
            for link in entry.get("raw_links", []) or []:
                rel_val = (link.get("rel") or "").lower()
                type_val = (link.get("type") or "").lower()
                href_val = link.get("href")
                if not href_val:
                    continue
                if rel_val in {"http://opds-spec.org/image/thumbnail", "http://opds-spec.org/thumbnail"} or ("thumbnail" in type_val and "image" in type_val):
                    entry["cover_thumbnail"] = href_val
                    break

        if not entry.get("contributors") and entry.get("authors"):
            _assign_contributors(entry, "AUTHORED", entry.get("authors"))

        explicit_oid = entry.get("opds_source_id")
        if explicit_oid:
            normalized_explicit = _strip_urn(explicit_oid) or explicit_oid
            entry["opds_source_id"] = normalized_explicit
        else:
            oid = _normalize_oid(entry)
            if oid:
                entry["opds_source_id"] = oid
        if not entry.get("id"):
            entry["id"] = entry.get("entry_id")
        entry["media_type"] = _infer_media_type(entry.get("detected_formats", []))
        original_published = entry.get("published")
        normalized_published = _to_date_str(entry.get("published_date") or original_published)
        if normalized_published is not None:
            entry["published_date"] = normalized_published
            entry["published"] = normalized_published
        else:
            entry["published"] = original_published
            if entry.get("published_date") is None:
                entry["published_date"] = original_published
        normalized_entries.append(entry)
    return normalized_entries


@dataclass
class SyncResult:
    created: int
    updated: int
    skipped: int
    entries: List[str]


PreviewRow = Dict[str, Any]


class OPDSSyncService:
    def __init__(self, probe_service: Optional[OPDSProbeService] = None) -> None:
        self._probe_service = probe_service or opds_probe_service
        self._max_sync = self._parse_sync_limit(os.getenv("OPDS_SYNC_MAX_ENTRIES"))
        self._book_repo = KuzuBookRepository()
        self._location_service: Optional[LocationService] = None

    async def quick_probe_sync(
        self,
        base_url: str,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        user_agent: Optional[str] = None,
        mapping: Optional[Dict[str, str]] = None,
        max_samples: Optional[int] = None,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        requested_limit = self._normalize_limit(max_samples)
        effective_limit = self._resolve_effective_limit(requested_limit)
        probe_limit = effective_limit if effective_limit is not None else 0
        probe = await self._probe_service.probe(
            base_url,
            username=username,
            password=password,
            user_agent=user_agent,
            max_samples=probe_limit,
        )
        entries = apply_mapping_to_samples(probe.get("samples", []), mapping)
        if effective_limit is not None:
            entries_to_apply = entries[:effective_limit]
        else:
            entries_to_apply = entries
        flask_app = current_app._get_current_object() if has_app_context() else None  # type: ignore[attr-defined]
        cover_auth: Optional[Tuple[str, str]] = None
        if username is not None and password is not None:
            cover_auth = (username, password)
        headers: Optional[Dict[str, str]] = None
        if user_agent:
            headers = {"User-Agent": user_agent}
        sync_result = await asyncio.to_thread(
            self._apply_entries,
            entries_to_apply,
            flask_app,
            cover_auth=cover_auth,
            cover_headers=headers,
            user_id=user_id,
        )
        return {
            "probe": probe,
            "sync": {
                "created": sync_result.created,
                "updated": sync_result.updated,
                "skipped": sync_result.skipped,
                "book_ids": sync_result.entries,
            },
        }

    def quick_probe_sync_sync(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return run_async(self.quick_probe_sync(*args, **kwargs))

    @staticmethod
    def _parse_sync_limit(raw_value: Optional[str]) -> Optional[int]:
        if raw_value is None:
            return None
        text = raw_value.strip()
        if not text:
            return None
        try:
            candidate = int(text)
        except (TypeError, ValueError):
            return None
        return candidate if candidate > 0 else None

    @staticmethod
    def _normalize_limit(value: Optional[int]) -> Optional[int]:
        if value is None:
            return None
        try:
            candidate = int(value)
        except (TypeError, ValueError):
            return None
        return candidate if candidate > 0 else None

    def _resolve_effective_limit(self, requested_limit: Optional[int]) -> Optional[int]:
        limit = requested_limit if requested_limit is not None and requested_limit > 0 else None
        if self._max_sync is not None:
            limit = min(limit, self._max_sync) if limit is not None else self._max_sync
        return limit

    def _get_location_service(self) -> LocationService:
        if self._location_service is None:
            self._location_service = LocationService()
        return self._location_service

    async def test_sync(
        self,
        base_url: str,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        user_agent: Optional[str] = None,
        mapping: Optional[Dict[str, str]] = None,
        max_samples: int = 10,
    ) -> Dict[str, Any]:
        max_samples = max(1, int(max_samples or 10))
        probe = await self._probe_service.probe(
            base_url,
            username=username,
            password=password,
            user_agent=user_agent,
            max_samples=max_samples,
        )
        entries = apply_mapping_to_samples(probe.get("samples", []), mapping)
        limited_entries = entries[:max_samples]
        preview_payload = await asyncio.to_thread(self._simulate_entries, limited_entries)
        return {
            "probe": probe,
            "preview": preview_payload.get("preview", []),
            "summary": {
                "would_create": preview_payload.get("would_create", 0),
                "would_update": preview_payload.get("would_update", 0),
                "skipped": preview_payload.get("skipped", 0),
            },
        }

    def test_sync_sync(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return run_async(self.test_sync(*args, **kwargs))

    # ------------------------------------------------------------------
    # Database upsert helpers
    # ------------------------------------------------------------------

    def _apply_entries(
        self,
        entries: List[Dict[str, Any]],
        flask_app=None,
        *,
        cover_auth: Optional[Tuple[str, str]] = None,
        cover_headers: Optional[Dict[str, str]] = None,
        user_id: Optional[str] = None,
    ) -> SyncResult:
        created = 0
        updated = 0
        skipped = 0
        book_ids: List[str] = []
        now = datetime.now(timezone.utc)
        location_service: Optional[LocationService] = None
        default_location_id: Optional[str] = None
        location_checked = False
        location_user_id = user_id or "__system__"

        context_manager = flask_app.app_context() if flask_app is not None else nullcontext()

        with context_manager:
            with safe_get_connection(operation="opds_sync") as conn:
                for entry in entries:
                    entry["opds_source_entry_hash"] = _compute_entry_hash(entry)
                    entry["opds_source_updated_at"] = _normalize_timestamp(entry.get("updated") or entry.get("published"))
                    oid = entry.get("opds_source_id")
                    if not oid:
                        skipped += 1
                        continue
                    book_id = self._find_book_id(conn, oid)
                    if book_id:
                        self._cache_cover_if_needed(entry, book_id, cover_auth=cover_auth, cover_headers=cover_headers)
                        success = self._update_book(conn, book_id, entry, now)
                        if success:
                            updated += 1
                            book_ids.append(book_id)
                            try:
                                self._sync_relationships(book_id, entry)
                            except Exception:
                                logger.exception("Failed to reconcile relationships for updated OPDS book %s", book_id)
                            try:
                                self._sync_contributors(book_id, entry)
                            except Exception:
                                logger.exception("Failed to reconcile contributors for updated OPDS book %s", book_id)
                        else:
                            skipped += 1
                    else:
                        new_id = str(uuid.uuid4())
                        self._cache_cover_if_needed(entry, new_id, cover_auth=cover_auth, cover_headers=cover_headers)
                        success = self._create_book(conn, new_id, entry, now)
                        if success:
                            created += 1
                            book_ids.append(new_id)
                            try:
                                self._sync_relationships(new_id, entry)
                            except Exception:
                                logger.exception("Failed to create relationships for new OPDS book %s", new_id)
                            try:
                                self._sync_contributors(new_id, entry)
                            except Exception:
                                logger.exception("Failed to create contributors for new OPDS book %s", new_id)
                            if not location_checked or not default_location_id:
                                try:
                                    location_service = location_service or self._get_location_service()
                                    default_location = location_service.get_default_location()
                                    if not default_location:
                                        try:
                                            location_service.setup_default_locations()
                                            default_location = location_service.get_default_location()
                                        except Exception:
                                            logger.exception("Failed to initialize default location for OPDS book %s", new_id)
                                    default_location_id = getattr(default_location, "id", None) if default_location else None
                                except Exception:
                                    logger.exception("Failed to resolve default location for OPDS book %s", new_id)
                                    default_location_id = None
                                location_checked = True
                            if default_location_id:
                                assigned = self._assign_default_location(conn, new_id, default_location_id)
                                if not assigned and location_service:
                                    try:
                                        location_service.add_book_to_location(new_id, default_location_id, location_user_id)
                                    except Exception:
                                        logger.exception("Failed to assign default location for OPDS book %s", new_id)
                        else:
                            skipped += 1
        return SyncResult(created=created, updated=updated, skipped=skipped, entries=book_ids)

    def _simulate_entries(self, entries: List[Dict[str, Any]]) -> Dict[str, Any]:
        would_create = 0
        would_update = 0
        skipped = 0
        preview: List[PreviewRow] = []
        def _process(conn) -> None:
            nonlocal would_create, would_update, skipped
            for entry in entries:
                oid = entry.get("opds_source_id")
                action = "create"
                reason: Optional[str] = None
                existing_id: Optional[str] = None

                if not oid:
                    skipped += 1
                    action = "skip"
                    reason = "missing_opds_source_id"
                else:
                    if conn is not None:
                        try:
                            existing_id = self._find_book_id(conn, oid)
                        except Exception:
                            existing_id = None
                    if existing_id:
                        would_update += 1
                        action = "update"
                    else:
                        would_create += 1
                        action = "create"

                safe_entry = json.loads(json.dumps(entry, default=str))
                row_payload = {
                    "title": entry.get("title"),
                    "subtitle": entry.get("subtitle"),
                    "description": entry.get("description"),
                    "language": entry.get("language"),
                    "categories": entry.get("categories"),
                    "raw_categories": entry.get("raw_categories"),
                    "cover_url": entry.get("cover_url"),
                    "opds_source_id": oid,
                    "entry_id": entry.get("entry_id") or entry.get("id"),
                    "action": action,
                    "reason": reason,
                    "authors": entry.get("authors"),
                    "contributors": entry.get("contributors"),
                    "detected_formats": entry.get("detected_formats"),
                    "series": entry.get("series"),
                    "series_order": entry.get("series_order"),
                    "page_count": entry.get("page_count"),
                    "publisher": entry.get("publisher"),
                    "published": entry.get("published"),
                    "published_date": entry.get("published_date"),
                    "rating": entry.get("rating"),
                    "average_rating": entry.get("average_rating"),
                    "tags": entry.get("tags"),
                    "media_type": entry.get("media_type"),
                    "entry": safe_entry,
                }
                identifiers = entry.get("identifiers")
                if identifiers is not None:
                    row_payload["identifiers"] = identifiers
                preview.append(row_payload)

        try:
            context = safe_get_connection(operation="opds_sync_preview")
        except Exception:
            context = nullcontext(None)

        try:
            with context as conn:  # type: ignore[arg-type]
                _process(conn)
        except Exception:
            _process(None)

        return {
            "would_create": would_create,
            "would_update": would_update,
            "skipped": skipped,
            "preview": preview,
        }

    def _cache_cover_if_needed(
        self,
        entry: Dict[str, Any],
        book_id: str,
        *,
        cover_auth: Optional[Tuple[str, str]] = None,
        cover_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        cover_url = entry.get("cover_url")
        if not cover_url:
            return
        if isinstance(cover_url, str) and cover_url.startswith("/covers/"):
            return
        if not has_app_context():
            return
        try:
            try:
                cached_url = process_image_from_url(str(cover_url), auth=cover_auth, headers=cover_headers)
            except TypeError as exc:
                if "unexpected keyword argument" in str(exc):
                    cached_url = process_image_from_url(str(cover_url))
                else:
                    raise
        except Exception:
            logger.exception("Failed to cache cover for OPDS book %s", book_id)
            return
        if cached_url:
            entry["cover_url"] = cached_url

    def _find_book_id(self, conn, oid: str) -> Optional[str]:  # type: ignore[no-untyped-def]
        try:
            result = conn.execute(
                "MATCH (b:Book {opds_source_id: $oid}) RETURN b.id AS id LIMIT 1",
                {"oid": oid},
            )
            if result and hasattr(result, "has_next") and result.has_next():  # type: ignore[attr-defined]
                row = result.get_next()  # type: ignore[attr-defined]
                if row:
                    return str(row[0])
        except Exception:
            # Column may not exist; fall back to matching against legacy identifiers
            try:
                result = conn.execute(
                    "MATCH (b:Book {google_books_id: $oid}) RETURN b.id AS id LIMIT 1",
                    {"oid": oid},
                )
                if result and hasattr(result, "has_next") and result.has_next():  # type: ignore[attr-defined]
                    row = result.get_next()  # type: ignore[attr-defined]
                    if row:
                        return str(row[0])
            except Exception:
                return None
        return None

    def _create_book(self, conn, book_id: str, entry: Dict[str, Any], now: datetime) -> bool:  # type: ignore[no-untyped-def]
        raw_categories_value = entry.get("raw_categories")
        normalized_raw_categories = self._normalize_categories(raw_categories_value)
        if normalized_raw_categories is None:
            normalized_raw_categories = self._normalize_categories(entry.get("categories"))
        raw_categories_json = json.dumps(normalized_raw_categories or [])

        title_value = entry.get("title")
        published_date_value = _to_kuzu_date(entry.get("published_date") or entry.get("published"))
        props = {
            "title": title_value,
            "normalized_title": _normalize_title(title_value),
            "subtitle": entry.get("subtitle"),
            "description": entry.get("description"),
            "language": self._normalize_language(entry.get("language")),
            "cover_url": entry.get("cover_url"),
            "opds_source_id": entry.get("opds_source_id"),
            "opds_source_updated_at": entry.get("opds_source_updated_at"),
            "opds_source_entry_hash": entry.get("opds_source_entry_hash"),
            "media_type": entry.get("media_type"),
            "page_count": entry.get("page_count"),
            "series": entry.get("series"),
            "series_order": entry.get("series_order"),
            "average_rating": entry.get("average_rating"),
            "published_date": published_date_value,
            "created_at": now,
            "updated_at": now,
            "raw_categories": raw_categories_json,
        }
        set_clause, params = _build_set_clause("b", props, prefix="create")
        params["book_id"] = book_id
        query = "CREATE (b:Book {id: $book_id})"
        if set_clause:
            query = f"{query} SET {set_clause}"
        try:
            conn.execute(query, params)
            return True
        except Exception:
            return False

    def _sync_relationships(self, book_id: str, entry: Dict[str, Any]) -> None:
        """Ensure category and publisher relationships reflect the entry payload."""
        raw_categories = entry.get("raw_categories")
        categories = entry.get("categories")
        category_source = raw_categories if raw_categories is not None else categories
        normalized_categories = self._normalize_categories(category_source)

        if category_source is not None:
            try:
                safe_execute_kuzu_query(
                    "MATCH (b:Book {id: $book_id})-[r:CATEGORIZED_AS]->() DELETE r",
                    {"book_id": book_id},
                    operation="opds_sync_clear_categories",
                )
            except Exception:
                logger.exception("Failed to clear category relationships for book %s", book_id)
            if normalized_categories:
                try:
                    run_async(self._book_repo._create_category_relationships_from_raw(book_id, normalized_categories))
                except Exception:
                    logger.exception("Failed to create category relationships for book %s", book_id)

        publisher = entry.get("publisher")
        if publisher is not None:
            normalized_publisher = self._normalize_publisher(publisher)
            try:
                safe_execute_kuzu_query(
                    "MATCH (b:Book {id: $book_id})-[r:PUBLISHED_BY]->() DELETE r",
                    {"book_id": book_id},
                    operation="opds_sync_clear_publisher",
                )
            except Exception:
                logger.exception("Failed to clear publisher relationship for book %s", book_id)
            if normalized_publisher:
                try:
                    run_async(self._book_repo._create_publisher_relationship(book_id, normalized_publisher))
                except Exception:
                    logger.exception("Failed to create publisher relationship for book %s", book_id)

    def _sync_contributors(self, book_id: str, entry: Dict[str, Any]) -> None:
        """Rebuild contributor relationships for the given book based on the entry payload."""

        def _resolve_contribution_type(raw_value: Any) -> ContributionType:
            if isinstance(raw_value, ContributionType):
                return raw_value
            if isinstance(raw_value, str):
                normalized = raw_value.strip().lower().replace(" ", "_")
                for member in ContributionType:
                    if member.value == normalized or member.name.lower() == normalized:
                        return member
            return ContributionType.AUTHORED

        def _extract_person(payload: Dict[str, Any], fallback_name: Optional[str]) -> Optional[Person]:
            name = (payload.get("name") or fallback_name or "").strip()
            if not name:
                return None

            person_id = payload.get("id") or payload.get("person_id")
            if person_id is not None:
                person_id = str(person_id)

            person = Person(
                id=person_id,
                name=name,
            )

            optional_fields = (
                "normalized_name",
                "birth_date",
                "death_date",
                "birth_year",
                "death_year",
                "birth_place",
                "bio",
                "website",
                "openlibrary_id",
                "wikidata_id",
                "imdb_id",
                "fuller_name",
                "title",
                "alternate_names",
                "official_links",
                "image_url",
            )
            for field_name in optional_fields:
                value = payload.get(field_name)
                if value is not None:
                    setattr(person, field_name, value)

            return person

        contributors_payload = entry.get("contributors") or []

        try:
            safe_execute_kuzu_query(
                "MATCH (p:Person)-[r:AUTHORED]->(b:Book {id: $book_id}) DELETE r",
                {"book_id": book_id},
                operation="opds_sync_clear_contributors",
            )
        except Exception:
            logger.exception("Failed to clear contributor relationships for book %s", book_id)

        prepared_contributions: List[BookContribution] = []
        for raw in contributors_payload:
            if isinstance(raw, BookContribution):
                prepared = raw
            else:
                if isinstance(raw, dict):
                    payload = raw
                else:
                    payload = getattr(raw, "__dict__", None) or {}

                person_payload = payload.get("person")
                person_obj: Optional[Person]
                if isinstance(person_payload, Person):
                    person_obj = person_payload
                elif isinstance(person_payload, dict):
                    person_obj = _extract_person(person_payload, payload.get("name"))
                elif person_payload is None:
                    person_obj = _extract_person(payload, payload.get("name"))
                else:
                    person_obj = _extract_person(getattr(person_payload, "__dict__", {}) or {}, payload.get("name"))

                if not person_obj:
                    continue

                contribution_type = payload.get("contribution_type") or payload.get("role")
                contribution_enum = _resolve_contribution_type(contribution_type)
                order_value = payload.get("order")
                notes = payload.get("notes")

                prepared = BookContribution(
                    person_id=str(person_obj.id or ""),
                    book_id=book_id,
                    contribution_type=contribution_enum,
                    order=order_value,
                    notes=notes,
                    person=person_obj,
                )
                auto_fetch = payload.get("auto_fetch_metadata")
                if auto_fetch is not None:
                    setattr(prepared, "auto_fetch_metadata", bool(auto_fetch))

            prepared_contributions.append(prepared)

        for index, contribution in enumerate(prepared_contributions):
            if contribution.order is None:
                contribution.order = index
            if getattr(contribution, "auto_fetch_metadata", None) is None:
                setattr(contribution, "auto_fetch_metadata", True)
            try:
                run_async(self._book_repo._create_contributor_relationship(book_id, contribution, contribution.order))
            except Exception:
                contributor_name = getattr(getattr(contribution, "person", None), "name", "<unknown>")
                logger.exception(
                    "Failed to create contributor relationship for book %s (contributor %s)",
                    book_id,
                    contributor_name,
                )

    def _assign_default_location(self, conn, book_id: str, location_id: str) -> bool:  # type: ignore[no-untyped-def]
        try:
            conn.execute(
                """
                MATCH (b:Book {id: $book_id}), (l:Location {id: $location_id})
                MERGE (b)-[rel:STORED_AT]->(l)
                ON CREATE SET rel.created_at = $created_at
                """,
                {
                    "book_id": book_id,
                    "location_id": location_id,
                    "created_at": datetime.now(timezone.utc),
                },
            )
            return True
        except Exception:
            logger.exception(
                "Failed to assign default location via direct query for OPDS book %s", book_id
            )
            return False

    def _update_book(self, conn, book_id: str, entry: Dict[str, Any], now: datetime) -> bool:  # type: ignore[no-untyped-def]
        raw_categories_value = entry.get("raw_categories")
        normalized_raw_categories = self._normalize_categories(raw_categories_value)
        if normalized_raw_categories is None:
            normalized_raw_categories = self._normalize_categories(entry.get("categories"))
        raw_categories_json = json.dumps(normalized_raw_categories or [])

        title_value = entry.get("title")
        published_date_value = _to_kuzu_date(entry.get("published_date") or entry.get("published"))
        update_fields = {
            "title": title_value,
            "normalized_title": _normalize_title(title_value),
            "subtitle": entry.get("subtitle"),
            "description": entry.get("description"),
            "language": self._normalize_language(entry.get("language")),
            "cover_url": entry.get("cover_url"),
            "opds_source_id": entry.get("opds_source_id"),
            "opds_source_updated_at": entry.get("opds_source_updated_at"),
            "opds_source_entry_hash": entry.get("opds_source_entry_hash"),
            "media_type": entry.get("media_type"),
            "page_count": entry.get("page_count"),
            "series": entry.get("series"),
            "series_order": entry.get("series_order"),
            "average_rating": entry.get("average_rating"),
            "published_date": published_date_value,
            "updated_at": now,
            "raw_categories": raw_categories_json,
        }
        set_clause, params = _build_set_clause("b", update_fields, prefix="update")
        if not set_clause:
            return True
        params["book_id"] = book_id
        query = f"MATCH (b:Book {{id: $book_id}}) SET {set_clause}"
        try:
            conn.execute(query, params)
            return True
        except Exception:
            return False

    def _normalize_language(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, (list, tuple, set)):
            for item in value:
                normalized = self._normalize_language(item)
                if normalized:
                    return normalized
            return None
        text = str(value).strip()
        return text or None

    def _normalize_categories(self, value: Any) -> Optional[List[str]]:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            try:
                decoded = json.loads(text)
            except json.JSONDecodeError:
                parts = [part.strip() for part in re.split(r"[;,|]", text) if part.strip()]
                return parts or []
            else:
                if isinstance(decoded, list):
                    return [str(item).strip() for item in decoded if str(item).strip()]
                return [str(decoded).strip()]
        if isinstance(value, (list, tuple, set)):
            normalized = [str(item).strip() for item in value if str(item).strip()]
            return normalized
        text = str(value).strip()
        return [text] if text else []

    def _normalize_publisher(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, (list, tuple, set)):
            for item in value:
                normalized = self._normalize_publisher(item)
                if normalized:
                    return normalized
            return None
        if isinstance(value, dict):
            for key in ("name", "title", "label"):
                candidate = value.get(key)
                if candidate:
                    normalized = str(candidate).strip()
                    if normalized:
                        return normalized
            return None
        text = str(value).strip()
        return text or None


opds_sync_service = OPDSSyncService()
__all__ = [
    "OPDSSyncService",
    "opds_sync_service",
    "apply_mapping_to_samples",
    "_infer_media_type",
    "_normalize_oid",
]
