"""Service responsible for probing an OPDS feed and collecting samples."""
from __future__ import annotations

import asyncio
import os
import re
import textwrap
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import requests
from requests import Response
from requests.auth import HTTPBasicAuth, HTTPDigestAuth

from .kuzu_async_helper import run_async

ATOM_NS = "{http://www.w3.org/2005/Atom}"
DC_NS = "{http://purl.org/dc/elements/1.1/}"
DCTERMS_NS = "{http://purl.org/dc/terms/}"


@dataclass
class ProbeConfig:
    max_depth: int
    max_samples: Optional[int]
    timeout: float


def _normalize_title(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value.strip().lower()


def _extract_element_text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None:
        return None
    pieces: List[str] = []
    try:
        for text in elem.itertext():
            if not text:
                continue
            stripped = text.strip()
            if stripped:
                pieces.append(stripped)
    except Exception:
        if elem.text:
            fallback = elem.text.strip()
            if fallback:
                return fallback
        return None
    if not pieces:
        base_text = (elem.text or "").strip()
        return base_text or None
    return "\n\n".join(pieces)


def _decode_rating_value(text: str) -> Optional[float]:
    cleaned = (text or "").strip()
    if not cleaned:
        return None
    filled_stars = cleaned.count("★")
    half_stars = cleaned.count("½")
    if filled_stars or half_stars:
        value = float(filled_stars) + (0.5 if half_stars else 0.0)
        return max(0.0, min(5.0, value))
    numbers = [float(match) for match in re.findall(r"\d+(?:\.\d+)?", cleaned)]
    if not numbers:
        return None
    value = numbers[0]
    if len(numbers) >= 2 and "/" in cleaned:
        denominator = numbers[1]
        if denominator:
            value = (value / denominator) * 5.0
    elif "%" in cleaned:
        value = value / 20.0
    return max(0.0, min(5.0, value))


def _parse_tags_value(text: str) -> List[str]:
    if not text:
        return []
    parts = re.split(r"[;,|]", text)
    return [part.strip() for part in parts if part and part.strip()]


def _normalize_description_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None

    expanded = text.expandtabs()
    dedented = textwrap.dedent(expanded)

    lines = dedented.splitlines()
    normalized_lines: List[str] = []
    first_content_seen = False

    for line in lines:
        working = line.rstrip()
        if not first_content_seen:
            stripped = working.lstrip()
            if stripped:
                normalized_lines.append(stripped)
                first_content_seen = True
                continue
        normalized_lines.append(working)

    normalized = "\n".join(normalized_lines).strip()
    return normalized or None


def extract_summary_metadata(summary: Optional[str]) -> Tuple[Optional[str], Optional[float], List[str]]:
    """Split Calibre-style metadata lines from the top of a summary.

    Returns the cleaned summary, a rating value (0-5 scale), and any tags extracted.
    """
    if not summary:
        return None, None, []

    rating_value: Optional[float] = None
    tags_value: List[str] = []
    cleaned_lines: List[str] = []
    metadata_section = True

    for line in summary.splitlines():
        stripped = line.strip()
        if metadata_section:
            if not stripped:
                # Skip blank lines at top of metadata section
                continue
            rating_match = re.match(r"^RATING:\s*(.+)$", stripped, flags=re.IGNORECASE)
            if rating_match and rating_value is None:
                rating_value = _decode_rating_value(rating_match.group(1))
                continue
            tags_match = re.match(r"^TAGS?:\s*(.+)$", stripped, flags=re.IGNORECASE)
            if tags_match and not tags_value:
                tags_value = _parse_tags_value(tags_match.group(1))
                continue
            # First non-metadata line terminates metadata section
            metadata_section = False
        cleaned_lines.append(line)

    cleaned_summary = _normalize_description_text("\n".join(cleaned_lines))
    return cleaned_summary, rating_value, tags_value


def _record_entry_field(field_inventory: Dict[str, List[str]], field_name: str) -> None:
    if not field_name:
        return
    entry_fields = field_inventory.setdefault("entry", [])
    if field_name not in entry_fields:
        entry_fields.append(field_name)


class OPDSProbeService:
    """Fetch samples from an OPDS feed for preview/mapping purposes."""

    def __init__(self) -> None:
        self._default_config = ProbeConfig(
            max_depth=self._env_int("OPDS_PROBE_MAX_DEPTH", fallback=5),
            max_samples=self._env_int("OPDS_PROBE_MAX_SAMPLES", fallback=40),
            timeout=self._env_float("OPDS_PROBE_TIMEOUT", fallback=15.0),
        )

    async def probe(
        self,
        base_url: str,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        user_agent: Optional[str] = None,
        max_depth: Optional[int] = None,
        max_samples: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_max_samples: Optional[int]
        if max_samples is None:
            normalized_max_samples = self._default_config.max_samples
        else:
            try:
                candidate = int(max_samples)
            except (TypeError, ValueError):
                candidate = self._default_config.max_samples or 0
            if candidate <= 0:
                normalized_max_samples = None
            else:
                normalized_max_samples = candidate

        config = ProbeConfig(
            max_depth=max_depth if max_depth is not None else self._default_config.max_depth,
            max_samples=normalized_max_samples,
            timeout=self._default_config.timeout,
        )
        return await asyncio.to_thread(
            self._probe_in_thread,
            base_url,
            username,
            password,
            user_agent,
            config,
        )

    def probe_sync(
        self,
        base_url: str,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        user_agent: Optional[str] = None,
        max_depth: Optional[int] = None,
        max_samples: Optional[int] = None,
    ) -> Dict[str, Any]:
        return run_async(
            self.probe(
                base_url,
                username=username,
                password=password,
                user_agent=user_agent,
                max_depth=max_depth,
                max_samples=max_samples,
            )
        )

    # ------------------------------------------------------------------
    # Environment helpers
    # ------------------------------------------------------------------

    def _env_int(self, key: str, *, fallback: int) -> int:
        value = os.getenv(key)
        if value is None:
            return fallback
        try:
            return int(value)
        except (TypeError, ValueError):
            return fallback

    def _env_float(self, key: str, *, fallback: float) -> float:
        value = os.getenv(key)
        if value is None:
            return fallback
        try:
            return float(value)
        except (TypeError, ValueError):
            return fallback

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _probe_in_thread(
        self,
        base_url: str,
        username: Optional[str],
        password: Optional[str],
        user_agent: Optional[str],
        config: ProbeConfig,
    ) -> Dict[str, Any]:
        base_url = (base_url or "").strip()
        if not base_url:
            raise ValueError("Base URL is required for OPDS probing")

        session = requests.Session()
        if user_agent:
            session.headers["User-Agent"] = user_agent
        elif "User-Agent" not in session.headers:
            session.headers["User-Agent"] = "MyBibliotheca/OPDSProbe"

        if username and password:
            session.auth = HTTPBasicAuth(username, password)

        attempts: List[Dict[str, Any]] = []
        visited: set[str] = set()
        queue: List[Tuple[str, int]] = [(base_url, 0)]
        content_samples: List[Dict[str, Any]] = []
        navigation_samples: List[Dict[str, Any]] = []
        seen_identifiers: set[str] = set()
        field_inventory: Dict[str, List[str]] = {
            "entry": [],
            "link_rels": [],
            "link_types": [],
        }

        try:
            while queue and (config.max_samples is None or len(content_samples) < config.max_samples):
                url, depth = queue.pop(0)
                if url in visited or depth > config.max_depth:
                    continue
                visited.add(url)
                try:
                    response = session.get(url, timeout=config.timeout)
                except Exception as err:
                    attempts.append({
                        "url": url,
                        "depth": depth,
                        "status": "error",
                        "error": str(err),
                    })
                    continue

                attempt_record: Dict[str, Any] = {
                    "url": url,
                    "depth": depth,
                    "status_code": response.status_code,
                }

                if response.status_code == 401 and "digest" in response.headers.get("WWW-Authenticate", "").lower():
                    # Retry once with digest auth if credentials provided.
                    if username and password:
                        session.auth = HTTPDigestAuth(username, password)
                        try:
                            response = session.get(url, timeout=config.timeout)
                            attempt_record["status_code"] = response.status_code
                            attempt_record["auth_mode"] = "digest"
                        except Exception as err:
                            attempt_record["status"] = "error"
                            attempt_record["error"] = str(err)
                            attempts.append(attempt_record)
                            continue
                    else:
                        attempt_record["status"] = "auth_required"
                        attempts.append(attempt_record)
                        continue

                attempts.append(attempt_record)

                if not response.ok:
                    continue

                try:
                    parsed = self._parse_feed(response, field_inventory, response.url or url)
                except Exception as parse_err:
                    attempt_record["status"] = "parse_error"
                    attempt_record["error"] = str(parse_err)
                    continue

                for entry in parsed.entries:
                    identifier = entry.get("entry_id") or entry.get("id")
                    if not identifier:
                        identifier = str(uuid.uuid4())
                    if identifier in seen_identifiers:
                        continue
                    seen_identifiers.add(identifier)
                    if self._entry_has_acquisition(entry):
                        if config.max_samples is None or len(content_samples) < config.max_samples:
                            content_samples.append(entry)
                    else:
                        if config.max_samples is not None and len(navigation_samples) < (config.max_samples * 2):
                            navigation_samples.append(entry)
                    if config.max_samples is not None and len(content_samples) >= config.max_samples:
                        break

                if depth < config.max_depth:
                    for link in parsed.catalog_links:
                        child_url = urljoin(url, link)
                        if child_url not in visited:
                            queue.append((child_url, depth + 1))
                    for link in parsed.acquisition_links:
                        child_url = urljoin(url, link)
                        if child_url not in visited:
                            queue.append((child_url, depth + 1))
        finally:
            session.close()

        if config.max_samples is None:
            combined_navigation: List[Dict[str, Any]] = []
        else:
            remaining_capacity = max(0, config.max_samples - len(content_samples))
            combined_navigation = navigation_samples[:remaining_capacity]
        all_samples = content_samples + combined_navigation
        mapping_suggestions = self._suggest_mapping(field_inventory)

        return {
            "samples": all_samples,
            "field_inventory": field_inventory,
            "diagnostics": {
                "attempts": attempts,
                "max_depth": config.max_depth,
                "max_samples": config.max_samples,
            },
            "mapping_suggestions": mapping_suggestions,
        }

    # ------------------------------------------------------------------

    @dataclass
    class ParsedFeed:
        entries: List[Dict[str, Any]]
        acquisition_links: List[str]
        catalog_links: List[str]

    def _parse_feed(self, response: Response, field_inventory: Dict[str, List[str]], response_url: Optional[str]) -> "OPDSProbeService.ParsedFeed":
        content = response.content
        if not content:
            return self.ParsedFeed(entries=[], acquisition_links=[], catalog_links=[])

        tree = ET.fromstring(content)
        acquisition_links: set[str] = set()
        catalog_links: set[str] = set()
        entries: List[Dict[str, Any]] = []
        base_url = response_url or response.url or ""

        for entry_elem in tree.findall(f"{ATOM_NS}entry"):
            entry_data = self._parse_entry(entry_elem, field_inventory)
            entries.append(entry_data)
            for link in entry_data.get("raw_links", []):
                raw_href = link.get("href")
                href = urljoin(base_url, raw_href) if raw_href else raw_href
                link["href"] = href
                if raw_href and "raw_href" not in link:
                    link["raw_href"] = raw_href
                if not href:
                    continue
                rel = link.get("rel")
                link_type = link.get("type")
                if self._is_acquisition_link(rel, link_type):
                    acquisition_links.add(href)
                elif self._is_feed_link(rel, link_type):
                    catalog_links.add(href)

        # Include top-level feed links (next, start, etc.)
        for link_elem in tree.findall(f"{ATOM_NS}link"):
            raw_href = link_elem.get("href")
            href = urljoin(base_url, raw_href) if raw_href else raw_href
            if not href:
                continue
            rel = link_elem.get("rel")
            link_type = link_elem.get("type")
            if self._is_feed_link(rel, link_type):
                catalog_links.add(href)

        return self.ParsedFeed(
            entries=entries,
            acquisition_links=list(acquisition_links),
            catalog_links=list(catalog_links),
        )

    def _is_acquisition_link(self, rel: Optional[str], link_type: Optional[str]) -> bool:
        rel_lower = (rel or "").lower()
        type_lower = (link_type or "").lower()
        if rel_lower.startswith("http://opds-spec.org/acquisition"):
            return True
        if "application/atom+xml" in type_lower and "acquisition" in type_lower:
            return True
        if "opds-acquisition" in type_lower:
            return True
        return False

    def _is_feed_link(self, rel: Optional[str], link_type: Optional[str]) -> bool:
        rel_lower = (rel or "").lower()
        type_lower = (link_type or "").lower()

        if rel_lower in {"self"}:
            return False
        if rel_lower in {"start", "up"}:
            return True
        if rel_lower in {"next", "previous", "prev", "first"}:
            return True
        if rel_lower.startswith("http://opds-spec.org/navigation"):
            return True
        if rel_lower.startswith("http://opds-spec.org/catalog"):
            return True
        if "application/atom+xml" in type_lower and "acquisition" not in type_lower:
            return True
        if "opds-catalog" in type_lower and "acquisition" not in type_lower:
            return True
        if "type=feed" in type_lower:
            return True
        return False

    def _entry_has_acquisition(self, entry: Dict[str, Any]) -> bool:
        for link in entry.get("raw_links", []) or []:
            if self._is_acquisition_link(link.get("rel"), link.get("type")):
                return True
        return False

    def _parse_entry(self, elem: ET.Element, field_inventory: Dict[str, List[str]]) -> Dict[str, Any]:
        entry: Dict[str, Any] = {}
        entry_id = elem.findtext(f"{ATOM_NS}id")
        title = elem.findtext(f"{ATOM_NS}title")
        summary_elem = elem.find(f"{ATOM_NS}summary")
        summary = _extract_element_text(summary_elem)
        summary, rating_value, tags_value = extract_summary_metadata(summary)
        content_elem = elem.find(f"{ATOM_NS}content")
        content_value = _extract_element_text(content_elem)
        if content_value:
            cleaned_content, content_rating, content_tags = extract_summary_metadata(content_value)
            content_value = cleaned_content
            if rating_value is None and content_rating is not None:
                rating_value = content_rating
            if content_tags:
                if tags_value:
                    combined_tags = list(dict.fromkeys([*tags_value, *content_tags]))
                    tags_value = combined_tags
                else:
                    tags_value = content_tags
        language = (
            elem.findtext(f"{ATOM_NS}language")
            or elem.findtext(f"{DC_NS}language")
            or elem.findtext(f"{DCTERMS_NS}language")
        )

        entry.update(
            {
                "entry_id": entry_id,
                "id": entry_id,
                "title": title,
                "normalized_title": _normalize_title(title),
                "summary": summary,
                "content": content_value,
                "language": language,
                "authors": self._extract_authors(elem, field_inventory),
                "categories": self._extract_categories(elem, field_inventory),
                "identifiers": self._extract_identifiers(elem, field_inventory),
                "raw_links": self._extract_links(elem, field_inventory),
                "published": self._first_nonempty([
                    elem.findtext(f"{ATOM_NS}published"),
                    elem.findtext(f"{DC_NS}date"),
                    elem.findtext(f"{DCTERMS_NS}issued"),
                ]),
                "updated": self._first_nonempty([
                    elem.findtext(f"{ATOM_NS}updated"),
                    elem.findtext(f"{DCTERMS_NS}modified"),
                ]),
            }
        )

        if rating_value is not None:
            entry["rating"] = rating_value
            _record_entry_field(field_inventory, "rating")
        if tags_value:
            entry["tags"] = tags_value
            _record_entry_field(field_inventory, "tags")

        dc_publisher = elem.findtext(f"{DC_NS}publisher") or elem.findtext(f"{DCTERMS_NS}publisher")
        if dc_publisher:
            entry["dc:publisher"] = dc_publisher
            _record_entry_field(field_inventory, "dc:publisher")
        issued = elem.findtext(f"{DCTERMS_NS}issued")
        if issued:
            entry["dcterms:issued"] = issued
            _record_entry_field(field_inventory, "dcterms:issued")
        if title:
            _record_entry_field(field_inventory, "title")
        if summary:
            _record_entry_field(field_inventory, "summary")
        if content_value:
            _record_entry_field(field_inventory, "content")
        if language:
            _record_entry_field(field_inventory, "language")
        if entry_id:
            _record_entry_field(field_inventory, "id")

        return entry

    def _extract_authors(self, elem: ET.Element, field_inventory: Dict[str, List[str]]) -> List[str]:
        authors: List[str] = []
        for author in elem.findall(f"{ATOM_NS}author"):
            name = author.findtext(f"{ATOM_NS}name")
            if name:
                authors.append(name.strip())
        if authors:
            _record_entry_field(field_inventory, "authors")
        return authors

    def _extract_categories(self, elem: ET.Element, field_inventory: Dict[str, List[str]]) -> List[str]:
        categories: List[str] = []
        for cat in elem.findall(f"{ATOM_NS}category"):
            term = cat.get("term") or cat.text
            if term:
                categories.append(term.strip())
        if categories:
            _record_entry_field(field_inventory, "categories")
        return categories

    def _extract_identifiers(self, elem: ET.Element, field_inventory: Dict[str, List[str]]) -> List[str]:
        identifiers: List[str] = []
        for tag in (f"{DC_NS}identifier", f"{DCTERMS_NS}identifier"):
            for ident in elem.findall(tag):
                if ident.text:
                    identifiers.append(ident.text.strip())
        if identifiers:
            _record_entry_field(field_inventory, "identifier")
        return identifiers

    def _extract_links(self, elem: ET.Element, field_inventory: Dict[str, List[str]]) -> List[Dict[str, Any]]:
        links: List[Dict[str, Any]] = []
        rels = set(field_inventory.get("link_rels", []))
        types = set(field_inventory.get("link_types", []))
        for link in elem.findall(f"{ATOM_NS}link"):
            link_info = {
                "rel": link.get("rel"),
                "type": link.get("type"),
                "href": link.get("href"),
                "title": link.get("title"),
            }
            rel = link_info.get("rel")
            if rel:
                rels.add(rel)
            link_type = link_info.get("type")
            if link_type:
                types.add(link_type)
            links.append(link_info)
        field_inventory["link_rels"] = sorted(rels)
        field_inventory["link_types"] = sorted(types)
        return links

    def _first_nonempty(self, values: List[Optional[str]]) -> Optional[str]:
        for value in values:
            if value:
                return value
        return None

    def _suggest_mapping(self, inventory: Dict[str, List[str]]) -> Dict[str, str]:
        entry_fields = set(inventory.get("entry", []))
        link_rels = set(inventory.get("link_rels", []))

        suggestions: Dict[str, str] = {}
        if "title" in entry_fields or "dc:title" in entry_fields:
            suggestions.setdefault("title", "entry.title")
        if "summary" in entry_fields:
            suggestions.setdefault("description", "entry.summary")
        elif "content" in entry_fields:
            suggestions.setdefault("description", "entry.content")
        if "id" in entry_fields:
            suggestions.setdefault("opds_source_id", "entry.id")
        if "authors" in entry_fields:
            suggestions.setdefault("contributors.AUTHORED", "entry.authors")
        if "language" in entry_fields or "dc:language" in entry_fields:
            suggestions.setdefault("language", "entry.language")
        if "categories" in entry_fields:
            suggestions.setdefault("categories", "entry.categories")
        elif "tags" in entry_fields:
            suggestions.setdefault("categories", "entry.tags")
        if "dc:publisher" in entry_fields or "dcterms:publisher" in entry_fields or "publisher" in entry_fields:
            suggestions.setdefault("publisher", "entry.dc:publisher")
        if "dcterms:issued" in entry_fields or "published" in entry_fields:
            suggestions.setdefault("published_date", "entry.dcterms:issued")
        if "http://opds-spec.org/image" in link_rels:
            suggestions.setdefault("cover_url", "link[rel=http://opds-spec.org/image].href")
        if "http://opds-spec.org/image/thumbnail" in link_rels:
            suggestions.setdefault("cover_thumbnail", "link[rel=http://opds-spec.org/image/thumbnail].href")
        if "rating" in entry_fields:
            suggestions.setdefault("average_rating", "entry.rating")
        return suggestions

    def probe_async(self, *args: Any, **kwargs: Any) -> "asyncio.Future[Dict[str, Any]]":
        raise NotImplementedError("Use probe() coroutine instead")


opds_probe_service = OPDSProbeService()
__all__ = ["OPDSProbeService", "opds_probe_service", "extract_summary_metadata"]
