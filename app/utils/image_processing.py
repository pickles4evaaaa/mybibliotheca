from __future__ import annotations

from io import BytesIO
import ipaddress
import socket
import time
from urllib.parse import urlparse
from pathlib import Path
import uuid
from typing import Any, Dict, Optional

import requests
from PIL import Image, ImageOps
from flask import current_app


MAX_REMOTE_IMAGE_BYTES = 5 * 1024 * 1024  # 5MB safety ceiling



def _get_base_dir() -> Path:
    """Resolve the repo base dir in both Docker and local dev."""
    return Path(current_app.root_path).parent


def get_covers_dir() -> Path:
    """Return the covers directory, creating it if needed.

    Order of precedence:
    - /app/data/covers (Docker)
    - {DATA_DIR}/covers if app.config.DATA_DIR is set
    - {repo_root}/data/covers as a last resort
    """
    covers_dir = Path('/app/data/covers')
    if not covers_dir.exists():
        data_dir = getattr(current_app.config, 'DATA_DIR', None)
        if data_dir:
            covers_dir = Path(data_dir) / 'covers'
        else:
            covers_dir = _get_base_dir() / 'data' / 'covers'

    covers_dir.mkdir(parents=True, exist_ok=True)
    return covers_dir


def _choose_format(original_mode: str, original_format: str | None) -> tuple[str, str]:
    """Decide on output format and extension.

    - Prefer JPEG for photographic covers.
    - If source has alpha (RGBA/LA/P with transparency), keep PNG to preserve edges.
    """
    mode = (original_mode or '').upper()
    fmt = (original_format or '').upper() if original_format else ''

    has_alpha = 'A' in mode or mode in ('P',)
    if has_alpha:
        return 'PNG', '.png'
    # Some rare images might be line art; but default to JPEG for size/compat
    return 'JPEG', '.jpg'


def _resize_high_quality(img: Image.Image, max_w: int = 1200, max_h: int = 1800) -> Image.Image:
    """High-quality downscale using LANCZOS within a bounding box, preserving aspect ratio."""
    # Use ImageOps.contain to preserve aspect ratio and fit in bounds
    return ImageOps.contain(img, (max_w, max_h), Image.Resampling.LANCZOS)


def _prepare_image(img: Image.Image, out_fmt: str) -> Image.Image:
    """Ensure the image is in a correct mode for saving in out_fmt (handle alpha on JPEG)."""
    if out_fmt.upper() == 'JPEG':
        # Drop alpha by compositing over white background
        if img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info):
            bg = Image.new('RGB', img.size, (255, 255, 255))
            bg.paste(img.convert('RGBA'), mask=img.convert('RGBA').split()[-1])
            return bg
        # Ensure RGB for JPEG
        if img.mode not in ('RGB',):
            return img.convert('RGB')
    return img


def ensure_safe_remote_image_url(url: str) -> str:
    """Validate that a remote image URL is safe to fetch.

    - Must be http/https with hostname.
    - Host must not resolve to private, loopback, multicast, or link-local ranges.
    Raises ValueError if the URL is unsafe.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https'):
        raise ValueError("Cover URL must use http or https scheme")
    if not parsed.hostname:
        raise ValueError("Cover URL must include a hostname")

    try:
        addr_info = socket.getaddrinfo(parsed.hostname, None)
    except socket.gaierror as exc:  # pragma: no cover - resolution failure
        raise ValueError(f"Unable to resolve cover host: {parsed.hostname}") from exc

    for info in addr_info:
        ip_str = info[4][0]
        ip_obj = ipaddress.ip_address(ip_str)
        if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local or ip_obj.is_multicast or ip_obj.is_reserved:
            raise ValueError("Cover URL resolves to a disallowed network range")

    return url


def process_image_bytes_and_store(image_bytes: bytes, filename_hint: str | None = None) -> str:
    """Process image bytes with LANCZOS resampling and store into covers dir.

    Returns the relative URL like "/covers/<uuid>.jpg|.png".
    """
    covers_dir = get_covers_dir()
    with Image.open(BytesIO(image_bytes)) as img:
        out_fmt, out_ext = _choose_format(img.mode, img.format)
        img_resized = _resize_high_quality(img)
        img_prepared = _prepare_image(img_resized, out_fmt)

        filename = f"{uuid.uuid4()}{out_ext}"
        out_path = covers_dir / filename

        save_kwargs = {}
        if out_fmt == 'JPEG':
            save_kwargs.update(dict(quality=92, optimize=True, progressive=True, subsampling=0))
        elif out_fmt == 'PNG':
            save_kwargs.update(dict(optimize=True))

        img_prepared.save(out_path, format=out_fmt, **save_kwargs)

    return f"/covers/{filename}"


def process_image_from_url(
    url: str,
    *,
    auth: Optional[Any] = None,
    headers: Optional[Dict[str, str]] = None,
) -> str:
    """Download image from URL, process and store, return relative URL.

    Adds safety to prevent deadlock when a single Gunicorn worker tries to HTTP GET its own /covers/* resource.
    If the URL points to an already-local cover (relative or loopback host + /covers/ path), we short‑circuit.
    Elevated logging uses ERROR so it appears even when LOG_LEVEL=error.
    """
    if not url:
        raise ValueError("Empty URL for cover processing")

    # Already a processed local cover path
    if url.startswith('/covers/'):
        current_app.logger.info(f"[COVER][SKIP] Already local cover path: {url}")
        return url

    parsed = urlparse(url)
    # Handle loopback self-call that would deadlock (single worker). Convert to direct file access.
    if parsed.scheme in ('http', 'https') and parsed.hostname in ('127.0.0.1', 'localhost', '0.0.0.0') and '/covers/' in parsed.path:
        # Derive filename and confirm exists
        covers_dir = get_covers_dir()
        fname = parsed.path.split('/covers/')[-1]
        local_path = covers_dir / fname
        if local_path.exists():
            current_app.logger.info(f"[COVER][SKIP] Loopback cover fetch avoided, using existing file: {local_path}")
            return f"/covers/{fname}"
        # Fall through to download if not present

    ensure_safe_remote_image_url(url)

    start_total = time.perf_counter()
    current_app.logger.info(f"[COVER][DL] Start url={url}")
    dl_start = time.perf_counter()
    # Shorter timeout to avoid long hangs; retries could be added later
    request_kwargs: Dict[str, Any] = {"timeout": 6, "stream": True}
    if auth is not None:
        request_kwargs["auth"] = auth
    if headers:
        request_kwargs["headers"] = headers
    resp = requests.get(url, **request_kwargs)
    resp.raise_for_status()
    content_length = resp.headers.get('Content-Length')
    if content_length:
        try:
            if int(content_length) > MAX_REMOTE_IMAGE_BYTES:
                resp.close()
                raise ValueError("Remote image exceeds maximum allowed size")
        except ValueError as err:
            resp.close()
            raise ValueError("Invalid Content-Length header for remote image") from err
    dl_time = time.perf_counter() - dl_start
    buf = BytesIO()
    copy_start = time.perf_counter()
    total_bytes = 0
    for chunk in resp.iter_content(chunk_size=16384):
        if not chunk:
            continue
        buf.write(chunk)
        total_bytes += len(chunk)
        if total_bytes > MAX_REMOTE_IMAGE_BYTES:
            resp.close()
            raise ValueError("Remote image download exceeded maximum allowed size")
    copy_time = time.perf_counter() - copy_start
    proc_start = time.perf_counter()
    out_url = process_image_bytes_and_store(buf.getvalue())
    proc_time = time.perf_counter() - proc_start
    total_time = time.perf_counter() - start_total
    current_app.logger.info(
        f"[COVER][TIMING] total={total_time:.3f}s download={dl_time:.3f}s copy={copy_time:.3f}s process={proc_time:.3f}s -> {out_url} src={url}"
    )
    return out_url


def process_image_from_filestorage(file_storage) -> str:
    """Process an uploaded FileStorage and store, returning the relative URL."""
    # Read all bytes (size already validated by caller)
    content = file_storage.read()
    # Reset stream position so caller can re-use if needed (not required here)
    try:
        file_storage.seek(0)
    except Exception:
        pass
    return process_image_bytes_and_store(content)
