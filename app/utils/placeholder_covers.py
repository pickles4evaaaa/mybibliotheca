"""Utilities for generating simple placeholder cover images (e.g. for Series).

We generate a deterministic (but currently randomized color) image with the
series initials when no cover is available. Stored in the standard covers dir.
"""

from __future__ import annotations

from pathlib import Path
import uuid
from PIL import Image, ImageDraw, ImageFont, ImageOps
from flask import current_app

from .image_processing import get_covers_dir


def _load_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for p in candidates:
        fp = Path(p)
        if fp.exists():
            try:
                return ImageFont.truetype(str(fp), size=size)
            except Exception:
                continue
    return ImageFont.load_default()


def create_series_placeholder(series_name: str, series_id: str | None = None) -> str:
    """Generate a neutral grey placeholder cover with series name wrapped.

    Returns relative /covers/<file>.jpg
    """
    name = (series_name or "Untitled Series").strip()
    width, height = 600, 900
    # Neutral mid grey background; subtle border
    img = Image.new("RGB", (width, height), (110, 114, 118))
    draw = ImageDraw.Draw(img)
    # Draw border
    draw.rectangle([0,0,width-1,height-1], outline=(140,144,148))
    # Choose font size adaptively
    base_font_size = 64
    font = _load_font(base_font_size)
    # Wrap text to fit width ~80% of cover
    max_width = int(width * 0.8)
    words = name.split()
    lines = []
    current = []
    for w in words:
        test = " ".join(current + [w])
        tw, th = draw.textbbox((0,0), test, font=font)[2:4]
        if tw > max_width and current:
            lines.append(" ".join(current))
            current = [w]
        else:
            current.append(w)
    if current:
        lines.append(" ".join(current))
    # If too many lines shrink font
    while len(lines) > 6 and base_font_size > 32:
        base_font_size -= 8
        font = _load_font(base_font_size)
        lines=[]; current=[]
        for w in words:
            test = " ".join(current + [w])
            tw, th = draw.textbbox((0,0), test, font=font)[2:4]
            if tw > max_width and current:
                lines.append(" ".join(current)); current=[w]
            else:
                current.append(w)
        if current: lines.append(" ".join(current))
    total_text_height = sum(draw.textbbox((0,0), ln, font=font)[3] for ln in lines) + (len(lines)-1)*8
    start_y = (height - total_text_height)/2
    for ln in lines:
        bbox = draw.textbbox((0,0), ln, font=font)
        tw = bbox[2]-bbox[0]
        tx = (width - tw)/2
        draw.text((tx, start_y), ln, font=font, fill=(255,255,255))
        start_y += bbox[3] + 8

    covers_dir = get_covers_dir()
    filename = f"placeholder_{uuid.uuid4().hex}.jpg"
    out_path = covers_dir / filename
    try:
        img.save(out_path, format="JPEG", quality=88, optimize=True, progressive=True)
    except Exception as e:
        current_app.logger.error(f"[SERIES][PLACEHOLDER] Failed to save placeholder: {e}")
        raise
    rel = f"/covers/{filename}"
    current_app.logger.info(f"[SERIES][PLACEHOLDER] Generated {rel} for '{series_name}'")
    return rel
