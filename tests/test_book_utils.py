from io import BytesIO

from PIL import Image

from app.utils.book_utils import _cover_ahash_hex


def test_cover_hash_works_with_current_pillow_pixel_api():
    image = Image.new("RGB", (12, 18), "navy")
    encoded = BytesIO()
    image.save(encoded, format="PNG")

    cover_hash = _cover_ahash_hex(encoded.getvalue())

    assert cover_hash is not None
    assert len(cover_hash) == 64
