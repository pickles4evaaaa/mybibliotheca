import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.infrastructure import persistence_state

persistence_state.get_persistence_state = lambda: {"backend": "kuzu", "migration_run_id": None}

import re  # noqa: E402

from app.routes.import_format_helpers import detect_csv_format  # noqa: E402
from app.utils import csv_io  # noqa: E402

GOODREADS = (
    "Book Id,Title,Author,My Rating,Exclusive Shelf,Bookshelves,Date Read,Date Added,Binding\n"
)


def _write(tmp_path, body: bytes, name="x.csv"):
    p = tmp_path / name
    p.write_bytes(body)
    return str(p)


def test_detect_format_survives_bad_bytes(tmp_path):
    path = _write(tmp_path, (GOODREADS).encode() + b"1,Caf\xe9,Me,5,read,,,,\n")
    fmt, _ = detect_csv_format(path)
    assert fmt == "goodreads"


def test_detect_format_header_bad_bytes(tmp_path):
    path = _write(tmp_path, b"Book Id,Title,Auth\xe9r,My Rating,Exclusive Shelf,Bookshelves\n1,a,b,5,read,\n")
    fmt, _ = detect_csv_format(path)
    assert fmt == "goodreads"


def test_undecodable_warning_counts():
    pass


def test_undecodable_warning(tmp_path):
    clean = _write(tmp_path, b"a,b\n1,2\n", "c.csv")
    bad = _write(tmp_path, b"a,b\n1,Caf\xe9\xff\n", "b.csv")
    assert csv_io.undecodable_warning(clean) is None
    assert "2 characters could not be decoded" in csv_io.undecodable_warning(bad)


def test_bom_stripped(tmp_path):
    p = _write(tmp_path, b"\xef\xbb\xbfTitle,Author\nx,y\n")
    with csv_io.open_csv(p) as fh:
        assert fh.readline().startswith("Title")


def test_no_strict_utf8_csv_opens_remain():
    root = Path(__file__).resolve().parent.parent / "app"
    offenders = []
    for rel in ("routes/import_routes.py", "routes/import_format_helpers.py", "onboarding_system.py"):
        for n, line in enumerate((root / rel).read_text().splitlines(), 1):
            if re.search(r"open\((temp_path|csv_file_path)[^)]*encoding='utf-8'\)", line):
                offenders.append(f"{rel}:{n}")
    assert not offenders, offenders
