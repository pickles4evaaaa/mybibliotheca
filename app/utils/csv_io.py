"""Shared CSV reading policy for every import entry point.

Import files are read in several passes (preview, format detection, row count,
row loop).  A UTF-8 BOM (Excel re-saves) is stripped and a stray non-UTF-8 byte
never aborts a job: undecodable bytes become U+FFFD instead.  Keep this module
dependency free so it can be imported from anywhere without cycles.
"""

from __future__ import annotations

CSV_ENCODING = 'utf-8-sig'
CSV_ERRORS = 'replace'


def open_csv(path):
    """Open a CSV for reading with the shared tolerant decoding policy."""
    return open(path, 'r', encoding=CSV_ENCODING, errors=CSV_ERRORS, newline='')


def count_undecodable_chars(path) -> int:
    """Number of replacement characters produced by decoding ``path``.

    Counts characters that were not valid UTF-8 (a genuine U+FFFD already in the
    file is counted too, which is acceptable for a warning).  Returns 0 on any
    I/O problem.
    """
    try:
        total = 0
        with open(path, 'r', encoding=CSV_ENCODING, errors=CSV_ERRORS, newline='') as fh:
            while True:
                chunk = fh.read(1 << 20)
                if not chunk:
                    break
                total += chunk.count('�')
        return total
    except Exception:
        return 0


def undecodable_warning(path):
    """Human readable warning if the file contained undecodable bytes, else None."""
    n = count_undecodable_chars(path)
    if not n:
        return None
    noun = 'character' if n == 1 else 'characters'
    return f"{n} {noun} could not be decoded as UTF-8 and {'was' if n == 1 else 'were'} replaced."
