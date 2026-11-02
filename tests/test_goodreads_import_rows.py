import csv
import io

from app.routes.import_format_helpers import get_goodreads_field_mappings
from app.simplified_book_service import SimplifiedBookService

HEADER = ["Book Id", "Title", "Author", "Author l-f", "Additional Authors", "ISBN", "ISBN13",
          "My Rating", "Average Rating", "Publisher", "Binding", "Number of Pages", "Year Published",
          "Original Publication Year", "Date Read", "Date Added", "Bookshelves",
          "Bookshelves with positions", "Exclusive Shelf", "My Review", "Spoiler", "Private Notes",
          "Read Count", "Owned Copies"]


def _row(**kw):
    base = dict.fromkeys(HEADER, "")
    base.update({"Book Id": "1", "Title": "Dune", "Author": "Frank Herbert", "Author l-f": "Herbert, Frank",
                 "ISBN": '="0441172717"', "ISBN13": '="9780441172719"', "My Rating": "5",
                 "Average Rating": "4.25", "Number of Pages": "604", "Year Published": "1990",
                 "Date Read": "2023/08/08", "Date Added": "2022/01/02", "Exclusive Shelf": "read",
                 "Read Count": "1", "Owned Copies": "0"})
    base.update(kw)
    text = io.StringIO()
    w = csv.DictWriter(text, fieldnames=HEADER, quoting=csv.QUOTE_ALL)
    w.writeheader()
    w.writerow(base)
    return next(csv.DictReader(io.StringIO(text.getvalue())))


def _build(row):
    svc = SimplifiedBookService.__new__(SimplifiedBookService)
    return svc.build_book_data_from_row(row, get_goodreads_field_mappings())


def test_read_row_date_and_status():
    b = _build(_row())
    assert b.reading_status == "read"
    assert b.date_read == "2023-08-08"
    assert b.isbn13 == "9780441172719"
    assert b.isbn10 == "0441172717"


def test_date_formats():
    for raw, exp in [("2023/8/8", "2023-08-08"), ("8/8/2023", "2023-08-08"), ("08/08/23", "2023-08-08"),
                     ('="2023/08/08"', "2023-08-08"), ("2023-08-08", "2023-08-08")]:
        assert _build(_row(**{"Date Read": raw})).date_read == exp, raw


def test_empty_date_and_isbn():
    b = _build(_row(**{"Date Read": "", "ISBN": '=""', "ISBN13": '=""', "Exclusive Shelf": "to-read"}))
    assert b.date_read is None and b.reading_status == "plan_to_read"
    assert not b.isbn13 and not b.isbn10


# ---------------------------------------------------------------------------
# End-to-end job tests against an in-memory stand-in for the PostgreSQL layer
# ---------------------------------------------------------------------------
import asyncio
import json
import uuid

import pytest

from app.services.postgres_service_facade import PostgresServiceFacade


class _FakeBookService:
    def __init__(self):
        self.books = {}

    def create_book_sync(self, book, user_id=None, **_kw):
        book.id = str(book.id or uuid.uuid4())
        self.books[book.id] = book
        return book

    def get_book_by_id_sync(self, book_id):
        return self.books.get(str(book_id))

    def get_book_by_isbn_sync(self, isbn):
        for b in self.books.values():
            if isbn and isbn in (b.isbn13, b.isbn10):
                return b
        return None

    def update_book_sync(self, book_id, updates):
        return self.books.get(str(book_id))

    def _populate_relationships(self, book):
        return None

    def replace_contributors_sync(self, *a, **k):
        return True

    def replace_categories_sync(self, *a, **k):
        return True

    def replace_publisher_sync(self, *a, **k):
        return True


class _FakeLibraryService:
    _ALLOWED = {"reading_status", "start_date", "finish_date", "ownership_status", "media_type",
                "location_id", "user_rating", "user_review", "personal_notes", "custom_metadata"}

    def __init__(self):
        self.rows = {}

    def get_user_book_sync(self, user_id, book_id):
        row = self.rows.get((str(user_id), str(book_id)))
        return dict(row) if row else None

    def add_book_to_user_library_sync(self, user_id, book_id, reading_status="library_only",
                                      ownership_status="owned", locations=None, custom_metadata=None):
        key = (str(user_id), str(book_id))
        if key in self.rows:
            return False
        json.dumps(custom_metadata or {})
        self.rows[key] = {"book_id": book_id, "reading_status": reading_status,
                          "ownership_status": ownership_status, "custom_metadata": dict(custom_metadata or {}),
                          "start_date": None, "finish_date": None, "user_rating": None,
                          "personal_notes": None, "user_review": None}
        return True

    def update_user_book_sync(self, user_id, book_id, updates):
        row = self.rows[(str(user_id), str(book_id))]
        changes = {name: value for name, value in updates.items() if name in self._ALLOWED}
        if "custom_metadata" in changes:
            # the real code json.dumps()es this JSONB column; a single UPDATE is atomic
            json.dumps(changes["custom_metadata"], ensure_ascii=False, separators=(",", ":"))
        row.update(changes)
        return True


class _FakeFacade(PostgresServiceFacade):
    def __init__(self):  # no database
        self.book_service = _FakeBookService()
        self.library_service = _FakeLibraryService()

    def get_book_by_title_author_sync(self, title, author):
        for b in self.book_service.books.values():
            names = [c.person.name.lower() for c in (b.contributors or [])]
            if (b.title or "").lower() == (title or "").lower() and (not author or author.lower() in names):
                return b
        return None


def _write_csv(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=HEADER, quoting=csv.QUOTE_ALL)
        w.writeheader()
        for r in rows:
            base = dict.fromkeys(HEADER, "")
            base.update(r)
            w.writerow(base)


@pytest.fixture
def pg_import(monkeypatch, tmp_path):
    import app.services as services
    import app.routes.import_routes as ir
    from app.services.personal_metadata_service import PersonalMetadataService

    facade = _FakeFacade()
    jobs = {}
    monkeypatch.setattr(services, "_get_book_service", lambda: facade)
    monkeypatch.setattr(SimplifiedBookService, "_is_postgres_backend", staticmethod(lambda: True))
    monkeypatch.setattr(PersonalMetadataService, "_postgres_library_service",
                        staticmethod(lambda: facade.library_service))
    monkeypatch.setattr(SimplifiedBookService, "__init__", lambda self: None)
    monkeypatch.setattr(ir, "safe_get_import_job", lambda u, t: jobs.get((u, t), {}))
    monkeypatch.setattr(ir, "safe_update_import_job",
                        lambda u, t, p: jobs.setdefault((u, t), {}).update(p) or True)
    monkeypatch.setattr(ir, "_append_import_error_to_log", lambda *a, **k: None)
    monkeypatch.setattr(ir, "get_default_book_format", lambda: "physical")

    async def no_custom_fields(*a, **k):
        return True, []
    monkeypatch.setattr(ir, "pre_analyze_and_create_custom_fields", no_custom_fields)
    import threading
    monkeypatch.setattr(threading, "Thread", lambda *a, **k: type("T", (), {"start": lambda self: None})())

    def run(rows=None, user_id="u1", path=None, **cfg):
        if path is None:
            path = tmp_path / f"{uuid.uuid4().hex}.csv"
            _write_csv(path, rows)
        config = {"task_id": "t1", "csv_file_path": str(path), "field_mappings": get_goodreads_field_mappings(),
                  "user_id": user_id, "enable_api_enrichment": False, "format_type": "goodreads",
                  "default_reading_status": ""}
        config.update(cfg)
        job_key = (user_id, "t1")
        jobs.pop(job_key, None)
        asyncio.run(ir.process_simple_import(config))
        return jobs[job_key]

    return facade, run


def _gr(i, **kw):
    row = {"Book Id": str(i), "Title": f"Book {i}", "Author": f"Author {i}", "Author l-f": f"{i}, Author",
           "ISBN": '=""', "ISBN13": f'="978000000{i:04d}"', "My Rating": "4", "Exclusive Shelf": "read",
           "Date Read": "2023/08/08", "Date Added": "2022/01/02", "Number of Pages": "100",
           "Year Published": "2001", "Read Count": "1", "Owned Copies": "0"}
    row.update(kw)
    return row


def test_job_sets_finish_date_for_new_books(pg_import):
    facade, run = pg_import
    job = run([_gr(1), _gr(2, **{"Date Read": "", "Exclusive Shelf": "to-read"})])
    assert job["status"] == "completed" and job["errors"] == 0
    rows = facade.library_service.rows
    assert len(rows) == 2
    read = [r for r in rows.values() if r["reading_status"] == "read"][0]
    assert read["finish_date"].date().isoformat() == "2023-08-08"


def test_job_sets_finish_date_when_book_already_exists(pg_import):
    """Book already in the shared library (other user / earlier import / duplicate ISBN)."""
    facade, run = pg_import
    run([_gr(1, **{"Date Read": "", "Exclusive Shelf": "to-read"})], user_id="other")
    job = run([_gr(1)], user_id="u1")
    assert job["errors"] == 0
    row = facade.library_service.rows[("u1", next(iter(facade.book_service.books)))]
    assert row["reading_status"] == "read"
    assert row["finish_date"].date().isoformat() == "2023-08-08"


def test_job_reimport_with_existing_dates_does_not_fail(pg_import):
    facade, run = pg_import
    run([_gr(1)])
    job = run([_gr(1, **{"Date Read": "2024/01/02"})])
    row = next(iter(facade.library_service.rows.values()))
    assert row["finish_date"].date().isoformat() == "2024-01-02"
    assert job["errors"] == 0


def test_job_reimport_row_without_date_keeps_existing_date(pg_import):
    """Existing user_books row has a datetime finish_date; a later row without Date Read must not fail."""
    facade, run = pg_import
    run([_gr(1)])
    job = run([_gr(1, **{"Date Read": "", "Exclusive Shelf": "read", "My Review": "great"})])
    assert job["errors"] == 0, job.get("error_messages")
    row = next(iter(facade.library_service.rows.values()))
    assert row["finish_date"].date().isoformat() == "2023-08-08"
    assert row["personal_notes"] == "great"  # the merge itself must not have failed silently


def test_job_imports_rating_and_review(pg_import):
    facade, run = pg_import
    run([_gr(1, **{"My Rating": "4", "My Review": "Loved it"}), _gr(2, **{"My Rating": "0"})])
    rows = {facade.book_service.books[k[1]].title: r for k, r in facade.library_service.rows.items()}
    assert rows["Book 1"]["user_rating"] == 4
    assert rows["Book 1"]["personal_notes"] == "Loved it"
    assert rows["Book 2"]["user_rating"] is None


def test_job_duplicate_rows_within_one_file(pg_import):
    facade, run = pg_import
    job = run([_gr(1), _gr(2), _gr(1, **{"Date Read": "2024/02/03", "Book Id": "99"})])
    assert job["status"] == "completed"
    assert job["processed"] == 3 and job["errors"] == 0
    assert len(facade.library_service.rows) == 2


def test_job_large_file_survives_bad_rows(pg_import):
    facade, run = pg_import
    rows = [_gr(i) for i in range(1, 301)]
    rows[10].update({"Title": "", "ISBN13": ""})
    rows[20]["Date Read"] = "not a date"
    rows[30]["ISBN13"] = '="123"'
    job = run(rows)
    assert job["status"] == "completed"
    assert job["processed"] == 300
    assert len(facade.library_service.rows) >= 298


def test_job_row_exceptions_are_isolated_and_reported(pg_import, monkeypatch):
    facade, run = pg_import
    original = facade.library_service.update_user_book_sync

    def flaky(user_id, book_id, updates):
        if facade.book_service.books[str(book_id)].title == "Book 3":
            raise RuntimeError("db hiccup")
        return original(user_id, book_id, updates)

    monkeypatch.setattr(facade.library_service, "update_user_book_sync", flaky)
    job = run([_gr(i) for i in range(1, 6)])
    assert job["status"] == "completed"
    assert job["processed"] == 5
    assert job["errors"] == 1 and job["success"] == 4
    assert job["error_messages"][0]["row_number"] == 3


def test_job_failed_merge_of_existing_book_is_an_error_not_a_success(pg_import, monkeypatch):
    facade, run = pg_import
    run([_gr(1)], user_id="other")

    def broken(user_id, book_id, updates):
        raise RuntimeError("boom")

    monkeypatch.setattr(facade.library_service, "update_user_book_sync", broken)
    job = run([_gr(1)], user_id="u1")
    assert job["errors"] == 1 and job["merged"] == 0


def test_job_survives_undecodable_bytes(pg_import, tmp_path):
    facade, run = pg_import
    rows = [_gr(i) for i in range(1, 6)]
    rows[3]["Title"] = "Café"
    path = tmp_path / "latin1.csv"
    _write_csv(path, rows)
    path.write_bytes(path.read_bytes().replace("Café".encode(), b"Caf\xe9"))
    job = run(path=path)
    assert job["status"] == "completed"
    assert job["processed"] == 5
    assert len(facade.library_service.rows) == 5
