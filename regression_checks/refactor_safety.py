"""Guardrails for the non-mutating structural refactor.

Run this module inside the disposable Docker test environment.  The caller
must provide ``MYBIBLIOTHECA_REFACTOR_TEST=1`` and a Kuzu path outside the
project's real ``data/`` directory.  This keeps accidental production-data
access an explicit failure instead of an implicit test assumption.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REAL_DATA_DIR = (PROJECT_ROOT / "data").resolve()
BASELINE_ROUTE_COUNT = 331
BASELINE_ROUTE_SHA256 = "4a248c61337b8581e3a2af82a7919879b297017b296083bbcbd43a481d79c95a"

COMPATIBILITY_SYMBOLS = {
    "app.routes.book_routes": (
        "book_bp",
        "fetch_book",
        "get_best_cover_for_book",
        "library",
        "search_book_details",
    ),
    "app.routes.import_routes": (
        "import_bp",
        "detect_csv_format",
        "auto_detect_fields",
        "process_simple_import",
    ),
    "app.utils.book_utils": (
        "normalize_cover_url",
        "search_book_by_title_author",
        "search_multiple_books_by_title_author",
    ),
    "app.auth": ("auth", "settings_server_partial"),
    "app.onboarding_system": ("onboarding_bp", "execute_onboarding"),
    "app.routes.stats_routes": ("stats_bp", "library_journey"),
    "app.routes.book_route_helpers": ("_normalize_personal_datetime", "_humanize_status"),
    "app.routes.import_format_helpers": ("detect_csv_format", "auto_detect_fields"),
    "app.routes.stats_helpers": ("_calculate_timeline_positions", "_build_network_data"),
    "app.utils.legacy_book_search": ("search_book_by_title_author", "normalize_goodreads_value"),
    "app.auth_settings": ("settings_server_partial",),
    "app.onboarding_state": ("get_onboarding_data", "clear_onboarding_session"),
    "app.utils.safe_kuzu_schema": ("initialize_schema",),
    "app.infrastructure.kuzu_book_repository": ("KuzuBookRepository",),
}


def _route_map(app) -> list[str]:
    return sorted(
        f"{rule.rule}|{sorted(rule.methods)}|{rule.endpoint}"
        for rule in app.url_map.iter_rules()
    )


def check_route_map() -> dict[str, object]:
    from app import create_app

    app = create_app()
    routes = _route_map(app)
    digest = hashlib.sha256("\n".join(routes).encode()).hexdigest()
    result = {
        "route_count": len(routes),
        "route_sha256": digest,
        "matches_baseline": len(routes) == BASELINE_ROUTE_COUNT
        and digest == BASELINE_ROUTE_SHA256,
    }
    if not result["matches_baseline"]:
        raise AssertionError(json.dumps(result, sort_keys=True))
    return result


def check_compatibility_imports() -> dict[str, list[str]]:
    missing: dict[str, list[str]] = {}
    for module_name, symbols in COMPATIBILITY_SYMBOLS.items():
        module = importlib.import_module(module_name)
        module_missing = [name for name in symbols if not hasattr(module, name)]
        if module_missing:
            missing[module_name] = module_missing
    if missing:
        raise AssertionError(json.dumps(missing, sort_keys=True))
    return {module_name: list(symbols) for module_name, symbols in COMPATIBILITY_SYMBOLS.items()}


def check_disposable_database_boundary() -> dict[str, str]:
    if os.environ.get("MYBIBLIOTHECA_REFACTOR_TEST") != "1":
        raise AssertionError("refactor checks require MYBIBLIOTHECA_REFACTOR_TEST=1")

    db_value = os.environ.get("KUZU_DB_PATH")
    if not db_value:
        raise AssertionError("refactor checks require an explicit KUZU_DB_PATH")
    db_path = Path(db_value).resolve()
    if db_path == REAL_DATA_DIR or REAL_DATA_DIR in db_path.parents:
        raise AssertionError(f"refactor test database is inside real data directory: {db_path}")

    data_value = os.environ.get("MYBIBLIOTHECA_REFACTOR_DATA")
    if not data_value:
        raise AssertionError("refactor checks require MYBIBLIOTHECA_REFACTOR_DATA")
    data_path = Path(data_value).resolve()
    if data_path == REAL_DATA_DIR or REAL_DATA_DIR in data_path.parents:
        raise AssertionError(f"refactor test data directory is inside real data directory: {data_path}")

    return {
        "project_root": str(PROJECT_ROOT),
        "kuzu_db_path": str(db_path),
        "test_data_dir": str(data_path),
    }


def run_checks() -> dict[str, object]:
    boundary = check_disposable_database_boundary()
    imports = check_compatibility_imports()
    routes = check_route_map()
    return {"boundary": boundary, "compatibility_imports": imports, "routes": routes}


if __name__ == "__main__":
    print(json.dumps(run_checks(), indent=2, sort_keys=True))
