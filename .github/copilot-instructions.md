# MyBibliotheca – GitHub Copilot Instructions

## About This Repository

**MyBibliotheca** is a self-hosted personal library and reading tracker built with Flask and KuzuDB graph database. It provides an open-source alternative to Goodreads and StoryGraph for managing your personal book collection, tracking reading progress, and generating reading statistics.

**Key Technologies:** Python 3.13+, Flask, KuzuDB (graph database), Bootstrap UI, Docker

## AI Engineering Guide

Concise, project-specific rules so an agent can contribute safely and fast. Focus on Kuzu graph patterns, lazy services, and schema preflight.

### Pattern Example
```python
def sync_method(self): return run_async(self.async_method())
```

### 1. Core Architecture (Graph + Services)
* Single KuzuDB instance (no horizontal scaling). Always assume WORKERS=1.
* Schema is created/augmented in `app/infrastructure/kuzu_graph.py::_initialize_schema()`; only additive changes are automatic unless `KUZU_FORCE_RESET=true`.
* Domain-first: entities in `app/domain/models.py` (dataclasses, enums for ReadingStatus, OwnershipStatus, etc.). Keep persistence concerns out of domain models.
* Service layer: lazy singletons exposed in `app/services/__init__.py` (e.g. `from app.services import book_service, user_service`). First attribute access triggers initialization and optional one-time migrations.
* Backward compatibility: `KuzuServiceFacade` is the abstraction for book-related operations during migration—prefer calling through facade unless extending a clearly separated service.

### 2. Service & Async Pattern
* Provide both async + sync variants: implement `async def _op(...)` then thin sync wrapper using `run_async` from `kuzu_async_helper`.
* Do NOT open new DB connections inside services; rely on existing singleton helpers (search for existing patterns before adding new factories).
* When adding a new service: place file in `app/services/`, expose a lazy getter + `_LazyService` entry in `services/__init__.py`, and add to `__all__`.

### 3. Schema & Data Evolution
* Add new node/rel properties by editing creation lists in `_initialize_schema()`; for existing deployments, code defensively: attempt access → on failure issue `ALTER TABLE ... ADD` (see Person / ReadingLog upgrade examples).
* Never drop/rename automatically. Write explicit one-off migration helpers or guarded env-flag paths.
* Use `KUZU_DEBUG=true` to surface file and schema introspection logging; `KUZU_FORCE_RESET=true` only for destructive resets (warn loudly in PRs).
* Master schema preflight: `app/schema/master_schema.json` (if present) is treated as declarative expectation. Startup preflight (invoked early via `schema_preflight_state` files) computes missing columns / relationships and applies only additive operations (CREATE / ALTER ADD). Environment flags: `DISABLE_SCHEMA_PREFLIGHT`, `PREFLIGHT_REL_ONLY`, `PREFLIGHT_NODES_ONLY`, `SKIP_PREFLIGHT_BACKUP`. A backup is created unless skipped. After successful application expect log line: `Schema preflight upgrade complete`.
* When introducing new functionality that needs columns: (1) Add to `master_schema.json` (increment version) (2) Add same column in `_initialize_schema()` create list (ensures greenfield DBs match) (3) Optionally add a defensive lazy `ALTER` try/except near first access to smooth upgrades if preflight was disabled.
* Avoid logic that assumes column presence without fallback; pattern: try query referencing column → on exception containing `Cannot find property <col>` → run `ALTER TABLE <Node> ADD <col> TYPE` → retry.

### 4. Graph Usage & Integrity
* Prefer relationship-based queries (e.g., user isolation via traversing OWNS rather than filtering raw properties).
* When deleting nodes with edges, use `DETACH DELETE` (or explicit pattern) to avoid orphaned rels.
* Maintain bidirectional semantics explicitly: e.g., `WRITTEN_BY` (Book→Person) plus `AUTHORED` (Person→Book). Follow established naming if adding new directional pairs.

### 4a. Concurrency & Safe Kuzu Access
* Use `SafeKuzuManager` (`app/utils/safe_kuzu_manager.py`) for thread-safe connection-per-operation access—do NOT resurrect raw global singletons.
* Pattern: `from app.utils.safe_kuzu_manager import safe_execute_query, safe_get_connection`.
	```python
	from app.utils.safe_kuzu_manager import safe_execute_query, safe_get_connection
	rows = safe_execute_query("MATCH (b:Book) RETURN COUNT(b) AS c", operation="count_books")
	with safe_get_connection(operation="batch_import") as conn:
			conn.execute("CREATE (t:Temp {id: $id})", {"id": some_id})
	```
* Each call acquires lock → initializes DB once → creates short‑lived `kuzu.Connection` → closes deterministically; this lowers corruption risk while allowing moderate intra-process concurrency (threads / Flask requests).
* For multi-query sequences needing consistency, wrap in one `with safe_get_connection(...)` rather than multiple helper calls to minimize lock churn.
* Backups: acquire quiesced window via `quiesce_for_backup()` (see `SimpleBackupService`)—avoid ad hoc pausing.
* Integrity / recovery env flags: `KUZU_RECOVERY_MODE` = FAIL_FAST | SOFT_RENAME | CLEAR_REBUILD (legacy flags still mapped). Log anomalies in `logs/corruption_events.log`.
* Slow query + debug instrumentation via `KUZU_QUERY_LOG=true` and `KUZU_SLOW_QUERY_MS` (default 150ms) for performance tuning—keep disabled in hot paths unless investigating.

### 5. Import & Background Jobs
* CSV/import flows use threaded jobs with in-memory `import_jobs` plus persisted status (`ImportJob` / `ImportTask` nodes). Update both for long-running tasks.
* Progress endpoint pattern: `/api/import/progress/<task_id>` returns structured status; update `processed`, `success`, `errors`, `current_book`, and append to `recent_activity`.
* Custom field auto-creation logic lives in pre-analysis steps (`pre_analyze_and_create_custom_fields()`); reuse rather than re-implement.

### 6. Adding Features Safely
1. Define/extend dataclass in `domain/models.py` (keep timestamps UTC via `now_utc`).
2. Extend schema (additive) in `_initialize_schema()` + optional lazy `ALTER` safeguard.
3. Create service (async + sync) and register lazy accessor.
4. Add routes under `app/routes/` mirroring existing naming (`*_routes.py`). Use services—do not query Kuzu directly from routes.
5. Update templates with CSRF tokens. Follow dynamic component patterns from `view_book_enhanced.html` for interactive pieces.

### 7. Testing & Local Dev
* Run `pytest -m unit` for quick cycles; full suite with `pytest tests/ -v`.
* For schema experiments create throwaway branch; never commit with `KUZU_FORCE_RESET` logic enabled by default.
* Docker path for DB: `/app/data/kuzu/bibliotheca.db`; local dev path defaults to `data/kuzu/bibliotheca.db`.

### 8. Debug Playbook
* Connection issues: check for lock files in `data/kuzu/`; enable `KUZU_DEBUG=true`.
* Missing column errors: replicate pattern used to add Person or ReadingLog columns (try access → `ALTER TABLE` inside guarded exception).
* Stalled import: inspect in-memory registry then query `MATCH (j:ImportJob {id: '<id>'}) RETURN j` for persisted record.

### 9. Security & Multi-User
* Isolation via relationships—never return a Book unless verified via an owning `OWNS` relation for requesting user (mirror existing route/service checks).
* Enforce CSRF tokens for all POST forms; reuse helper injection from `template_context`.
* Keep password operations inside auth/user services; do not re-hash ad hoc.

### 10. PR & Code Style Expectations
* Keep new instructions minimal; no broad refactors bundled with feature change.
* Avoid introducing alternative patterns (e.g., a second async wrapper model). Extend existing abstractions.
* Provide migration notes in PR description if schema touched: list new properties & relationships.

### 11. Common Gotchas
* Multiple service instantiation → subtle double-migration: always import from `app.services` module root.
* Forgetting sync wrapper → UI routes blocking on coroutine objects.
* Direct DB access in templates/routes → breaks abstraction & testability.
* Destructive schema ops committed by accident—never include `DROP TABLE` outside explicit `force_reset` branch.

### 12. Quick Reference Imports
```python
from app.services import book_service, user_service, person_service
book = book_service.get_book(book_id)  # lazy init
logs = user_service.list_reading_logs(user_id)
```

Keep this file lean: update when patterns materially change. If unsure, search existing usage before adding a new abstraction.
