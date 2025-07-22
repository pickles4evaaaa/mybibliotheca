# MyBibliotheca - AI Coding Instructions

## 🏗️ Architecture Overview

MyBibliotheca is a **graph-native personal library management system** built with Flask and KuzuDB. The architecture follows domain-driven design with clear service boundaries.

### Core Components
- **KuzuDB**: Single graph database instance (no horizontal scaling due to KuzuDB limitations)
- **Services Layer**: Decomposed services with async/sync dual interfaces via `KuzuAsyncHelper`
- **Domain Models**: Rich models in `app/domain/models.py` with validation logic
- **Repository Pattern**: Graph-aware repositories in `app/infrastructure/kuzu_repositories.py`
- **Service Facade**: `KuzuServiceFacade` provides backward compatibility during migration

## 🔧 Development Patterns

### Database Access Pattern
```python
# Always use the singleton pattern for KuzuDB
from app.infrastructure.kuzu_graph import get_graph_storage, get_kuzu_database
storage = get_graph_storage()  # Single global instance
```

### Async/Sync Bridge Pattern
```python
# Services support both sync and async interfaces
from app.services.kuzu_async_helper import run_async

# In services
async def async_method(self): pass
def sync_method(self): return run_async(self.async_method())
```

### Service Import Pattern
```python
# Use lazy service instances from services.__init__.py
from app.services import book_service, user_service
result = book_service.get_book(book_id)  # Auto-initializes on first use
```

## ⚙️ Critical Development Constraints

### KuzuDB Limitations
- **Single Worker Only**: `WORKERS=1` in production (KuzuDB doesn't support concurrent access)
- **Connection Singleton**: Use `get_kuzu_database()` for single global instance
- **Schema Changes**: Require careful migration via `_initialize_schema()`
- **Relationship Cleanup**: Always use `DETACH DELETE` to remove nodes with relationships

### Background Processing
- Import operations use threading with progress tracking in `import_jobs` dict
- Long-running tasks store status in KuzuDB via `store_job_in_kuzu()`
- Progress endpoints: `/api/import/progress/<task_id>` for real-time updates

## 🚀 Key Developer Workflows

### Running the Application
```bash
# Docker (recommended - handles KuzuDB properly)
docker-compose up -d

# Local development (requires careful setup)
.venv/bin/python run.py
```

### Testing
```bash
# Run with pytest configuration from pytest.ini
pytest tests/ -v
pytest -m "unit" --tb=short  # Unit tests only
```

### Database Debug/Reset
```bash
# Force schema reset (DANGER: destroys all data)
KUZU_FORCE_RESET=true python run.py

# Debug mode for verbose KuzuDB logging
KUZU_DEBUG=true python run.py
```

## 📁 Critical Files to Understand

### Core Infrastructure
- `app/infrastructure/kuzu_graph.py` - Graph database layer with schema definition
- `app/services/__init__.py` - Lazy service initialization pattern
- `app/services/kuzu_service_facade.py` - Backward compatibility facade
- `app/services/kuzu_async_helper.py` - Async/sync bridge utilities

### Domain Logic
- `app/domain/models.py` - Rich domain models with validation
- `app/routes/import_routes.py` - Complex CSV import with background processing
- `app/routes/book_routes.py` - Core book management routes

### Templates Architecture
- `app/templates/view_book_enhanced.html` - Dynamic AJAX forms with contributor management
- `app/templates/**/import_*_progress.html` - Real-time progress tracking UIs

## 🐛 Common Debugging Patterns

### KuzuDB Connection Issues
- Check for `.lock` files in `data/kuzu/` directory
- Verify single connection pattern usage
- Use `KUZU_DEBUG=true` for verbose logging

### Import System Debugging
- Background imports run in threads - check `import_jobs` dict for status
- Progress tracked via `/api/import/progress/<task_id>` endpoints  
- Custom fields auto-creation happens in `pre_analyze_and_create_custom_fields()`

### Service Integration Issues
- Use `KuzuServiceFacade` for backward compatibility during service migration
- Check async/sync method pairing in services
- Verify lazy initialization in service imports

## 🔒 Security & Production Notes

- **Secret Keys**: Generate with `secrets.token_urlsafe(32)` in production
- **Multi-user Isolation**: User data isolated via relationship queries, not schema
- **CSRF**: All forms need CSRF tokens via `csrf_token()` template function
- **File Uploads**: Temporary files in import system need cleanup in `finally` blocks

## 🎯 When Adding New Features

1. **New Entities**: Add to `app/domain/models.py` → repository → service → routes
2. **Database Schema**: Modify `_initialize_schema()` in `kuzu_graph.py`
3. **Background Jobs**: Follow import system pattern with progress tracking
4. **AJAX Forms**: Use contributor/category management patterns from `view_book_enhanced.html`
5. **Service Methods**: Provide both async and sync versions using `run_async()`
