from flask import Blueprint, jsonify
from ..utils.safe_kuzu_manager import get_safe_kuzu_manager

# Lightweight health/introspection blueprint to validate DB without side effects

db_health = Blueprint('db_health', __name__, url_prefix='/api/db')

@db_health.get('/integrity')
def db_integrity():
    """Return basic integrity info: user/book counts and corruption flags (if any)."""
    mgr = get_safe_kuzu_manager()
    info = {
        'database_path': getattr(mgr, 'database_path', 'unknown'),
        'initialized': getattr(mgr, '_is_initialized', False),
    }
    user_count = 0
    book_count = 0
    try:
        from ..infrastructure.kuzu_graph import safe_execute_kuzu_query
        uc = safe_execute_kuzu_query("MATCH (u:User) RETURN COUNT(u) as c")
        bc = safe_execute_kuzu_query("MATCH (b:Book) RETURN COUNT(b) as c")
        def _extract(res):
            if not res: return 0
            if hasattr(res, 'has_next') and res.has_next():
                row = res.get_next()
                if isinstance(row, (list, tuple)) and row: return int(row[0])
            if isinstance(res, list) and res:
                first = res[0]
                if isinstance(first, dict):
                    for k in ('c','count','col_0'): 
                        if k in first:
                            try: return int(first[k])
                            except Exception: pass
                elif isinstance(first, (list, tuple)) and first:
                    try: return int(first[0])
                    except Exception: pass
            return 0
        user_count = _extract(uc)
        book_count = _extract(bc)
    except Exception as e:
        info['query_error'] = str(e)
    info['user_count'] = user_count
    info['book_count'] = book_count
    info['empty_database'] = (user_count == 0 and book_count == 0)
    return jsonify(info)
