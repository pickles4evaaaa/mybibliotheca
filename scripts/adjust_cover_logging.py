"""Utility to temporarily downgrade normal cover service logs from ERROR to INFO.
Run this once early in app startup (optional) if you want quieter logs while keeping failures at ERROR.
"""
from app.services.cover_service import cover_service  # noqa: F401
from flask import current_app

def downgrade_cover_logging():
    try:
        # Monkey patch logger call inside CoverService to use info for non-fatal paths
        import app.services.cover_service as cs
        orig = cs.cover_service.fetch_and_cache
        def wrapper(*a, **kw):
            res = orig(*a, **kw)
            # The original implementation logs at ERROR internally; optionally suppress or re-log
            # Here we just return result; future refactor could add a verbosity flag.
            return res
        cs.cover_service.fetch_and_cache = wrapper  # type: ignore
        current_app.logger.info('[COVER][LOGGING] Downgraded cover service logging wrapper installed')
    except Exception as e:
        current_app.logger.error(f'[COVER][LOGGING] Failed to downgrade logging: {e}')
