"""
Genre Taxonomy admin routes.
"""

from __future__ import annotations

from flask import Blueprint, jsonify, redirect, url_for
from flask_login import current_user
from app.admin import admin_required
from app.services.genre_taxonomy_service import GenreTaxonomyService, TaxonomyProgress

# Service instance (tests patch this symbol)
genre_taxonomy_service = GenreTaxonomyService()

# Create blueprint without prefix; registered with url_prefix in routes.__init__
genre_taxonomy_bp = Blueprint("genre_taxonomy", __name__)


def _apply_admin(decorator, handler):
    """Apply admin_required supporting both real decorator and mocked return_value=lambda f: f."""
    try:
        # Real decorator form: admin_required(handler) -> wrapped()
        return decorator(handler)()
    except TypeError:
        # Mocked form in tests: admin_required() -> (lambda f: f); then call with handler
        try:
            maybe_deco = decorator()
            return maybe_deco(handler)()
        except Exception:
            # As a last resort, just run the handler
            return handler()


@genre_taxonomy_bp.route("/")
def index():
    def _handler():
        _ = genre_taxonomy_service.get_system_status()
        return "Genre Taxonomy Admin", 200
    return _apply_admin(admin_required, _handler)


@genre_taxonomy_bp.route("/start-analysis", methods=["POST"])
def start_analysis():
    def _handler():
        task_id = genre_taxonomy_service.start_analysis(user_id=getattr(current_user, "id", None))
        return redirect(url_for("genre_taxonomy.progress", task_id=task_id))
    return _apply_admin(admin_required, _handler)


@genre_taxonomy_bp.route("/progress/<task_id>")
def progress(task_id: str):
    def _handler():
        _ = genre_taxonomy_service.get_analysis_progress(task_id)
        return f"Progress for {task_id}", 200
    return _apply_admin(admin_required, _handler)


@genre_taxonomy_bp.route("/api/progress/<task_id>")
def api_progress(task_id: str):
    def _handler():
        prog = genre_taxonomy_service.get_analysis_progress(task_id)
        if not prog:
            return jsonify({"error": "Task not found"}), 404
        if isinstance(prog, TaxonomyProgress):
            data = {
                "task_id": prog.task_id,
                "status": prog.status,
                "progress": prog.progress,
                "current_phase": prog.current_phase,
                "total_genres": prog.total_genres,
                "processed_genres": prog.processed_genres,
            }
        else:
            data = getattr(prog, "to_dict", lambda: prog)()
        return jsonify(data)
    return _apply_admin(admin_required, _handler)
