from flask import Blueprint, jsonify, request
from app.services.cover_service import cover_service

cover_bp = Blueprint('cover_api', __name__, url_prefix='/api/cover')

@cover_bp.route('/schedule', methods=['POST'])
def schedule_cover():
    data = request.get_json(force=True, silent=True) or {}
    isbn = data.get('isbn')
    title = data.get('title')
    author = data.get('author')
    prefer = data.get('prefer_provider')
    job = cover_service.schedule_async_processing(isbn=isbn, title=title, author=author, prefer_provider=prefer)
    return jsonify({'job': job}), 202

@cover_bp.route('/status/<job_id>', methods=['GET'])
def cover_status(job_id):
    job = cover_service.get_job(job_id)
    if not job:
        return jsonify({'error': 'not_found'}), 404
    return jsonify({'job': job})
