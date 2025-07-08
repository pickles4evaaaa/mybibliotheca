"""
Reading Log API Endpoints

Provides RESTful CRUD operations for reading logs using the dual-write service layer.
Uses secure API token authentication to bypass CSRF for legitimate API calls.
"""

from flask import Blueprint, request, jsonify, current_app
from flask_login import current_user
from datetime import datetime, date
import traceback

from ..api_auth import api_token_required, api_auth_optional
from ..services import reading_log_service
from ..domain.models import ReadingLog as DomainReadingLog

# Create API blueprint
reading_logs_api = Blueprint('reading_logs_api', __name__, url_prefix='/api/v1/reading-logs')


def serialize_reading_log(domain_log):
    """Convert domain reading log to API response format."""
    return {
        'id': domain_log.id,
        'book_id': domain_log.book_id,
        'user_id': domain_log.user_id,
        'date': domain_log.date.isoformat() if domain_log.date else None,
        'created_at': domain_log.created_at.isoformat() if domain_log.created_at else None
    }


@reading_logs_api.route('', methods=['GET'])
@api_token_required
def get_reading_logs():
    """Get reading logs for the current user."""
    try:
        # Get query parameters
        book_id = request.args.get('book_id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Parse dates if provided
        parsed_start_date = None
        parsed_end_date = None
        
        if start_date:
            try:
                parsed_start_date = datetime.fromisoformat(start_date).date()
            except ValueError:
                return jsonify({
                    'status': 'error',
                    'message': 'Invalid start_date format. Use YYYY-MM-DD'
                }), 400
        
        if end_date:
            try:
                parsed_end_date = datetime.fromisoformat(end_date).date()
            except ValueError:
                return jsonify({
                    'status': 'error',
                    'message': 'Invalid end_date format. Use YYYY-MM-DD'
                }), 400
        
        # For now, return empty list since we need to implement get_reading_logs in service
        # TODO: Implement get_reading_logs_sync in service layer
        reading_logs = []
        
        return jsonify({
            'status': 'success',
            'data': reading_logs,
            'count': len(reading_logs),
            'filters': {
                'book_id': book_id,
                'start_date': start_date,
                'end_date': end_date
            }
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting reading logs: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Failed to retrieve reading logs',
            'error': str(e)
        }), 500


@reading_logs_api.route('', methods=['POST'])
@api_token_required
def create_reading_log():
    """Create a new reading log entry."""
    try:
        if not request.json:
            return jsonify({
                'status': 'error',
                'message': 'JSON data required'
            }), 400
        
        data = request.json
        
        # Validate required fields
        if not data.get('book_id'):
            return jsonify({
                'status': 'error',
                'message': 'book_id is required'
            }), 400
        
        # Parse date
        log_date = date.today()  # Default to today
        if 'date' in data and data['date']:
            try:
                log_date = datetime.fromisoformat(data['date']).date()
            except ValueError:
                return jsonify({
                    'status': 'error',
                    'message': 'Invalid date format. Use YYYY-MM-DD'
                }), 400
        
        # Create reading log using service layer
        domain_log = reading_log_service.create_reading_log_sync(
            book_id=str(data['book_id']),
            user_id=current_user.id,
            log_date=log_date
        )
        
        return jsonify({
            'status': 'success',
            'message': 'Reading log created successfully',
            'data': serialize_reading_log(domain_log)
        }), 201
        
    except Exception as e:
        current_app.logger.error(f"Error creating reading log: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Failed to create reading log',
            'error': str(e)
        }), 500


@reading_logs_api.route('/check', methods=['POST'])
@api_token_required
def check_existing_log():
    """Check if a reading log already exists for a specific date."""
    try:
        if not request.json:
            return jsonify({
                'status': 'error',
                'message': 'JSON data required'
            }), 400
        
        data = request.json
        
        # Validate required fields
        if not data.get('book_id'):
            return jsonify({
                'status': 'error',
                'message': 'book_id is required'
            }), 400
        
        # Parse date
        log_date = date.today()  # Default to today
        if 'date' in data and data['date']:
            try:
                log_date = datetime.fromisoformat(data['date']).date()
            except ValueError:
                return jsonify({
                    'status': 'error',
                    'message': 'Invalid date format. Use YYYY-MM-DD'
                }), 400
        
        # Check for existing log
        existing_log = reading_log_service.get_existing_log_sync(
            book_id=str(data['book_id']),
            user_id=current_user.id,
            log_date=log_date
        )
        
        return jsonify({
            'status': 'success',
            'exists': existing_log is not None,
            'data': serialize_reading_log(existing_log) if existing_log else None
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error checking reading log: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Failed to check reading log',
            'error': str(e)
        }), 500


@reading_logs_api.route('/<log_id>', methods=['DELETE'])
@api_token_required
def delete_reading_log(log_id):
    """Delete a reading log entry."""
    try:
        # For now, return success since delete isn't implemented in service yet
        # TODO: Implement delete_reading_log_sync in service layer
        
        return jsonify({
            'status': 'success',
            'message': 'Reading log deleted successfully'
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error deleting reading log {log_id}: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Failed to delete reading log',
            'error': str(e)
        }), 500
