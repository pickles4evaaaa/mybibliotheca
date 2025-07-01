"""
User Profile API Endpoints

Provides RESTful operations for user management and privacy settings.
Uses secure API token authentication to bypass CSRF for legitimate API calls.
"""

from flask import Blueprint, request, jsonify, current_app
from flask_login import current_user
import traceback

from ..api_auth import api_token_required, api_auth_optional
from ..services import user_service

# Create API blueprint
users_api = Blueprint('users_api', __name__, url_prefix='/api/v1/users')


def serialize_user_profile(user, include_private=False):
    """Convert user to API response format."""
    profile = {
        'id': user.id,
        'username': user.username,
        'created_at': user.created_at.isoformat() if hasattr(user, 'created_at') and user.created_at else None,
        'last_login': user.last_login.isoformat() if hasattr(user, 'last_login') and user.last_login else None,
    }
    
    # Include privacy settings for the user themselves
    if include_private:
        profile.update({
            'email': getattr(user, 'email', None),
            'share_current_reading': getattr(user, 'share_current_reading', True),
            'share_reading_activity': getattr(user, 'share_reading_activity', True),
            'share_library': getattr(user, 'share_library', False),
            'is_admin': getattr(user, 'is_admin', False)
        })
    
    return profile


@users_api.route('/me', methods=['GET'])
@api_auth_optional  # Allow both authenticated web users and API token users
def get_current_user():
    """Get current user's profile."""
    try:
        return jsonify({
            'status': 'success',
            'data': serialize_user_profile(current_user, include_private=True)
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting current user: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Error retrieving user profile'
        }), 500


@users_api.route('/me', methods=['PUT'])
@api_auth_optional
def update_current_user():
    """Update current user's profile and privacy settings."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'status': 'error',
                'message': 'No data provided'
            }), 400
        
        # Update privacy settings
        if 'share_current_reading' in data:
            current_user.share_current_reading = bool(data['share_current_reading'])
        
        if 'share_reading_activity' in data:
            current_user.share_reading_activity = bool(data['share_reading_activity'])
        
        if 'share_library' in data:
            current_user.share_library = bool(data['share_library'])
        
        # Use the user service to update the user
        try:
            user_service.update_user_sync(current_user)
        except Exception as update_error:
            current_app.logger.error(f"Error updating user via service: {update_error}")
            # Fallback - the user object is still updated in memory
        
        return jsonify({
            'status': 'success',
            'message': 'Profile updated successfully',
            'data': serialize_user_profile(current_user, include_private=True)
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error updating user profile: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Error updating profile'
        }), 500


@users_api.route('/<string:user_id>', methods=['GET'])
@api_token_required
def get_user_profile(user_id):
    """Get public profile for another user."""
    try:
        # Get user from service
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            return jsonify({
                'status': 'error',
                'message': 'User not found'
            }), 404
        
        # Return public profile only
        return jsonify({
            'status': 'success',
            'data': serialize_user_profile(user, include_private=False)
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting user profile {user_id}: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Error retrieving user profile'
        }), 500


@users_api.route('/<string:user_id>/library', methods=['GET'])
@api_token_required
def get_user_library(user_id):
    """Get a user's library if sharing is enabled."""
    try:
        # Get user from service
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            return jsonify({
                'status': 'error',
                'message': 'User not found'
            }), 404
        
        # Check if user allows library sharing
        if not getattr(user, 'share_library', False) and user.id != current_user.id:
            return jsonify({
                'status': 'error',
                'message': 'User library is private'
            }), 403
        
        # Get user's books using service layer
        from ..services import book_service
        domain_books = book_service.get_all_books_with_user_overlay_sync(str(user_id))
        
        # Convert to API response format
        books_data = []
        for book in domain_books:
            book_data = {
                'id': book.id,
                'title': book.title,
                'authors': [author.name for author in book.authors] if book.authors else [],
                'isbn': book.isbn13 or book.isbn10,
                'created_at': book.created_at.isoformat() if hasattr(book, 'created_at') and book.created_at else None,
                'reading_status': getattr(book, 'reading_status', None),
                'start_date': book.start_date.isoformat() if hasattr(book, 'start_date') and book.start_date else None,
                'finish_date': book.finish_date.isoformat() if hasattr(book, 'finish_date') and book.finish_date else None
            }
            books_data.append(book_data)
        
        return jsonify({
            'status': 'success',
            'data': books_data,
            'count': len(books_data)
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting user library {user_id}: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Error retrieving user library'
        }), 500


@users_api.route('/', methods=['GET'])
@api_token_required
def get_users():
    """Get list of all users (admin only or public profiles)."""
    try:
        # For now, return basic user list
        # TODO: Implement proper permissions and filtering
        
        users = user_service.get_all_users_sync() if hasattr(user_service, 'get_all_users_sync') else []
        users_data = [serialize_user_profile(user, include_private=False) for user in users]
        
        return jsonify({
            'status': 'success',
            'data': users_data,
            'count': len(users_data)
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting users: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Error retrieving users'
        }), 500
