"""
User Profile API Endpoints

Provides RESTful operations for user management and privacy settings.
Uses secure API token authentication to bypass CSRF for legitimate API calls.
"""

from flask import Blueprint, request, jsonify, current_app
from flask_login import current_user
from typing import Dict, Any, Optional, Union, List
import traceback

from ..api_auth import api_token_required, api_auth_optional
from ..services import user_service

# Create API blueprint
users_api = Blueprint('users_api', __name__, url_prefix='/api/v1/users')


def serialize_user_profile(user: Any, include_private: bool = False) -> Dict[str, Any]:
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
        # Ensure we have a valid authenticated user
        if not current_user.is_authenticated:
            return jsonify({
                'status': 'error',
                'message': 'User not authenticated'
            }), 401
        
        # Get the actual user object to avoid LocalProxy issues
        user = user_service.get_user_by_id_sync(current_user.id)
        if not user:
            return jsonify({
                'status': 'error',
                'message': 'User not found'
            }), 404
        
        return jsonify({
            'status': 'success',
            'data': serialize_user_profile(user, include_private=True)
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
        
        # Ensure we have a valid user object
        if not current_user.is_authenticated:
            return jsonify({
                'status': 'error',
                'message': 'User not authenticated'
            }), 401
        
        # Get the actual user object from the service to avoid LocalProxy issues
        user = user_service.get_user_by_id_sync(current_user.id)
        if not user:
            return jsonify({
                'status': 'error',
                'message': 'User not found'
            }), 404
        
        # Validate that user object has required attributes
        required_attrs = ['share_current_reading', 'share_reading_activity', 'share_library']
        missing_attrs = [attr for attr in required_attrs if not hasattr(user, attr)]
        if missing_attrs:
            current_app.logger.warning(f"User object missing attributes: {missing_attrs}")
            # Set default values for missing attributes
            for attr in missing_attrs:
                if attr == 'share_current_reading':
                    setattr(user, attr, True)
                elif attr == 'share_reading_activity':
                    setattr(user, attr, True)
                elif attr == 'share_library':
                    setattr(user, attr, False)
        
        # Update privacy settings on the actual user object
        if 'share_current_reading' in data:
            user.share_current_reading = bool(data['share_current_reading'])
        
        if 'share_reading_activity' in data:
            user.share_reading_activity = bool(data['share_reading_activity'])
        
        if 'share_library' in data:
            user.share_library = bool(data['share_library'])
        
        # Use the user service to update the user
        try:
            user_service.update_user_sync(user)
        except Exception as update_error:
            current_app.logger.error(f"Error updating user via service: {update_error}")
            # Fallback - attempt to update the current_user proxy directly
            try:
                if hasattr(current_user, 'share_current_reading') and 'share_current_reading' in data:
                    current_user.share_current_reading = bool(data['share_current_reading'])
                if hasattr(current_user, 'share_reading_activity') and 'share_reading_activity' in data:
                    current_user.share_reading_activity = bool(data['share_reading_activity'])
                if hasattr(current_user, 'share_library') and 'share_library' in data:
                    current_user.share_library = bool(data['share_library'])
            except Exception as fallback_error:
                current_app.logger.error(f"Fallback user update also failed: {fallback_error}")
                return jsonify({
                    'status': 'error',
                    'message': 'Failed to update user settings'
                }), 500
        
        return jsonify({
            'status': 'success',
            'message': 'Profile updated successfully',
            'data': serialize_user_profile(user, include_private=True)
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
    """
    Get a user's library if sharing is enabled.
    
    Returns comprehensive book data including:
    - Universal book fields (title, authors, isbn, etc.)
    - User-specific personal metadata overlay (reading_status, ownership_status, etc.)
    - Custom metadata (global + personal)
    
    Field mapping aligns with COMPREHENSIVE_FIELD_DOCUMENTATION.md:
    - Book Table: Universal book metadata (title, isbn, description, etc.)
    - Personal Metadata (HAS_PERSONAL_METADATA): user-specific data (reading_status, ownership_status, user_rating, etc.)
    - Custom fields: global + personal custom metadata
    """
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
    # Returns books with user overlay data from personal metadata relationships
        from ..services import book_service
        domain_books = book_service.get_all_books_with_user_overlay_sync(str(user_id))
        
        # Convert to API response format using comprehensive field mapping
        books_data = []
        for book in domain_books:
            book_data = serialize_book_for_api(book)
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


def serialize_book_for_api(book: Union[Dict[str, Any], Any]) -> Dict[str, Any]:
    """
    Convert book object to API response format.
    
    Handles both dict and object types from service layer.
    Aligns with comprehensive field documentation:
    - Universal book fields (Book)
    - Personal overlay fields (HAS_PERSONAL_METADATA)
    - Custom metadata fields
    """
    if isinstance(book, dict):
        return _serialize_book_dict(book)
    else:
        return _serialize_book_object(book)


def _serialize_book_dict(book: Dict[str, Any]) -> Dict[str, Any]:
    """Serialize book dictionary to API format."""
    return {
        # Universal book fields (Book table)
        'id': book.get('id'),
        'title': book.get('title'),
        'subtitle': book.get('subtitle'),
        'authors': book.get('authors', []) if isinstance(book.get('authors'), list) else [],
        'isbn': book.get('isbn13') or book.get('isbn10') or book.get('isbn'),
        'description': book.get('description'),
        'publisher': book.get('publisher'),
        'published_date': book.get('published_date'),
        'page_count': book.get('page_count'),
        'language': book.get('language'),
        'cover_url': book.get('cover_url'),
        'average_rating': book.get('average_rating'),
        'categories': book.get('categories', []),
        'series': book.get('series'),
        'series_volume': book.get('series_volume'),
        'created_at': _format_datetime(book.get('created_at')),
        
    # User-specific fields (personal metadata overlay)
        'reading_status': book.get('reading_status'),
        'ownership_status': book.get('ownership_status'),
        'media_type': book.get('media_type'),
        'user_rating': book.get('user_rating'),
        'personal_notes': book.get('personal_notes'),
        'review': book.get('review'),
        'start_date': _format_datetime(book.get('start_date')),
        'finish_date': _format_datetime(book.get('finish_date')),
        'date_added': _format_datetime(book.get('date_added')),
        'location_id': book.get('location_id'),
        'custom_metadata': book.get('custom_metadata', {})
    }


def _serialize_book_object(book: Any) -> Dict[str, Any]:
    """Serialize book object to API format."""
    return {
        # Universal book fields (Book table)
        'id': getattr(book, 'id', None),
        'title': getattr(book, 'title', None),
        'subtitle': getattr(book, 'subtitle', None),
        'authors': _format_authors(getattr(book, 'authors', [])),
        'isbn': getattr(book, 'isbn13', None) or getattr(book, 'isbn10', None),
        'description': getattr(book, 'description', None),
        'publisher': getattr(book, 'publisher', None),
        'published_date': getattr(book, 'published_date', None),
        'page_count': getattr(book, 'page_count', None),
        'language': getattr(book, 'language', None),
        'cover_url': getattr(book, 'cover_url', None),
        'average_rating': getattr(book, 'average_rating', None),
        'categories': getattr(book, 'categories', []),
        'series': getattr(book, 'series', None),
        'series_volume': getattr(book, 'series_volume', None),
        'created_at': _format_datetime(getattr(book, 'created_at', None)),
        
    # User-specific fields (personal metadata overlay)
        'reading_status': getattr(book, 'reading_status', None),
        'ownership_status': getattr(book, 'ownership_status', None),
        'media_type': getattr(book, 'media_type', None),
        'user_rating': getattr(book, 'user_rating', None),
        'personal_notes': getattr(book, 'personal_notes', None),
        'review': getattr(book, 'review', None),
        'start_date': _format_datetime(getattr(book, 'start_date', None)),
        'finish_date': _format_datetime(getattr(book, 'finish_date', None)),
        'date_added': _format_datetime(getattr(book, 'date_added', None)),
        'location_id': getattr(book, 'location_id', None),
        'custom_metadata': getattr(book, 'custom_metadata', {})
    }


def _format_authors(authors: List[Any]) -> List[str]:
    """Format authors list for API response."""
    if not authors:
        return []
    
    formatted_authors = []
    for author in authors:
        if isinstance(author, dict):
            formatted_authors.append(author.get('name', str(author)))
        elif hasattr(author, 'name'):
            formatted_authors.append(author.name)
        else:
            formatted_authors.append(str(author))
    
    return formatted_authors


def _format_datetime(dt: Any) -> Optional[str]:
    """Format datetime object to ISO string."""
    if not dt:
        return None
    
    try:
        if hasattr(dt, 'isoformat'):
            return dt.isoformat()
        elif isinstance(dt, str):
            return dt
        else:
            return None
    except (AttributeError, ValueError):
        return None
