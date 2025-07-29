"""
Location management routes for bibliotheca.
"""

from flask import Blueprint, render_template, request, flash, redirect, url_for, jsonify
from flask_login import login_required, current_user
import os

from app.location_service import LocationService
from app.utils.safe_kuzu_manager import safe_get_connection

bp = Blueprint('locations', __name__, url_prefix='/locations')

def get_location_service():
    """
    Get location service instance with safe connection.
    Uses current_user.id from Flask context.
    """
    from flask_login import current_user
    return LocationService()


@bp.route('/')
@login_required
def manage_locations():
    """Manage user locations page."""
    location_service = get_location_service()
    # Get all locations, not just those with books
    locations = location_service.get_all_locations()
    
    # Get book counts for each location (filtered by user)
    book_counts = location_service.get_all_location_book_counts(str(current_user.id))
    
    # Create location data with book counts for template
    locations_with_counts = []
    for location in locations:
        location_data = {
            'location': location,
            'book_count': book_counts.get(location.id, 0) if location.id else 0
        }
        locations_with_counts.append(location_data)
    
    return render_template('locations/manage.html', 
                         locations_with_counts=locations_with_counts, 
                         book_counts=book_counts)


@bp.route('/add', methods=['GET', 'POST'])
@login_required
def add_location():
    """Add a new location."""
    if request.method == 'POST':
        location_service = get_location_service()
        name = request.form.get('name', '').strip()
        description = request.form.get('description', '').strip()
        location_type = request.form.get('location_type', 'home')
        address = request.form.get('address', '').strip()
        is_default = 'is_default' in request.form
        
        if not name:
            flash('Location name is required.', 'error')
            return redirect(url_for('locations.add_location'))
        
        try:
            location = location_service.create_location(
                name=name,
                description=description if description else None,
                location_type=location_type,
                address=address if address else None,
                is_default=is_default
            )
            flash(f'Location "{location.name}" created successfully.', 'success')
            return redirect(url_for('locations.manage_locations'))
        except Exception as e:
            flash(f'Error creating location: {str(e)}', 'error')
            return redirect(url_for('locations.add_location'))
    
    return render_template('locations/add.html')


@bp.route('/<location_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_location(location_id):
    """Edit a location."""
    location_service = get_location_service()
    location = location_service.get_location(location_id)
    
    if not location:
        flash('Location not found.', 'error')
        return redirect(url_for('locations.manage_locations'))
    
    if request.method == 'POST':
        updates = {}
        
        name = request.form.get('name', '').strip()
        if name:
            updates['name'] = name
        
        description = request.form.get('description', '').strip()
        updates['description'] = description if description else None
        
        location_type = request.form.get('location_type')
        if location_type:
            updates['location_type'] = location_type
        
        address = request.form.get('address', '').strip()
        updates['address'] = address if address else None
        
        is_default = 'is_default' in request.form
        updates['is_default'] = is_default
        
        try:
            updated_location = location_service.update_location(location_id, **updates)
            if updated_location:
                flash(f'Location "{updated_location.name}" updated successfully.', 'success')
            else:
                flash('Failed to update location.', 'error')
        except Exception as e:
            flash(f'Error updating location: {str(e)}', 'error')
        
        return redirect(url_for('locations.manage_locations'))
    
    return render_template('locations/edit.html', location=location)


@bp.route('/<location_id>/delete', methods=['POST'])
@login_required
def delete_location(location_id):
    """Delete a location."""
    location_service = get_location_service()
    location = location_service.get_location(location_id)
    
    if not location:
        flash('Location not found.', 'error')
        return redirect(url_for('locations.manage_locations'))
    
    try:
        if location_service.delete_location(location_id):
            flash(f'Location "{location.name}" deleted successfully.', 'success')
        else:
            flash('Cannot delete location. You must have at least one active location.', 'error')
    except Exception as e:
        flash(f'Error deleting location: {str(e)}', 'error')
    
    return redirect(url_for('locations.manage_locations'))


@bp.route('/api/user_locations')
@login_required
def api_user_locations():
    """API endpoint to get user locations for dropdowns."""
    location_service = get_location_service()
    # Get all available locations, not just those with books
    locations = location_service.get_all_locations()
    
    return jsonify([
        {
            'id': loc.id,
            'name': loc.name,
            'location_type': loc.location_type,
            'is_default': loc.is_default
        }
        for loc in locations
    ])


@bp.route('/<location_id>')
@login_required
def view_location(location_id):
    """View location details and books at this location."""
    location_service = get_location_service()
    location = location_service.get_location(location_id)
    
    if not location:
        flash('Location not found.', 'error')
        return redirect(url_for('locations.manage_locations'))
    
    # Get books at this location
    book_ids = location_service.get_books_at_location(location_id, str(current_user.id))
    
    # Get full book objects for display
    books = []
    if book_ids:
        from app.services import book_service
        for book_id in book_ids:
            try:
                book = book_service.get_book_by_uid_sync(book_id, str(current_user.id))
                if book:
                    books.append(book)
            except Exception as e:
                # Handle book retrieval error gracefully
                pass
    
    # Get book count
    book_count = location_service.get_location_book_count(location_id, str(current_user.id))
    
    return render_template('locations/view.html', 
                         location=location, 
                         books=books, 
                         book_count=book_count)


@bp.route('/api/set_book_location', methods=['POST'])
@login_required
def api_set_book_location():
    """API endpoint to set a book's location."""
    try:
        data = request.get_json()
        book_id = data.get('book_id')
        location_id = data.get('location_id')
        
        if not book_id:
            return jsonify({'success': False, 'error': 'Book ID is required'}), 400
        
        location_service = get_location_service()
        
        # Verify the location exists (if location_id is provided)
        if location_id:
            location = location_service.get_location(location_id)
            if not location:
                return jsonify({'success': False, 'error': 'Location not found'}), 404
        
        # Update the book's location
        result = location_service.set_book_location(book_id, location_id, str(current_user.id))
        
        if result:
            return jsonify({'success': True, 'message': 'Book location updated successfully'})
        else:
            return jsonify({'success': False, 'error': 'Failed to update book location'}), 500
        
    except Exception as e:
        return jsonify({'success': False, 'error': 'Server error'}), 500
