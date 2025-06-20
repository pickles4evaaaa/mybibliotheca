"""
Location management routes for bibliotheca.
"""

from flask import Blueprint, render_template, request, flash, redirect, url_for, jsonify
from flask_login import login_required, current_user
import os

from app.redis_services import RedisBookService
from app.location_service import LocationService
from app.infrastructure.redis_graph import RedisGraphConnection

bp = Blueprint('locations', __name__, url_prefix='/locations')

def get_location_service():
    """Get location service instance."""
    redis_url = os.getenv('REDIS_URL', 'redis://redis-graph:6379/0')
    connection = RedisGraphConnection(redis_url=redis_url)
    return LocationService(connection.client)


@bp.route('/')
@login_required
def manage_locations():
    """Manage user locations page."""
    location_service = get_location_service()
    locations = location_service.get_user_locations(str(current_user.id))
    return render_template('locations/manage.html', locations=locations)


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
                user_id=str(current_user.id),
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
    
    if not location or location.user_id != str(current_user.id):
        flash('Location not found or access denied.', 'error')
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
    
    if not location or location.user_id != str(current_user.id):
        flash('Location not found or access denied.', 'error')
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
    locations = location_service.get_user_locations(str(current_user.id))
    
    return jsonify([
        {
            'id': loc.id,
            'name': loc.name,
            'location_type': loc.location_type,
            'is_default': loc.is_default
        }
        for loc in locations
    ])
