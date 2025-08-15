"""
Routes for custom metadata management.

Provides endpoints for managing custom field definitions and import mapping templates.
"""

from flask import Blueprint, render_template, request, redirect, url_for, jsonify, flash, current_app
from flask_login import login_required, current_user
from datetime import datetime
import uuid

from .domain.models import CustomFieldDefinition, ImportMappingTemplate, CustomFieldType
from .services import custom_field_service, import_mapping_service

metadata_bp = Blueprint('metadata', __name__, url_prefix='/metadata')


@metadata_bp.route('/')
@login_required
def index():
    """Metadata management dashboard."""
    try:
        # Get user's custom fields with calculated usage
        user_fields = custom_field_service.get_user_fields_with_calculated_usage_sync(current_user.id)
        if user_fields is None:
            user_fields = []
        # Shareable concept removed; no separate popular fields
        popular_fields = []
        
        # Get user's import templates
        import_templates = import_mapping_service.get_user_templates_sync(current_user.id)
        if import_templates is None:
            import_templates = []
        
        return render_template(
            'metadata/index.html',
            user_fields=user_fields,
            popular_fields=popular_fields,
            import_templates=import_templates
        )
    except Exception as e:
        flash(f'Error loading metadata: {str(e)}', 'error')
        return redirect(url_for('main.index'))


@metadata_bp.route('/fields')
@login_required
def fields():
    """Custom fields management page."""
    try:
        # Get user's custom fields with calculated usage
        user_fields = custom_field_service.get_user_fields_with_calculated_usage_sync(current_user.id)
        if user_fields is None:
            user_fields = []
        # Shareable fields removed
        shareable_fields = []
        
        return render_template(
            'metadata/fields.html',
            user_fields=user_fields,
            shareable_fields=shareable_fields,
            field_types=CustomFieldType
        )
    except Exception as e:
        flash(f'Error loading custom fields: {str(e)}', 'error')
        return redirect(url_for('metadata.index'))


@metadata_bp.route('/fields/create', methods=['GET', 'POST'])
@login_required
def create_field():
    """Create a new custom field definition."""
    if request.method == 'POST':
        try:
            # Get form data
            name = request.form.get('name', '').strip()
            display_name = request.form.get('display_name', '').strip()
            field_type = request.form.get('field_type', 'text')
            description = request.form.get('description', '').strip()
            # is_shareable removed (all definitions visible)
            is_shareable = False
            is_global = request.form.get('is_global') == 'on'
            
            current_app.logger.info(f"ℹ️ [METADATA_ROUTES] Received request to create custom field '{name}' for user {current_user.id}")

            # Validation
            if not name or not display_name:
                flash('Name and Display Name are required', 'error')
                return redirect(request.url)
            
            # Check if field name already exists for this user
            existing_fields = custom_field_service.get_user_fields_sync(current_user.id)
            if existing_fields is None:
                existing_fields = []
            if any(field.get('name') == name for field in existing_fields):
                flash('A field with this name already exists', 'error')
                return redirect(request.url)
            
            current_app.logger.info(f"ℹ️ [METADATA_ROUTES] Validation passed for custom field '{name}'. Creating definition.")

            # Create field definition
            field_def = CustomFieldDefinition(
                id=str(uuid.uuid4()),
                name=name,
                display_name=display_name,
                field_type=CustomFieldType(field_type),
                description=description if description else None,
                created_by_user_id=current_user.id,
                is_shareable=is_shareable,
                is_global=is_global
            )
            
            # Handle field type specific configuration
            if field_type in ['rating_5', 'rating_10']:
                rating_max = 5 if field_type == 'rating_5' else 10
                field_def.rating_max = rating_max
                
                # Get custom rating labels if provided
                rating_labels = {}
                for i in range(1, rating_max + 1):
                    label = request.form.get(f'rating_label_{i}', '').strip()
                    if label:
                        rating_labels[i] = label
                field_def.rating_labels = rating_labels
            
            elif field_type in ['list', 'tags']:
                predefined_options = request.form.get('predefined_options', '').strip()
                if predefined_options:
                    field_def.predefined_options = [opt.strip() for opt in predefined_options.split(',') if opt.strip()]
                field_def.allow_custom_options = request.form.get('allow_custom_options') == 'on'
            
            # Set additional properties
            default_value = request.form.get('default_value', '').strip()
            if default_value:
                field_def.default_value = default_value
                
            placeholder_text = request.form.get('placeholder_text', '').strip()
            if placeholder_text:
                field_def.placeholder_text = placeholder_text
                
            help_text = request.form.get('help_text', '').strip()
            if help_text:
                field_def.help_text = help_text
            
            current_app.logger.info(f"ℹ️ [METADATA_ROUTES] Custom field definition for '{name}' created. Saving...")
            
            # Check if custom field service is available (not a stub)
            if hasattr(custom_field_service, '__class__') and 'StubService' in str(custom_field_service.__class__):
                flash('Custom fields functionality is not yet available in this version.', 'warning')
                current_app.logger.warning(f"⚠️ [METADATA_ROUTES] Custom field service is not implemented (stub service)")
                return redirect(url_for('metadata.fields'))
            
            # Save field definition - convert field_def to dict for service call
            field_data = {
                'id': field_def.id,
                'name': field_def.name,
                'display_name': field_def.display_name,
                'field_type': field_def.field_type.value if hasattr(field_def.field_type, 'value') else str(field_def.field_type),
                'description': field_def.description,
                'is_global': field_def.is_global,
                'is_shareable': False,
                'default_value': getattr(field_def, 'default_value', None),
                'placeholder_text': getattr(field_def, 'placeholder_text', None),
                'help_text': getattr(field_def, 'help_text', None),
                'rating_max': getattr(field_def, 'rating_max', None),
                'rating_labels': getattr(field_def, 'rating_labels', {}),
                'predefined_options': getattr(field_def, 'predefined_options', []),
                'allow_custom_options': getattr(field_def, 'allow_custom_options', False)
            }
            
            success = custom_field_service.create_field_sync(current_user.id, field_data)
            
            if success:
                flash(f'Custom field "{display_name}" created successfully!', 'success')
                current_app.logger.info(f"✅ [METADATA_ROUTES] Successfully saved custom field '{name}' for user {current_user.id}")
            else:
                flash('Error creating custom field. Please check logs for details.', 'error')
                current_app.logger.error(f"❌ [METADATA_ROUTES] Failed to save custom field '{name}' for user {current_user.id}")

            return redirect(url_for('metadata.fields'))
            
        except Exception as e:
            current_app.logger.error(f"❌ [METADATA_ROUTES] Unhandled exception in create_field: {e}", exc_info=True)
            flash(f'Error creating custom field: {str(e)}', 'error')
            return redirect(request.url)
    
    return render_template('metadata/create_field.html', field_types=CustomFieldType)


@metadata_bp.route('/fields/<field_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_field(field_id):
    """Edit an existing custom field definition."""
    try:
        field_def = custom_field_service.get_field_by_id_sync(field_id)
        if not field_def:
            flash('Custom field not found', 'error')
            return redirect(url_for('metadata.index'))
        
        # Check ownership
        if field_def.get('created_by_user_id') != current_user.id:
            flash('You can only edit your own custom fields', 'error')
            return redirect(url_for('metadata.index'))
        
        if request.method == 'POST':
            # Get updated field data
            field_data = {
                'name': request.form.get('name', '').strip(),
                'display_name': request.form.get('display_name', '').strip(),
                'description': request.form.get('description', '').strip(),
                'field_type': request.form.get('field_type', 'text'),
                'is_global': request.form.get('is_global') == 'on'
            }
            
            # Basic validation
            if not field_data['name']:
                flash('Field name is required', 'error')
                return render_template('metadata/edit_field.html', field_def=field_def)
            
            if not field_data['display_name']:
                field_data['display_name'] = field_data['name']
            
            # Update field definition
            updated_field = custom_field_service.update_field_sync(field_id, current_user.id, field_data)
            
            if updated_field:
                flash(f'Custom field "{field_data["display_name"]}" updated successfully!', 'success')
                return redirect(url_for('metadata.index'))
            else:
                flash('Failed to update custom field', 'error')
        
        return render_template('metadata/edit_field.html', field_def=field_def)
        
    except Exception as e:
        flash(f'Error editing custom field: {str(e)}', 'error')
        return redirect(url_for('metadata.index'))


@metadata_bp.route('/fields/<field_id>/delete', methods=['POST'])
@login_required
def delete_field(field_id):
    """Delete a custom field definition."""
    try:
        field_def = custom_field_service.get_field_by_id_sync(field_id)
        if not field_def:
            flash('Custom field not found', 'error')
            return redirect(url_for('metadata.fields'))
        
        # Check ownership
        if field_def.get('created_by_user_id') != current_user.id:
            flash('You can only delete your own custom fields', 'error')
            return redirect(url_for('metadata.fields'))
        
        # Delete field definition
        success = custom_field_service.delete_field_sync(field_id, current_user.id)
        
        if success:
            flash(f'Custom field "{field_def.get("display_name", field_def.get("name"))}" deleted successfully!', 'success')
        else:
            flash('Failed to delete custom field', 'error')
        
    except Exception as e:
        flash(f'Error deleting custom field: {str(e)}', 'error')
    
    return redirect(url_for('metadata.index'))


@metadata_bp.route('/api/fields/search')
@login_required
def search_fields():
    """API endpoint to search custom fields."""
    try:
        query = request.args.get('q', '').strip()
        if not query:
            return jsonify([])
        
        # Search user's fields and shareable fields
        # TODO: Implement search_fields_sync in KuzuCustomFieldService
        # For now, return empty results
        fields = []
        # fields = custom_field_service.search_fields_sync(query, current_user.id)
        if fields is None:
            fields = []
        
        # Return simplified field data for API
        results = []
        for field in fields[:20]:  # Limit to 20 results
            results.append({
                'id': field.id,
                'name': field.name,
                'display_name': field.display_name,
                'field_type': field.field_type.value,
                'description': field.description,
                'is_shareable': field.is_shareable,
                'is_global': field.is_global,
                'created_by_me': field.created_by_user_id == current_user.id
            })
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@metadata_bp.route('/templates')
@login_required
def templates():
    """Import mapping templates management page."""
    try:
        # Get user's templates
        user_templates = import_mapping_service.get_user_templates_sync(current_user.id)
        if user_templates is None:
            user_templates = []
        
        return render_template(
            'metadata/templates.html',
            user_templates=user_templates
        )
    except Exception as e:
        flash(f'Error loading import templates: {str(e)}', 'error')
        return redirect(url_for('metadata.index'))


@metadata_bp.route('/templates/<template_id>/delete', methods=['POST'])
@login_required
def delete_template(template_id):
    """Delete an import mapping template."""
    try:
        template = import_mapping_service.get_template_by_id_sync(template_id)
        if not template:
            flash('Template not found', 'error')
            return redirect(url_for('metadata.templates'))
        
        # Prevent deletion of system templates
        if template.user_id == '__system__':
            flash('System default templates cannot be deleted', 'error')
            return redirect(url_for('metadata.templates'))
        
        # Check ownership
        if template.user_id != current_user.id:
            flash('You can only delete your own templates', 'error')
            return redirect(url_for('metadata.templates'))
        
        # Delete template
        import_mapping_service.delete_template_sync(template_id, current_user.id)
        
        flash(f'Template "{template.name}" deleted successfully!', 'success')
        
    except Exception as e:
        flash(f'Error deleting template: {str(e)}', 'error')
    
    return redirect(url_for('metadata.templates'))
