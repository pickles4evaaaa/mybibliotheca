"""
Genre/Category routes for Bibliotheca.

Provides endpoints for managing genres and categories,
similar to the person management functionality.
"""

from flask import Blueprint, current_app, render_template, request, redirect, url_for, jsonify, flash, abort
from flask_login import login_required, current_user
from datetime import datetime, timezone
import uuid
import traceback
import random

from app.services import book_service
from app.domain.models import Category, ReadingStatus

# Global helper function for dict/object attribute access
def get_attr(obj, attr, default=None):
    """Safely get attribute from dict or object"""
    if isinstance(obj, dict):
        return obj.get(attr, default)
    return getattr(obj, attr, default)

def debug_log(message, category="DEBUG"):
    """Debug logging function"""
    if current_app:
        current_app.logger.debug(f"[{category}] {message}")
    else:
        print(f"[{category}] {message}")

def generate_random_color():
    """Generate a random pleasant color for categories."""
    colors = [
        '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7',
        '#DDA0DD', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9',
        '#F8C471', '#82E0AA', '#F1948A', '#85C1E9', '#F9E79F',
        '#D2B4DE', '#AED6F1', '#A9DFBF', '#F5B7B1', '#D5DBDB',
        '#FF7675', '#74B9FF', '#00B894', '#FDCB6E', '#E17055',
        '#6C5CE7', '#FD79A8', '#00CEC9', '#55A3FF', '#A29BFE'
    ]
    return random.choice(colors)

def calculate_category_level(category, book_service):
    """Calculate the hierarchy level of a category (0 = root, 1 = first level child, etc.)"""
    if not category or not hasattr(category, 'parent_id') or not category.parent_id:
        return 0
    
    level = 0
    current_parent_id = category.parent_id
    
    # Walk up the hierarchy to count levels
    while current_parent_id:
        level += 1
        parent = book_service.get_category_by_id_sync(current_parent_id, None)
        if not parent or not hasattr(parent, 'parent_id'):
            break
        current_parent_id = parent.parent_id
        
        # Safety check to prevent infinite loops
        if level > 10:  # Reasonable max depth
            break
    
    return level

# Create the blueprint
genres_bp = Blueprint('genres', __name__)

@genres_bp.route('/')
@login_required
def index():
    """Display all categories in a paginated view."""
    try:
        # Get all categories from service
        all_categories = book_service.list_all_categories_sync()
        if all_categories is None:
            all_categories = []
        
        # Calculate book counts for each category using the working method
        def calculate_book_counts(categories):
            for cat in categories:
                cat_id = get_attr(cat, 'id')
                if cat_id:
                    # Use the same method that works in category details
                    category_books = book_service.get_books_by_category_sync(cat_id, str(current_user.id))
                    book_count = len(category_books or [])
                    
                    # Set the book count
                    if isinstance(cat, dict):
                        cat['book_count'] = book_count
                    else:
                        cat.book_count = book_count
                else:
                    if isinstance(cat, dict):
                        cat['book_count'] = 0
                    else:
                        cat.book_count = 0
        
        # Calculate book counts before processing
        calculate_book_counts(all_categories)
        
        # Convert dictionaries to objects for template compatibility
        processed_categories = []
        
        # Define CategoryObj class outside the loop
        class CategoryObj:
            def __init__(self, data):
                for key, value in data.items():
                    setattr(self, key, value)
                # Add missing properties that don't exist in DB schema but are expected by templates
                if not hasattr(self, 'parent_id'):
                    self.parent_id = None
                if not hasattr(self, 'level'):
                    self.level = 0
                if not hasattr(self, 'book_count'):
                    self.book_count = 0
                if not hasattr(self, 'children'):
                    self.children = []
        
        for cat in all_categories:
            if isinstance(cat, dict):                
                cat_obj = CategoryObj(cat)
                processed_categories.append(cat_obj)
            else:
                # Add missing properties to existing objects
                if not hasattr(cat, 'parent_id'):
                    cat.parent_id = None
                if not hasattr(cat, 'level'):
                    cat.level = 0
                if not hasattr(cat, 'book_count'):
                    cat.book_count = 0
                if not hasattr(cat, 'children'):
                    cat.children = []
                processed_categories.append(cat)
        
        # Get children for all categories
        def get_children_for_categories(categories):
            for cat in categories:
                cat_id = get_attr(cat, 'id')
                if cat_id:
                    children = book_service.get_category_children_sync(cat_id, str(current_user.id))
                    if isinstance(cat, dict):
                        cat['children'] = children or []
                    else:
                        cat.children = children or []
                else:
                    if isinstance(cat, dict):
                        cat['children'] = []
                    else:
                        cat.children = []
        
        # Populate children for all categories
        get_children_for_categories(processed_categories)
        
        # Use the processed categories
        all_categories = processed_categories

        # Sort categories by name
        all_categories.sort(key=lambda x: get_attr(x, 'name', '').lower())
        
        # Pagination parameters
        page = request.args.get('page', 1, type=int)
        per_page_param = request.args.get('per_page', '12')
        
        # Valid per page options
        valid_per_page = [6, 12, 24, 48, 96]
        
        # Handle per_page parameter
        if per_page_param == 'all':
            per_page = len(all_categories)
        else:
            try:
                per_page = int(per_page_param)
                if per_page not in valid_per_page:
                    per_page = 12  # Default fallback
            except (ValueError, TypeError):
                per_page = 12  # Default fallback
        
        # Calculate pagination
        total = len(all_categories)
        start = (page - 1) * per_page
        end = start + per_page
        categories = all_categories[start:end]
        
        # Create pagination info
        has_prev = page > 1
        has_next = end < total
        prev_num = page - 1 if has_prev else None
        next_num = page + 1 if has_next else None
        total_pages = (total + per_page - 1) // per_page if per_page > 0 else 1
        
        pagination = {
            'has_prev': has_prev,
            'has_next': has_next,
            'prev_num': prev_num,
            'next_num': next_num,
            'page': page,
            'pages': total_pages,
            'total_pages': total_pages,
            'per_page': per_page,
            'total': total,
            'total_count': total
        }
        
        # Calculate stats for the template
        root_categories = book_service.get_root_categories_sync(str(current_user.id)) or []
        
        # Calculate total category count (all categories, not just on current page)
        total_category_count = len(all_categories)
        
        # Calculate subcategories (categories with parents)
        subcategory_count = 0
        for category in all_categories:
            parent_id = get_attr(category, 'parent_id')
            if parent_id:  # Has a parent, so it's a subcategory
                subcategory_count += 1
        
        # Calculate total books across all categories (prevent double counting)
        total_book_count = 0
        counted_books = set()  # Track books to prevent double counting
        for category in all_categories:
            category_id = get_attr(category, 'id')
            if category_id:
                category_books = book_service.get_books_by_category_sync(category_id, str(current_user.id)) or []
                for book in category_books:
                    book_id = get_attr(book, 'id')
                    if book_id and book_id not in counted_books:
                        counted_books.add(book_id)
                        total_book_count += 1
        
        return render_template('genres/index.html', 
                             categories=categories, 
                             pagination=pagination,
                             valid_per_page=valid_per_page,
                             root_categories=root_categories,
                             total_book_count=total_book_count,
                             subcategory_count=subcategory_count,
                             total_category_count=total_category_count)
        
    except Exception as e:
        current_app.logger.error(f"Error loading genres index: {e}")
        flash('Error loading categories.', 'error')
        return render_template('genres/index.html', categories=[], pagination=None)

@genres_bp.route('/hierarchy')
@login_required
def hierarchy_view():
    """Display categories in a hierarchical tree view."""
    try:
        current_app.logger.info("Loading hierarchy view...")
        root_categories = book_service.get_root_categories_sync(str(current_user.id))
        current_app.logger.info(f"Got {len(root_categories) if root_categories else 0} root categories")
        
        # Function to add book counts to categories using the working method
        def add_book_counts_to_categories(categories):
            for category in categories:
                category_id = get_attr(category, 'id')
                if category_id:
                    # Use the same method that works in category details
                    category_books = book_service.get_books_by_category_sync(category_id, str(current_user.id))
                    book_count = len(category_books or [])
                    
                    if isinstance(category, dict):
                        category['book_count'] = book_count
                    else:
                        category.book_count = book_count
                else:
                    if isinstance(category, dict):
                        category['book_count'] = 0
                    else:
                        category.book_count = 0
        
        # Add book counts to root categories
        add_book_counts_to_categories(root_categories)
        
        # For each root category, we'll build the full tree structure
        def build_tree(categories, current_level=0):
            tree = []
            for category in categories:
                # Set the correct level for this category
                if isinstance(category, dict):
                    category['level'] = current_level
                else:
                    category.level = current_level
                
                category_id = get_attr(category, 'id')
                if category_id:
                    children = book_service.get_category_children_sync(category_id, str(current_user.id))
                    if children:
                        # Add book counts to children
                        add_book_counts_to_categories(children)
                        # Set children attribute safely for both dicts and objects
                        if isinstance(category, dict):
                            category['children'] = build_tree(children, current_level + 1)
                        else:
                            category.children = build_tree(children, current_level + 1)
                    else:
                        if isinstance(category, dict):
                            category['children'] = []
                        else:
                            category.children = []
                    tree.append(category)
            return tree
        
        category_tree = build_tree(root_categories, 0) if root_categories else []
        
        # Calculate hierarchy statistics
        all_categories = book_service.list_all_categories_sync() or []
        total_categories = len(all_categories)
        root_categories_count = len(root_categories) if root_categories else 0
        
        # Calculate max depth
        def get_max_depth(tree, current_depth=0):
            if not tree:
                return current_depth
            max_depth = current_depth
            for category in tree:
                children = get_attr(category, 'children', [])
                if children:
                    depth = get_max_depth(children, current_depth + 1)
                    max_depth = max(max_depth, depth)
            return max_depth
        
        max_depth = get_max_depth(category_tree)
        
        # Calculate total books across all categories
        all_categories = book_service.list_all_categories_sync() or []
        total_books = 0
        for category in all_categories:
            category_id = get_attr(category, 'id')
            if category_id:
                category_books = book_service.get_books_by_category_sync(category_id, str(current_user.id))
                total_books += len(category_books or [])
        
        hierarchy_stats = {
            'total_categories': total_categories,
            'root_categories': root_categories_count,
            'max_depth': max_depth,
            'total_books': total_books
        }
        
        current_app.logger.info(f"Hierarchy stats: {hierarchy_stats}")
        
        return render_template('genres/hierarchy.html', 
                             category_tree=category_tree,
                             root_categories=category_tree,  # Also pass as root_categories for template compatibility
                             hierarchy_stats=hierarchy_stats)
        
    except Exception as e:
        current_app.logger.error(f"Error loading hierarchy view: {e}")
        current_app.logger.error(traceback.format_exc())
        flash('Error loading hierarchy view.', 'error')
        return redirect(url_for('genres.index'))

@genres_bp.route('/merge', methods=['GET', 'POST'])
@login_required
def merge_categories():
    """Merge two or more categories into one."""
    from ..forms import MergeCategoriesForm
    
    if request.method == 'GET':
        try:
            current_app.logger.info("Loading merge categories page...")
            form = MergeCategoriesForm()
            all_categories = book_service.list_all_categories_sync()
            if all_categories is None:
                all_categories = []
            all_categories.sort(key=lambda c: get_attr(c, 'name', '').lower())
            current_app.logger.info(f"Rendering merge page with form and {len(all_categories)} categories")
            return render_template('genres/merge.html', categories=all_categories, form=form)
        
        except Exception as e:
            current_app.logger.error(f"Error loading merge categories page: {e}")
            current_app.logger.error(traceback.format_exc())
            flash('Error loading merge page.', 'error')
            return redirect(url_for('genres.index'))
    
    # POST - perform merge
    try:
        form = MergeCategoriesForm()
        if form.validate_on_submit():
            primary_category_id = form.target_id.data
            merge_category_ids = [form.source_id.data]
            
            if not primary_category_id:
                flash('Please select a target category.', 'error')
                return redirect(url_for('genres.merge_categories'))
            
            if not merge_category_ids or not merge_category_ids[0]:
                flash('Please select a source category to merge.', 'error')
                return redirect(url_for('genres.merge_categories'))
            
            # Get category names for confirmation message
            primary_category = book_service.get_category_by_id_sync(primary_category_id, None)
            merge_categories = [book_service.get_category_by_id_sync(cat_id, None) for cat_id in merge_category_ids]
            merge_categories = [cat for cat in merge_categories if cat]  # Filter out None values
            
            if not primary_category:
                flash('Target category not found.', 'error')
                return redirect(url_for('genres.merge_categories'))
            
            # Perform merge
            success = book_service.merge_categories_sync(primary_category_id, merge_category_ids)
            
            if success:
                merge_names = [get_attr(cat, 'name', 'Unknown') for cat in merge_categories]
                primary_name = get_attr(primary_category, 'name', 'Unknown')
                flash(f'Successfully merged "{merge_names[0]}" into "{primary_name}".', 'success')
                return redirect(url_for('genres.category_details', category_id=primary_category_id))
            else:
                flash('Error merging categories. Please try again.', 'error')
                return redirect(url_for('genres.merge_categories'))
        else:
            # Form validation failed - show form with errors
            all_categories = book_service.list_all_categories_sync()
            if all_categories is None:
                all_categories = []
            all_categories.sort(key=lambda c: get_attr(c, 'name', '').lower())
            return render_template('genres/merge.html', categories=all_categories, form=form)
        
    except Exception as e:
        current_app.logger.error(f"Error merging categories: {e}")
        flash('Error merging categories. Please try again.', 'error')
        return redirect(url_for('genres.merge_categories'))

@genres_bp.route('/category/<category_id>')
@login_required
def category_details(category_id):
    """Display detailed information about a specific category."""
    try:
        category = book_service.get_category_by_id_sync(category_id, str(current_user.id))
        if not category:
            flash('Category not found.', 'error')
            return redirect(url_for('genres.index'))
        
        # Calculate and set the correct level
        category.level = calculate_category_level(category, book_service)
        
        # Get category's books
        category_books = book_service.get_books_by_category_sync(category_id, str(current_user.id))
        
        # Get subcategories
        subcategories = book_service.get_category_children_sync(category_id, str(current_user.id))
        
        # Get parent category if it exists
        parent_category = None
        if hasattr(category, 'parent_id') and category.parent_id:
            parent_category = book_service.get_category_by_id_sync(category.parent_id, str(current_user.id))
        
        # Calculate total books including descendants
        total_books_with_descendants = len(category_books or [])
        if subcategories:
            for subcat in subcategories:
                subcat_id = subcat.get('id') if isinstance(subcat, dict) else getattr(subcat, 'id', None)
                if subcat_id:
                    subcat_books = book_service.get_books_by_category_sync(subcat_id, str(current_user.id), include_subcategories=True)
                    if subcat_books:
                        total_books_with_descendants += len(subcat_books)
        
        return render_template('genres/details.html',
                             category=category,
                             books=category_books or [],
                             subcategories=subcategories or [],
                             parent_category=parent_category,
                             total_books_with_descendants=total_books_with_descendants)
        
    except Exception as e:
        current_app.logger.error(f"Error loading category details: {e}")
        flash('Error loading category details.', 'error')
        return redirect(url_for('genres.index'))

@genres_bp.route('/add', methods=['GET', 'POST'])
@login_required
def add_category():
    """Add a new category."""
    from ..forms import CategoryForm
    
    if request.method == 'GET':
        form = CategoryForm()
        parent_id = request.args.get('parent_id')
        return render_template('genres/add_edit.html', form=form, parent_id=parent_id)
    
    # POST - create category
    try:
        form = CategoryForm()
        if form.validate_on_submit():
            # Create new category
            category_data = {
                'name': form.name.data,
                'description': form.description.data,
                'parent_id': form.parent_id.data if form.parent_id.data else None,
                'color': form.color.data if form.color.data else generate_random_color(),
                'icon': form.icon.data if form.icon.data else None,
                'aliases': [alias.strip() for alias in form.aliases.data.split('\n') if alias.strip()] if form.aliases.data else []
            }
            
            new_category = book_service.create_category_sync(category_data)
            if new_category:
                flash(f'Category "{form.name.data}" created successfully.', 'success')
                return redirect(url_for('genres.category_details', category_id=new_category.id))
            else:
                flash('Error creating category.', 'error')
                return render_template('genres/add_edit.html', form=form)
        else:
            return render_template('genres/add_edit.html', form=form)
            
    except Exception as e:
        current_app.logger.error(f"Error creating category: {e}")
        flash('Error creating category.', 'error')
        return redirect(url_for('genres.index'))

@genres_bp.route('/edit/<category_id>', methods=['GET', 'POST'])
@login_required
def edit_category(category_id):
    """Edit an existing category."""
    from ..forms import CategoryForm
    
    try:
        category = book_service.get_category_by_id_sync(category_id, str(current_user.id))
        if not category:
            flash('Category not found.', 'error')
            return redirect(url_for('genres.index'))
        
        if request.method == 'GET':
            form = CategoryForm(current_category_id=category_id, obj=category)
            return render_template('genres/add_edit.html', form=form, category=category)
        
        # POST - update category
        form = CategoryForm(current_category_id=category_id)
        
        # Populate form with submitted data
        if form.validate_on_submit():
            # Create updated category data
            updated_category_data = {
                'id': category.id,
                'name': form.name.data,
                'description': form.description.data,
                'parent_id': form.parent_id.data if form.parent_id.data else None,
                # Preserve existing values
                'normalized_name': getattr(category, 'normalized_name', ''),
                'level': getattr(category, 'level', 0),
                'color': form.color.data if form.color.data else getattr(category, 'color', None),
                'icon': form.icon.data if form.icon.data else getattr(category, 'icon', None),
                'aliases': [alias.strip() for alias in form.aliases.data.split('\n') if alias.strip()] if form.aliases.data else getattr(category, 'aliases', []),
                'book_count': getattr(category, 'book_count', 0),
                'user_book_count': getattr(category, 'user_book_count', 0),
                'created_at': getattr(category, 'created_at', None),
                'updated_at': datetime.now(timezone.utc)
            }
            
            # Create new Category object with updated data
            from ..domain.models import Category
            updated_category = Category(**updated_category_data)
            
            success = book_service.update_category_sync(updated_category)
            if success:
                flash(f'Category "{form.name.data}" updated successfully.', 'success')
                return redirect(url_for('genres.category_details', category_id=category_id))
            else:
                flash('Error updating category.', 'error')
                return render_template('genres/add_edit.html', form=form, category=category)
        else:
            return render_template('genres/add_edit.html', form=form, category=category)
            
    except Exception as e:
        current_app.logger.error(f"Error editing category: {e}")
        flash('Error editing category.', 'error')
        return redirect(url_for('genres.index'))

@genres_bp.route('/delete/<category_id>', methods=['POST'])
@login_required
def delete_category(category_id):
    """Delete a category."""
    try:
        category = book_service.get_category_by_id_sync(category_id, str(current_user.id))
        if not category:
            flash('Category not found.', 'error')
            return redirect(url_for('genres.index'))
        
        category_name = get_attr(category, 'name', 'Unknown')
        
        # Check if category has subcategories (prevent deletion to maintain hierarchy integrity)
        subcategories = book_service.get_category_children_sync(category_id, str(current_user.id))
        
        if subcategories and len(subcategories) > 0:
            flash(f'Cannot delete category "{category_name}" because it contains {len(subcategories)} subcategories.', 'error')
            return redirect(url_for('genres.category_details', category_id=category_id))
        
        # Delete the category (DETACH DELETE will automatically remove book associations)
        success = book_service.delete_category_sync(category_id)
        if success:
            flash(f'Category "{category_name}" deleted successfully. All book associations have been removed.', 'success')
            return redirect(url_for('genres.index'))
        else:
            flash('Error deleting category.', 'error')
            return redirect(url_for('genres.category_details', category_id=category_id))
            
    except Exception as e:
        current_app.logger.error(f"Error deleting category: {e}")
        flash('Error deleting category.', 'error')
        return redirect(url_for('genres.index'))

@genres_bp.route('/bulk-delete', methods=['POST'])
@login_required
def bulk_delete_categories():
    """Bulk delete multiple categories."""
    try:
        selected_categories = request.form.getlist('selected_categories')
        force_delete = request.form.get('force_delete') == 'true'
        
        if not selected_categories:
            flash('No categories selected for deletion.', 'error')
            return redirect(url_for('genres.index'))
        
        current_app.logger.info(f"Bulk deleting {len(selected_categories)} categories, force_delete={force_delete}")
        
        deleted_count = 0
        skipped_count = 0
        error_count = 0
        
        for category_id in selected_categories:
            try:
                category = book_service.get_category_by_id_sync(category_id, str(current_user.id))
                if not category:
                    current_app.logger.warning(f"Category {category_id} not found, skipping")
                    skipped_count += 1
                    continue
                
                category_name = get_attr(category, 'name', 'Unknown')
                
                # Only check for subcategories (not books) unless force_delete is enabled
                if not force_delete:
                    subcategories = book_service.get_category_children_sync(category_id, str(current_user.id))
                    
                    if subcategories and len(subcategories) > 0:
                        current_app.logger.warning(f"Skipping category {category_name} - has subcategories")
                        skipped_count += 1
                        continue
                
                # Delete the category
                success = book_service.delete_category_sync(category_id)
                if success:
                    current_app.logger.info(f"Successfully deleted category {category_name}")
                    deleted_count += 1
                else:
                    current_app.logger.error(f"Failed to delete category {category_name}")
                    error_count += 1
                    
            except Exception as e:
                current_app.logger.error(f"Error deleting category {category_id}: {e}")
                error_count += 1
        
        # Provide feedback
        if deleted_count > 0:
            flash(f'Successfully deleted {deleted_count} categories and removed their book associations.', 'success')
        if skipped_count > 0:
            flash(f'Skipped {skipped_count} categories (had subcategories).', 'warning')
        if error_count > 0:
            flash(f'Failed to delete {error_count} categories.', 'error')
        
        return redirect(url_for('genres.index'))
        
    except Exception as e:
        current_app.logger.error(f"Error in bulk delete: {e}")
        flash('Error deleting categories.', 'error')
        return redirect(url_for('genres.index'))

@genres_bp.route('/search')
@login_required
def search_categories():
    """Search categories by query string."""
    try:
        query = request.args.get('q', '').strip()
        
        if not query:
            return render_template('genres/search.html', query=query, results=[])
        
        # Search categories
        results = book_service.search_categories_sync(query, limit=50, user_id=str(current_user.id))
        if results is None:
            results = []
        
        return render_template('genres/search.html', query=query, results=results)
        
    except Exception as e:
        current_app.logger.error(f"Error searching categories: {e}")
        return render_template('genres/search.html', query='', results=[])

@genres_bp.route('/api/search')
@login_required
def api_search_categories():
    """API endpoint for searching categories (used in autocomplete)."""
    try:
        query = request.args.get('q', '').strip()
        limit = min(int(request.args.get('limit', 10)), 50)
        
        if not query:
            return jsonify([])
        
        # Search categories
        results = book_service.search_categories_sync(query, limit=limit, user_id=str(current_user.id))
        if results is None:
            results = []
        
        # Format results for API response
        api_results = []
        for category in results[:limit]:
            api_results.append({
                'id': get_attr(category, 'id', ''),
                'name': get_attr(category, 'name', ''),
                'description': get_attr(category, 'description', ''),
                'book_count': get_attr(category, 'book_count', 0)
            })
        
        return jsonify(api_results)
        
    except Exception as e:
        current_app.logger.error(f"Error in API search: {e}")
        return jsonify([])


@genres_bp.route('/api/create', methods=['POST'])
@login_required
def api_create_category():
    """API endpoint for creating a new category."""
    try:
        data = request.get_json()
        if not data or not data.get('name'):
            return jsonify({'error': 'Category name is required'}), 400
        
        # Check if category already exists
        existing = book_service.search_categories_sync(data['name'], limit=1, user_id=str(current_user.id))
        if existing and len(existing) > 0:
            # Return the existing category
            category = existing[0]
            return jsonify({
                'id': get_attr(category, 'id', ''),
                'name': get_attr(category, 'name', ''),
                'description': get_attr(category, 'description', ''),
                'book_count': get_attr(category, 'book_count', 0)
            })
        
        # Create new category
        category_data = {
            'name': data['name'].strip(),
            'description': data.get('description', '').strip() or None,
            'parent_id': data.get('parent_id') or None,
            'color': data.get('color', '').strip() or generate_random_color(),
            'icon': data.get('icon', '').strip() or None,
            'aliases': []
        }
        
        new_category = book_service.create_category_sync(category_data)
        if new_category:
            return jsonify({
                'id': get_attr(new_category, 'id', ''),
                'name': get_attr(new_category, 'name', ''),
                'description': get_attr(new_category, 'description', ''),
                'book_count': 0
            })
        else:
            return jsonify({'error': 'Failed to create category'}), 500
            
    except Exception as e:
        current_app.logger.error(f"Error creating category via API: {e}")
        return jsonify({'error': 'Internal server error'}), 500
