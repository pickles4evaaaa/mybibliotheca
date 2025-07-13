"""
Genre/Category routes for Bibliotheca.

Provides endpoints for managing genres and categories,
similar to the person management functionality.
"""

from flask import Blueprint, current_app, render_template, request, redirect, url_for, jsonify, flash, abort
from flask_login import login_required, current_user
from datetime import datetime
import uuid
import traceback

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
        
        debug_log(f"📊 [GENRES] Found {len(all_categories)} total categories", "GENRE_VIEW")
        
        # Convert dictionaries to objects for template compatibility
        processed_categories = []
        
        for cat in all_categories:
            if isinstance(cat, dict):
                # Create a simple object from dictionary
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
                processed_categories.append(cat)
        
        # Use the processed categories
        all_categories = processed_categories

        # Sort categories by name
        all_categories.sort(key=lambda x: get_attr(x, 'name', '').lower())
        
        # Pagination parameters
        page = request.args.get('page', 1, type=int)
        per_page = 12  # Number of categories per page
        
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
        
        pagination = {
            'has_prev': has_prev,
            'has_next': has_next,
            'prev_num': prev_num,
            'next_num': next_num,
            'page': page,
            'pages': (total + per_page - 1) // per_page,
            'per_page': per_page,
            'total': total
        }
        
        return render_template('genres/index.html', 
                             categories=categories, 
                             pagination=pagination)
        
    except Exception as e:
        current_app.logger.error(f"Error loading genres index: {e}")
        flash('Error loading categories.', 'error')
        return render_template('genres/index.html', categories=[], pagination=None)

@genres_bp.route('/hierarchy')
@login_required
def hierarchy_view():
    """Display categories in a hierarchical tree view."""
    try:
        root_categories = book_service.get_root_categories_sync(str(current_user.id))
        
        # For each root category, we'll build the full tree structure
        def build_tree(categories):
            tree = []
            for category in categories:
                children = book_service.get_category_children_sync(category.id, str(current_user.id))
                if children:
                    category.children = build_tree(children)
                else:
                    category.children = []
                tree.append(category)
            return tree
        
        category_tree = build_tree(root_categories)
        
        # Calculate hierarchy statistics
        all_categories = book_service.list_all_categories_sync() or []
        total_categories = len(all_categories)
        root_categories_count = len(root_categories)
        
        # Calculate max depth
        def get_max_depth(tree, current_depth=0):
            if not tree:
                return current_depth
            max_depth = current_depth
            for category in tree:
                if hasattr(category, 'children') and category.children:
                    depth = get_max_depth(category.children, current_depth + 1)
                    max_depth = max(max_depth, depth)
            return max_depth
        
        max_depth = get_max_depth(category_tree)
        
        # Calculate total books across all categories
        total_books = 0
        for category in all_categories:
            book_count = getattr(category, 'book_count', 0)
            if book_count:
                total_books += book_count
        
        hierarchy_stats = {
            'total_categories': total_categories,
            'root_categories': root_categories_count,
            'max_depth': max_depth,
            'total_books': total_books
        }
        
        return render_template('genres/hierarchy.html', 
                             category_tree=category_tree,
                             hierarchy_stats=hierarchy_stats)
        
    except Exception as e:
        current_app.logger.error(f"Error loading hierarchy view: {e}")
        flash('Error loading hierarchy view.', 'error')
        return redirect(url_for('genres.index'))

@genres_bp.route('/merge', methods=['GET', 'POST'])
@login_required
def merge_categories():
    """Merge two or more categories into one."""
    from ..forms import MergeCategoriesForm
    
    if request.method == 'GET':
        try:
            form = MergeCategoriesForm()
            all_categories = book_service.list_all_categories_sync()
            if all_categories is None:
                all_categories = []
            all_categories.sort(key=lambda c: get_attr(c, 'name', '').lower())
            return render_template('genres/merge.html', categories=all_categories, form=form)
        
        except Exception as e:
            current_app.logger.error(f"Error loading merge categories page: {e}")
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

# Add other necessary routes here as needed...
