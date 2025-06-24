"""
Genre/Category routes for Bibliotheca.

Provides endpoints for managing hierarchical genres and categories,
similar to the person management functionality.
"""

from flask import Blueprint, current_app, render_template, request, redirect, url_for, jsonify, flash, abort
from flask_login import login_required, current_user
from datetime import datetime
import uuid
import traceback

from .services import book_service
from .domain.models import Category, ReadingStatus

# Create blueprint
genres_bp = Blueprint('genres', __name__, url_prefix='/genres')


@genres_bp.route('/')
@login_required
def index():
    """Display all genres/categories with management options."""
    try:
        # Helper function to handle potential coroutine returns
        def safe_call_sync_method(method, *args, **kwargs):
            """Safely call a sync method that might return a coroutine."""
            import asyncio
            import inspect
            
            try:
                result = method(*args, **kwargs)
                
                # If it's a coroutine, we need to run it
                if inspect.iscoroutine(result):
                    try:
                        # Try to get existing event loop
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            # If loop is running, create a new thread to run the coroutine
                            import concurrent.futures
                            with concurrent.futures.ThreadPoolExecutor() as executor:
                                future = executor.submit(asyncio.run, result)
                                return future.result()
                        else:
                            return loop.run_until_complete(result)
                    except RuntimeError:
                        # If no event loop, create one
                        return asyncio.run(result)
                else:
                    return result
            except Exception as e:
                current_app.logger.error(f"Error in safe_call_sync_method: {e}")
                return None
        
        all_categories = safe_call_sync_method(book_service.list_all_categories_sync, str(current_user.id))
        
        # Ensure we have a list
        if not isinstance(all_categories, list):
            print(f"⚠️ [GENRES] Expected list, got {type(all_categories)}")
            all_categories = []
        
        # Separate root categories from all categories for different views
        root_categories = [cat for cat in all_categories if cat.parent_id is None]
        
        # Sort categories by level then by name
        all_categories.sort(key=lambda c: (c.level, c.name.lower()))
        root_categories.sort(key=lambda c: c.name.lower())
        
        print(f"📊 [GENRES] Displaying {len(all_categories)} categories ({len(root_categories)} root)")
        
        return render_template('genres/index.html', 
                             categories=all_categories,
                             root_categories=root_categories)
        
    except Exception as e:
        print(f"❌ [GENRES] Error loading genres page: {e}")
        import traceback
        traceback.print_exc()
        current_app.logger.error(f"Error loading genres page: {e}")
        flash('Error loading genres page.', 'error')
        return redirect(url_for('main.library'))


@genres_bp.route('/<category_id>')
@login_required 
def category_details(category_id):
    """Display detailed information about a category."""
    try:
        print(f"🔍 [GENRE] Starting category details page for category_id: {category_id}, user: {current_user.id}")
        
        # Get category details
        category = book_service.get_category_by_id_sync(category_id, str(current_user.id))
        
        if not category:
            print(f"❌ [GENRE] Category not found for ID: {category_id}")
            flash('Category not found.', 'error')
            return redirect(url_for('genres.index'))
        
        print(f"✅ [GENRE] Found category: {category.name} (ID: {category.id})")
        
        # Get books in this category for current user
        print(f"🔍 [GENRE] Getting books by category for user {current_user.id}")
        books = book_service.get_books_by_category_sync(category_id, str(current_user.id), include_subcategories=True)
        print(f"📊 [GENRE] Got {len(books)} books for category")
        
        # Get category hierarchy info
        children = book_service.get_category_children_sync(category_id, str(current_user.id))
        
        # Calculate total books including descendants
        total_books_with_descendants = len(books)
        
        # Build breadcrumb trail
        breadcrumbs = []
        current_cat = category
        while current_cat:
            breadcrumbs.insert(0, current_cat)
            if current_cat.parent_id:
                current_cat = book_service.get_category_by_id_sync(current_cat.parent_id, str(current_user.id))
            else:
                break
        
        print(f"✅ [GENRE] Rendering template with {len(breadcrumbs)} breadcrumbs and {len(children)} children")
        return render_template('genres/details.html', 
                             category=category,
                             books=books,
                             children=children,
                             breadcrumbs=breadcrumbs,
                             total_books_with_descendants=total_books_with_descendants)
    
    except Exception as e:
        print(f"❌ [GENRE] Error loading category details for {category_id}: {e}")
        import traceback
        traceback.print_exc()
        current_app.logger.error(f"Error loading category details: {e}")
        flash('Error loading category details.', 'error')
        return redirect(url_for('genres.index'))


@genres_bp.route('/add', methods=['GET', 'POST'])
@login_required
def add_category():
    """Add a new category/genre."""
    if request.method == 'POST':
        try:
            # Get form data
            name = request.form.get('name', '').strip()
            description = request.form.get('description', '').strip()
            parent_id = request.form.get('parent_id', '').strip() or None
            color = request.form.get('color', '').strip() or None
            icon = request.form.get('icon', '').strip() or None
            aliases_str = request.form.get('aliases', '').strip()
            
            if not name:
                flash('Name is required.', 'error')
                return render_template('genres/add.html')
            
            # Parse aliases
            aliases = []
            if aliases_str:
                aliases = [alias.strip() for alias in aliases_str.split(',') if alias.strip()]
            
            # Determine level
            level = 0
            if parent_id:
                parent = book_service.get_category_by_id_sync(parent_id, None)
                if parent:
                    level = parent.level + 1
                else:
                    flash('Invalid parent category.', 'error')
                    return render_template('genres/add.html')
            
            # Create category object
            category = Category(
                id=str(uuid.uuid4()),
                name=name,
                description=description if description else None,
                parent_id=parent_id,
                level=level,
                color=color,
                icon=icon,
                aliases=aliases,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
            # Create category in storage
            created_category = book_service.create_category_sync(category)
            
            if created_category:
                flash(f'Category "{name}" added successfully!', 'success')
                return redirect(url_for('genres.category_details', category_id=created_category.id))
            else:
                flash('Error creating category. Please try again.', 'error')
                
        except Exception as e:
            current_app.logger.error(f"Error adding category: {e}")
            flash('Error adding category. Please try again.', 'error')
    
    # Get all categories for parent selection
    all_categories = book_service.list_all_categories_sync()
    if not isinstance(all_categories, list):
        all_categories = []
    
    return render_template('genres/add.html', categories=all_categories)


@genres_bp.route('/<category_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_category(category_id):
    """Edit an existing category."""
    try:
        category = book_service.get_category_by_id_sync(category_id, None)
        if not category:
            flash('Category not found.', 'error')
            return redirect(url_for('genres.index'))
        
        if request.method == 'POST':
            # Get form data
            name = request.form.get('name', '').strip()
            description = request.form.get('description', '').strip()
            parent_id = request.form.get('parent_id', '').strip() or None
            color = request.form.get('color', '').strip() or None
            icon = request.form.get('icon', '').strip() or None
            aliases_str = request.form.get('aliases', '').strip()
            
            if not name:
                flash('Name is required.', 'error')
                return render_template('genres/edit.html', category=category)
            
            # Parse aliases
            aliases = []
            if aliases_str:
                aliases = [alias.strip() for alias in aliases_str.split(',') if alias.strip()]
            
            # Check for circular reference if parent is changing
            if parent_id and parent_id != category.parent_id:
                # Make sure we're not creating a circular reference
                parent = book_service.get_category_by_id_sync(parent_id, None)
                if parent:
                    # Check if the new parent is a descendant of this category
                    temp_parent = parent
                    while temp_parent:
                        if temp_parent.id == category.id:
                            flash('Cannot set a descendant as parent (would create circular reference).', 'error')
                            return render_template('genres/edit.html', category=category)
                        if temp_parent.parent_id:
                            temp_parent = book_service.get_category_by_id_sync(temp_parent.parent_id, None)
                        else:
                            break
            
            # Determine new level
            level = 0
            if parent_id:
                parent = book_service.get_category_by_id_sync(parent_id, None)
                if parent:
                    level = parent.level + 1
            
            # Update category data
            category.name = name
            category.description = description if description else None
            category.parent_id = parent_id
            category.level = level
            category.color = color
            category.icon = icon
            category.aliases = aliases
            category.updated_at = datetime.now()
            
            # Update in storage
            updated_category = book_service.update_category_sync(category)
            
            if updated_category:
                flash(f'Category "{name}" updated successfully!', 'success')
                return redirect(url_for('genres.category_details', category_id=category.id))
            else:
                flash('Error updating category. Please try again.', 'error')
        
        # Get all categories for parent selection (excluding this category and its descendants)
        all_categories = book_service.list_all_categories_sync()
        if not isinstance(all_categories, list):
            all_categories = []
        
        # Filter out this category and its descendants to prevent circular references
        valid_parents = []
        for cat in all_categories:
            if cat.id != category.id:
                # Check if this category is a descendant of the current category
                temp_cat = cat
                is_descendant = False
                while temp_cat and temp_cat.parent_id:
                    if temp_cat.parent_id == category.id:
                        is_descendant = True
                        break
                    temp_cat = next((c for c in all_categories if c.id == temp_cat.parent_id), None)
                
                if not is_descendant:
                    valid_parents.append(cat)
        
        return render_template('genres/edit.html', category=category, categories=valid_parents)
    
    except Exception as e:
        current_app.logger.error(f"Error editing category {category_id}: {e}")
        flash('Error editing category. Please try again.', 'error')
        return redirect(url_for('genres.index'))


@genres_bp.route('/<category_id>/delete', methods=['POST'])
@login_required
def delete_category(category_id):
    """Delete a category (with confirmation)."""
    try:
        category = book_service.get_category_by_id_sync(category_id, None)
        if not category:
            flash('Category not found.', 'error')
            return redirect(url_for('genres.index'))
        
        # Check if category has children or books
        children = book_service.get_category_children_sync(category_id, None)
        books = book_service.get_books_by_category_sync(category_id, include_subcategories=False)
        
        if children:
            flash(f'Cannot delete category "{category.name}" because it has {len(children)} child categories. Move or delete the child categories first.', 'error')
            return redirect(url_for('genres.category_details', category_id=category_id))
        
        if books:
            flash(f'Cannot delete category "{category.name}" because it is used by {len(books)} books. Remove the category from those books first.', 'error')
            return redirect(url_for('genres.category_details', category_id=category_id))
        
        # Delete the category
        success = book_service.delete_category_sync(category_id)
        
        if success:
            flash(f'Category "{category.name}" deleted successfully.', 'success')
        else:
            flash('Error deleting category. Please try again.', 'error')
        
        return redirect(url_for('genres.index'))
        
    except Exception as e:
        current_app.logger.error(f"Error deleting category {category_id}: {e}")
        flash('Error deleting category. Please try again.', 'error')
        return redirect(url_for('genres.index'))


@genres_bp.route('/bulk_delete', methods=['POST'])
@login_required
def bulk_delete_categories():
    """Delete multiple categories selected from the genres view."""
    try:
        selected_categories = request.form.getlist('selected_categories')
        force_delete = request.form.get('force_delete') == 'true'
        
        if not selected_categories:
            flash('No categories selected for deletion.', 'warning')
            return redirect(url_for('genres.index'))
        
        deleted_count = 0
        errors = []
        
        for category_id in selected_categories:
            try:
                category = book_service.get_category_by_id_sync(category_id, None)
                if not category:
                    continue
                
                # Check constraints unless force delete
                if not force_delete:
                    children = book_service.get_category_children_sync(category_id, None)
                    books = book_service.get_books_by_category_sync(category_id, include_subcategories=False)
                    
                    if children or books:
                        errors.append(f'"{category.name}" has {len(children)} children and {len(books)} books')
                        continue
                
                # Delete the category
                success = book_service.delete_category_sync(category_id)
                if success:
                    deleted_count += 1
                else:
                    errors.append(f'Failed to delete "{category.name}"')
                    
            except Exception as e:
                errors.append(f'Error deleting category: {str(e)}')
        
        # Show results
        if deleted_count > 0:
            flash(f'Successfully deleted {deleted_count} categories.', 'success')
        
        if errors:
            flash(f'Some categories could not be deleted: {"; ".join(errors[:3])}', 'warning')
        
        return redirect(url_for('genres.index'))
        
    except Exception as e:
        current_app.logger.error(f"Error in bulk delete categories: {e}")
        flash('Error deleting categories. Please try again.', 'error')
        return redirect(url_for('genres.index'))


@genres_bp.route('/merge', methods=['GET', 'POST'])
@login_required
def merge_categories():
    """Merge two or more categories into one."""
    if request.method == 'GET':
        try:
            all_categories = book_service.list_all_categories_sync()
            if all_categories is None:
                all_categories = []
            all_categories.sort(key=lambda c: c.name.lower())
            return render_template('genres/merge.html', categories=all_categories)
        
        except Exception as e:
            current_app.logger.error(f"Error loading merge categories page: {e}")
            flash('Error loading merge page.', 'error')
            return redirect(url_for('genres.index'))
    
    # POST - perform merge
    try:
        primary_category_id = request.form.get('primary_category')
        merge_category_ids = request.form.getlist('merge_categories')
        
        if not primary_category_id:
            flash('Please select a primary category.', 'error')
            return redirect(url_for('genres.merge'))
        
        if not merge_category_ids:
            flash('Please select at least one category to merge.', 'error')
            return redirect(url_for('genres.merge'))
        
        # Remove primary category from merge list if it was included
        merge_category_ids = [cat_id for cat_id in merge_category_ids if cat_id != primary_category_id]
        
        if not merge_category_ids:
            flash('Please select categories to merge (different from primary category).', 'error')
            return redirect(url_for('genres.merge'))
        
        # Get category names for confirmation message
        primary_category = book_service.get_category_by_id_sync(primary_category_id, None)
        merge_categories = [book_service.get_category_by_id_sync(cat_id, None) for cat_id in merge_category_ids]
        merge_categories = [cat for cat in merge_categories if cat]  # Filter out None values
        
        if not primary_category:
            flash('Primary category not found.', 'error')
            return redirect(url_for('genres.merge'))
        
        # Perform merge
        success = book_service.merge_categories_sync(primary_category_id, merge_category_ids)
        
        if success:
            merge_names = [cat.name for cat in merge_categories]
            flash(f'Successfully merged {len(merge_names)} categories into "{primary_category.name}": {", ".join(merge_names)}.', 'success')
            return redirect(url_for('genres.category_details', category_id=primary_category_id))
        else:
            flash('Error merging categories. Please try again.', 'error')
            return redirect(url_for('genres.merge'))
        
    except Exception as e:
        current_app.logger.error(f"Error merging categories: {e}")
        flash('Error merging categories. Please try again.', 'error')
        return redirect(url_for('genres.merge'))


@genres_bp.route('/api/search')
@login_required
def api_search_categories():
    """API endpoint for searching categories (used in autocomplete)."""
    try:
        query = request.args.get('q', '').strip()
        limit = min(int(request.args.get('limit', 10)), 50)
        
        if not query:
            return jsonify([])
        
        categories = book_service.search_categories_sync(query, limit, str(current_user.id))
        
        results = []
        for category in categories:
            results.append({
                'id': category.id,
                'name': category.name,
                'full_path': category.full_path if hasattr(category, 'full_path') else category.name,
                'level': category.level,
                'book_count': getattr(category, 'book_count', 0),
                'description': category.description
            })
        
        return jsonify(results)
        
    except Exception as e:
        current_app.logger.error(f"Error searching categories: {e}")
        return jsonify([])


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
        
        return render_template('genres/hierarchy.html', category_tree=category_tree)
        
    except Exception as e:
        current_app.logger.error(f"Error loading hierarchy view: {e}")
        flash('Error loading hierarchy view.', 'error')
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
        categories = book_service.search_categories_sync(query)
        
        if not isinstance(categories, list):
            categories = []
        
        # Sort by relevance (exact matches first, then by name)
        def sort_key(cat):
            name_lower = cat.name.lower()
            query_lower = query.lower()
            
            if name_lower == query_lower:
                return (0, name_lower)  # Exact match
            elif name_lower.startswith(query_lower):
                return (1, name_lower)  # Starts with query
            else:
                return (2, name_lower)  # Contains query
        
        categories.sort(key=sort_key)
        
        return render_template('genres/search.html', 
                             query=query, 
                             results=categories)
        
    except Exception as e:
        current_app.logger.error(f"Error searching categories: {e}")
        flash('Error searching categories.', 'error')
        return render_template('genres/search.html', query='', results=[])


@genres_bp.route('/test-auto-mapping')
@login_required  
def test_auto_mapping():
    """Test automatic genre mapping from a known book with categories."""
    try:
        from .utils import get_google_books_cover
        from .domain.models import Book as DomainBook, Publisher
        
        # Test with a book known to have categories - The Hobbit
        test_isbn = "9780547928227"
        
        print(f"🧪 [TEST] Testing automatic genre mapping with ISBN: {test_isbn}")
        
        # Fetch from Google Books
        google_data = get_google_books_cover(test_isbn, fetch_title_author=True)
        
        if google_data and google_data.get('categories'):
            print(f"📚 [TEST] Found categories from Google Books: {google_data.get('categories')}")
            
            # Create a domain book with categories
            domain_book = DomainBook(
                title=google_data.get('title', 'Test Book'),
                isbn13=test_isbn,
                description=google_data.get('description'),
                publisher=Publisher(name=google_data.get('publisher')) if google_data.get('publisher') else None,
                page_count=google_data.get('page_count'),
                language=google_data.get('language', 'en'),
                cover_url=google_data.get('cover'),
                raw_categories=google_data.get('categories'),  # This should trigger automatic processing
            )
            
            # Create the book and process categories
            created_book = book_service.find_or_create_book_sync(domain_book)
            
            if created_book:
                # Get the categories that were created
                book_categories = book_service.get_book_categories_sync(created_book.id)
                
                flash(f'✅ Test successful! Created book "{created_book.title}" with {len(book_categories)} categories: {", ".join([cat.name for cat in book_categories])}', 'success')
                print(f"✅ [TEST] Book created with {len(book_categories)} categories")
                
                # Also add to user's library for testing
                book_service.add_book_to_user_library_sync(
                    user_id=current_user.id,
                    book_id=created_book.id,
                    reading_status=ReadingStatus.PLAN_TO_READ
                )
                
                flash(f'📚 Book also added to your library!', 'info')
            else:
                flash('❌ Failed to create test book', 'error')
        else:
            flash('❌ No categories found for test book or API call failed', 'error')
            
    except Exception as e:
        flash(f'❌ Test failed: {str(e)}', 'error')
        print(f"❌ [TEST] Error: {e}")
        traceback.print_exc()
    
    return redirect(url_for('genres.index'))
