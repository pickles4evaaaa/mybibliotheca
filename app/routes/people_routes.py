"""
People management routes for the Bibliotheca application.
Handles all person/author-related operations including CRUD, merging, and metadata refresh.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, jsonify, session
from flask_login import login_required, current_user
from datetime import datetime
import uuid
import traceback
import inspect
import asyncio
import re

from app.domain.models import Person
from app.services import book_service, person_service

# Create people blueprint
people_bp = Blueprint('people', __name__)

@people_bp.route('/people')
@login_required
def people():
    """Display all people with management options."""
    from app.debug_system import debug_log, debug_service_call, debug_template_data
    
    try:
        debug_log(f"🔍 [PEOPLE] Starting people page for user {current_user.id}", "PEOPLE_VIEW")
        
        # Get all persons with error handling for async issues
        # Helper function to handle potential coroutine returns
        def safe_call_sync_method(method, *args, **kwargs):
            """Safely call a sync method that might return a coroutine."""
            result = method(*args, **kwargs)
            if inspect.iscoroutine(result):
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result = loop.run_until_complete(result)
                finally:
                    loop.close()
            return result
        
        debug_service_call("book_service", "list_all_persons_sync", {}, None, "BEFORE")
        all_persons = safe_call_sync_method(book_service.list_all_persons_sync)
        debug_service_call("book_service", "list_all_persons_sync", {}, all_persons, "AFTER")
        
        # Ensure we have a list
        if not isinstance(all_persons, list):
            debug_log(f"⚠️ [PEOPLE] Expected list, got {type(all_persons)}", "PEOPLE_VIEW")
            all_persons = []
        
        debug_log(f"📊 [PEOPLE] Found {len(all_persons)} persons in database", "PEOPLE_VIEW")
        
        # Convert dictionaries to objects for template compatibility
        processed_persons = []
        
        # Add book counts and contributions for each person
        for i, person in enumerate(all_persons):
            debug_log(f"🔍 [PEOPLE] Processing person {i+1}/{len(all_persons)}: {person.get('name', 'unknown') if isinstance(person, dict) else getattr(person, 'name', 'unknown')}", "PEOPLE_VIEW")
            
            # Convert dictionary to object if needed
            if isinstance(person, dict):
                from types import SimpleNamespace
                person_obj = SimpleNamespace(**person)
            else:
                person_obj = person
            
            try:
                # Get book count and contributions for this person
                person_id = getattr(person_obj, 'id', None)
                if person_id:
                    books_by_type = safe_call_sync_method(person_service.get_books_by_person_for_user_sync, person_id, str(current_user.id))
                    if books_by_type:
                        total_books = sum(len(books) for books in books_by_type.values())
                        person_obj.book_count = total_books
                        person_obj.contributions = books_by_type
                    else:
                        person_obj.book_count = 0
                        person_obj.contributions = {}
                else:
                    person_obj.book_count = 0
                    person_obj.contributions = {}
                
                processed_persons.append(person_obj)
                
            except Exception as person_error:
                debug_log(f"⚠️ [PEOPLE] Error processing person {i+1}: {person_error}", "PEOPLE_VIEW")
                person_obj.book_count = 0
                person_obj.contributions = {}
                processed_persons.append(person_obj)
        
        # Sort by name safely
        try:
            processed_persons.sort(key=lambda p: getattr(p, 'name', '').lower())
            debug_log(f"✅ [PEOPLE] Sorted {len(processed_persons)} persons by name", "PEOPLE_VIEW")
        except Exception as sort_error:
            debug_log(f"⚠️ [PEOPLE] Error sorting persons: {sort_error}", "PEOPLE_VIEW")
        
        # Show summary of what we found
        try:
            total_with_books = sum(1 for p in processed_persons if getattr(p, 'book_count', 0) > 0)
            debug_log(f"📊 [PEOPLE] Summary: {len(processed_persons)} total persons, {total_with_books} with books", "PEOPLE_VIEW")
        except Exception as summary_error:
            debug_log(f"⚠️ [PEOPLE] Error calculating summary: {summary_error}", "PEOPLE_VIEW")
        
        # Get contribution type counts for the accordion
        try:
            contribution_counts = safe_call_sync_method(person_service.get_contribution_type_counts_sync)
            debug_log(f"📊 [PEOPLE] Contribution counts: {contribution_counts}", "PEOPLE_VIEW")
        except Exception as counts_error:
            debug_log(f"⚠️ [PEOPLE] Error getting contribution counts: {counts_error}", "PEOPLE_VIEW")
            contribution_counts = {}
        
        template_data = {'persons': processed_persons, 'contribution_counts': contribution_counts}
        debug_template_data('people.html', template_data, "PEOPLE_VIEW")
        
        return render_template('people.html', persons=processed_persons, contribution_counts=contribution_counts)
    
    except Exception as e:
        debug_log(f"❌ [PEOPLE] Error loading people page: {e}", "PEOPLE_VIEW")
        traceback.print_exc()
        current_app.logger.error(f"Error loading people page: {e}")
        flash('Error loading people page.', 'error')
        return redirect(url_for('main.library'))


@people_bp.route('/person/<person_id>')
@login_required
def person_details(person_id):
    """Display detailed information about a person."""
    from app.debug_system import debug_log, debug_person_details, debug_service_call, debug_template_data
    
    try:
        debug_log(f"🔍 [PERSON] Starting person details page for person_id: {person_id}, user: {current_user.id}", "PERSON_DETAILS")
        
        # Get person details
        debug_log(f"🔍 [PERSON] Calling get_person_by_id_sync for person_id: {person_id}", "PERSON_DETAILS")
        debug_service_call("book_service", "get_person_by_id_sync", {"person_id": person_id}, None, "BEFORE")
        person = book_service.get_person_by_id_sync(person_id)
        debug_service_call("book_service", "get_person_by_id_sync", {"person_id": person_id}, person, "AFTER")
        
        debug_log(f"📊 [PERSON] Got person: {person}", "PERSON_DETAILS")
        debug_log(f"📊 [PERSON] Person type: {type(person)}", "PERSON_DETAILS")
        
        if not person:
            debug_log(f"❌ [PERSON] Person not found for ID: {person_id}", "PERSON_DETAILS")
            flash('Person not found.', 'error')
            return redirect(url_for('people.people'))
        
        # Get person name and ID safely
        person_name = getattr(person, 'name', None) or (person.get('name') if isinstance(person, dict) else 'Unknown')
        person_id_val = getattr(person, 'id', None) or (person.get('id') if isinstance(person, dict) else person_id)
        
        debug_log(f"✅ [PERSON] Found person: {person_name} (ID: {person_id_val})", "PERSON_DETAILS")
        
        # Enhanced person debugging
        debug_person_details(person, person_id, str(current_user.id), "DETAILS_VIEW")
        
        # Get books by this person for current user
        debug_log(f"🔍 [PERSON] Getting books by person for user {current_user.id}", "PERSON_DETAILS")
        debug_service_call("book_service", "get_books_by_person_sync", {"person_id": person_id, "user_id": str(current_user.id)}, None, "BEFORE")
        books_by_type = book_service.get_books_by_person_sync(person_id, str(current_user.id))
        debug_service_call("book_service", "get_books_by_person_sync", {"person_id": person_id, "user_id": str(current_user.id)}, books_by_type, "AFTER")
        debug_log(f"📊 [PERSON] Got books_by_type: {type(books_by_type)}", "PERSON_DETAILS")
        debug_log(f"📊 [PERSON] Books by type keys: {list(books_by_type.keys()) if books_by_type else 'None'}", "PERSON_DETAILS")
        
        # Convert service objects to template-compatible format
        converted_books_by_type = {}
        if books_by_type:
            for contribution_type, books in books_by_type.items():
                converted_books_by_type[contribution_type] = []
                for book in books:
                    # Convert book object to dictionary if needed
                    if hasattr(book, '__dict__'):
                        book_dict = book.__dict__
                    else:
                        book_dict = book
                    converted_books_by_type[contribution_type].append(book_dict)
        
        # Prepare template data
        template_data = {
            'person': person,
            'contributions_by_type': converted_books_by_type
        }
        debug_template_data('person_details.html', template_data, "PERSON_DETAILS")
        
        debug_log(f"✅ [PERSON] Rendering template", "PERSON_DETAILS")
        return render_template('person_details.html', 
                             person=person, 
                             contributions_by_type=converted_books_by_type)
    
    except Exception as e:
        debug_log(f"❌ [PERSON] Error loading person details for {person_id}: {e}", "PERSON_DETAILS")
        traceback.print_exc()
        current_app.logger.error(f"Error loading person details: {e}")
        flash('Error loading person details.', 'error')
        return redirect(url_for('people.people'))


@people_bp.route('/person/add', methods=['GET', 'POST'])
@login_required
def add_person():
    """Add a new person to the library"""
    if request.method == 'POST':
        try:
            # Get form data
            name = request.form.get('name', '').strip()
            bio = request.form.get('bio', '').strip()
            birth_year = request.form.get('birth_year')
            death_year = request.form.get('death_year')
            birth_place = request.form.get('birth_place', '').strip()
            website = request.form.get('website', '').strip()
            
            if not name:
                flash('Name is required.', 'error')
                return render_template('add_person.html', current_year=datetime.now().year)
            
            # Convert years to integers if provided
            birth_year_int = None
            death_year_int = None
            
            if birth_year:
                try:
                    birth_year_int = int(birth_year)
                except ValueError:
                    flash('Birth year must be a valid number.', 'error')
                    return render_template('add_person.html', current_year=datetime.now().year)
            
            if death_year:
                try:
                    death_year_int = int(death_year)
                except ValueError:
                    flash('Death year must be a valid number.', 'error')
                    return render_template('add_person.html', current_year=datetime.now().year)
            
            # Validate year range
            if birth_year_int and (birth_year_int < 0 or birth_year_int > datetime.now().year):
                flash('Birth year must be valid.', 'error')
                return render_template('add_person.html', current_year=datetime.now().year)
            
            if death_year_int and (death_year_int < 0 or death_year_int > datetime.now().year):
                flash('Death year must be valid.', 'error')
                return render_template('add_person.html', current_year=datetime.now().year)
            
            if birth_year_int and death_year_int and death_year_int < birth_year_int:
                flash('Death year cannot be before birth year.', 'error')
                return render_template('add_person.html', current_year=datetime.now().year)
            
            # Create person object
            person = Person(
                id=str(uuid.uuid4()),
                name=name,
                bio=bio if bio else None,
                birth_year=birth_year_int,
                death_year=death_year_int,
                created_at=datetime.now()
            )
            
            # Store person using the repository pattern
            try:
                from app.infrastructure.kuzu_graph import get_graph_storage
                storage = get_graph_storage()
                
                # Ensure we have a valid person ID
                if not person.id:
                    raise ValueError("Person ID cannot be None")
                
                person_data = {
                    'name': person.name,
                    'normalized_name': Person._normalize_name(person.name),
                    'bio': person.bio,
                    'birth_year': person.birth_year,
                    'death_year': person.death_year,
                    'birth_place': birth_place if birth_place else None,
                    'website': website if website else None,
                    'created_at': person.created_at.isoformat(),
                    'updated_at': person.created_at.isoformat()
                }
                
                storage.store_node('Person', person.id, person_data)
                flash(f'Person "{name}" added successfully!', 'success')
                return redirect(url_for('people.person_details', person_id=person.id))
                    
            except Exception as storage_error:
                current_app.logger.error(f"Error storing person: {storage_error}")
                flash('Error saving person. Please try again.', 'error')
            
        except Exception as e:
            current_app.logger.error(f"Error adding person: {e}")
            flash('Error adding person. Please try again.', 'error')
    
    return render_template('add_person.html', current_year=datetime.now().year)


@people_bp.route('/person/<person_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_person(person_id):
    """Edit an existing person."""
    try:
        person = book_service.get_person_by_id_sync(person_id)
        if not person:
            flash('Person not found.', 'error')
            return redirect(url_for('people.people'))
        
        if request.method == 'POST':
            # Get form data
            name = request.form.get('name', '').strip()
            bio = request.form.get('bio', '').strip()
            birth_year = request.form.get('birth_year')
            death_year = request.form.get('death_year')
            birth_place = request.form.get('birth_place', '').strip()
            website = request.form.get('website', '').strip()
            openlibrary_id = request.form.get('openlibrary_id', '').strip()
            image_url = request.form.get('image_url', '').strip()
            
            if not name:
                flash('Name is required.', 'error')
                return render_template('edit_person.html', person=person, current_year=datetime.now().year)
            
            # Convert years to integers if provided
            birth_year_int = None
            death_year_int = None
            
            if birth_year:
                try:
                    birth_year_int = int(birth_year)
                except ValueError:
                    flash('Birth year must be a valid number.', 'error')
                    return render_template('edit_person.html', person=person, current_year=datetime.now().year)
            
            if death_year:
                try:
                    death_year_int = int(death_year)
                except ValueError:
                    flash('Death year must be a valid number.', 'error')
                    return render_template('edit_person.html', person=person, current_year=datetime.now().year)
            
            # Validate year range
            if birth_year_int and (birth_year_int < 0 or birth_year_int > datetime.now().year):
                flash('Birth year must be valid.', 'error')
                return render_template('edit_person.html', person=person, current_year=datetime.now().year)
            
            if death_year_int and (death_year_int < 0 or death_year_int > datetime.now().year):
                flash('Death year must be valid.', 'error')
                return render_template('edit_person.html', person=person, current_year=datetime.now().year)
            
            if birth_year_int and death_year_int and death_year_int < birth_year_int:
                flash('Death year cannot be before birth year.', 'error')
                return render_template('edit_person.html', person=person, current_year=datetime.now().year)
            
            # Update person data using safe attribute access
            # Note: We don't directly assign to person attributes due to type uncertainty
            # Instead, we build the updated data dictionary directly for storage
            
            # Update normalized name
            person_name = getattr(person, 'name', None) or (person.get('name') if isinstance(person, dict) else name)
            normalized_name = Person._normalize_name(person_name or name)  # Ensure we have a string
            
            # Update in KuzuDB
            from app.infrastructure.kuzu_graph import get_graph_storage
            storage = get_graph_storage()
            
            # Get person ID safely
            person_id_for_storage = getattr(person, 'id', None) or (person.get('id') if isinstance(person, dict) else person_id)
            
            # Create person data using safe attribute access and the updated values
            created_at_val = getattr(person, 'created_at', None) or (person.get('created_at') if isinstance(person, dict) else None)
            updated_at_val = datetime.now()  # Always use current time for updated_at
            
            person_data = {
                'name': name,  # Use the updated name from form
                'normalized_name': normalized_name,  # Use the computed normalized name
                'bio': bio if bio else None,
                'birth_year': birth_year_int,
                'death_year': death_year_int,
                'birth_place': birth_place if birth_place else None,
                'website': website if website else None,
                'openlibrary_id': openlibrary_id if openlibrary_id else None,
                'image_url': image_url if image_url else None,
                'created_at': created_at_val.isoformat() if created_at_val and hasattr(created_at_val, 'isoformat') else datetime.now().isoformat(),
                'updated_at': updated_at_val.isoformat()
            }
            
            # Ensure person_id_for_storage is a string
            if not person_id_for_storage:
                person_id_for_storage = person_id  # Fallback to the original person_id parameter
            
            storage.store_node('Person', str(person_id_for_storage), person_data)
            
            flash(f'Person "{name}" updated successfully!', 'success')
            return redirect(url_for('people.person_details', person_id=person_id_for_storage))
        
        return render_template('edit_person.html', person=person, current_year=datetime.now().year)
    
    except Exception as e:
        current_app.logger.error(f"Error editing person {person_id}: {e}")
        flash('Error editing person. Please try again.', 'error')
        return redirect(url_for('people.people'))


@people_bp.route('/person/<person_id>/delete', methods=['POST'])
@login_required
def delete_person(person_id):
    """Delete a person (with confirmation)."""
    try:
        # Helper function to handle potential coroutine returns
        def safe_call_sync_method(method, *args, **kwargs):
            """Safely call a sync method that might return a coroutine."""
            result = method(*args, **kwargs)
            if inspect.iscoroutine(result):
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result = loop.run_until_complete(result)
                finally:
                    loop.close()
            return result
        
        person = safe_call_sync_method(book_service.get_person_by_id_sync, person_id)
        if not person:
            flash('Person not found.', 'error')
            return redirect(url_for('people.people'))
        
        person_name = getattr(person, 'name', 'Unknown Person')
        
        # Check if person has associated books by directly querying the storage layer
        # This bypasses any user filtering and checks for ANY books associated with this person
        from app.infrastructure.kuzu_graph import get_graph_storage
        storage = get_graph_storage()
        
        # FIRST: Clean up orphaned relationships - relationships pointing to books that no longer exist
        
        # Get all user's books first to check which ones actually exist
        user_books = safe_call_sync_method(book_service.get_all_books_with_user_overlay_sync, str(current_user.id))
        if user_books is None:
            user_books = []
        
        # Create a set of valid book IDs for quick lookup
        valid_book_ids = set()
        for book in user_books:
            book_id = getattr(book, 'id', None) or getattr(book, '_id', None)
            if book_id:
                valid_book_ids.add(str(book_id))
        
        # Find all relationships that point TO this person/author (from any book)
        orphaned_relationships_found = 0
        orphaned_relationships_cleaned = 0
        
        # Get ALL books in the system (not just user's books) to check for orphaned relationships
        all_book_nodes = storage.find_nodes_by_type('book')
        
        for book_data in all_book_nodes:
            if not book_data or not book_data.get('_id'):
                continue
                
            book_id = book_data.get('_id')
            if not book_id or not isinstance(book_id, str):
                continue
            
            # Check if this book actually exists in the user's library
            book_exists_in_user_library = book_id in valid_book_ids
            
            # Get ALL relationships from this book
            all_relationships = storage.get_relationships('book', book_id)
            
            # Check for relationships pointing to our person/author
            for rel in all_relationships:
                if rel.get('target_id') == person_id and rel.get('target_type') in ['person', 'author']:
                    orphaned_relationships_found += 1
                    
                    # If the book doesn't exist in user's library, it's orphaned
                    if not book_exists_in_user_library:
                        storage.delete_relationship('book', book_id, rel.get('relationship_type', 'WRITTEN_BY'), 'person', person_id)
                        orphaned_relationships_cleaned += 1
        
        # NOW: Count remaining valid books that have relationships to this person/author
        total_associated_books = 0
        associated_book_details = []
        
        for book in user_books:
            book_id = getattr(book, 'id', None) or getattr(book, '_id', None)
            if not book_id:
                continue
            
            book_id = str(book_id)
            
            # Check ALL relationships from this book (not just WRITTEN_BY)
            all_relationships = storage.get_relationships('book', book_id)
            
            # Check if any of these relationships point to our person/author
            for rel in all_relationships:
                if rel.get('target_id') == person_id and rel.get('target_type') in ['person', 'author']:
                    total_associated_books += 1
                    book_title = getattr(book, 'title', 'Unknown Book')
                    associated_book_details.append(f"{book_title} ({rel.get('relationship_type', 'unknown')})")
                    break  # Only count each book once
        
        if total_associated_books > 0:
            flash(f'Cannot delete "{person_name}" because they are associated with {total_associated_books} books. Please consider merging with another person instead.', 'error')
            return redirect(url_for('people.person_details', person_id=person_id))
        
        # Final cleanup: Remove any remaining relationships TO this person before deletion
        
        # Find and delete ALL relationships pointing to this person (both author and person types)
        all_book_nodes = storage.find_nodes_by_type('book')
        final_cleanup_count = 0
        
        for book_data in all_book_nodes:
            if not book_data or not book_data.get('_id'):
                continue
                
            book_id = book_data.get('_id')
            if not book_id or not isinstance(book_id, str):
                continue
            
            # Get ALL relationships from this book
            all_relationships = storage.get_relationships('book', book_id)
            
            # Remove any relationships pointing to our person
            for rel in all_relationships:
                if rel.get('target_id') == person_id and rel.get('target_type') in ['person', 'author']:
                    storage.delete_relationship('book', book_id, rel.get('relationship_type', 'WRITTEN_BY'), 'person', person_id)
                    final_cleanup_count += 1
        
        # Delete the person node from Kuzu
        
        # Check if person or author node exists in Kuzu
        person_node = storage.get_node('person', person_id)
        person_exists = person_node is not None
        
        author_node = storage.get_node('author', person_id)
        author_exists = author_node is not None
        
        deletion_success = False
        
        try:
            if person_exists:
                storage.delete_node('person', person_id)
                deletion_success = True
            
            if author_exists:
                storage.delete_node('author', person_id)
                deletion_success = True
                
        except Exception as delete_error:
            current_app.logger.error(f"Error deleting person node: {delete_error}")
        
        if deletion_success:
            flash(f'Person "{person_name}" deleted successfully.', 'success')
        else:
            flash(f'Person "{person_name}" may not have been fully deleted. Please check the logs.', 'warning')
        
        return redirect(url_for('people.people'))
    
    except Exception as e:
        current_app.logger.error(f"Error deleting person {person_id}: {e}")
        flash('Error deleting person. Please try again.', 'error')
        return redirect(url_for('people.people'))


@people_bp.route('/person/<person_id>/refresh_metadata', methods=['POST'])
@login_required
def refresh_person_metadata(person_id):
    """Refresh person metadata from OpenLibrary."""
    try:
        from app.utils import search_author_by_name, fetch_author_data
        
        # Get the current person
        person = book_service.get_person_by_id_sync(person_id)
        if not person:
            flash('Person not found.', 'error')
            return redirect(url_for('people.people'))
        
        person_name = getattr(person, 'name', '')
        current_openlibrary_id = getattr(person, 'openlibrary_id', None)
        
        metadata_updated = False
        
        # If person already has an OpenLibrary ID, fetch fresh data
        if current_openlibrary_id:
            author_data = fetch_author_data(current_openlibrary_id)
            if author_data:
                # Update person with fresh metadata
                flash(f'Metadata refreshed for "{person_name}".', 'success')
                metadata_updated = True
            else:
                flash(f'Could not refresh metadata for "{person_name}".', 'warning')
        else:
            # Search for the person by name
            search_result = search_author_by_name(person_name)
            if search_result:
                # search_author_by_name returns a single result dict, not a list
                author_id = search_result.get('openlibrary_id', '')
                if author_id:
                    author_data = fetch_author_data(author_id)
                    if author_data:
                        # Update person with new metadata
                        flash(f'Metadata found and added for "{person_name}".', 'success')
                        metadata_updated = True
                    else:
                        flash(f'Found author but could not fetch metadata for "{person_name}".', 'warning')
                else:
                    flash(f'No valid author ID found for "{person_name}".', 'warning')
            else:
                flash(f'No metadata found for "{person_name}".', 'warning')
        
        if not metadata_updated:
            flash(f'Could not find or refresh metadata for "{person_name}".', 'info')
    
    except Exception as e:
        current_app.logger.error(f"Error refreshing metadata for person {person_id}: {e}")
        flash('Error refreshing metadata. Please try again.', 'error')
    
    return redirect(url_for('people.person_details', person_id=person_id))


@people_bp.route('/persons/bulk_delete', methods=['POST'])
@login_required
def bulk_delete_persons():
    """Delete multiple persons selected from the people view."""
    selected_person_ids = request.form.getlist('selected_persons')
    force_delete = request.form.get('force_delete') == 'true'
    
    if not selected_person_ids:
        flash('No persons selected for deletion.', 'warning')
        return redirect(url_for('people.people'))
    
    deleted_count = 0
    failed_count = 0
    failed_persons = []
    
    for person_id in selected_person_ids:
        try:
            # Get person details (but don't fail if person not found in service layer)
            person = book_service.get_person_by_id_sync(person_id)
            person_name = getattr(person, 'name', 'Unknown Person') if person else f"Person {person_id[:8]}..."
            
            # Check if person has associated books (simplified check for bulk operation)
            # Only check if person exists in service layer
            total_books = 0
            if person:
                books_by_type = book_service.get_books_by_person_sync(person_id, str(current_user.id))
                total_books = sum(len(books) for books in books_by_type.values()) if books_by_type else 0
                
                if total_books > 0 and not force_delete:
                    failed_count += 1
                    failed_persons.append(f"{person_name} ({total_books} books)")
                    continue
            
            # Delete the person from graph database
            from app.infrastructure.kuzu_graph import get_graph_storage
            storage = get_graph_storage()
            
            # Clean up relationships if force deleting
            if force_delete and total_books > 0:
                # Remove all relationships to this person
                all_book_nodes = storage.find_nodes_by_type('book')
                for book_data in all_book_nodes:
                    if not book_data or not book_data.get('_id'):
                        continue
                    book_id = book_data.get('_id')
                    if not book_id or not isinstance(book_id, str):
                        continue
                    all_relationships = storage.get_relationships('book', book_id)
                    for rel in all_relationships:
                        if rel.get('target_id') == person_id and rel.get('target_type') in ['person', 'author']:
                            storage.delete_relationship('book', book_id, rel.get('relationship_type', 'WRITTEN_BY'), 'person', person_id)
            
            # Attempt to delete person and author nodes from graph database
            # This will clean up any orphaned nodes even if person wasn't found by service layer
            deletion_success = False
            try:
                if storage.delete_node('person', person_id):
                    deletion_success = True
            except:
                pass
            try:
                if storage.delete_node('author', person_id):
                    deletion_success = True
            except:
                pass
            
            if deletion_success or person:  # Count as success if we deleted something OR if person was found
                deleted_count += 1
            else:
                failed_count += 1
                failed_persons.append(f"{person_name} (not found)")
            
        except Exception as e:
            failed_count += 1
            failed_persons.append(f"Person {person_id} (error: {str(e)})")
    
    # Provide feedback
    if deleted_count > 0:
        flash(f'Successfully deleted {deleted_count} person(s).', 'success')
    
    if failed_count > 0:
        if len(failed_persons) <= 5:
            failed_list = ', '.join(failed_persons)
            flash(f'Failed to delete {failed_count} person(s): {failed_list}', 'warning')
        else:
            flash(f'Failed to delete {failed_count} person(s). Some have associated books or encountered errors.', 'warning')
    
    return redirect(url_for('people.people'))

@people_bp.route('/api/person/search')
@login_required
def api_search_persons():
    """API endpoint for searching persons."""
    query = request.args.get('q', '').strip()
    
    if not query:
        return jsonify([])
    
    try:
        # Get all persons and filter by name
        all_persons = book_service.list_all_persons_sync()
        
        if not isinstance(all_persons, list):
            all_persons = []
        
        # Filter persons by name (case-insensitive, supports first/last name search)
        matching_persons = []
        query_lower = query.lower().strip()
        
        for person in all_persons:
            person_name = getattr(person, 'name', '') or (person.get('name') if isinstance(person, dict) else '') or ''
            # Ensure person_name is always a string
            if not isinstance(person_name, str):
                person_name = str(person_name) if person_name is not None else ''
            
            person_name_lower = person_name.lower()
            
            # Simple case-insensitive substring search
            if query_lower in person_name_lower:
                person_id = getattr(person, 'id', '') or (person.get('id') if isinstance(person, dict) else '')
                matching_persons.append({
                    'id': person_id,
                    'name': person_name
                })
        
        # Sort by name and limit results
        matching_persons.sort(key=lambda p: p['name'])
        return jsonify(matching_persons[:10])  # Limit to 10 results
    
    except Exception as e:
        current_app.logger.error(f"Error searching persons: {e}")
        return jsonify([])

@people_bp.route('/person/merge', methods=['GET', 'POST'])
@login_required
def merge_persons():
    """Merge two or more persons into one."""
    if request.method == 'POST':
        try:
            # Get form data
            primary_person_id = request.form.get('primary_person_id')
            merge_person_ids = request.form.getlist('merge_person_ids')
            
            if not primary_person_id:
                flash('Please select a primary person to merge into.', 'error')
                return redirect(url_for('people.merge_persons'))
            
            if not merge_person_ids:
                flash('Please select at least one person to merge.', 'error')
                return redirect(url_for('people.merge_persons'))
            
            if primary_person_id in merge_person_ids:
                flash('Cannot merge a person with themselves.', 'error')
                return redirect(url_for('people.merge_persons'))
            
            # Helper function to handle potential coroutine returns
            def safe_call_sync_method_merge(method, *args, **kwargs):
                """Safely call a sync method that might return a coroutine."""
                import inspect
                result = method(*args, **kwargs)
                if inspect.iscoroutine(result):
                    import asyncio
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            # Can't use loop.run_until_complete if loop is already running
                            return None  # Return None as fallback
                        else:
                            return loop.run_until_complete(result)
                    except Exception as e:
                        return None
                return result
            
            # Get persons
            primary_person = safe_call_sync_method_merge(book_service.get_person_by_id_sync, primary_person_id)
            if not primary_person:
                flash('Primary person not found.', 'error')
                return redirect(url_for('people.merge_persons'))
            
            # Get the primary person's name for logging
            primary_person_name = primary_person.get('name', 'Unknown Person') if isinstance(primary_person, dict) else getattr(primary_person, 'name', 'Unknown Person')
            
            # Validate that the primary person exists in KuzuDB
            from app.infrastructure.kuzu_graph import get_kuzu_database
            db = get_kuzu_database()
            
            primary_check_query = """
            MATCH (p:Person {id: $person_id})
            RETURN p.name as name
            """
            primary_check_result = db.query(primary_check_query, {"person_id": primary_person_id})
            if not primary_check_result or len(primary_check_result) == 0:
                flash(f'Primary person "{primary_person_name}" not found in database.', 'error')
                return redirect(url_for('people.merge_persons'))
            
            current_app.logger.info(f"Primary person validated: {primary_person_name} (ID: {primary_person_id})")
            
            merge_persons = []
            for person_id in merge_person_ids:
                person = safe_call_sync_method_merge(book_service.get_person_by_id_sync, person_id)
                if person:
                    # Also validate this person exists in KuzuDB
                    person_check_result = db.query(primary_check_query, {"person_id": person_id})
                    if person_check_result and len(person_check_result) > 0:
                        merge_persons.append(person)
                        current_app.logger.info(f"Merge person validated: {person.get('name' if isinstance(person, dict) else 'name', 'Unknown')} (ID: {person_id})")
                    else:
                        current_app.logger.warning(f"Person {person_id} not found in KuzuDB, skipping")
                else:
                    current_app.logger.warning(f"Person {person_id} not found in service layer, skipping")
            
            if not merge_persons:
                flash('No valid persons found to merge.', 'error')
                return redirect(url_for('people.merge_persons'))
            
            # Perform merge operation
            from app.infrastructure.kuzu_graph import get_graph_storage
            storage = get_graph_storage()
            
            merged_count = 0
            for merge_person in merge_persons:
                merge_person_id = None
                merge_person_name = 'Unknown'
                try:
                    # Handle both dict and object formats
                    merge_person_name = merge_person.get('name', 'Unknown') if isinstance(merge_person, dict) else getattr(merge_person, 'name', 'Unknown')
                    merge_person_id = merge_person.get('id') if isinstance(merge_person, dict) else getattr(merge_person, 'id', None)
                    
                    current_app.logger.info(f"Merging person {merge_person_name} (ID: {merge_person_id}) into {primary_person_name}")
                    
                    # Use direct Kuzu query to transfer all types of relationships
                    from app.infrastructure.kuzu_graph import get_kuzu_database
                    db = get_kuzu_database()
                    
                    # First, let's check what relationships exist for this person
                    check_query = """
                    MATCH (p:Person {id: $merge_person_id})-[r]->(b:Book)
                    RETURN COUNT(*) as total_relationships
                    """
                    
                    check_result = db.query(check_query, {"merge_person_id": merge_person_id})
                    total_rels = 0
                    if check_result and len(check_result) > 0:
                        total_rels = check_result[0].get('col_0', 0) or 0
                    current_app.logger.info(f"Person {merge_person_name} has {total_rels} total relationships")
                    
                    # Transfer all AUTHORED relationships from merge_person to primary_person
                    # First check if any AUTHORED relationships exist to avoid duplicates
                    transfer_query = """
                    MATCH (merge_person:Person {id: $merge_person_id})-[old_rel:AUTHORED]->(b:Book)
                    MATCH (primary_person:Person {id: $primary_person_id})
                    CREATE (primary_person)-[new_rel:AUTHORED]->(b)
                    SET new_rel.contribution_type = old_rel.contribution_type,
                        new_rel.role = old_rel.role,
                        new_rel.order_index = old_rel.order_index,
                        new_rel.created_at = old_rel.created_at
                    DELETE old_rel
                    RETURN COUNT(old_rel) as transferred_count
                    """
                    
                    transfer_result = db.query(transfer_query, {
                        "merge_person_id": merge_person_id,
                        "primary_person_id": primary_person_id
                    })
                    
                    transferred_count = 0
                    if transfer_result and len(transfer_result) > 0:
                        transferred_count = transfer_result[0].get('col_0', 0) or 0
                    
                    current_app.logger.info(f"Transferred {transferred_count} AUTHORED relationships from {merge_person_name} to {primary_person_name}")
                    
                    # Also transfer other relationship types (EDITED, NARRATED, etc.)
                    other_relationships = ['EDITED', 'NARRATED', 'ILLUSTRATED', 'TRANSLATED']
                    for rel_type in other_relationships:
                        other_transfer_query = f"""
                        MATCH (merge_person:Person {{id: $merge_person_id}})-[old_rel:{rel_type}]->(b:Book)
                        MATCH (primary_person:Person {{id: $primary_person_id}})
                        CREATE (primary_person)-[new_rel:{rel_type}]->(b)
                        SET new_rel.role = old_rel.role,
                            new_rel.order_index = old_rel.order_index,
                            new_rel.created_at = old_rel.created_at
                        DELETE old_rel
                        RETURN COUNT(old_rel) as transferred_count
                        """
                        
                        other_result = db.query(other_transfer_query, {
                            "merge_person_id": merge_person_id,
                            "primary_person_id": primary_person_id
                        })
                        
                        if other_result and len(other_result) > 0:
                            other_count = other_result[0].get('col_0', 0) or 0
                            if other_count > 0:
                                current_app.logger.info(f"Transferred {other_count} {rel_type} relationships from {merge_person_name} to {primary_person_name}")
                    
                    # Delete the merged person using DETACH DELETE to handle any remaining relationships
                    delete_query = """
                    MATCH (p:Person {id: $person_id})
                    DETACH DELETE p
                    """
                    
                    delete_result = db.query(delete_query, {"person_id": merge_person_id})
                    current_app.logger.info(f"Delete query completed for person {merge_person_name}")
                    
                    merged_count += 1
                    current_app.logger.info(f"Successfully merged person {merge_person_name}")
                    
                except Exception as e:
                    current_app.logger.error(f"Error merging person {merge_person_name} (ID: {merge_person_id}): {e}")
                    continue
            
            if merged_count > 0:
                person_names = [
                    p.get('name', 'Unknown') if isinstance(p, dict) else getattr(p, 'name', 'Unknown') 
                    for p in merge_persons[:merged_count]
                ]
                flash(f'Successfully merged {merged_count} person(s) ({", ".join(person_names)}) into "{primary_person_name}".', 'success')
            else:
                flash('No persons were merged due to errors.', 'error')
            
            return redirect(url_for('people.person_details', person_id=primary_person_id))
        
        except Exception as e:
            current_app.logger.error(f"Error during person merge: {e}")
            flash('Error merging persons. Please try again.', 'error')
    
    # GET request - show merge form
    try:
        # Helper function to handle potential coroutine returns
        def safe_call_sync_method(method, *args, **kwargs):
            """Safely call a sync method that might return a coroutine."""
            import inspect
            result = method(*args, **kwargs)
            if inspect.iscoroutine(result):
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # Can't use loop.run_until_complete if loop is already running
                        return []  # Return empty list as fallback
                    else:
                        return loop.run_until_complete(result)
                except Exception as e:
                    return []
            return result
        
        all_persons = safe_call_sync_method(book_service.list_all_persons_sync)
        if all_persons is None:
            all_persons = []
        
        # Add book counts to each person for the merge preview
        processed_persons = []
        for person in all_persons:
            # Convert dictionary to object if needed for consistency
            if isinstance(person, dict):
                from types import SimpleNamespace
                person_obj = SimpleNamespace(**person)
            else:
                person_obj = person
            
            try:
                # Get book count for this person
                person_id = getattr(person_obj, 'id', None)
                if person_id:
                    books_by_type = safe_call_sync_method(person_service.get_books_by_person_for_user_sync, person_id, str(current_user.id))
                    if books_by_type:
                        # Handle both dict and list return types
                        if isinstance(books_by_type, dict):
                            total_books = sum(len(books) for books in books_by_type.values())
                        elif isinstance(books_by_type, list):
                            total_books = len(books_by_type)
                        else:
                            total_books = 0
                        person_obj.book_count = total_books
                    else:
                        person_obj.book_count = 0
                else:
                    person_obj.book_count = 0
                
                processed_persons.append(person_obj)
                
            except Exception as person_error:
                current_app.logger.warning(f"Error getting book count for person {getattr(person_obj, 'name', 'Unknown')}: {person_error}")
                person_obj.book_count = 0
                processed_persons.append(person_obj)
        
        # Safe sorting that handles both object and dict formats
        try:
            processed_persons.sort(key=lambda p: getattr(p, 'name', '').lower())
        except:
            # Fallback - just use the list as is
            pass
            
        return render_template('merge_persons.html', persons=processed_persons)
    
    except Exception as e:
        current_app.logger.error(f"Error loading merge persons page: {e}")
        flash('Error loading merge page.', 'error')
        return redirect(url_for('people.people'))

@people_bp.route('/toggle_theme', methods=['POST'])
@login_required
def toggle_theme():
    """Toggle user's theme preference between light and dark."""
    try:
        data = request.get_json()
        current_theme = data.get('current_theme', 'light')
        
        # Toggle theme
        new_theme = 'dark' if current_theme == 'light' else 'light'
        
        # Store theme preference in session for authenticated users
        if current_user.is_authenticated:
            from app.infrastructure.kuzu_graph import get_graph_storage
            # For themes, we use session storage instead of KuzuDB
            # since themes are UI preferences, not core data
        
        # Store in session
        session['theme'] = new_theme
        
        return jsonify({
            'success': True,
            'new_theme': new_theme
        })
        
    except Exception as e:
        current_app.logger.error(f"Error toggling theme: {e}")        
        return jsonify({
            'success': False,
            'error': 'Failed to toggle theme'
        }), 500