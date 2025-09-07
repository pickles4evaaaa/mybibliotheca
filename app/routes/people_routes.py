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
from app.services.kuzu_series_service import get_series_service  # type: ignore
from app.utils.safe_kuzu_manager import SafeKuzuManager, get_safe_kuzu_manager

# Helper function for query result conversion
def _convert_query_result_to_list(result) -> list:
    """Convert KuzuDB query result to list of dictionaries."""
    if not result:
        return []
    
    data = []
    while result.has_next():
        row = result.get_next()
        record = {}
        for i in range(len(row)):
            column_name = result.get_column_names()[i]
            record[column_name] = row[i]
        data.append(record)
    
    return data

# Create people blueprint
people_bp = Blueprint('people', __name__)

@people_bp.route('/people')
@login_required
def people():
    """Display all people with management options."""
    
    try:
        
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
        
        all_persons = safe_call_sync_method(book_service.list_all_persons_sync, str(current_user.id))
        
        # Ensure we have a list
        if not isinstance(all_persons, list):
            all_persons = []
        
        # Convert dictionaries to objects for template compatibility
        processed_persons = []
        
        # Add book counts and contributions for each person
        for i, person in enumerate(all_persons):
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
                    # Use global book count (all books authored by this person)
                    all_books_by_person = safe_call_sync_method(person_service.get_books_by_person_sync, person_id)
                    if all_books_by_person:
                        person_obj.book_count = len(all_books_by_person)
                        # For the template, organize by relationship type
                        books_by_type = {}
                        for book in all_books_by_person:
                            rel_type = book.get('relationship_type', 'authored')
                            if rel_type not in books_by_type:
                                books_by_type[rel_type] = []
                            books_by_type[rel_type].append(book)
                        person_obj.contributions = books_by_type
                    else:
                        person_obj.book_count = 0
                        person_obj.contributions = {}
                else:
                    person_obj.book_count = 0
                    person_obj.contributions = {}
                
                processed_persons.append(person_obj)
                
            except Exception as person_error:
                person_obj.book_count = 0
                person_obj.contributions = {}
                processed_persons.append(person_obj)
        
        # Sort by name safely
        try:
            processed_persons.sort(key=lambda p: getattr(p, 'name', '').lower())
        except Exception as sort_error:
            pass
        
        # Show summary of what we found
        try:
            total_with_books = sum(1 for p in processed_persons if getattr(p, 'book_count', 0) > 0)
        except Exception as summary_error:
            pass
        
        # Get contribution type counts for the accordion
        try:
            contribution_counts = safe_call_sync_method(person_service.get_contribution_type_counts_sync)
        except Exception as counts_error:
            contribution_counts = {}
        
        template_data = {'persons': processed_persons, 'contribution_counts': contribution_counts}
        
        return render_template('people.html', persons=processed_persons, contribution_counts=contribution_counts)
    
    except Exception as e:
        traceback.print_exc()
        current_app.logger.error(f"Error loading people page: {e}")
        flash('Error loading people page.', 'error')
        return redirect(url_for('main.library'))


@people_bp.route('/person/<person_id>')
@login_required
def person_details(person_id):
    """Display detailed information about a person."""
    
    try:
        # Get person details
        person = book_service.get_person_by_id_sync(person_id)
        
        if not person:
            flash('Person not found.', 'error')
            return redirect(url_for('people.people'))
        
        # Get person name and ID safely
        person_name = getattr(person, 'name', None) or (person.get('name') if isinstance(person, dict) else 'Unknown')
        person_id_val = getattr(person, 'id', None) or (person.get('id') if isinstance(person, dict) else person_id)
        
        # Get books by this person for current user
        books_by_type = book_service.get_books_by_person_sync(person_id, str(current_user.id))
        
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

        # Series associations: find distinct series for books person contributed to
        try:
            series_ids = set()
            for books in converted_books_by_type.values():
                for b in books:
                    sid = None
                    if b.get('series') and isinstance(b.get('series'), dict):
                        sid = b['series'].get('id')
                    elif b.get('series') and hasattr(b.get('series'), 'id'):
                        sid = b.get('series').id  # type: ignore[attr-defined]
                    if sid:
                        series_ids.add(sid)
            series_objs = []
            if series_ids:
                ssvc = get_series_service()
                for sid in series_ids:
                    s = ssvc.get_series(sid)
                    if s:
                        series_objs.append(s)
                # Attach to person object for template
                try:
                    person.series_associations = series_objs  # type: ignore[attr-defined]
                except Exception:
                    pass
        except Exception:
            pass
        
        return render_template('person_details.html', 
                             person=person, 
                             contributions_by_type=converted_books_by_type)
    
    except Exception as e:
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
                birth_place=birth_place if birth_place else None,
                website=website if website else None,
                created_at=datetime.now()
            )
            
            # Use the repository to create the person (includes automatic OpenLibrary metadata enrichment)
            try:
                from app.infrastructure.kuzu_repositories import KuzuPersonRepository
                person_repo = KuzuPersonRepository()
                
                created_person = person_repo.create(person)
                if created_person:
                    flash(f'Person "{name}" added successfully with OpenLibrary metadata!', 'success')
                    return redirect(url_for('people.person_details', person_id=person.id))
                else:
                    flash('Error saving person. Please try again.', 'error')
                    
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
            import json
            
            name = request.form.get('name', '').strip()
            bio = request.form.get('bio', '').strip()
            birth_year = request.form.get('birth_year')
            death_year = request.form.get('death_year')
            birth_place = request.form.get('birth_place', '').strip()
            website = request.form.get('website', '').strip()
            openlibrary_id = request.form.get('openlibrary_id', '').strip()
            image_url = request.form.get('image_url', '').strip()
            
            # New comprehensive fields
            birth_date = request.form.get('birth_date', '').strip()
            death_date = request.form.get('death_date', '').strip()
            title = request.form.get('title', '').strip()
            fuller_name = request.form.get('fuller_name', '').strip()
            wikidata_id = request.form.get('wikidata_id', '').strip()
            imdb_id = request.form.get('imdb_id', '').strip()
            alternate_names_str = request.form.get('alternate_names', '').strip()
            official_links_str = request.form.get('official_links', '').strip()
            
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
            
            # Process alternate names
            alternate_names_json = None
            if alternate_names_str:
                try:
                    # Try to parse as existing JSON first
                    if alternate_names_str.startswith('[') and alternate_names_str.endswith(']'):
                        alternate_names_list = json.loads(alternate_names_str)
                    else:
                        # Split by newlines or commas
                        if '\n' in alternate_names_str:
                            alternate_names_list = [name.strip() for name in alternate_names_str.split('\n') if name.strip()]
                        else:
                            alternate_names_list = [name.strip() for name in alternate_names_str.split(',') if name.strip()]
                    
                    if alternate_names_list:
                        alternate_names_json = json.dumps(alternate_names_list)
                except json.JSONDecodeError:
                    # If JSON parsing fails, treat as comma-separated
                    alternate_names_list = [name.strip() for name in alternate_names_str.split(',') if name.strip()]
                    if alternate_names_list:
                        alternate_names_json = json.dumps(alternate_names_list)
            
            # Process official links
            official_links_json = None
            if official_links_str:
                try:
                    # Try to parse as existing JSON first
                    if official_links_str.startswith('[') and official_links_str.endswith(']'):
                        links_data = json.loads(official_links_str)
                    else:
                        # Split by newlines and create simple link objects
                        urls = [url.strip() for url in official_links_str.split('\n') if url.strip() and url.strip().startswith('http')]
                        links_data = []
                        for url in urls:
                            links_data.append({
                                'title': 'Official Link',
                                'url': url,
                                'type': ''
                            })
                    
                    if links_data:
                        official_links_json = json.dumps(links_data)
                except json.JSONDecodeError:
                    # If JSON parsing fails, treat as URL list
                    urls = [url.strip() for url in official_links_str.split('\n') if url.strip() and url.strip().startswith('http')]
                    if urls:
                        links_data = [{'title': 'Official Link', 'url': url, 'type': ''} for url in urls]
                        official_links_json = json.dumps(links_data)
            
            # Update person data using safe attribute access
            # Note: We don't directly assign to person attributes due to type uncertainty
            # Instead, we build the updated data dictionary directly for storage
            
            # Update normalized name
            person_name = getattr(person, 'name', None) or (person.get('name') if isinstance(person, dict) else name)
            normalized_name = Person._normalize_name(person_name or name)  # Ensure we have a string
            
            # Update in KuzuDB using SafeKuzuManager
            safe_manager = get_safe_kuzu_manager()
            
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
                # New comprehensive fields
                'birth_date': birth_date if birth_date else None,
                'death_date': death_date if death_date else None,
                'title': title if title else None,
                'fuller_name': fuller_name if fuller_name else None,
                'wikidata_id': wikidata_id if wikidata_id else None,
                'imdb_id': imdb_id if imdb_id else None,
                'alternate_names': alternate_names_json,
                'official_links': official_links_json,
                'created_at': created_at_val.isoformat() if created_at_val and hasattr(created_at_val, 'isoformat') else datetime.now().isoformat(),
                'updated_at': updated_at_val.isoformat()
            }
            
            # Ensure person_id_for_storage is a string
            if not person_id_for_storage:
                person_id_for_storage = person_id  # Fallback to the original person_id parameter
            
            # Update person using direct Cypher query
            update_query = f"""
            MATCH (p:Person {{id: $person_id}})
            SET p.name = $name,
                p.normalized_name = $normalized_name,
                p.bio = $bio,
                p.birth_year = $birth_year,
                p.death_year = $death_year,
                p.birth_place = $birth_place,
                p.website = $website,
                p.openlibrary_id = $openlibrary_id,
                p.image_url = $image_url,
                p.birth_date = $birth_date,
                p.death_date = $death_date,
                p.title = $title,
                p.fuller_name = $fuller_name,
                p.wikidata_id = $wikidata_id,
                p.imdb_id = $imdb_id,
                p.alternate_names = $alternate_names,
                p.official_links = $official_links,
                p.updated_at = $updated_at
            RETURN p.id
            """
            
            parameters = {
                'person_id': str(person_id_for_storage),
                'name': name,
                'normalized_name': normalized_name,
                'bio': bio if bio else None,
                'birth_year': birth_year_int,
                'death_year': death_year_int,
                'birth_place': birth_place if birth_place else None,
                'website': website if website else None,
                'openlibrary_id': openlibrary_id if openlibrary_id else None,
                'image_url': image_url if image_url else None,
                'birth_date': birth_date if birth_date else None,
                'death_date': death_date if death_date else None,
                'title': title if title else None,
                'fuller_name': fuller_name if fuller_name else None,
                'wikidata_id': wikidata_id if wikidata_id else None,
                'imdb_id': imdb_id if imdb_id else None,
                'alternate_names': alternate_names_json,
                'official_links': official_links_json,
                'updated_at': updated_at_val.isoformat()
            }
            
            result = safe_manager.execute_query(update_query, parameters)
            
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
        safe_manager = get_safe_kuzu_manager()
        
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
        all_books_query = "MATCH (b:Book) RETURN b.id as id"
        all_book_result = safe_manager.execute_query(all_books_query)
        all_book_nodes = _convert_query_result_to_list(all_book_result)
        
        for book_data in all_book_nodes:
            if not book_data or not book_data.get('id'):
                continue
                
            book_id = book_data.get('id')
            if not book_id or not isinstance(book_id, str):
                continue
            
            # Check if this book actually exists in the user's library
            book_exists_in_user_library = book_id in valid_book_ids
            
            # Get ALL relationships from this book to our person
            book_relationships_query = """
            MATCH (b:Book {id: $book_id})-[r]->(p:Person {id: $person_id})
            RETURN r
            """
            
            rel_result = safe_manager.execute_query(book_relationships_query, {
                'book_id': book_id,
                'person_id': person_id
            })
            all_relationships = _convert_query_result_to_list(rel_result)
            
            # Check for relationships pointing to our person/author
            for rel_data in all_relationships:
                orphaned_relationships_found += 1
                
                # If the book doesn't exist in user's library, it's orphaned
                if not book_exists_in_user_library:
                    delete_rel_query = """
                    MATCH (b:Book {id: $book_id})-[r]->(p:Person {id: $person_id})
                    DELETE r
                    """
                    safe_manager.execute_query(delete_rel_query, {
                        'book_id': book_id,
                        'person_id': person_id
                    })
                    orphaned_relationships_cleaned += 1
        
        # NOW: Count remaining valid books that have relationships to this person/author
        total_associated_books = 0
        associated_book_details = []
        
        for book in user_books:
            book_id = getattr(book, 'id', None) or getattr(book, '_id', None)
            if not book_id:
                continue
            
            book_id = str(book_id)
            
            # Check ALL relationships from this book to our person
            book_relationships_query = """
            MATCH (b:Book {id: $book_id})-[r]->(p:Person {id: $person_id})
            RETURN r
            """
            
            rel_result = safe_manager.execute_query(book_relationships_query, {
                'book_id': book_id,
                'person_id': person_id
            })
            all_relationships = _convert_query_result_to_list(rel_result)
            
            # Check if any of these relationships point to our person/author
            for rel_data in all_relationships:
                total_associated_books += 1
                book_title = getattr(book, 'title', 'Unknown Book')
                associated_book_details.append(f"{book_title} (relationship)")
                break  # Only count each book once
        
        if total_associated_books > 0:
            flash(f'Cannot delete "{person_name}" because they are associated with {total_associated_books} books. Please consider merging with another person instead.', 'error')
            return redirect(url_for('people.person_details', person_id=person_id))
        
        # Final cleanup: Remove any remaining relationships TO this person before deletion
        
        # Find and delete ALL relationships pointing to this person (both author and person types)
        all_books_query = "MATCH (b:Book) RETURN b.id as id"
        all_book_result = safe_manager.execute_query(all_books_query)
        all_book_nodes = _convert_query_result_to_list(all_book_result)
        final_cleanup_count = 0
        
        for book_data in all_book_nodes:
            if not book_data or not book_data.get('id'):
                continue
                
            book_id = book_data.get('id')
            if not book_id or not isinstance(book_id, str):
                continue
            
            # Get ALL relationships from this book to our person
            book_relationships_query = """
            MATCH (b:Book {id: $book_id})-[r]->(p:Person {id: $person_id})
            RETURN r
            """
            
            rel_result = safe_manager.execute_query(book_relationships_query, {
                'book_id': book_id,
                'person_id': person_id
            })
            all_relationships = _convert_query_result_to_list(rel_result)
            
            # Remove any relationships pointing to our person
            for rel_data in all_relationships:
                delete_rel_query = """
                MATCH (b:Book {id: $book_id})-[r]->(p:Person {id: $person_id})
                DELETE r
                """
                safe_manager.execute_query(delete_rel_query, {
                    'book_id': book_id,
                    'person_id': person_id
                })
                final_cleanup_count += 1
        
        # Delete the person node from Kuzu
        
        # Check if person node exists in Kuzu
        person_check_query = "MATCH (p:Person {id: $person_id}) RETURN p.name as name"
        person_result = safe_manager.execute_query(person_check_query, {'person_id': person_id})
        person_data = _convert_query_result_to_list(person_result)
        person_exists = len(person_data) > 0
        
        deletion_success = False
        
        try:
            if person_exists:
                delete_person_query = "MATCH (p:Person {id: $person_id}) DELETE p"
                safe_manager.execute_query(delete_person_query, {'person_id': person_id})
                deletion_success = True
                
        except Exception as delete_error:
            current_app.logger.error(f"Error deleting person node: {delete_error}")
        
        flash(f'Person "{person_name}" deleted successfully.', 'success')
        return redirect(url_for('people.people'))
    
    except Exception as e:
        current_app.logger.error(f"Error deleting person {person_id}: {e}")
        flash('Error deleting person. Please try again.', 'error')
        return redirect(url_for('people.people'))


def parse_openlibrary_date(date_str):
    """Parse OpenLibrary date string and extract year as integer.
    
    Args:
        date_str: Date string like "21 September 1947" or "1947"
        
    Returns:
        Integer year or None if parsing fails
    """
    if not date_str or not isinstance(date_str, str):
        return None
    
    # Try to extract year from various formats
    import re
    
    # Look for 4-digit year pattern
    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', date_str)
    if year_match:
        try:
            return int(year_match.group(1))
        except ValueError:
            pass
    
    # Try parsing just the beginning if it's a year
    try:
        if len(date_str) >= 4 and date_str[:4].isdigit():
            return int(date_str[:4])
    except ValueError:
        pass
    
    return None


def parse_comprehensive_openlibrary_data(author_data):
    """Parse comprehensive OpenLibrary author data into Person field updates.
    
    Args:
        author_data: Dictionary from OpenLibrary API
        
    Returns:
        Dictionary of field updates for Person model
    """
    import json
    
    updates = {}
    
    # Basic information
    if author_data.get('name'):
        updates['name'] = author_data['name']
    
    # Full birth/death dates
    if author_data.get('birth_date'):
        updates['birth_date'] = author_data['birth_date']
        # Also extract year for backward compatibility
        birth_year = parse_openlibrary_date(author_data['birth_date'])
        if birth_year:
            updates['birth_year'] = birth_year
    
    if author_data.get('death_date'):
        updates['death_date'] = author_data['death_date']
        # Also extract year for backward compatibility
        death_year = parse_openlibrary_date(author_data['death_date'])
        if death_year:
            updates['death_year'] = death_year
    
    # Biography and basic info
    if author_data.get('bio'):
        updates['bio'] = author_data['bio']
    
    if author_data.get('title'):
        updates['title'] = author_data['title']
    
    if author_data.get('fuller_name') or author_data.get('personal_name'):
        updates['fuller_name'] = author_data.get('fuller_name') or author_data.get('personal_name')
    
    # External service IDs
    updates['openlibrary_id'] = author_data.get('openlibrary_id', '')
    
    if author_data.get('remote_ids'):
        remote_ids = author_data['remote_ids']
        if remote_ids.get('wikidata'):
            updates['wikidata_id'] = remote_ids['wikidata']
        if remote_ids.get('imdb'):
            updates['imdb_id'] = remote_ids['imdb']
    
    # Alternate names
    if author_data.get('alternate_names'):
        # Store as JSON string for database storage
        updates['alternate_names'] = json.dumps(author_data['alternate_names'])
    
    # Links - process all official links
    if author_data.get('links'):
        # Store all links as JSON
        links_data = []
        for link in author_data['links']:
            if isinstance(link, dict):
                links_data.append({
                    'title': link.get('title', ''),
                    'url': link.get('url', ''),
                    'type': link.get('type', {}).get('key', '') if link.get('type') else ''
                })
        if links_data:
            updates['official_links'] = json.dumps(links_data)
        
        # Also set the first official website as the main website field
        for link in author_data['links']:
            if isinstance(link, dict) and link.get('url'):
                if 'official' in link.get('title', '').lower() or 'website' in link.get('title', '').lower():
                    updates['website'] = link['url']
                    break
    
    # Wikipedia URL handling
    if author_data.get('wikipedia_url'):
        updates['website'] = author_data['wikipedia_url']
    elif author_data.get('website_url'):
        updates['website'] = author_data['website_url']
    
    # Photo/image
    if author_data.get('photo_url'):
        updates['image_url'] = author_data['photo_url']
    
    return updates


@people_bp.route('/person/<person_id>/refresh_metadata', methods=['POST'])
@login_required
def refresh_person_metadata(person_id):
    """Refresh person metadata from OpenLibrary."""
    try:
        from app.utils import search_author_by_name, fetch_author_data
        
        # Get the current person
        person = person_service.get_person_by_id_sync(person_id)
        if not person:
            flash('Person not found.', 'error')
            return redirect(url_for('people.people'))
        
        # Handle both dict and object formats
        if isinstance(person, dict):
            person_name = person.get('name', '')
            current_openlibrary_id = person.get('openlibrary_id', None)
        else:
            person_name = getattr(person, 'name', '')
            current_openlibrary_id = getattr(person, 'openlibrary_id', None)
        
        current_app.logger.info(f"Person data retrieved: name='{person_name}', openlibrary_id='{current_openlibrary_id}'")
        current_app.logger.info(f"Full person object: {person}")
        current_app.logger.info(f"Person object type: {type(person)}")
        if hasattr(person, '__dict__'):
            current_app.logger.info(f"Person attributes: {person.__dict__}")
        elif isinstance(person, dict):
            current_app.logger.info(f"Person dict keys: {person.keys()}")
            current_app.logger.info(f"Person dict: {person}")
        
        metadata_updated = False
        
        # If person already has an OpenLibrary ID, fetch fresh data
        if current_openlibrary_id and current_openlibrary_id.strip():
            current_app.logger.info(f"✅ Person has OpenLibrary ID - fetching fresh metadata for person '{person_name}' with OpenLibrary ID: {current_openlibrary_id}")
            author_data = fetch_author_data(current_openlibrary_id)
            current_app.logger.info(f"OpenLibrary API returned data: {author_data}")
            if author_data:
                # Update person with fresh metadata using comprehensive parser
                updates = parse_comprehensive_openlibrary_data(author_data)
                # Apply field policy filtering (people entity)
                try:
                    from app.utils.metadata_settings import get_field_policy
                    filtered = {}
                    for field, value in updates.items():
                        pol = get_field_policy('people', field)
                        mode = pol.get('mode','both')
                        if mode == 'none':
                            continue  # skip field entirely
                        if mode == 'google':
                            # no google people provider implemented yet; skip to avoid overwrite
                            continue
                        # openlibrary or both -> retain value
                        filtered[field] = value
                    updates = filtered
                except Exception:
                    pass
                
                current_app.logger.info(f"Updating person {person_id} with comprehensive data: {updates}")
                updated_person = person_service.update_person_sync(person_id, updates)
                if updated_person:
                    flash(f'Metadata refreshed for "{person_name}".', 'success')
                    metadata_updated = True
                else:
                    current_app.logger.error(f"Failed to update person {person_id} in database")
                    flash(f'Error updating metadata for "{person_name}".', 'error')
            else:
                current_app.logger.warning(f"No author data returned from OpenLibrary for ID: {current_openlibrary_id}")
                flash(f'Could not refresh metadata for "{person_name}".', 'warning')
        else:
            # Search for the person by name
            current_app.logger.info(f"❌ No OpenLibrary ID found (ID='{current_openlibrary_id}') - searching OpenLibrary for person by name: '{person_name}'")
            if not person_name or not person_name.strip():
                current_app.logger.error(f"❌ Person name is empty! Cannot search OpenLibrary. Person ID: {person_id}")
                flash(f'Person name is missing - cannot search for metadata.', 'error')
                return redirect(url_for('people.person_details', person_id=person_id))
            
            search_result = search_author_by_name(person_name)
            current_app.logger.info(f"OpenLibrary search returned: {search_result}")
            if search_result:
                # search_author_by_name returns a single result dict, not a list
                author_id = search_result.get('openlibrary_id', '')
                if author_id:
                    author_data = fetch_author_data(author_id)
                    current_app.logger.info(f"Detailed author data from OpenLibrary: {author_data}")
                    if author_data:
                        # Update person with new metadata using comprehensive parser
                        updates = parse_comprehensive_openlibrary_data(author_data)
                        # Set the OpenLibrary ID
                        updates['openlibrary_id'] = author_id
                        # Apply field policy filtering
                        try:
                            from app.utils.metadata_settings import get_field_policy
                            filtered = {}
                            for field, value in updates.items():
                                pol = get_field_policy('people', field)
                                mode = pol.get('mode','both')
                                if mode == 'none':
                                    continue
                                if mode == 'google':
                                    continue
                                filtered[field] = value
                            updates = filtered
                        except Exception:
                            pass
                        
                        current_app.logger.info(f"Updating person {person_id} with comprehensive data: {updates}")
                        updated_person = person_service.update_person_sync(person_id, updates)
                        if updated_person:
                            flash(f'Metadata found and added for "{person_name}".', 'success')
                            metadata_updated = True
                        else:
                            current_app.logger.error(f"Failed to update person {person_id} in database")
                            flash(f'Error updating metadata for "{person_name}".', 'error')
                    else:
                        current_app.logger.warning(f"No detailed author data returned for OpenLibrary ID: {author_id}")
                        flash(f'Found author but could not fetch metadata for "{person_name}".', 'warning')
                else:
                    current_app.logger.warning(f"No OpenLibrary ID found in search result: {search_result}")
                    flash(f'No valid author ID found for "{person_name}".', 'warning')
            else:
                current_app.logger.warning(f"No search results returned from OpenLibrary for: '{person_name}'")
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
            
            # Delete the person from graph database using SafeKuzuManager
            safe_manager = get_safe_kuzu_manager()
            
            # Clean up relationships if force deleting
            if force_delete and total_books > 0:
                # Remove all relationships to this person using direct Cypher
                cleanup_query = """
                MATCH (b:Book)-[r]->(p:Person {id: $person_id})
                DELETE r
                """
                safe_manager.execute_query(cleanup_query, {'person_id': person_id})
            
            # Attempt to delete person nodes from graph database
            # This will clean up any orphaned nodes even if person wasn't found by service layer
            deletion_success = False
            try:
                delete_person_query = """
                MATCH (p:Person {id: $person_id})
                DELETE p
                RETURN count(p) as deleted_count
                """
                delete_result = safe_manager.execute_query(delete_person_query, {'person_id': person_id})
                delete_data = _convert_query_result_to_list(delete_result)
                if delete_data and delete_data[0]['deleted_count'] > 0:
                    deletion_success = True
            except Exception as delete_error:
                current_app.logger.error(f"Error deleting person {person_id}: {delete_error}")
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
            safe_manager = get_safe_kuzu_manager()
            
            primary_check_query = """
            MATCH (p:Person {id: $person_id})
            RETURN p.name as name
            """
            primary_check_result = safe_manager.execute_query(primary_check_query, {"person_id": primary_person_id})
            primary_check_data = _convert_query_result_to_list(primary_check_result)
            if not primary_check_data:
                flash(f'Primary person "{primary_person_name}" not found in database.', 'error')
                return redirect(url_for('people.merge_persons'))
            
            current_app.logger.info(f"Primary person validated: {primary_person_name} (ID: {primary_person_id})")
            
            merge_persons = []
            for person_id in merge_person_ids:
                person = safe_call_sync_method_merge(book_service.get_person_by_id_sync, person_id)
                if person:
                    # Also validate this person exists in KuzuDB
                    person_check_result = safe_manager.execute_query(primary_check_query, {"person_id": person_id})
                    person_check_data = _convert_query_result_to_list(person_check_result)
                    if person_check_data:
                        merge_persons.append(person)
                        current_app.logger.info(f"Merge person validated: {person.get('name' if isinstance(person, dict) else 'name', 'Unknown')} (ID: {person_id})")
                    else:
                        current_app.logger.warning(f"Person {person_id} not found in KuzuDB, skipping")
                else:
                    current_app.logger.warning(f"Person {person_id} not found in service layer, skipping")
            
            if not merge_persons:
                flash('No valid persons found to merge.', 'error')
                return redirect(url_for('people.merge_persons'))
            
            # Perform merge operation using SafeKuzuManager
            merged_count = 0
            for merge_person in merge_persons:
                merge_person_id = None
                merge_person_name = 'Unknown'
                try:
                    # Handle both dict and object formats
                    merge_person_name = merge_person.get('name', 'Unknown') if isinstance(merge_person, dict) else getattr(merge_person, 'name', 'Unknown')
                    merge_person_id = merge_person.get('id') if isinstance(merge_person, dict) else getattr(merge_person, 'id', None)
                    
                    current_app.logger.info(f"Merging person {merge_person_name} (ID: {merge_person_id}) into {primary_person_name}")
                    
                    # Use SafeKuzuManager for all database operations
                    safe_manager = get_safe_kuzu_manager()
                    
                    # First, let's check what relationships exist for this person
                    check_query = """
                    MATCH (p:Person {id: $merge_person_id})-[r]->(b:Book)
                    RETURN COUNT(*) as total_relationships
                    """
                    
                    check_result = safe_manager.execute_query(check_query, {"merge_person_id": merge_person_id})
                    check_data = _convert_query_result_to_list(check_result)
                    total_rels = check_data[0]['total_relationships'] if check_data else 0
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
                    
                    transfer_result = safe_manager.execute_query(transfer_query, {
                        "merge_person_id": merge_person_id,
                        "primary_person_id": primary_person_id
                    })
                    
                    transfer_data = _convert_query_result_to_list(transfer_result)
                    transferred_count = transfer_data[0]['transferred_count'] if transfer_data else 0
                    
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
                        
                        other_result = safe_manager.execute_query(other_transfer_query, {
                            "merge_person_id": merge_person_id,
                            "primary_person_id": primary_person_id
                        })
                        
                        other_data = _convert_query_result_to_list(other_result)
                        other_count = other_data[0]['transferred_count'] if other_data else 0
                        if other_count > 0:
                            current_app.logger.info(f"Transferred {other_count} {rel_type} relationships from {merge_person_name} to {primary_person_name}")
                    
                    # Delete the merged person using DETACH DELETE to handle any remaining relationships
                    delete_query = """
                    MATCH (p:Person {id: $person_id})
                    DETACH DELETE p
                    """
                    
                    delete_result = safe_manager.execute_query(delete_query, {"person_id": merge_person_id})
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
            # For themes, we use session storage instead of KuzuDB
            # since themes are UI preferences, not core data
            pass
        
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