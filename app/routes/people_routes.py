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
from app.services import book_service, get_series_service

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
                    all_books_by_person = safe_call_sync_method(book_service.get_books_by_person_sync, person_id)
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
            contribution_counts = safe_call_sync_method(book_service.get_contribution_type_counts_sync)
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
            
            # Persist through the shared catalog service.
            try:
                created_person = book_service.create_person_sync(person)
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
            
            person_id_for_storage = getattr(person, 'id', None) or (person.get('id') if isinstance(person, dict) else person_id)
            person_data = {
                'name': name, 'bio': bio or None, 'birth_year': birth_year_int,
                'death_year': death_year_int, 'birth_place': birth_place or None,
                'website': website or None, 'openlibrary_id': openlibrary_id or None,
                'image_url': image_url or None, 'birth_date': birth_date or None,
                'death_date': death_date or None, 'title': title or None,
                'fuller_name': fuller_name or None, 'wikidata_id': wikidata_id or None,
                'imdb_id': imdb_id or None, 'alternate_names': alternate_names_json or '[]',
                'official_links': official_links_json or '[]',
            }
            updated_person = book_service.update_person_sync(str(person_id_for_storage), person_data)
            if not updated_person:
                flash('Error saving person changes. Please try again.', 'error')
                return render_template('edit_person.html', person=person, current_year=datetime.now().year)
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
    """Delete an unreferenced person through the shared persistence service."""
    try:
        person = book_service.get_person_by_id_sync(person_id)
        if not person:
            flash('Person not found.', 'error')
            return redirect(url_for('people.people'))
        person_name = person.get('name', 'Unknown Person') if isinstance(person, dict) else getattr(person, 'name', 'Unknown Person')
        if not book_service.delete_person_sync(person_id):
            flash(f'Cannot delete "{person_name}" while they are associated with books. Merge the person or remove their contributions first.', 'error')
            return redirect(url_for('people.person_details', person_id=person_id))
        flash(f'Person "{person_name}" deleted successfully.', 'success')
        return redirect(url_for('people.people'))
    except Exception as e:
        current_app.logger.exception('Error deleting person %s', person_id)
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
        person = book_service.get_person_by_id_sync(person_id)
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
                updated_person = book_service.update_person_sync(person_id, updates)
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
                        updated_person = book_service.update_person_sync(person_id, updates)
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
    """Delete selected persons, optionally removing their book links."""
    selected_person_ids = request.form.getlist('selected_persons')
    force_delete = request.form.get('force_delete') == 'true'
    if not selected_person_ids:
        flash('No persons selected for deletion.', 'warning')
        return redirect(url_for('people.people'))

    deleted_count = 0
    failed_persons = []
    for person_id in selected_person_ids:
        try:
            person = book_service.get_person_by_id_sync(person_id)
            name = (person.get('name') if isinstance(person, dict) else getattr(person, 'name', None)) if person else None
            if not person:
                failed_persons.append(f'Person {person_id[:8]}... (not found)')
            elif book_service.delete_person_sync(person_id, force=force_delete):
                deleted_count += 1
            else:
                grouped = book_service.get_books_by_person_sync(person_id, str(current_user.id)) or {}
                book_count = sum(len(books) for books in grouped.values())
                failed_persons.append(f'{name or person_id} ({book_count} linked books)')
        except Exception as exc:
            current_app.logger.exception('Bulk person deletion failed for %s', person_id)
            failed_persons.append(f'Person {person_id[:8]}... (error)')

    if deleted_count:
        flash(f'Successfully deleted {deleted_count} person(s).', 'success')
    if failed_persons:
        flash('Could not delete: ' + ', '.join(failed_persons[:5]), 'warning')
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

        primary = book_service.get_person_by_id_sync(primary_person_id)
        if not primary:
            flash('Primary person not found.', 'error')
            return redirect(url_for('people.merge_persons'))
        primary_name = primary.get('name', 'Unknown Person') if isinstance(primary, dict) else getattr(primary, 'name', 'Unknown Person')
        valid_ids = [person_id for person_id in merge_person_ids if book_service.get_person_by_id_sync(person_id)]
        if not valid_ids:
            flash('No valid persons found to merge.', 'error')
            return redirect(url_for('people.merge_persons'))
        merged_count = book_service.merge_persons_sync(primary_person_id, valid_ids)
        if merged_count:
            flash(f'Successfully merged {merged_count} person(s) into "{primary_name}".', 'success')
        else:
            flash('No persons were merged because the database update failed.', 'error')
        return redirect(url_for('people.person_details', person_id=primary_person_id))

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
                    books_by_type = safe_call_sync_method(book_service.get_books_by_person_for_user_sync, person_id, str(current_user.id))
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
