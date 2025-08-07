from datetime import date, timedelta, datetime
import pytz
import calendar
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import requests
import os
from flask import current_app

def search_author_by_name(author_name):
    """Search for authors on OpenLibrary by name and return the best match with most comprehensive data."""
    if not author_name:
        return None
    
    # OpenLibrary search API endpoint for authors
    url = f"https://openlibrary.org/search/authors.json?q={author_name}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        docs = data.get('docs', [])
        if not docs:
            return None
        
        # Find exact matches first and score them by comprehensiveness
        exact_matches = []
        close_matches = []
        
        for i, doc in enumerate(docs):
            name = doc.get('name', '')
            key = doc.get('key', '')
            author_id = key.replace('/authors/', '') if key.startswith('/authors/') else key
            # Calculate a scoring metric for matching
            score = 0
            
            # Calculate comprehensiveness score for this search result
            score = 0
            if doc.get('birth_date'):
                score += 10
            if doc.get('death_date'):
                score += 5
            if doc.get('alternate_names'):
                score += 8
            if doc.get('top_subjects'):
                score += 3
            work_count = doc.get('work_count', 0)
            if work_count > 0:
                score += min(work_count // 5, 10)  # Up to 10 points for work count
            
            result_data = {
                'name': name,
                'author_id': author_id,
                'doc': doc,
                'score': score
            }
            
            # Check for exact name match (case insensitive)
            if name.lower().strip() == author_name.lower().strip():
                exact_matches.append(result_data)
            elif author_name.lower() in name.lower() or name.lower() in author_name.lower():
                close_matches.append(result_data)
        
        # Sort exact matches by comprehensiveness score (highest first)
        exact_matches.sort(key=lambda x: x['score'], reverse=True)
        
        # Choose the best match from exact matches, or fall back to close matches
        candidates = exact_matches if exact_matches else close_matches
        if not candidates:
            # Fall back to first result if no good matches
            candidates = [{'name': docs[0].get('name', ''), 
                          'author_id': docs[0].get('key', '').replace('/authors/', ''),
                          'doc': docs[0], 'score': 0}]
        
        # Log scoring results
        if exact_matches:
            for match in exact_matches:
                print(f"  - {match['name']} ({match['author_id']}): {match['score']} points")
        
        # Try to get detailed data for the best candidate
        best_match = candidates[0]
        author_id = best_match['author_id']
        
        # Fetch detailed data for this author
        detailed_data = fetch_author_data(author_id)
        if detailed_data:
            detailed_data['openlibrary_id'] = author_id
            detailed_data['name'] = best_match['name']
            return detailed_data
        else:
            # Return enhanced basic info from search results if detailed fetch fails
            doc = best_match['doc']
            return {
                'openlibrary_id': author_id,
                'name': best_match['name'],
                'birth_date': doc.get('birth_date', ''),
                'death_date': doc.get('death_date', ''),
                'bio': None,
                'photo_url': None,
                'alternate_names': doc.get('alternate_names', []),
                'top_subjects': doc.get('top_subjects', [])
            }
        
        # If no exact match, return the first result as best match
        first_match = docs[0]
        name = first_match.get('name', '')
        key = first_match.get('key', '')
        author_id = key.replace('/authors/', '') if key.startswith('/authors/') else key
        
        detailed_data = fetch_author_data(author_id)
        if detailed_data:
            detailed_data['openlibrary_id'] = author_id
            detailed_data['name'] = name
            return detailed_data
        else:
            return {
                'openlibrary_id': author_id,
                'name': name,
                'birth_date': first_match.get('birth_date'),
                'death_date': first_match.get('death_date'),
                'bio': None,
                'photo_url': None
            }
            
    except Exception as e:
        return None

def fetch_book_data(isbn):
    """Enhanced OpenLibrary API lookup with comprehensive field mapping and timeout handling."""
    
    if not isbn:
        return None
        
    url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
    
    try:
        response = requests.get(url, timeout=15)  # Increased timeout
        print(f"📖 [OPENLIBRARY] Response status: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        
        print(f"📖 [OPENLIBRARY] Response data keys: {list(data.keys())}")
        
        book_key = f"ISBN:{isbn}"
        if book_key in data:
            print(f"📖 [OPENLIBRARY] Found book data for {book_key}")
            book = data[book_key]
            
            # Extract OpenLibrary ID from the key field
            openlibrary_id = None
            if 'key' in book:
                key = book['key']
                # Key format is typically "/books/OL12345M" - extract the ID part
                if key.startswith('/books/'):
                    openlibrary_id = key.replace('/books/', '')
            
            title = book.get('title', '')
            subtitle = book.get('subtitle', '')
            
            # Extract individual authors with IDs for better Person entity creation
            authors_list = []
            author_ids = []
            for author in book.get('authors', []):
                name = ''
                ol_id = ''
                if isinstance(author, dict):
                    name = author.get('name', '')
                    key = author.get('key', '')
                    if key:
                        ol_id = key.split('/')[-1]
                elif isinstance(author, str):
                    name = author
                if name:
                    authors_list.append(name)
                    author_ids.append(ol_id)
            
            # Keep backward compatibility with joined authors string
            authors = ', '.join(authors_list) if authors_list else ''
            
            # Enhanced cover image handling - get best quality available
            cover_data = book.get('cover', {})
            cover_url = None
            for size in ['large', 'medium', 'small']:
                if size in cover_data:
                    cover_url = cover_data[size]
                    break
            
            # Enhanced description handling
            description = ''
            
            # Try multiple sources for description
            desc_sources = ['description', 'notes', 'summary', 'excerpt']
            for source in desc_sources:
                if source in book:
                    desc_data = book.get(source)
                    print(f"📖 [OPENLIBRARY] Found {source}: {type(desc_data)} - {str(desc_data)[:100] if desc_data else 'None'}...")
                    
                    if isinstance(desc_data, dict):
                        if 'value' in desc_data:
                            description = desc_data['value']
                            print(f"📖 [OPENLIBRARY] Using description from {source}.value")
                            break
                        elif 'text' in desc_data:
                            description = desc_data['text']
                            print(f"📖 [OPENLIBRARY] Using description from {source}.text")
                            break
                    elif isinstance(desc_data, str) and desc_data.strip():
                        description = desc_data.strip()
                        print(f"📖 [OPENLIBRARY] Using description from {source}")
                        break
            
            if not description:
                print(f"📖 [OPENLIBRARY] No description found in any source")
            else:
                print(f"📖 [OPENLIBRARY] Final description: {description[:100]}...")
            
            # Publication info
            published_date = book.get('publish_date', '')
            page_count = book.get('number_of_pages')
            
            # Enhanced subjects/categories processing with better filtering
            subjects = book.get('subjects', [])
            categories = []
            # Filter out overly generic or unhelpful categories
            exclude_categories = {
                'accessible book', 'protected daisy', 'lending library',
                'in library', 'fiction', 'non-fiction', 'literature',
                'reading level-adult', 'adult', 'juvenile', 'young adult',
                'large type books', 'large print books'
            }
            
            for subject in subjects[:15]:  # Limit to 15 most relevant categories
                category_name = ''
                if isinstance(subject, dict):
                    category_name = subject.get('name', '')
                else:
                    category_name = str(subject)
                
                # Clean and filter categories
                category_name = category_name.strip().lower()
                if (category_name and 
                    len(category_name) > 2 and 
                    category_name not in exclude_categories and
                    not category_name.startswith('places--') and
                    not category_name.startswith('people--')):
                    # Capitalize properly
                    categories.append(category_name.title())
            
            # Remove duplicates while preserving order
            seen = set()
            unique_categories = []
            for cat in categories:
                if cat.lower() not in seen:
                    unique_categories.append(cat)
                    seen.add(cat.lower())
            
            # Language extraction
            languages = book.get('languages', [])
            language = ''
            if languages and len(languages) > 0:
                lang_item = languages[0]
                if isinstance(lang_item, dict):
                    language = lang_item.get('key', '').split('/')[-1]
                else:
                    language = str(lang_item)
            
            # ISBN extraction from identifiers
            identifiers = book.get('identifiers', {})
            isbn_10 = identifiers.get('isbn_10', [])
            isbn_13 = identifiers.get('isbn_13', [])
            
            # Get the first available ISBN
            primary_isbn = isbn
            if isbn_13 and len(isbn_13) > 0:
                primary_isbn = isbn_13[0]
            elif isbn_10 and len(isbn_10) > 0:
                primary_isbn = isbn_10[0]
            
            # Publisher information
            publishers = book.get('publishers', [])
            publisher = ''
            if publishers and len(publishers) > 0:
                pub = publishers[0]
                if isinstance(pub, dict):
                    publisher = pub.get('name', '')
                else:
                    publisher = str(pub)
            
            result = {
                'title': title,
                'subtitle': subtitle,
                'authors': authors,  # Backward compatibility
                'authors_list': authors_list,  # New field for individual authors
                'author_ids': author_ids,  # OpenLibrary IDs for authors
                'description': description,
                'published_date': published_date,
                'page_count': page_count,
                'categories': unique_categories,
                'cover_url': cover_url,
                'language': language,
                'isbn': primary_isbn,
                'publisher': publisher,
                'openlibrary_id': openlibrary_id,
                'source': 'OpenLibrary'
            }
            
            print(f"✅ [OPENLIBRARY] Successfully retrieved data for ISBN {isbn}:")
            print(f"    openlibrary_id='{openlibrary_id}'")
            print(f"    title='{title}'")
            print(f"    authors={len(authors_list)} items")
            print(f"    description='{description[:100] if description else None}...'")
            print(f"    cover_url='{cover_url}'")
            print(f"    categories={len(unique_categories)} items")
            return result
        else:
            print(f"❌ [OPENLIBRARY] No book data found for ISBN {isbn}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ [OPENLIBRARY] Request error for ISBN {isbn}: {e}")
        return None
    except Exception as e:
        print(f"❌ [OPENLIBRARY] Unexpected error for ISBN {isbn}: {e}")
        return None

def fetch_author_data(author_id):
    """Fetch detailed author information from OpenLibrary API using author ID."""
    if not author_id:
        print(f"[OPENLIBRARY] No author ID provided")
        return None
        
    url = f"https://openlibrary.org/authors/{author_id}.json"
    print(f"[OPENLIBRARY] Fetching author data from: {url}")
    try:
        response = requests.get(url, timeout=10)
        print(f"[OPENLIBRARY] Response status: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        print(f"[OPENLIBRARY] Raw response data: {data}")
        
        # Extract author information
        name = data.get('name', '')
        birth_date = data.get('birth_date', '')
        death_date = data.get('death_date', '')
        bio = data.get('bio', '')
        
        # Handle bio if it's a dict
        if isinstance(bio, dict):
            bio = bio.get('value', '')
        
        # Extract photo URL
        photos = data.get('photos', [])
        photo_url = None
        if photos and len(photos) > 0:
            photo_id = photos[0]
            photo_url = f"https://covers.openlibrary.org/a/id/{photo_id}-L.jpg"
        
        # Extract alternate names
        alternate_names = data.get('alternate_names', [])
        
        # Extract Wikipedia link
        links = data.get('links', [])
        wikipedia_url = None
        for link in links:
            if isinstance(link, dict):
                url_val = link.get('url', '')
                if 'wikipedia.org' in url_val:
                    wikipedia_url = url_val
                    break
        
        result = {
            'name': name,
            'birth_date': birth_date,
            'death_date': death_date,
            'bio': bio,
            'photo_url': photo_url,
            'alternate_names': alternate_names,
            'wikipedia_url': wikipedia_url,
            'openlibrary_id': author_id,
            'source': 'OpenLibrary'
        }
        print(f"[OPENLIBRARY] Processed author data: {result}")
        return result
        
    except requests.exceptions.RequestException as e:
        print(f"[OPENLIBRARY] Error fetching author data from OpenLibrary for ID {author_id}: {e}")
        return None
    except Exception as e:
        print(f"[OPENLIBRARY] Unexpected error processing OpenLibrary author data for ID {author_id}: {e}")
        return None

def get_google_books_cover(isbn, fetch_title_author=False):
    """
    Fetch cover image from Google Books API using ISBN.
    
    Args:
        isbn (str): The ISBN to search for
        fetch_title_author (bool): If True, also return title and author info
    
    Returns:
        dict: Contains cover_url and optionally title/author info
    """
    print(f"📚 [GOOGLE_BOOKS] Fetching data for ISBN: {isbn}")
    
    if not isbn:
        print(f"❌ [GOOGLE_BOOKS] No ISBN provided")
        return None
        
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
    print(f"📚 [GOOGLE_BOOKS] Request URL: {url}")
    
    try:
        response = requests.get(url, timeout=10)
        print(f"📚 [GOOGLE_BOOKS] Response status: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        
        print(f"📚 [GOOGLE_BOOKS] Response data keys: {list(data.keys())}")
        
        if 'items' in data and len(data['items']) > 0:
            print(f"📚 [GOOGLE_BOOKS] Found {len(data['items'])} items")
            book_item = data['items'][0]
            book_info = book_item['volumeInfo']
            google_books_id = book_item.get('id', '')
            
            result = {}
            
            # Store Google Books ID
            result['google_books_id'] = google_books_id
            
            # Get cover image
            image_links = book_info.get('imageLinks', {})
            cover_url = None
            
            # Prefer higher quality images
            for size in ['extraLarge', 'large', 'medium', 'small', 'thumbnail', 'smallThumbnail']:
                if size in image_links:
                    cover_url = image_links[size]
                    # Convert to HTTPS if needed
                    if cover_url and cover_url.startswith('http:'):
                        cover_url = cover_url.replace('http:', 'https:')
                    break
            
            result['cover_url'] = cover_url
            
            if fetch_title_author:
                # Get title and author information
                title = book_info.get('title', '')
                subtitle = book_info.get('subtitle', '')
                authors = book_info.get('authors', [])
                
                # Get description
                description = book_info.get('description', '')
                
                # Get categories/genres
                categories = book_info.get('categories', [])
                
                # Get additional contributors
                contributors = []
                
                # Google Books sometimes has contributors in different fields
                if 'authors' in book_info and book_info['authors']:
                    for author in book_info['authors']:
                        contributors.append({'name': author, 'role': 'author'})
                
                # Check for other contributor types that might be in the data
                if 'editors' in book_info and book_info['editors']:
                    for editor in book_info['editors']:
                        contributors.append({'name': editor, 'role': 'editor'})
                
                if 'translators' in book_info and book_info['translators']:
                    for translator in book_info['translators']:
                        contributors.append({'name': translator, 'role': 'translator'})
                
                # Get publisher and publication date
                publisher = book_info.get('publisher', '')
                published_date = book_info.get('publishedDate', '')
                
                # Get page count
                page_count = book_info.get('pageCount')
                
                # Get language
                language = book_info.get('language', 'en')
                
                # Get average rating and rating count
                average_rating = book_info.get('averageRating')
                rating_count = book_info.get('ratingsCount')
                
                # Get ISBN data from industryIdentifiers
                isbn_10 = None
                isbn_13 = None
                if 'industryIdentifiers' in book_info:
                    for identifier in book_info['industryIdentifiers']:
                        if identifier.get('type') == 'ISBN_10':
                            isbn_10 = identifier.get('identifier')
                        elif identifier.get('type') == 'ISBN_13':
                            isbn_13 = identifier.get('identifier')
                
                result.update({
                    'title': title,
                    'subtitle': subtitle,
                    'authors': ', '.join(authors) if authors else '',
                    'authors_list': authors,
                    'description': description,
                    'categories': categories,
                    'contributors': contributors,
                    'publisher': publisher,
                    'published_date': published_date,
                    'page_count': page_count,
                    'language': language,
                    'average_rating': average_rating,
                    'rating_count': rating_count,
                    'isbn_10': isbn_10,
                    'isbn_13': isbn_13,
                    'source': 'Google Books'
                })
                
                print(f"✅ [GOOGLE_BOOKS] Enhanced data for ISBN {isbn}:")
                print(f"    google_books_id='{google_books_id}'")
                print(f"    title='{title}'")
                print(f"    authors={len(authors)} items")
                print(f"    description='{description[:100] if description else None}...'")
                print(f"    categories={len(categories)} items")
                print(f"    contributors={len(contributors)} items")
                print(f"    publisher='{publisher}'")
            
            print(f"✅ [GOOGLE_BOOKS] Successfully retrieved data for ISBN {isbn}: {list(result.keys())}")
            return result
        else:
            print(f"❌ [GOOGLE_BOOKS] No items found for ISBN {isbn}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ [GOOGLE_BOOKS] Request error for ISBN {isbn}: {e}")
        return None
    except Exception as e:
        print(f"❌ [GOOGLE_BOOKS] Unexpected error for ISBN {isbn}: {e}")
        return None

def generate_month_review_image(books, month, year):
    """
    Generate a monthly reading review image showing books read in the given month.
    """
    if not books:
        return None
    
    # Image dimensions
    width = 1200
    height = 800
    bg_color = (245, 245, 245)  # Light gray background
    
    # Create image
    img = Image.new('RGB', (width, height), bg_color)
    draw = ImageDraw.Draw(img)
    
    # Try to load a font
    try:
        title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 36)
        book_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 20)
        stat_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 24)
    except (OSError, IOError):
        # Fallback to default font if custom fonts aren't available
        title_font = ImageFont.load_default()
        book_font = ImageFont.load_default()
        stat_font = ImageFont.load_default()
    
    # Colors
    title_color = (50, 50, 50)
    text_color = (80, 80, 80)
    accent_color = (100, 150, 200)
    
    # Title
    month_name = calendar.month_name[month]
    title = f"Reading Review - {month_name} {year}"
    
    # Get title dimensions for centering
    title_bbox = draw.textbbox((0, 0), title, font=title_font)
    title_width = title_bbox[2] - title_bbox[0]
    title_x = (width - title_width) // 2
    
    draw.text((title_x, 40), title, fill=title_color, font=title_font)
    
    # Statistics
    total_books = len(books)
    total_pages = sum(book.get('page_count', 0) for book in books if book.get('page_count'))
    
    stats_y = 120
    stats_text = f"Books Read: {total_books}    Total Pages: {total_pages:,}"
    stats_bbox = draw.textbbox((0, 0), stats_text, font=stat_font)
    stats_width = stats_bbox[2] - stats_bbox[0]
    stats_x = (width - stats_width) // 2
    
    draw.text((stats_x, stats_y), stats_text, fill=accent_color, font=stat_font)
    
    # Book list
    book_list_y = 200
    max_books_to_show = 15  # Limit to prevent overcrowding
    books_to_show = books[:max_books_to_show]
    
    for i, book in enumerate(books_to_show):
        y_pos = book_list_y + (i * 35)
        
        if y_pos > height - 100:  # Leave space at bottom
            remaining = len(books) - i
            if remaining > 0:
                more_text = f"... and {remaining} more books"
                draw.text((50, y_pos), more_text, fill=text_color, font=book_font)
            break
        
        # Book title and author
        title = book.get('title', 'Unknown Title')
        authors = book.get('authors', 'Unknown Author')
        page_count = book.get('page_count', 0)
        
        # Truncate long titles
        if len(title) > 50:
            title = title[:47] + "..."
        
        book_text = f"• {title}"
        if authors and authors != 'Unknown Author':
            if len(authors) > 30:
                authors = authors[:27] + "..."
            book_text += f" by {authors}"
        
        if page_count > 0:
            book_text += f" ({page_count} pages)"
        
        draw.text((50, y_pos), book_text, fill=text_color, font=book_font)
    
    # Convert to bytes for return
    img_buffer = BytesIO()
    img.save(img_buffer, format='PNG', quality=95)
    img_buffer.seek(0)
    
    return img_buffer


def normalize_goodreads_value(value, field_type='text'):
    """
    Normalize values from Goodreads CSV exports that use Excel text formatting.
    Goodreads exports often have values like ="123456789" or ="" to force text formatting.
    """
    if not value or not isinstance(value, str):
        return value.strip() if value else ''
    
    # Remove Excel text formatting: ="value" -> value
    if value.startswith('="') and value.endswith('"'):
        value = value[2:-1]  # Remove =" prefix and " suffix
    elif value.startswith('=') and value.endswith('"'):
        value = value[1:-1]  # Remove = prefix and " suffix  
    elif value == '=""':
        value = ''  # Empty quoted value
    
    # Handle standard quoted values for backwards compatibility
    elif value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    
    # Special handling for ISBN fields
    if field_type == 'isbn' and value:
        # Remove any remaining quotes or formatting for ISBNs
        value = value.replace('"', '').replace("'", "").replace('-', '').replace(' ', '')
        # Only return if it looks like a valid ISBN (10 or 13 digits)
        if value.isdigit() and len(value) in [10, 13]:
            return value
        elif len(value) >= 10:  # Be more lenient for partial matches
            return value
    
    return value.strip() if value else ''

def search_multiple_books_by_title_author(title, author=None, limit=10):
    """Search for multiple books from both OpenLibrary and Google Books APIs by title and optionally author."""
    if not title:
        print(f"[MULTI_API] No title provided for book search")
        return []
    
    all_results = []
    
    # Search OpenLibrary first
    print(f"[MULTI_API] Searching OpenLibrary for: '{title}' by '{author}'")
    try:
        ol_results = _search_openlibrary_multiple(title, author, limit//2)
        if ol_results:
            all_results.extend(ol_results)
            print(f"[MULTI_API] OpenLibrary returned {len(ol_results)} results")
    except Exception as e:
        print(f"[MULTI_API] OpenLibrary search failed: {e}")
    
    # Search Google Books
    print(f"[MULTI_API] Searching Google Books for: '{title}' by '{author}'")
    try:
        gb_results = search_google_books_by_title_author(title, author, limit//2)
        if gb_results:
            all_results.extend(gb_results)
            print(f"[MULTI_API] Google Books returned {len(gb_results)} results")
    except Exception as e:
        print(f"[MULTI_API] Google Books search failed: {e}")
    
    # Deduplicate results by title similarity
    unique_results = []
    seen_titles = set()
    
    for result in all_results:
        result_title = result.get('title', '').lower().strip()
        
        # Skip if we've seen a very similar title
        is_duplicate = False
        for seen_title in seen_titles:
            # Consider titles duplicates if they're very similar
            if (result_title in seen_title or seen_title in result_title) and len(result_title) > 3:
                is_duplicate = True
                break
        
        if not is_duplicate:
            unique_results.append(result)
            seen_titles.add(result_title)
    
    # Sort results by relevance (prefer exact matches, then partial matches)
    def calculate_relevance_score(result):
        score = 0
        result_title = result.get('title', '').lower().strip()
        result_authors = result.get('authors_list', [])
        
        # Exact title match
        if result_title == title.lower().strip():
            score += 100
        # Partial title match
        elif title.lower() in result_title or result_title in title.lower():
            score += 50
        
        # Author match
        if author and result_authors:
            for result_author in result_authors:
                if author.lower() in result_author.lower():
                    score += 30
        
        # Prefer results with ISBNs
        if result.get('isbn13') or result.get('isbn10'):
            score += 10
        
        # Slight preference for Google Books (usually more complete metadata)
        if result.get('source') == 'Google Books':
            score += 5
        
        return score
    
    unique_results.sort(key=calculate_relevance_score, reverse=True)
    
    # Limit to requested number of results
    final_results = unique_results[:limit]
    
    print(f"[MULTI_API] Returning {len(final_results)} unique results from {len(all_results)} total results")
    return final_results


def _search_openlibrary_multiple(title, author=None, limit=10):
    """Internal function to search OpenLibrary (extracted from original function)."""
    if not title:
        print(f"[OPENLIBRARY] No title provided for book search")
        return []
    
    # Build search query
    query_parts = [title]
    if author:
        query_parts.append(author)
    
    query = ' '.join(query_parts)
    url = f"https://openlibrary.org/search.json?q={query}&limit={limit}"
    
    print(f"[OPENLIBRARY] Searching for multiple books: title='{title}', author='{author}' at {url}")
    
    try:
        response = requests.get(url, timeout=15)
        print(f"[OPENLIBRARY] Multiple book search response status: {response.status_code}")
        
        # Handle different response codes more gracefully
        if response.status_code == 404:
            print(f"[OPENLIBRARY] OpenLibrary API returned 404 for query: {query}")
            return []
        elif response.status_code != 200:
            print(f"[OPENLIBRARY] OpenLibrary API returned {response.status_code}")
            return []
            
        data = response.json()
        
        docs = data.get('docs', [])
        if not docs:
            print(f"[OPENLIBRARY] No search results found for '{title}' by '{author}'")
            return []
            
        print(f"[OPENLIBRARY] Found {len(docs)} book search results")
        
        # Score matches based on title similarity and author match
        scored_matches = []
        
        for i, doc in enumerate(docs):
            doc_title = doc.get('title', '')
            doc_authors = doc.get('author_name', []) if isinstance(doc.get('author_name'), list) else [doc.get('author_name', '')]
            doc_isbn = doc.get('isbn', [])
            
            # Get best ISBN (prefer 13-digit)
            best_isbn = None
            if doc_isbn:
                for isbn in doc_isbn:
                    if len(isbn) == 13:
                        best_isbn = isbn
                        break
                if not best_isbn and doc_isbn:
                    best_isbn = doc_isbn[0]
            
            print(f"[OPENLIBRARY] Result {i}: title='{doc_title}', authors={doc_authors}, isbn={best_isbn}")
            
            # Calculate match score
            score = 0
            
            # Title similarity (basic)
            if title.lower() in doc_title.lower() or doc_title.lower() in title.lower():
                score += 50
            if title.lower().strip() == doc_title.lower().strip():
                score += 50  # Exact title match bonus
            
            # Author similarity
            if author:
                for doc_author in doc_authors:
                    if doc_author and author.lower() in doc_author.lower():
                        score += 30
                    if doc_author and author.lower().strip() == doc_author.lower().strip():
                        score += 20  # Exact author match bonus
            
            # Prefer results with ISBN
            if best_isbn:
                score += 10
            
            # Prefer more recent publications (if available)
            if doc.get('first_publish_year'):
                try:
                    year = int(doc.get('first_publish_year'))
                    if year > 1950:  # Reasonable cutoff
                        score += min((year - 1950) // 10, 5)  # Up to 5 bonus points for newer books
                except:
                    pass
            
            # Build book data from search result
            result = {
                'title': doc_title,
                'author': ', '.join(doc_authors) if doc_authors else '',
                'authors_list': doc_authors,
                'isbn': best_isbn,
                'isbn13': best_isbn if best_isbn and len(best_isbn) == 13 else '',
                'isbn10': best_isbn if best_isbn and len(best_isbn) == 10 else '',
                'publisher': ', '.join(doc.get('publisher', [])) if isinstance(doc.get('publisher'), list) else doc.get('publisher', ''),
                'published_date': str(doc.get('first_publish_year', '')) if doc.get('first_publish_year') else '',
                'page_count': doc.get('number_of_pages_median'),
                'cover': None,
                'cover_url': None,
                'description': '',  # Search results don't include descriptions
                'language': doc.get('language', [''])[0] if doc.get('language') else 'en',
                'openlibrary_id': doc.get('key', '').replace('/works/', '') if doc.get('key') else None,
                'categories': [],
                'score': score
            }
            
            # Try to get cover image if we have an OpenLibrary work ID
            if result.get('openlibrary_id'):
                cover_url = f"https://covers.openlibrary.org/w/id/{result['openlibrary_id']}-L.jpg"
                try:
                    cover_response = requests.head(cover_url, timeout=5)
                    if cover_response.status_code == 200:
                        result['cover'] = cover_url
                        result['cover_url'] = cover_url
                except:
                    pass
            
            scored_matches.append(result)
        
        # Sort by score (highest first)
        scored_matches.sort(key=lambda x: x['score'], reverse=True)
        
        print(f"[OPENLIBRARY] Returning {len(scored_matches)} book results")
        return scored_matches
        
    except Exception as e:
        print(f"[OPENLIBRARY] Failed to search for multiple books '{title}' by '{author}': {e}")
        return []


def search_book_by_title_author(title, author=None):
    """Search for books on OpenLibrary by title and optionally author, return the best match."""
    if not title:
        print(f"[OPENLIBRARY] No title provided for book search")
        return None
    
    # Build search query
    query_parts = [title]
    if author:
        query_parts.append(author)
    
    query = ' '.join(query_parts)
    url = f"https://openlibrary.org/search.json?q={query}&limit=10"
    
    print(f"[OPENLIBRARY] Searching for book: title='{title}', author='{author}' at {url}")
    
    try:
        response = requests.get(url, timeout=15)
        print(f"[OPENLIBRARY] Book search response status: {response.status_code}")
        
        # Handle different response codes more gracefully
        if response.status_code == 404:
            print(f"[OPENLIBRARY] OpenLibrary API returned 404 for query: {query}")
            return None
        elif response.status_code != 200:
            print(f"[OPENLIBRARY] OpenLibrary API returned {response.status_code}")
            return None
            
        data = response.json()
        
        docs = data.get('docs', [])
        if not docs:
            print(f"[OPENLIBRARY] No search results found for '{title}' by '{author}'")
            return None
            
        print(f"[OPENLIBRARY] Found {len(docs)} book search results")
        
        # Score matches based on title similarity and author match
        scored_matches = []
        
        for i, doc in enumerate(docs):
            doc_title = doc.get('title', '')
            doc_authors = doc.get('author_name', []) if isinstance(doc.get('author_name'), list) else [doc.get('author_name', '')]
            doc_isbn = doc.get('isbn', [])
            
            # Get best ISBN (prefer 13-digit)
            best_isbn = None
            if doc_isbn:
                for isbn in doc_isbn:
                    if len(isbn) == 13:
                        best_isbn = isbn
                        break
                if not best_isbn and doc_isbn:
                    best_isbn = doc_isbn[0]
            
            print(f"[OPENLIBRARY] Result {i}: title='{doc_title}', authors={doc_authors}, isbn={best_isbn}")
            
            # Calculate match score
            score = 0
            
            # Title similarity (basic)
            if title.lower() in doc_title.lower() or doc_title.lower() in title.lower():
                score += 50
            if title.lower().strip() == doc_title.lower().strip():
                score += 50  # Exact title match bonus
            
            # Author similarity
            if author:
                for doc_author in doc_authors:
                    if doc_author and author.lower() in doc_author.lower():
                        score += 30
                    if doc_author and author.lower().strip() == doc_author.lower().strip():
                        score += 20  # Exact author match bonus
            
            # Prefer results with ISBN
            if best_isbn:
                score += 10
            
            # Prefer more recent publications (if available)
            if doc.get('first_publish_year'):
                try:
                    year = int(doc.get('first_publish_year'))
                    if year > 1950:  # Reasonable cutoff
                        score += min((year - 1950) // 10, 5)  # Up to 5 bonus points for newer books
                except:
                    pass
            
            scored_matches.append({
                'doc': doc,
                'score': score,
                'title': doc_title,
                'authors': doc_authors,
                'isbn': best_isbn
            })
        
        # Sort by score (highest first)
        scored_matches.sort(key=lambda x: x['score'], reverse=True)
        
        if scored_matches:
            best_match = scored_matches[0]
            print(f"[OPENLIBRARY] Best match: '{best_match['title']}' by {best_match['authors']} (score: {best_match['score']}, ISBN: {best_match['isbn']})")
            
            # If we have an ISBN, fetch full book data using existing function
            if best_match['isbn']:
                print(f"[OPENLIBRARY] Fetching full data using ISBN: {best_match['isbn']}")
                full_data = fetch_book_data(best_match['isbn'])
                if full_data:
                    return full_data
            
            # Otherwise, build book data from search result
            doc = best_match['doc']
            result = {
                'title': best_match['title'],
                'author': ', '.join(best_match['authors']) if best_match['authors'] else '',
                'authors_list': best_match['authors'],
                'isbn': best_match['isbn'],
                'publisher': ', '.join(doc.get('publisher', [])) if isinstance(doc.get('publisher'), list) else doc.get('publisher', ''),
                'published_date': str(doc.get('first_publish_year', '')) if doc.get('first_publish_year') else '',
                'page_count': doc.get('number_of_pages_median'),
                'cover': None,  # Search results don't include cover URLs
                'description': '',  # Search results don't include descriptions
                'language': doc.get('language', [''])[0] if doc.get('language') else 'en',
                'openlibrary_id': doc.get('key', '').replace('/works/', '') if doc.get('key') else None
            }
            
            # Try to get cover image if we have an OpenLibrary work ID
            if result.get('openlibrary_id'):
                cover_url = f"https://covers.openlibrary.org/w/id/{result['openlibrary_id']}-L.jpg"
                try:
                    cover_response = requests.head(cover_url, timeout=5)
                    if cover_response.status_code == 200:
                        result['cover'] = cover_url
                        result['cover_url'] = cover_url
                except:
                    pass
            
            print(f"[OPENLIBRARY] Returning book data: {result}")
            return result
        
    except Exception as e:
        print(f"[OPENLIBRARY] Failed to search for book '{title}' by '{author}': {e}")
        return None
    
    return None


def search_google_books_by_title_author(title, author=None, limit=10):
    """Search Google Books API by title and optionally author."""
    if not title:
        print(f"[GOOGLE_BOOKS] No title provided for book search")
        return []
    
    # Build search query for Google Books
    query_parts = [f'intitle:"{title}"']
    if author:
        query_parts.append(f'inauthor:"{author}"')
    
    query = '+'.join(query_parts)
    url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults={min(limit, 40)}"
    
    print(f"[GOOGLE_BOOKS] Searching for multiple books: title='{title}', author='{author}' at {url}")
    
    try:
        response = requests.get(url, timeout=15)
        print(f"[GOOGLE_BOOKS] Multiple book search response status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"[GOOGLE_BOOKS] Google Books API returned {response.status_code}")
            return []
            
        data = response.json()
        
        items = data.get('items', [])
        if not items:
            print(f"[GOOGLE_BOOKS] No search results found for '{title}' by '{author}'")
            return []
            
        print(f"[GOOGLE_BOOKS] Found {len(items)} book search results")
        
        results = []
        for i, item in enumerate(items):
            try:
                volume_info = item.get('volumeInfo', {})
                
                book_title = volume_info.get('title', '')
                book_authors = volume_info.get('authors', [])
                book_description = volume_info.get('description', '')
                book_publisher = volume_info.get('publisher', '')
                book_published_date = volume_info.get('publishedDate', '')
                book_page_count = volume_info.get('pageCount', 0)
                book_language = volume_info.get('language', 'en')
                book_categories = volume_info.get('categories', [])
                
                # Get ISBNs
                isbn13 = None
                isbn10 = None
                industry_identifiers = volume_info.get('industryIdentifiers', [])
                for identifier in industry_identifiers:
                    if identifier.get('type') == 'ISBN_13':
                        isbn13 = identifier.get('identifier')
                    elif identifier.get('type') == 'ISBN_10':
                        isbn10 = identifier.get('identifier')
                
                # Get cover image
                image_links = volume_info.get('imageLinks', {})
                cover_url = image_links.get('large') or image_links.get('medium') or image_links.get('small') or image_links.get('thumbnail')
                
                result = {
                    'title': book_title,
                    'author': ', '.join(book_authors) if book_authors else '',
                    'authors_list': book_authors,
                    'description': book_description,
                    'publisher': book_publisher,
                    'published_date': book_published_date,
                    'page_count': book_page_count,
                    'isbn13': isbn13,
                    'isbn10': isbn10,
                    'isbn': isbn13 or isbn10,  # Prefer ISBN13
                    'cover_url': cover_url,
                    'cover': cover_url,
                    'language': book_language,
                    'categories': book_categories,
                    'google_books_id': item.get('id'),
                    'source': 'Google Books'
                }
                
                print(f"[GOOGLE_BOOKS] Result {i}: title='{book_title}', authors={book_authors}, isbn={isbn13 or isbn10}")
                
                # Only add if we have at least a title
                if result['title']:
                    results.append(result)
                
                # Limit results
                if len(results) >= limit:
                    break
                    
            except Exception as item_error:
                print(f"[GOOGLE_BOOKS] Error processing item {i}: {item_error}")
                continue
        
        print(f"[GOOGLE_BOOKS] Returning {len(results)} valid results")
        return results
        
    except Exception as e:
        print(f"[GOOGLE_BOOKS] Failed to search for book '{title}' by '{author}': {e}")
        return []
