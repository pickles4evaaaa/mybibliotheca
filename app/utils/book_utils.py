from datetime import date, timedelta, datetime
import pytz
from ..services import reading_log_service
import calendar
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import requests
import os
from flask import current_app

def search_author_by_name(author_name):
    """Search for authors on OpenLibrary by name and return the best match."""
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
            
        # Find the best match (usually the first result)
        for doc in docs:
            name = doc.get('name', '')
            key = doc.get('key', '')
            
            # Basic name matching - could be enhanced with fuzzy matching
            if name.lower().strip() == author_name.lower().strip():
                author_id = key.replace('/authors/', '') if key.startswith('/authors/') else key
                
                # Fetch detailed data for this author
                detailed_data = fetch_author_data(author_id)
                if detailed_data:
                    detailed_data['openlibrary_id'] = author_id
                    detailed_data['name'] = name
                    return detailed_data
                else:
                    # Return basic info if detailed fetch fails
                    return {
                        'openlibrary_id': author_id,
                        'name': name,
                        'birth_date': doc.get('birth_date'),
                        'death_date': doc.get('death_date'),
                        'bio': None,
                        'photo_url': None
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
        current_app.logger.warning(f"Failed to search for author '{author_name}': {e}")
        return None

def fetch_book_data(isbn):
    """Enhanced OpenLibrary API lookup with comprehensive field mapping and timeout handling."""
    print(f"📖 [OPENLIBRARY] Fetching data for ISBN: {isbn}")
    
    if not isbn:
        print(f"❌ [OPENLIBRARY] No ISBN provided")
        return None
        
    url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
    print(f"📖 [OPENLIBRARY] Request URL: {url}")
    
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
        return None
        
    url = f"https://openlibrary.org/authors/{author_id}.json"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
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
        
        return {
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
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching author data from OpenLibrary for ID {author_id}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error processing OpenLibrary author data for ID {author_id}: {e}")
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
