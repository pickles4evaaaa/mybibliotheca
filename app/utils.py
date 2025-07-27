from datetime import date, timedelta, datetime
import pytz
from .services import reading_log_service
import calendar
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import requests
import os
from flask import current_app

def fetch_book_data(isbn):
    """Enhanced OpenLibrary API lookup with comprehensive field mapping and timeout handling."""
    if not isbn:
        return None
        
    url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
    try:
        response = requests.get(url, timeout=15)  # Increased timeout
        response.raise_for_status()
        data = response.json()
        
        book_key = f"ISBN:{isbn}"
        if book_key in data:
            book = data[book_key]
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
            
            # Try to enhance OpenLibrary cover URL to highest resolution
            if cover_url and 'covers.openlibrary.org' in cover_url:
                # Only enhance if we're confident it will work
                if '-L.jpg' in cover_url:
                    # Try XL but don't validate - let frontend handle it
                    xl_url = cover_url.replace('-L.jpg', '-XL.jpg')
                    current_app.logger.info(f"Enhanced cover to XL size: {xl_url}")
                    cover_url = xl_url
                elif '-M.jpg' in cover_url:
                    # Enhance M to L size
                    enhanced_url = cover_url.replace('-M.jpg', '-L.jpg')
                    current_app.logger.info(f"Enhanced cover to L size: {enhanced_url}")
                    cover_url = enhanced_url
            
            # Enhanced description handling
            description = book.get('notes', {})
            if isinstance(description, dict):
                description = description.get('value', '')
            elif not isinstance(description, str):
                description = ''
            
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
            categories = unique_categories[:10]  # Final limit of 10 categories
            
            # Publisher info
            publishers = book.get('publishers', [])
            publisher = ''
            if publishers:
                if isinstance(publishers[0], dict):
                    publisher = publishers[0].get('name', '')
                else:
                    publisher = str(publishers[0])
            
            # Language info
            languages = book.get('languages', [])
            language = 'en'  # Default
            if languages:
                if isinstance(languages[0], dict):
                    lang_key = languages[0].get('key', '')
                    if lang_key:
                        language = lang_key.split('/')[-1]
                else:
                    language = str(languages[0])
            
            # Enhanced metadata
            openlibrary_id = book.get('key', '').replace('/books/', '') if book.get('key') else ''
            
            return {
                'title': title,
                'subtitle': subtitle,
                'author': authors,  # Keep for backward compatibility
                'authors_list': authors_list,  # Enhanced: Individual authors for better Person creation
                'cover': cover_url,
                'description': description,
                'published_date': published_date,
                'page_count': page_count,
                'categories': categories,  # Enhanced: Filtered and cleaned categories list
                'publisher': publisher,
                'language': language,
                'openlibrary_id': openlibrary_id,
                'author_ids': author_ids
            }
        return None
    
    except (requests.exceptions.RequestException, requests.exceptions.Timeout, ValueError) as e:
        # Log the error for debugging but don't crash the bulk import
        current_app.logger.warning(f"Failed to fetch OpenLibrary book data for ISBN {isbn}: {e}")
        return None

def fetch_author_data(author_id):
    """Fetch author metadata from OpenLibrary author endpoint."""
    if not author_id:
        return None
    url = f"https://openlibrary.org/authors/{author_id}.json"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        # Parse bio
        bio = data.get('bio', '')
        if isinstance(bio, dict):
            bio = bio.get('value', '')
        # Parse birth and death dates
        birth_date = data.get('birth_date')
        death_date = data.get('death_date')
        # Extract photo URL
        photo_url = None
        photos = data.get('photos', [])
        if photos:
            photo_id = photos[0]
            photo_url = f"https://covers.openlibrary.org/a/id/{photo_id}-L.jpg"
        return {
            'birth_date': birth_date,
            'death_date': death_date,
            'bio': bio,
            'photo_url': photo_url
        }
    except Exception as e:
        current_app.logger.warning(f"Failed to fetch author data for {author_id}: {e}")
        return None

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
        
        for doc in docs:
            name = doc.get('name', '')
            key = doc.get('key', '')
            author_id = key.replace('/authors/', '') if key.startswith('/authors/') else key
            
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
        
        # Try to get detailed data for the best candidate
        best_match = candidates[0]
        author_id = best_match['author_id']
        
        current_app.logger.info(f"[OPENLIBRARY] Choosing author '{best_match['name']}' (ID: {author_id}, score: {best_match['score']}) from {len(candidates)} candidates")
        
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
            
    except Exception as e:
        current_app.logger.warning(f"Failed to search for author '{author_name}': {e}")
        return None

def get_google_books_cover(isbn, fetch_title_author=False):
    """Enhanced Google Books API lookup with comprehensive field mapping."""
    if not isbn:
        return None
        
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
    try:
        resp = requests.get(url, timeout=15)  # Increased timeout
        resp.raise_for_status()  # Raise exception for bad status codes
        data = resp.json()
        items = data.get("items")
        if items:
            volume_info = items[0]["volumeInfo"]
            image_links = volume_info.get("imageLinks", {})
            
            # Get best quality cover image available
            cover_url = None
            for size in ['extraLarge', 'large', 'medium', 'thumbnail', 'smallThumbnail']:
                if size in image_links:
                    cover_url = image_links[size]
                    break
            
            # Force HTTPS and enhance Google Books URLs for higher resolution
            if cover_url and cover_url.startswith('http://'):
                cover_url = cover_url.replace('http://', 'https://')
            
            # Enhance Google Books cover URLs for higher resolution
            if cover_url and 'books.google.com' in cover_url:
                # Add zoom parameter for higher quality if not already present
                if 'zoom=' not in cover_url:
                    separator = '&' if '?' in cover_url else '?'
                    cover_url = f"{cover_url}{separator}zoom=1"
                
                # Replace small thumbnail sizes with larger ones in the URL
                if 'zoom=0' in cover_url:
                    cover_url = cover_url.replace('zoom=0', 'zoom=1')
                if 'zoom=2' in cover_url:
                    cover_url = cover_url.replace('zoom=2', 'zoom=1')
                if 'zoom=3' in cover_url:
                    cover_url = cover_url.replace('zoom=3', 'zoom=1')
            
            if fetch_title_author:
                title = volume_info.get('title', '')
                subtitle = volume_info.get('subtitle', '')
                authors_list = volume_info.get('authors', [])
                authors = ", ".join(authors_list) if authors_list else ""
                description = volume_info.get('description', '')
                published_date = volume_info.get('publishedDate', '')
                page_count = volume_info.get('pageCount')
                language = volume_info.get('language', 'en')
                average_rating = volume_info.get('averageRating')
                rating_count = volume_info.get('ratingsCount')
                publisher = volume_info.get('publisher', '')
                
                # Enhanced category processing with better filtering
                raw_categories = volume_info.get('categories', [])
                categories = []
                
                # Filter and clean Google Books categories
                exclude_categories = {
                    'fiction', 'non-fiction', 'literature', 'general',
                    'juvenile fiction', 'juvenile nonfiction', 'young adult fiction',
                    'literary criticism', 'criticism', 'biography & autobiography'
                }
                
                for cat in raw_categories:
                    if isinstance(cat, str):
                        # Split complex categories (e.g., "Fiction / Science Fiction / General")
                        parts = [part.strip() for part in cat.split('/')]
                        for part in parts:
                            clean_part = part.strip().lower()
                            if (clean_part and 
                                len(clean_part) > 2 and 
                                clean_part not in exclude_categories and
                                not clean_part.startswith('general')):
                                # Capitalize properly
                                categories.append(part.strip().title())
                
                # Remove duplicates while preserving order
                seen = set()
                unique_categories = []
                for cat in categories:
                    if cat.lower() not in seen:
                        unique_categories.append(cat)
                        seen.add(cat.lower())
                categories = unique_categories[:10]  # Limit to 10 most relevant
                
                # Enhanced ISBN extraction from industryIdentifiers
                isbn10 = None
                isbn13 = None
                asin = None
                
                industry_identifiers = volume_info.get('industryIdentifiers', [])
                for identifier in industry_identifiers:
                    id_type = identifier.get('type', '')
                    id_value = identifier.get('identifier', '')
                    
                    if id_type == 'ISBN_10':
                        isbn10 = id_value
                    elif id_type == 'ISBN_13':
                        isbn13 = id_value
                    elif id_type == 'OTHER' and 'ASIN' in str(id_value):
                        asin = id_value
                
                # Additional metadata
                google_books_id = items[0].get('id', '')
                
                return {
                    'cover': cover_url,
                    'title': title,
                    'subtitle': subtitle,
                    'author': authors,  # Keep for backward compatibility
                    'authors_list': authors_list,  # Enhanced: Individual authors
                    'description': description,
                    'published_date': published_date,
                    'page_count': page_count,
                    'categories': categories,  # Enhanced: Filtered and cleaned categories
                    'publisher': publisher,
                    'language': language,
                    'average_rating': average_rating,
                    'rating_count': rating_count,
                    'isbn10': isbn10,
                    'isbn13': isbn13,
                    'asin': asin,
                    'google_books_id': google_books_id
                }
            return cover_url
    except Exception as e:
        current_app.logger.error(f"Failed to fetch Google Books data for ISBN {isbn}: {e}")
        return None
    if fetch_title_author:
        return None
    return None

def format_date(date):
    return date.strftime("%Y-%m-%d") if date else None

def calculate_reading_streak(user_id, streak_offset=0):
    """
    Calculate reading streak for a specific user with foolproof logic.
    Currently returns the streak_offset until the reading log system is fully implemented.
    """
    try:
        # TODO: Implement proper reading log system
        # For now, return the streak_offset as a fallback
        current_app.logger.debug(f"Reading log system not fully implemented, returning streak offset: {streak_offset}")
        return streak_offset
            
    except Exception as e:
        current_app.logger.error(f"Error calculating reading streak for user {user_id}: {e}")
        return streak_offset

def get_reading_streak(timezone=None):
    """
    Legacy function for backward compatibility
    Uses current user's streak calculation
    """
    from flask_login import current_user
    if not current_user.is_authenticated:
        return 0
    return current_user.get_reading_streak()

def generate_month_review_image(books, month, year):
    import calendar
    from PIL import Image, ImageDraw, ImageFont
    from io import BytesIO
    import requests
    import os

    img_size = 1080
    cols = 4
    cover_w, cover_h = 200, 300
    padding = 30
    # Increase title_height to give more space for the text
    title_height = 220
    grid_w = cols * cover_w + (cols - 1) * padding
    rows = ((len(books) - 1) // cols) + 1 if books else 1
    grid_h = rows * cover_h + (rows - 1) * padding
    # Move grid lower to avoid overlap
    grid_top = title_height + 40
    grid_left = (img_size - grid_w) // 2

    # Try bookshelf background
    bg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'static', 'bookshelf.png'))
    print("Looking for bookshelf background at:", bg_path)
    try:
        bg = Image.open(bg_path).convert('RGBA').resize((img_size, img_size))
        print("Bookshelf background loaded!")
    except Exception as e:
        print("Failed to load bookshelf background:", e)
        bg = Image.new('RGBA', (img_size, img_size), (255, 230, 200, 255))

    draw = ImageDraw.Draw(bg)

    # Draw month title in white
    month_name = f"{str(calendar.month_name[month]).upper()} {year}"
    max_width = img_size - 80  # 40px margin on each side
    font_size = 220
    font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    if not os.path.exists(font_path):
        font_path = os.path.join(os.path.dirname(__file__), "static", "Arial.ttf")
    
    # Initialize font and width variables
    font = ImageFont.load_default()  # Default fallback
    w = 0
    
    while font_size > 10:
        try:
            font = ImageFont.truetype(font_path, font_size)
        except Exception as e:
            print("Font load failed:", e)
            font = ImageFont.load_default()
        bbox = draw.textbbox((0, 0), month_name, font=font)
        w, h = bbox[2] - bbox[0], bbox[3] - bbox[1]
        if w <= max_width:
            break
        font_size -= 10
    shadow_offset = 4
    # Draw shadow for readability
    draw.text(((img_size - w) // 2 + shadow_offset, 40 + shadow_offset), month_name, fill=(0,0,0,128), font=font)
    # Draw main text in white
    draw.text(((img_size - w) // 2, 40), month_name, fill=(255, 255, 255), font=font)

    # Place covers
    for idx, book in enumerate(books):
        row = idx // cols
        col = idx % cols
        x = grid_left + col * (cover_w + padding)
        y = grid_top + row * (cover_h + padding)
        cover_url = getattr(book, 'cover_url', None)
        try:
            if cover_url:
                r = requests.get(cover_url, timeout=10)
                cover = Image.open(BytesIO(r.content)).convert("RGBA")
                cover = cover.resize((cover_w, cover_h))
            else:
                raise Exception("No cover")
        except Exception:
            cover = Image.new('RGBA', (cover_w, cover_h), (220, 220, 220, 255))
        bg.paste(cover, (x, y), cover if cover.mode == 'RGBA' else None)

    return bg.convert('RGB')

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
    
    # Additional cleaning for ISBN fields
    if field_type == 'isbn':
        # Remove any remaining quotes, equals, or whitespace
        value = value.replace('"', '').replace('=', '').strip()
        # Validate that it looks like an ISBN (digits, X, hyphens only)
        if value and not all(c.isdigit() or c in 'X-' for c in value):
            # If it doesn't look like an ISBN, it might be corrupted
            print(f"WARNING: Potentially corrupted ISBN value: '{value}'")
    
    return value.strip()