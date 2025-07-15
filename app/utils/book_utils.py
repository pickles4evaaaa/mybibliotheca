from datetime import date, timedelta, datetime
import pytz
from ..services import reading_log_service
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
            
            return {
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
                'source': 'OpenLibrary'
            }
        else:
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from OpenLibrary for ISBN {isbn}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error processing OpenLibrary data for ISBN {isbn}: {e}")
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
    if not isbn:
        return None
        
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'items' in data and len(data['items']) > 0:
            book_info = data['items'][0]['volumeInfo']
            
            result = {}
            
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
                
                result['title'] = title
                result['subtitle'] = subtitle
                result['authors'] = ', '.join(authors) if authors else ''
                result['source'] = 'Google Books'
            
            return result
        else:
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching cover from Google Books for ISBN {isbn}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error processing Google Books data for ISBN {isbn}: {e}")
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


def normalize_goodreads_value(value):
    """
    Normalize values from Goodreads CSV exports.
    Handles Excel-style text formatting where values might be quoted.
    """
    if value is None:
        return None
    
    # Convert to string if not already
    value = str(value).strip()
    
    # Handle empty strings
    if not value:
        return ""
    
    # Handle Excel text formatting: ="value"
    if value.startswith('="') and value.endswith('"'):
        value = value[2:-1]
    
    # Handle standard quoted values
    elif value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    
    # Special handling for ISBN fields
    if value and any(char.isdigit() for char in value):
        # Remove any remaining quotes or formatting
        value = value.replace('"', '').replace("'", "")
    
    return value.strip()
