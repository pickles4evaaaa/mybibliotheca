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
    url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
    try:
        response = requests.get(url, timeout=10)  # 10 second timeout
        response.raise_for_status()
        data = response.json()
        
        book_key = f"ISBN:{isbn}"
        if book_key in data:
            book = data[book_key]
            title = book.get('title', '')
            subtitle = book.get('subtitle', '')
            
            # Extract individual authors for better Person entity creation
            authors_list = []
            authors_data = book.get('authors', [])
            for author in authors_data:
                if isinstance(author, dict) and 'name' in author:
                    authors_list.append(author['name'])
                elif isinstance(author, str):
                    authors_list.append(author)
            
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
            
            # Enhanced subjects/categories processing
            subjects = book.get('subjects', [])
            categories = []
            for subject in subjects[:10]:  # Limit to 10 most relevant categories
                if isinstance(subject, dict):
                    categories.append(subject.get('name', ''))
                else:
                    categories.append(str(subject))
            # Remove empty categories and join
            categories = [cat for cat in categories if cat.strip()]
            
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
                'categories': categories,  # Enhanced: List of categories instead of comma-separated string
                'publisher': publisher,
                'language': language,
                'openlibrary_id': openlibrary_id
            }
        return None
    
    except (requests.exceptions.RequestException, requests.exceptions.Timeout, ValueError) as e:
        # Log the error for debugging but don't crash the bulk import
        current_app.logger.warning(f"Failed to fetch OpenLibrary book data for ISBN {isbn}: {e}")
        return None

def get_google_books_cover(isbn, fetch_title_author=False):
    """Enhanced Google Books API lookup with comprehensive field mapping."""
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
    try:
        resp = requests.get(url, timeout=10)  # Increased timeout
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
            
            # Force HTTPS for cover URLs
            if cover_url and cover_url.startswith('http://'):
                cover_url = cover_url.replace('http://', 'https://')
            
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
                
                # Enhanced category processing
                categories = volume_info.get('categories', [])
                
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
                    'categories': categories,
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
        pass
    if fetch_title_author:
        return None
    return None

def format_date(date):
    return date.strftime("%Y-%m-%d") if date else None

def calculate_reading_streak(user_id, streak_offset=0):
    """
    Calculate reading streak for a specific user with foolproof logic
    """
    try:
        # Get all reading logs for this user using the service
        # This would need to be implemented in the reading log service
        if hasattr(reading_log_service, 'get_user_log_dates_sync'):
            log_dates = reading_log_service.get_user_log_dates_sync(user_id)
        else:
            # Fallback - return the offset
            return streak_offset
        
        if not log_dates:
            return streak_offset
        
        # Sort in descending order (most recent first)
        log_dates.sort(reverse=True)
        
        today = date.today()
        streak = 0
        
        # Check if there's a log for today or yesterday
        # (allow for timezone differences and late logging)
        most_recent = log_dates[0]
        days_since_recent = (today - most_recent).days
        
        # If the most recent log is more than 1 day old, streak is broken
        if days_since_recent > 1:
            return streak_offset
        
        # Start counting the streak
        current_date = most_recent
        
        for log_date in log_dates:
            # If this date continues the streak (same day or previous day)
            if log_date == current_date:
                streak += 1
                current_date = current_date - timedelta(days=1)
            else:
                # Check if there's a gap
                days_gap = (current_date - log_date).days
                if days_gap == 0:
                    # Same date, skip (already counted)
                    continue
                elif days_gap == 1:
                    # Previous day, continue streak
                    streak += 1
                    current_date = log_date - timedelta(days=1)
                else:
                    # Gap found, streak ends
                    break
        
        return streak + streak_offset
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
    month_name = f"{calendar.month_name[month].upper()} {year}"
    max_width = img_size - 80  # 40px margin on each side
    font_size = 220
    font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    if not os.path.exists(font_path):
        font_path = os.path.join(os.path.dirname(__file__), "static", "Arial.ttf")
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