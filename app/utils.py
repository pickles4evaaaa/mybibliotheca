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
    """Fetch book data with timeout and error handling"""
    url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
    try:
        response = requests.get(url, timeout=10)  # 10 second timeout
        response.raise_for_status()
        data = response.json()
        
        book_key = f"ISBN:{isbn}"
        if book_key in data:
            book = data[book_key]
            title = book.get('title', '')
            
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
            
            cover_url = book.get('cover', {}).get('large') or book.get('cover', {}).get('medium') or book.get('cover', {}).get('small')
            
            # Extract additional metadata
            description = book.get('notes', {}).get('value') if isinstance(book.get('notes'), dict) else book.get('notes')
            published_date = book.get('publish_date', '')
            page_count = book.get('number_of_pages')
            subjects = book.get('subjects', [])
            categories = ', '.join([s['name'] if isinstance(s, dict) else str(s) for s in subjects[:5]])  # Limit to 5 categories
            publishers = book.get('publishers', [])
            publisher = publishers[0]['name'] if publishers and isinstance(publishers[0], dict) else (publishers[0] if publishers else '')
            languages = book.get('languages', [])
            language = languages[0]['key'].split('/')[-1] if languages and isinstance(languages[0], dict) else (languages[0] if languages else '')
            
            return {
                'title': title,
                'author': authors,  # Keep for backward compatibility
                'authors_list': authors_list,  # New: Individual authors for better Person creation
                'cover': cover_url,
                'description': description,
                'published_date': published_date,
                'page_count': page_count,
                'categories': categories,
                'publisher': publisher,
                'language': language
            }
        return None
    
    except (requests.exceptions.RequestException, requests.exceptions.Timeout, ValueError) as e:
        # Log the error for debugging but don't crash the bulk import
        current_app.logger.warning(f"Failed to fetch book data for ISBN {isbn}: {e}")
        return None

def get_google_books_cover(isbn, fetch_title_author=False):
    """Get book cover from Google Books API with HTTPS enforcement."""
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
    try:
        resp = requests.get(url, timeout=5)
        data = resp.json()
        items = data.get("items")
        if items:
            volume_info = items[0]["volumeInfo"]
            image_links = volume_info.get("imageLinks", {})
            cover_url = image_links.get("thumbnail") or image_links.get("smallThumbnail")
            
            # Force HTTPS for cover URLs
            if cover_url and cover_url.startswith('http://'):
                cover_url = cover_url.replace('http://', 'https://')
            
            if fetch_title_author:
                title = volume_info.get('title')
                # Get individual authors instead of joining them
                authors_list = volume_info.get('authors', [])
                # Keep for backward compatibility
                authors = ", ".join(authors_list) if authors_list else ""
                description = volume_info.get('description', '')
                published_date = volume_info.get('publishedDate', '')
                page_count = volume_info.get('pageCount')
                categories = ', '.join(volume_info.get('categories', []))
                publisher = volume_info.get('publisher', '')
                language = volume_info.get('language', '')
                average_rating = volume_info.get('averageRating')
                rating_count = volume_info.get('ratingsCount')
                
                return {
                    'cover': cover_url,
                    'title': title,
                    'author': authors,  # Keep for backward compatibility
                    'authors_list': authors_list,  # New: Individual authors
                    'description': description,
                    'published_date': published_date,
                    'page_count': page_count,
                    'categories': categories,
                    'publisher': publisher,
                    'language': language,
                    'average_rating': average_rating,
                    'rating_count': rating_count
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