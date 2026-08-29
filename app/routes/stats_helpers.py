"""Pure helpers for statistics and timeline presentation.

This module contains only data shaping and calendar/network calculations.  The
original stats_routes module re-exports these private helpers so existing
internal callers keep their names and behavior.
"""

from __future__ import annotations

import calendar
import json
import re
from datetime import datetime
from typing import Optional


def _safe_date_to_isoformat(date_obj):
    if date_obj and hasattr(date_obj, 'isoformat'):
        return date_obj.isoformat()
    return None


def _get_value(obj, key, default=None):
    """Safely get a value from either a dict or object."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    else:
        return getattr(obj, key, default)


def _extract_authors(book):
    """Extract author names with robust fallback logic."""
    contributors = _get_value(book, 'contributors', [])
    if not contributors:
        author = _get_value(book, 'author', None)
        return [author] if author else ['Unknown Author']
    
    author_names = []
    for contributor in contributors:
        try:
            contrib_type = _get_value(contributor, 'contribution_type', '')
            contrib_type_str = str(contrib_type).lower()
            
            if 'author' in contrib_type_str or contrib_type_str == 'authored':
                person = _get_value(contributor, 'person', None)
                if person:
                    name = _get_value(person, 'name', None)
                    if name:
                        author_names.append(name)
                else:
                    name = _get_value(contributor, 'name', None) or _get_value(contributor, 'author_name', None)
                    if name:
                        author_names.append(name)
        except Exception:
            continue
    
    return author_names if author_names else ['Unknown Author']


def _extract_categories(book):
    """Extract category names."""
    categories = _get_value(book, 'categories', [])
    if not categories:
        return []
    
    category_names = []
    for cat in categories:
        name = _get_value(cat, 'name', None)
        if name:
            category_names.append(name)
        elif isinstance(cat, str):
            category_names.append(cat)
    
    return category_names


def _extract_publisher_name(book):
    """Extract publisher name from book data, handling both string and object types."""
    publisher = _get_value(book, "publisher", None)
    if publisher is None:
        return None
    
    # Handle Publisher object
    if hasattr(publisher, 'name'):
        return str(publisher.name)
    
    # Handle string publisher name
    return str(publisher)


def _extract_date(book, field_name):
    """Extract and normalize date fields to ISO strings."""
    # Handle both 'publication_date' and 'published_date' for compatibility
    if field_name == 'publication_date':
        date_value = _get_value(book, 'publication_date', None) or _get_value(book, 'published_date', None)
    else:
        date_value = _get_value(book, field_name, None)
    
    if not date_value:
        return None
    
    try:
        if isinstance(date_value, datetime):
            return date_value.date().isoformat()
        elif hasattr(date_value, 'isoformat'):
            return date_value.isoformat()
        elif isinstance(date_value, str):
            # Normalize common short formats
            s = date_value.strip()
            if len(s) == 4 and s.isdigit():
                # Year-only
                return f"{s}-01-01"
            if len(s) == 7 and s[4] == '-':  # YYYY-MM
                return f"{s}-01"
            # Try to parse and reformat
            try:
                parsed = datetime.fromisoformat(s.replace('Z', '+00:00'))
                return parsed.date().isoformat()
            except Exception:
                return s
        else:
            return str(date_value)
    except Exception:
        return None


def _get_book_date(book, date_type):
    """Get the appropriate date from book based on date_type."""
    date_value = None
    
    # Prefer the pre-computed timeline_date if present to honor earlier fallback logic
    timeline_date = book.get('timeline_date') if isinstance(book, dict) else None
    if timeline_date:
        s = timeline_date.strip() if isinstance(timeline_date, str) else timeline_date
        try:
            if isinstance(s, str):
                return _parse_flexible_date(s)
            # If it's already a date/datetime
            if hasattr(s, 'year'):
                return datetime(s.year, getattr(s, 'month', 1), getattr(s, 'day', 1))
        except Exception:
            # Fallback to legacy logic below if parsing fails
            pass
    
    # Primary selection based on requested type
    if date_type == 'finish_date' and book.get('finish_date'):
        date_value = book['finish_date']
    elif date_type == 'start_date' and book.get('start_date'):
        date_value = book['start_date']
    elif date_type == 'publication_date' and book.get('publication_date'):
        date_value = book['publication_date']
    elif date_type == 'date_added' and book.get('date_added'):
        date_value = book['date_added']

    # Fallback sequence if we still don't have a value:
    # 1. date_added (user context)
    # 2. publication_date (bibliographic)
    # 3. start_date (reading context)
    # 4. finish_date
    if not date_value and book.get('date_added'):
        date_value = book['date_added']
    if not date_value and book.get('publication_date'):
        date_value = book['publication_date']
    if not date_value and book.get('start_date'):
        date_value = book['start_date']
    if not date_value and book.get('finish_date'):
        date_value = book['finish_date']
    
    # Convert string dates to datetime if needed
    if isinstance(date_value, str):
        try:
            return _parse_flexible_date(date_value)
        except Exception:
            return None
    
    return date_value


def _parse_flexible_date(s: str) -> datetime:
    """Parse common date shapes to a datetime: YYYY, YYYY-MM, YYYY-MM-DD, handles 00 for month/day and slash formats."""
    if not s:
        raise ValueError('empty date string')
    s = s.strip()
    # Normalize Zulu suffix
    s = s.replace('Z', '+00:00')
    # Replace slashes with hyphens
    s2 = s.replace('/', '-')
    # If full ISO parses, use it
    try:
        return datetime.fromisoformat(s2)
    except Exception:
        pass
    # Year only
    if len(s2) == 4 and s2.isdigit():
        return datetime(int(s2), 1, 1)
    # YYYY-MM (allow 00 month)
    if re.match(r'^\d{4}-\d{2}$', s2):
        year = int(s2[:4])
        month = int(s2[5:7]) or 1
        return datetime(year, month, 1)
    # YYYY-MM-DD (allow 00 month/day)
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})', s2)
    if m:
        year = int(m.group(1))
        month = int(m.group(2)) or 1
        day = int(m.group(3)) or 1
        # Clamp day to 28 to avoid invalid dates for Feb/April
        day = min(day, 28)
        return datetime(year, month, day)
    # As a last resort, extract first 4-digit year and use Jan 1
    m = re.search(r'(\d{4})', s2)
    if m:
        return datetime(int(m.group(1)), 1, 1)
    # Could not parse
    raise ValueError(f'unrecognized date format: {s}')


def _calculate_timeline_positions(books, date_type):
    """Calculate positions for books on the timeline with clustering for nearby books."""
    if not books:
        return []
    
    # Convert dates to timestamps for calculation
    book_positions = []
    for book in books:
        date_value = _get_book_date(book, date_type)
        if date_value:
            timestamp = date_value.timestamp()
            book_positions.append({
                'book': book,
                'timestamp': timestamp,
                'year': date_value.year
            })
    
    if not book_positions:
        return []
    
    # Sort by timestamp
    book_positions.sort(key=lambda x: x['timestamp'])
    
    # Calculate time range
    min_timestamp = book_positions[0]['timestamp']
    max_timestamp = book_positions[-1]['timestamp']
    time_range = max_timestamp - min_timestamp
    
    if time_range == 0:
        # All books have the same date - create a single cluster
        cluster = {
            'type': 'cluster',
            'books': [item['book'] for item in book_positions],
            'x_position': 50,
            'y_position': 50,
            'timestamp': min_timestamp,
            'year': book_positions[0]['year']
        }
        return [cluster]
    
    # Group books by proximity for clustering
    clusters = []
    current_cluster_books = [book_positions[0]]
    cluster_threshold = time_range * 0.03  # 3% of total time range for clustering
    
    for i in range(1, len(book_positions)):
        time_diff = book_positions[i]['timestamp'] - current_cluster_books[-1]['timestamp']
        if time_diff <= cluster_threshold:
            current_cluster_books.append(book_positions[i])
        else:
            # Finish current cluster/individual book
            if len(current_cluster_books) >= 3:  # Cluster if 3 or more books
                avg_timestamp = sum(item['timestamp'] for item in current_cluster_books) / len(current_cluster_books)
                avg_year = sum(item['year'] for item in current_cluster_books) / len(current_cluster_books)
                clusters.append({
                    'type': 'cluster',
                    'books': [item['book'] for item in current_cluster_books],
                    'timestamp': avg_timestamp,
                    'year': int(avg_year)
                })
            else:
                # Individual books
                for item in current_cluster_books:
                    clusters.append({
                        'type': 'individual',
                        'book': item['book'],
                        'timestamp': item['timestamp'],
                        'year': item['year']
                    })
            
            current_cluster_books = [book_positions[i]]
    
    # Handle final cluster/books
    if len(current_cluster_books) >= 3:
        avg_timestamp = sum(item['timestamp'] for item in current_cluster_books) / len(current_cluster_books)
        avg_year = sum(item['year'] for item in current_cluster_books) / len(current_cluster_books)
        clusters.append({
            'type': 'cluster',
            'books': [item['book'] for item in current_cluster_books],
            'timestamp': avg_timestamp,
            'year': int(avg_year)
        })
    else:
        for item in current_cluster_books:
            clusters.append({
                'type': 'individual',
                'book': item['book'],
                'timestamp': item['timestamp'],
                'year': item['year']
            })
    
    # Calculate positions for clusters and individual books
    usable_height = 85
    top_margin = 8
    
    for i, cluster in enumerate(clusters):
        # X position based on time (10% to 90% of width)
        x_percent = 10 + ((cluster['timestamp'] - min_timestamp) / time_range) * 80
        
        # Y position with some vertical variation
        if len(clusters) == 1:
            y_percent = top_margin + usable_height * 0.5
        else:
            # Distribute vertically with some randomness based on position
            base_y = top_margin + usable_height * 0.3
            variation = (hash(str(cluster['timestamp'])) % 40) / 100  # ±20%
            y_percent = base_y + variation * usable_height * 0.4
        
        # Ensure positions stay within bounds
        cluster['x_position'] = max(5, min(95, x_percent))
        cluster['y_position'] = max(10, min(80, y_percent))
    
    return clusters


def _clean_book_data_for_json(book):
    """Clean book data to ensure safe JSON encoding in HTML attributes."""
    import html
    
    # Create a cleaned copy of the book data
    cleaned = {}
    for key, value in book.items():
        if isinstance(value, str):
            # Remove or escape problematic characters
            cleaned_value = value.replace('"', '&quot;').replace("'", '&#39;').replace('\n', ' ').replace('\r', ' ')
            # Limit length to prevent overly long attributes
            if len(cleaned_value) > 500:
                cleaned_value = cleaned_value[:500] + '...'
            cleaned[key] = cleaned_value
        elif value is None:
            cleaned[key] = ''
        else:
            cleaned[key] = value
    
    return cleaned


def _get_status_color(status):
    """Get color for reading status (same as D3.js version)."""
    status_colors = {
        'read': '#28a745',
        'reading': '#17a2b8', 
        'plan_to_read': '#ffc107',
        'on_hold': '#fd7e14',
        'did_not_finish': '#dc3545'
    }
    return status_colors.get(status, '#6c757d')


def _calculate_timeline_stats(timeline_items):
    """Calculate statistics for the timeline."""
    if not timeline_items:
        return {
            'total_books': 0,
            'date_range': '-',
            'avg_rating': '-',
            'total_pages': '-'
        }
    
    # Flatten books from clusters and individuals
    all_books = []
    all_years = []
    
    for item in timeline_items:
        if item.get('type') == 'cluster':
            all_books.extend(item['books'])
            all_years.append(item['year'])
        else:
            all_books.append(item['book'])
            all_years.append(item['year'])
    
    # Calculate stats
    total_books = len(all_books)
    
    # Date range
    date_range = f"{min(all_years)} - {max(all_years)}" if all_years else '-'
    
    # Average rating  
    ratings = [book.get('user_rating') for book in all_books if book.get('user_rating') and book.get('user_rating') > 0]
    avg_rating = f"{sum(ratings) / len(ratings):.1f}" if ratings else '-'
    
    # Total pages
    pages = [book.get('page_count') for book in all_books if book.get('page_count') and book.get('page_count') > 0]
    total_pages = f"{sum(pages):,}" if pages else '-'
    
    return {
        'total_books': total_books,
        'date_range': date_range,
        'avg_rating': avg_rating,
        'total_pages': total_pages
    }


def _generate_empty_calendar(year, month):
    """Generate empty calendar structure."""
    cal = calendar.monthcalendar(year, month)
    days = []
    
    for week in cal:
        for day in week:
            if day == 0:  # Empty day from previous/next month
                days.append({
                    'day': None,
                    'date': None,
                    'books': [],
                    'clusters': [],
                    'is_current_month': False
                })
            else:
                days.append({
                    'day': day,
                    'date': datetime(year, month, day).date(),
                    'books': [],
                    'clusters': [],
                    'is_current_month': True
                })
    
    return {
        'year': year,
        'month': month,
        'month_name': calendar.month_name[month],
        'days': days,
        'weeks': len(cal)
    }


def _generate_calendar_with_logs(year, month, logs):
    """Generate calendar with reading logs placed on appropriate days and enhanced heatmap data."""
    cal = calendar.monthcalendar(year, month)
    days = []
    
    # Group logs by day
    logs_by_day = {}
    for log in logs:
        day = log['day']
        if day not in logs_by_day:
            logs_by_day[day] = []
        logs_by_day[day].append(log)
    
    # Calculate activity intensity metrics
    all_activity_counts = []
    for day_logs in logs_by_day.values():
        all_activity_counts.append(len(day_logs))
    
    max_activity = max(all_activity_counts) if all_activity_counts else 0
    avg_activity = sum(all_activity_counts) / len(all_activity_counts) if all_activity_counts else 0
    
    # Calculate intensity thresholds
    intensity_thresholds = [
        0,
        max(1, int(max_activity * 0.25)),
        max(1, int(max_activity * 0.5)),
        max(2, int(max_activity * 0.75)),
        max_activity
    ] if max_activity > 0 else [0, 0, 0, 0, 0]
    
    for week in cal:
        for day in week:
            if day == 0:  # Empty day from previous/next month
                days.append({
                    'day': None,
                    'date': None,
                    'logs': [],
                    'clusters': [],
                    'is_current_month': False,
                    'activity_intensity': 0,
                    'day_of_week': 0,
                    'is_weekend': False,
                    'day_pattern': 'empty'
                })
            else:
                day_date = datetime(year, month, day).date()
                day_logs = logs_by_day.get(day, [])
                
                # Calculate activity intensity (0-4 scale)
                activity_count = len(day_logs)
                intensity = 0
                for i in range(1, len(intensity_thresholds)):
                    if activity_count >= intensity_thresholds[i]:
                        intensity = i
                
                # Determine day of week (0=Monday, 6=Sunday)
                day_of_week = day_date.weekday()
                is_weekend = day_of_week >= 5  # Saturday=5, Sunday=6
                
                # Determine day pattern based on activity and day type
                day_pattern = 'normal'
                if activity_count > avg_activity:
                    day_pattern = 'high_activity'
                elif is_weekend and activity_count > 0:
                    day_pattern = 'weekend_active'
                elif activity_count == 0:
                    day_pattern = 'inactive'
                
                # Separate individual logs from clusters (3+ logs = cluster)
                individual_logs = day_logs[:2] if len(day_logs) <= 2 else []
                clusters = []
                
                if len(day_logs) >= 3:
                    clusters.append({
                        'count': len(day_logs),
                        'date': day_date.isoformat(),
                        'logs': day_logs
                    })
                
                days.append({
                    'day': day,
                    'date': day_date,
                    'logs': individual_logs,
                    'clusters': clusters,
                    'is_current_month': True,
                    'activity_intensity': intensity,
                    'activity_count': activity_count,
                    'day_of_week': day_of_week,
                    'is_weekend': is_weekend,
                    'day_pattern': day_pattern
                })
    
    return {
        'year': year,
        'month': month,
        'month_name': calendar.month_name[month],
        'days': days,
        'weeks': len(cal),
        'max_activity': max_activity,
        'avg_activity': round(avg_activity, 1),
        'intensity_thresholds': intensity_thresholds
    }


def _calculate_current_streak(days):
    """Calculate the current reading streak (consecutive days with activity)."""
    streak = 0
    today = datetime.now().date()
    
    # Reverse the days to start from the most recent
    for day in reversed(days):
        if not day.get('is_current_month') or not day.get('day'):
            continue
            
        day_date = day.get('date')
        if isinstance(day_date, str):
            day_date = datetime.strptime(day_date, '%Y-%m-%d').date()
        elif hasattr(day_date, 'date'):
            day_date = day_date.date()
            
        # Only count days up to today
        if day_date > today:
            continue
            
        activity_count = day.get('activity_count', 0)
        if activity_count > 0:
            streak += 1
        else:
            break  # Streak is broken
            
        # Stop if we've gone too far back
        if (today - day_date).days > 30:
            break
            
    return streak


def _calculate_weekend_activity(days):
    """Calculate weekend vs weekday activity ratio."""
    weekend_activity = 0
    weekday_activity = 0
    
    for day in days:
        if not day.get('is_current_month') or not day.get('day'):
            continue
            
        activity_count = day.get('activity_count', 0)
        is_weekend = day.get('is_weekend', False)
        
        if is_weekend:
            weekend_activity += activity_count
        else:
            weekday_activity += activity_count
    
    total_activity = weekend_activity + weekday_activity
    if total_activity == 0:
        return 0
        
    return round((weekend_activity / total_activity) * 100, 1)


def _generate_calendar_with_books(year, month, books):
    """Generate calendar with books placed on appropriate days."""
    cal = calendar.monthcalendar(year, month)
    days = []
    
    # Group books by day
    books_by_day = {}
    for book in books:
        day = book['day']
        if day not in books_by_day:
            books_by_day[day] = []
        books_by_day[day].append(book)
    
    for week in cal:
        for day in week:
            if day == 0:  # Empty day from previous/next month
                days.append({
                    'day': None,
                    'date': None,
                    'books': [],
                    'clusters': [],
                    'is_current_month': False
                })
            else:
                day_books = books_by_day.get(day, [])
                
                # Apply clustering logic - if 3+ books on same day, create cluster
                if len(day_books) >= 3:
                    days.append({
                        'day': day,
                        'date': datetime(year, month, day).date(),
                        'books': [],
                        'clusters': [{
                            'type': 'cluster',
                            'count': len(day_books),
                            'books': day_books,
                            'date': datetime(year, month, day).date()
                        }],
                        'is_current_month': True
                    })
                else:
                    days.append({
                        'day': day,
                        'date': datetime(year, month, day).date(),
                        'books': day_books,
                        'clusters': [],
                        'is_current_month': True
                    })
    
    return {
        'year': year,
        'month': month,
        'month_name': calendar.month_name[month],
        'days': days,
        'weeks': len(cal)
    }


def _build_network_data(user_books):
    """Build network data structure for the Interactive Library Network Explorer."""
    import json  # For handling custom metadata that might be JSON strings
    
    # Initialize data structures
    network_data = {
        "books": {},
        "contributors": {},  # People with their specific contribution types
        "categories": {},
        "series": {},
        "publishers": {},
        "custom_fields": {},  # Custom field values as nodes
        "contributor_relationships": [],
        "category_relationships": [],
        "series_relationships": [],
        "publisher_relationships": [],
        "custom_field_relationships": []
    }
    
    # Process each book
    for book in user_books:
        book_id = _get_value(book, "uid", "") or _get_value(book, "id", "")
        if not book_id:
            continue
            
        # Extract book data
        book_data = {
            "id": book_id,
            "title": _get_value(book, "title", "Unknown Title"),
            "cover_url": _get_value(book, "cover_url", None),
            "reading_status": _get_value(book, "reading_status", None),
            "user_rating": _get_value(book, "user_rating", None),
            "page_count": _get_value(book, "page_count", None),
            "finish_date": _extract_date(book, "finish_date"),
            "date_added": _extract_date(book, "date_added"),
            "publisher": _extract_publisher_name(book),
            "series_name": _get_value(book, "series_name", None),
            "series_volume": _get_value(book, "series_volume", None)
        }
        
        # Get status color for visual encoding
        book_data["status_color"] = _get_network_status_color(book_data["reading_status"])
        
        # Store book
        network_data["books"][book_id] = book_data
        
        # Extract detailed contributors with contribution types
        contributors = _get_value(book, "contributors", [])
        if contributors:
            for contributor in contributors:
                person = _get_value(contributor, "person", None)
                contribution_type = _get_value(contributor, "contribution_type", None)
                
                if person and contribution_type:
                    person_name = _get_value(person, "name", "Unknown")
                    person_id = _get_value(person, "id", "")
                    
                    # Get contribution type value
                    if hasattr(contribution_type, 'value'):
                        contrib_type = contribution_type.value
                    else:
                        contrib_type = str(contribution_type).lower()
                    
                    # Create unique contributor node ID based on person and contribution type
                    contributor_id = f"contributor_{person_id}_{contrib_type}" if person_id else f"contributor_{person_name.replace(' ', '_').lower()}_{contrib_type}"
                    
                    if contributor_id not in network_data["contributors"]:
                        network_data["contributors"][contributor_id] = {
                            "id": contributor_id,
                            "person_id": person_id,
                            "name": person_name,
                            "contribution_type": contrib_type,
                            "book_count": 0,
                            "books": []
                        }
                    
                    network_data["contributors"][contributor_id]["book_count"] += 1
                    network_data["contributors"][contributor_id]["books"].append(book_id)
                    
                    # Create relationship
                    network_data["contributor_relationships"].append({
                        "book_id": book_id,
                        "contributor_id": contributor_id,
                        "type": contrib_type
                    })
        
        # Fallback: Extract basic authors if no detailed contributors
        if not contributors:
            authors = _extract_authors(book)
            for author in authors:
                contributor_id = f"contributor_{author.replace(' ', '_').lower()}_authored"
                if contributor_id not in network_data["contributors"]:
                    network_data["contributors"][contributor_id] = {
                        "id": contributor_id,
                        "person_id": None,
                        "name": author,
                        "contribution_type": "authored",
                        "book_count": 0,
                        "books": []
                    }
                
                network_data["contributors"][contributor_id]["book_count"] += 1
                network_data["contributors"][contributor_id]["books"].append(book_id)
                
                # Create relationship
                network_data["contributor_relationships"].append({
                    "book_id": book_id,
                    "contributor_id": contributor_id,
                    "type": "authored"
                })
        
        # Extract categories and create relationships
        categories = _extract_categories(book)
        for category in categories:
            category_id = f"category_{category.replace(' ', '_').lower()}"
            if category_id not in network_data["categories"]:
                network_data["categories"][category_id] = {
                    "id": category_id,
                    "name": category,
                    "book_count": 0,
                    "books": [],
                    "color": _get_category_color(category)
                }
            
            network_data["categories"][category_id]["book_count"] += 1
            network_data["categories"][category_id]["books"].append(book_id)
            
            # Create relationship
            network_data["category_relationships"].append({
                "book_id": book_id,
                "category_id": category_id,
                "type": "categorized_as"
            })
        
        # Extract series and create relationships
        if book_data["series_name"]:
            series_id = f"series_{book_data['series_name'].replace(' ', '_').lower()}"
            if series_id not in network_data["series"]:
                network_data["series"][series_id] = {
                    "id": series_id,
                    "name": book_data["series_name"],
                    "book_count": 0,
                    "books": []
                }
            
            network_data["series"][series_id]["book_count"] += 1
            network_data["series"][series_id]["books"].append(book_id)
            
            # Create relationship
            network_data["series_relationships"].append({
                "book_id": book_id,
                "series_id": series_id,
                "type": "part_of_series",
                "volume": book_data["series_volume"]
            })
        
        # Extract publishers and create relationships
        if book_data["publisher"]:
            # Handle case where publisher might be an object instead of string
            publisher_name = book_data["publisher"]
            if hasattr(publisher_name, 'name'):
                publisher_name = publisher_name.name
            elif not isinstance(publisher_name, str):
                publisher_name = str(publisher_name)
            
            publisher_id = f"publisher_{publisher_name.replace(' ', '_').lower()}"
            if publisher_id not in network_data["publishers"]:
                network_data["publishers"][publisher_id] = {
                    "id": publisher_id,
                    "name": publisher_name,
                    "book_count": 0,
                    "books": []
                }
            
            network_data["publishers"][publisher_id]["book_count"] += 1
            network_data["publishers"][publisher_id]["books"].append(book_id)
            
            # Create relationship
            network_data["publisher_relationships"].append({
                "book_id": book_id,
                "publisher_id": publisher_id,
                "type": "published_by"
            })
        
        # Extract custom fields and create relationships
        custom_metadata = _get_value(book, "custom_metadata", {})
        if custom_metadata:
            # Handle case where custom_metadata might be a string (JSON) instead of dict
            if isinstance(custom_metadata, str):
                try:
                    custom_metadata = json.loads(custom_metadata)
                except (json.JSONDecodeError, TypeError, NameError):
                    custom_metadata = {}
            
            # Ensure it's a dictionary before iterating
            if isinstance(custom_metadata, dict):
                for field_name, field_value in custom_metadata.items():
                    if field_value and str(field_value).strip():
                        # Clean field value for ID generation
                        clean_value = str(field_value).strip()
                        if len(clean_value) > 50:  # Truncate very long values
                            clean_value = clean_value[:50] + "..."
                        
                        custom_field_id = f"custom_{field_name}_{clean_value.replace(' ', '_').lower()}"
                        
                        if custom_field_id not in network_data["custom_fields"]:
                            network_data["custom_fields"][custom_field_id] = {
                                "id": custom_field_id,
                                "field_name": field_name,
                                "field_value": clean_value,
                                "display_name": field_name.replace('_', ' ').title(),
                                "book_count": 0,
                                "books": []
                            }
                        
                        network_data["custom_fields"][custom_field_id]["book_count"] += 1
                        network_data["custom_fields"][custom_field_id]["books"].append(book_id)
                        
                        # Create relationship
                        network_data["custom_field_relationships"].append({
                            "book_id": book_id,
                            "custom_field_id": custom_field_id,
                            "type": "has_custom_field",
                            "field_name": field_name,
                            "field_value": field_value
                        })
    
    return network_data


def _get_network_status_color(status):
    """Get color for reading status in network visualization."""
    status_colors = {
        "read": "#28a745",           # Green
        "reading": "#007bff",        # Blue  
        "plan_to_read": "#ffc107",   # Yellow
        "on_hold": "#fd7e14",        # Orange
        "did_not_finish": "#dc3545", # Red
        "library_only": "#6c757d"   # Gray
    }
    return status_colors.get(status, "#6c757d")


def _get_category_color(category_name):
    """Get color for category based on name."""
    # Simple hash-based color assignment for consistency
    import hashlib
    hash_obj = hashlib.md5(category_name.encode())
    hash_hex = hash_obj.hexdigest()
    
    # Convert first 6 characters to color, ensure good contrast
    color = f"#{hash_hex[:6]}"
    return color

__all__ = [
    "_safe_date_to_isoformat",
    "_get_value",
    "_extract_authors",
    "_extract_categories",
    "_extract_publisher_name",
    "_extract_date",
    "_get_book_date",
    "_parse_flexible_date",
    "_calculate_timeline_positions",
    "_clean_book_data_for_json",
    "_get_status_color",
    "_calculate_timeline_stats",
    "_generate_empty_calendar",
    "_generate_calendar_with_logs",
    "_calculate_current_streak",
    "_calculate_weekend_activity",
    "_generate_calendar_with_books",
    "_build_network_data",
    "_get_network_status_color",
    "_get_category_color",
]

