import hashlib

def _build_network_data(user_books):
    """Build network data structure for the Interactive Library Network Explorer."""
    
    # Initialize data structures
    network_data = {
        "books": {},
        "authors": {},
        "categories": {},
        "series": {},
        "publishers": {},
        "author_relationships": [],
        "category_relationships": [],
        "series_relationships": [],
        "publisher_relationships": []
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
            "publisher": _get_value(book, "publisher", None),
            "series_name": _get_value(book, "series_name", None),
            "series_volume": _get_value(book, "series_volume", None)
        }
        
        # Get status color for visual encoding
        book_data["status_color"] = _get_network_status_color(book_data["reading_status"])
        
        # Store book
        network_data["books"][book_id] = book_data
        
        # Extract authors and create relationships
        authors = _extract_authors(book)
        for author in authors:
            author_id = f"author_{author.replace(' ', '_').lower()}"
            if author_id not in network_data["authors"]:
                network_data["authors"][author_id] = {
                    "id": author_id,
                    "name": author,
                    "book_count": 0,
                    "books": []
                }
            
            network_data["authors"][author_id]["book_count"] += 1
            network_data["authors"][author_id]["books"].append(book_id)
            
            # Create relationship
            network_data["author_relationships"].append({
                "book_id": book_id,
                "author_id": author_id,
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
            publisher_id = f"publisher_{book_data['publisher'].replace(' ', '_').lower()}"
            if publisher_id not in network_data["publishers"]:
                network_data["publishers"][publisher_id] = {
                    "id": publisher_id,
                    "name": book_data["publisher"],
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
    hash_obj = hashlib.md5(category_name.encode())
    hash_hex = hash_obj.hexdigest()
    
    # Convert first 6 characters to color, ensure good contrast
    color = f"#{hash_hex[:6]}"
    return color
