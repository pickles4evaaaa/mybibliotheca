"""
Library search utilities for matching books against search queries.
"""


def library_book_matches_query(book_dict, search_query):
    """
    Check if a book (as dict) matches the given search query.
    
    Searches across title, normalized_title, subtitle, author, and description fields.
    
    Args:
        book_dict: Dictionary representation of a book
        search_query: Search string to match against
        
    Returns:
        bool: True if the book matches the query, False otherwise
    """
    if not search_query:
        return True
    
    if not isinstance(book_dict, dict):
        return False
    
    search_lower = search_query.casefold()
    
    # Check title
    title = book_dict.get('title', '') or ''
    if search_lower in title.casefold():
        return True
    
    # Check normalized_title
    normalized_title = book_dict.get('normalized_title', '') or ''
    if search_lower in normalized_title.casefold():
        return True
    
    # Check subtitle
    subtitle = book_dict.get('subtitle', '') or ''
    if search_lower in subtitle.casefold():
        return True
    
    # Check author
    author = book_dict.get('author', '') or ''
    if search_lower in author.casefold():
        return True
    
    # Check description
    description = book_dict.get('description', '') or ''
    if search_lower in description.casefold():
        return True
    
    return False
