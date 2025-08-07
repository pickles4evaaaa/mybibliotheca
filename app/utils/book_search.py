"""
Enhanced book search functionality that queries multiple APIs and ranks results.

This module provides title-based search across Google Books API and OpenLibrary,
with intelligent ranking and deduplication of results.
"""

import requests
from difflib import SequenceMatcher
from typing import List, Dict, Optional, Any
import re
from urllib.parse import quote_plus
import time


def normalize_title(title: str) -> str:
    """Normalize title for comparison by removing articles, punctuation, and extra spaces."""
    if not title:
        return ""
    
    # Convert to lowercase
    title = title.lower().strip()
    
    # Remove common articles at the beginning
    articles = ['the', 'a', 'an']
    words = title.split()
    if words and words[0] in articles:
        words = words[1:]
        title = ' '.join(words)
    
    # Remove punctuation and normalize spaces
    title = re.sub(r'[^\w\s]', ' ', title)
    title = re.sub(r'\s+', ' ', title).strip()
    
    return title


def calculate_title_similarity(search_title: str, result_title: str) -> float:
    """Calculate similarity score between search title and result title."""
    if not search_title or not result_title:
        return 0.0
    
    # Normalize both titles
    norm_search = normalize_title(search_title)
    norm_result = normalize_title(result_title)
    
    if not norm_search or not norm_result:
        return 0.0
    
    # Use SequenceMatcher for similarity
    similarity = SequenceMatcher(None, norm_search, norm_result).ratio()
    
    # Boost exact matches
    if norm_search == norm_result:
        similarity = 1.0
    
    # Boost if search title is contained in result title or vice versa
    elif norm_search in norm_result or norm_result in norm_search:
        similarity = max(similarity, 0.85)
    
    return similarity


def select_best_publication_date(date1: str, date2: str, year1: Optional[int], year2: Optional[int]) -> tuple[str, Optional[int]]:
    """
    Select the best publication date from two sources.
    Prefers full dates over just years when they're from the same year.
    
    Returns:
        tuple: (best_published_date, best_publication_year)
    """
    # If both dates are empty, return what we have
    if not date1 and not date2:
        return date1 or date2 or '', year1 or year2
    
    # If only one has a date, use it
    if date1 and not date2:
        return date1, year1 or year2
    if date2 and not date1:
        return date2, year2 or year1
    
    # Both have dates - determine which is better
    # Check if either looks like a full date (has month/day info)
    date1_is_full = bool(re.search(r'\d{1,2}[-/]\d{1,2}[-/]\d{4}|\d{4}-\d{1,2}-\d{1,2}|[A-Za-z]{3,}', date1))
    date2_is_full = bool(re.search(r'\d{1,2}[-/]\d{1,2}[-/]\d{4}|\d{4}-\d{1,2}-\d{1,2}|[A-Za-z]{3,}', date2))
    
    # If years are the same or very close, prefer the full date
    if year1 and year2 and abs(year1 - year2) <= 1:
        if date1_is_full and not date2_is_full:
            return date1, year1
        elif date2_is_full and not date1_is_full:
            return date2, year2
    
    # If one is clearly a year and the other has more info, prefer the detailed one
    if date1_is_full and not date2_is_full:
        return date1, year1 or year2
    elif date2_is_full and not date1_is_full:
        return date2, year2 or year1
    
    # Default to the first one
    return date1, year1 or year2


def search_google_books(title: str, max_results: int = 20) -> List[Dict[str, Any]]:
    """Search Google Books API for books by title."""
    print(f"📚 [GOOGLE_BOOKS_SEARCH] Searching for: '{title}'")
    
    if not title:
        return []
    
    # Prepare search query
    query = quote_plus(title)
    url = f"https://www.googleapis.com/books/v1/volumes?q=intitle:{query}&maxResults={max_results}"
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        results = []
        items = data.get('items', [])
        print(f"📚 [GOOGLE_BOOKS_SEARCH] Found {len(items)} items")
        
        for item in items:
            try:
                volume_info = item.get('volumeInfo', {})
                
                # Extract basic information
                book_title = volume_info.get('title', '')
                subtitle = volume_info.get('subtitle', '')
                authors = volume_info.get('authors', [])
                publisher = volume_info.get('publisher', '')
                published_date = volume_info.get('publishedDate', '')
                page_count = volume_info.get('pageCount')
                language = volume_info.get('language', 'en')
                description = volume_info.get('description', '')
                categories = volume_info.get('categories', [])
                average_rating = volume_info.get('averageRating')
                rating_count = volume_info.get('ratingsCount')
                
                # Extract ISBNs
                isbn_10 = None
                isbn_13 = None
                industry_identifiers = volume_info.get('industryIdentifiers', [])
                for identifier in industry_identifiers:
                    if identifier.get('type') == 'ISBN_10':
                        isbn_10 = identifier.get('identifier')
                    elif identifier.get('type') == 'ISBN_13':
                        isbn_13 = identifier.get('identifier')
                
                # Extract cover image
                image_links = volume_info.get('imageLinks', {})
                cover_url = None
                for size in ['extraLarge', 'large', 'medium', 'small', 'thumbnail']:
                    if size in image_links:
                        cover_url = image_links[size]
                        if cover_url and cover_url.startswith('http:'):
                            cover_url = cover_url.replace('http:', 'https:')
                        break
                
                # Extract publication year
                publication_year = None
                if published_date:
                    year_match = re.search(r'(\d{4})', published_date)
                    if year_match:
                        publication_year = int(year_match.group(1))
                
                # Calculate similarity score
                similarity_score = calculate_title_similarity(title, book_title)
                
                result = {
                    'title': book_title,
                    'subtitle': subtitle,
                    'authors': authors,
                    'author': ', '.join(authors) if authors else '',
                    'publication_year': publication_year,
                    'published_date': published_date,
                    'page_count': page_count,
                    'cover_url': cover_url,
                    'isbn_10': isbn_10,
                    'isbn_13': isbn_13,
                    'publisher': publisher,
                    'language': language,
                    'description': description,
                    'categories': categories,
                    'average_rating': average_rating,
                    'rating_count': rating_count,
                    'google_books_id': item.get('id'),
                    'openlibrary_id': None,  # Will be filled if found in OpenLibrary
                    'source': 'Google Books',
                    'similarity_score': similarity_score
                }
                
                results.append(result)
                
            except Exception as e:
                print(f"❌ [GOOGLE_BOOKS_SEARCH] Error processing item: {e}")
                continue
        
        print(f"✅ [GOOGLE_BOOKS_SEARCH] Processed {len(results)} valid results")
        return results
        
    except requests.exceptions.RequestException as e:
        print(f"❌ [GOOGLE_BOOKS_SEARCH] Request error: {e}")
        return []
    except Exception as e:
        print(f"❌ [GOOGLE_BOOKS_SEARCH] Unexpected error: {e}")
        return []


def search_openlibrary(title: str, max_results: int = 20) -> List[Dict[str, Any]]:
    """Search OpenLibrary API for books by title."""
    print(f"📖 [OPENLIBRARY_SEARCH] Searching for: '{title}'")
    
    if not title:
        return []
    
    # Prepare search query
    query = quote_plus(title)
    url = f"https://openlibrary.org/search.json?title={query}&limit={max_results}"
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        results = []
        docs = data.get('docs', [])
        print(f"📖 [OPENLIBRARY_SEARCH] Found {len(docs)} items")
        
        for doc in docs:
            try:
                # Extract basic information
                book_title = doc.get('title', '')
                subtitle = doc.get('subtitle', '')
                authors = doc.get('author_name', [])
                publisher = doc.get('publisher', [])
                publisher_str = publisher[0] if publisher else ''
                
                # Extract publication year
                first_publish_year = doc.get('first_publish_year')
                publish_year = doc.get('publish_year', [])
                publication_year = first_publish_year or (publish_year[0] if publish_year else None)
                
                # Extract ISBNs
                isbn_list = doc.get('isbn', [])
                isbn_10 = None
                isbn_13 = None
                
                for isbn in isbn_list:
                    if len(isbn) == 10:
                        isbn_10 = isbn
                    elif len(isbn) == 13:
                        isbn_13 = isbn
                    # Take the first of each type we find
                    if isbn_10 and isbn_13:
                        break
                
                # Extract other metadata
                language = doc.get('language', [])
                language_str = language[0] if language else 'en'
                
                subject = doc.get('subject', [])
                categories = subject[:5] if subject else []  # Limit to first 5 subjects
                
                # Extract cover image
                cover_i = doc.get('cover_i')
                cover_url = None
                if cover_i:
                    cover_url = f"https://covers.openlibrary.org/b/id/{cover_i}-L.jpg"
                
                # Extract OpenLibrary ID
                openlibrary_id = None
                key = doc.get('key')
                if key:
                    # Key format: /works/OL123456W
                    match = re.search(r'/works/(.+)', key)
                    if match:
                        openlibrary_id = match.group(1)
                
                # Calculate similarity score
                similarity_score = calculate_title_similarity(title, book_title)
                
                result = {
                    'title': book_title,
                    'subtitle': subtitle,
                    'authors': authors,
                    'author': ', '.join(authors) if authors else '',
                    'publication_year': publication_year,
                    'published_date': str(publication_year) if publication_year else '',
                    'page_count': None,  # OpenLibrary search doesn't return page count
                    'cover_url': cover_url,
                    'isbn_10': isbn_10,
                    'isbn_13': isbn_13,
                    'publisher': publisher_str,
                    'language': language_str,
                    'description': None,  # Not available in search results
                    'categories': categories,
                    'average_rating': None,
                    'rating_count': None,
                    'google_books_id': None,  # Will be filled if found in Google Books
                    'openlibrary_id': openlibrary_id,
                    'source': 'OpenLibrary',
                    'similarity_score': similarity_score
                }
                
                results.append(result)
                
            except Exception as e:
                print(f"❌ [OPENLIBRARY_SEARCH] Error processing item: {e}")
                continue
        
        print(f"✅ [OPENLIBRARY_SEARCH] Processed {len(results)} valid results")
        return results
        
    except requests.exceptions.RequestException as e:
        print(f"❌ [OPENLIBRARY_SEARCH] Request error: {e}")
        return []
    except Exception as e:
        print(f"❌ [OPENLIBRARY_SEARCH] Unexpected error: {e}")
        return []


def merge_and_rank_results(search_title: str, google_results: List[Dict], 
                          openlibrary_results: List[Dict], max_results: int = 10) -> List[Dict]:
    """
    Merge results from both APIs, deduplicate by ISBN, and rank by similarity.
    
    When the same book is found in both APIs (matching ISBN), merge the data
    and keep both source IDs.
    """
    print(f"🔀 [MERGE_RANK] Merging {len(google_results)} Google + {len(openlibrary_results)} OpenLibrary results")
    
    merged_results = {}
    
    # Process Google Books results first
    for result in google_results:
        isbn_key = result.get('isbn_13') or result.get('isbn_10') or f"google_{result.get('google_books_id')}"
        if isbn_key:
            merged_results[isbn_key] = result.copy()
    
    # Process OpenLibrary results and merge with Google Books where ISBN matches
    for result in openlibrary_results:
        isbn_key = result.get('isbn_13') or result.get('isbn_10') or f"openlibrary_{result.get('openlibrary_id')}"
        
        if isbn_key in merged_results:
            # Found matching ISBN - merge data
            existing = merged_results[isbn_key]
            
            # Merge OpenLibrary ID
            existing['openlibrary_id'] = result.get('openlibrary_id')
            
            # Smart merge publication date information
            best_date, best_year = select_best_publication_date(
                existing.get('published_date', ''),
                result.get('published_date', ''),
                existing.get('publication_year'),
                result.get('publication_year')
            )
            existing['published_date'] = best_date
            existing['publication_year'] = best_year
            
            # Fill in missing data from OpenLibrary (except dates which we handled above)
            for key, value in result.items():
                if key not in ['published_date', 'publication_year'] and (key not in existing or not existing[key]):
                    existing[key] = value
            
            # Update source to indicate both
            existing['source'] = 'Google Books + OpenLibrary'
            
            # Use the higher similarity score
            existing['similarity_score'] = max(
                existing.get('similarity_score', 0),
                result.get('similarity_score', 0)
            )
            
            print(f"🔗 [MERGE_RANK] Merged book with ISBN {isbn_key[:20]}...")
            
        else:
            # New book from OpenLibrary
            merged_results[isbn_key] = result.copy()
    
    # Convert to list and sort by similarity score
    final_results = list(merged_results.values())
    final_results.sort(key=lambda x: x.get('similarity_score', 0), reverse=True)
    
    # Limit to max_results
    final_results = final_results[:max_results]
    
    print(f"✅ [MERGE_RANK] Final results: {len(final_results)} books")
    for i, result in enumerate(final_results[:5]):  # Log top 5
        print(f"  {i+1}. '{result.get('title', '')}' (score: {result.get('similarity_score', 0):.3f}, source: {result.get('source', '')})")
    
    return final_results


def search_books_by_title(title: str, max_results: int = 10) -> List[Dict[str, Any]]:
    """
    Search for books by title across Google Books and OpenLibrary APIs.
    
    Args:
        title: The book title to search for
        max_results: Maximum number of results to return (default: 10)
    
    Returns:
        List of book dictionaries sorted by title similarity, with the following fields:
        - title, subtitle, authors, author (comma-separated)
        - publication_year, published_date, page_count
        - cover_url, isbn_10, isbn_13, publisher, language
        - description, categories, average_rating, rating_count
        - google_books_id, openlibrary_id, source, similarity_score
    """
    print(f"🔍 [BOOK_SEARCH] Starting search for: '{title}'")
    
    if not title or not title.strip():
        print(f"❌ [BOOK_SEARCH] Empty title provided")
        return []
    
    title = title.strip()
    
    # Search both APIs in parallel (could be made truly async in the future)
    google_results = search_google_books(title, max_results * 2)  # Get more to allow for better ranking
    
    # Add small delay to be respectful to APIs
    time.sleep(0.5)
    
    openlibrary_results = search_openlibrary(title, max_results * 2)
    
    # Merge, deduplicate, and rank results
    final_results = merge_and_rank_results(title, google_results, openlibrary_results, max_results)
    
    print(f"🎯 [BOOK_SEARCH] Search complete. Returning {len(final_results)} results for '{title}'")
    
    return final_results


def search_books_with_display_fields(title: str, max_results: int = 10, isbn_required: bool = False) -> Dict[str, Any]:
    """
    Search for books and return results formatted for display.
    
    Args:
        title: The book title to search for
        max_results: Maximum number of results to return
        isbn_required: If True, only return books that have ISBN-10 or ISBN-13
    
    Returns:
        Dict with 'results' containing the book list and 'metadata' with search info
    """
    results = search_books_by_title(title, max_results)
    
    # Filter for ISBN if required
    if isbn_required:
        results = [book for book in results if book.get('isbn_10') or book.get('isbn_13')]
    
    # Format results for display - keep only essential fields visible, store full data
    display_results = []
    for result in results:
        display_item = {
            # Display fields
            'title': result.get('title', ''),
            'author': result.get('author', ''),
            'publication_year': result.get('publication_year'),
            'page_count': result.get('page_count'),
            'similarity_score': result.get('similarity_score', 0),
            'source': result.get('source', ''),
            
            # Full data stored for later use
            'full_data': result
        }
        display_results.append(display_item)
    
    return {
        'results': display_results,
        'metadata': {
            'search_title': title,
            'total_results': len(display_results),
            'max_results': max_results,
            'search_timestamp': time.time(),
            'isbn_required': isbn_required
        }
    }
