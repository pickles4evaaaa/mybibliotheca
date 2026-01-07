"""Tests for the library_search utility."""
import pytest
from app.utils.library_search import library_book_matches_query


def test_library_book_matches_query_with_title():
    """Test that search matches book title."""
    book = {
        'title': 'The Great Gatsby',
        'author': 'F. Scott Fitzgerald',
        'description': 'A classic novel'
    }
    assert library_book_matches_query(book, 'gatsby')
    assert library_book_matches_query(book, 'great')
    assert library_book_matches_query(book, 'GATSBY')  # Case insensitive
    assert not library_book_matches_query(book, 'tolstoy')


def test_library_book_matches_query_with_author():
    """Test that search matches book author."""
    book = {
        'title': 'War and Peace',
        'author': 'Leo Tolstoy',
        'description': 'Epic novel'
    }
    assert library_book_matches_query(book, 'tolstoy')
    assert library_book_matches_query(book, 'leo')
    assert not library_book_matches_query(book, 'dickens')


def test_library_book_matches_query_with_description():
    """Test that search matches book description."""
    book = {
        'title': '1984',
        'author': 'George Orwell',
        'description': 'A dystopian social science fiction novel'
    }
    assert library_book_matches_query(book, 'dystopian')
    assert library_book_matches_query(book, 'fiction')
    assert not library_book_matches_query(book, 'romance')


def test_library_book_matches_query_with_subtitle():
    """Test that search matches book subtitle."""
    book = {
        'title': 'Sapiens',
        'subtitle': 'A Brief History of Humankind',
        'author': 'Yuval Noah Harari',
        'description': 'History book'
    }
    assert library_book_matches_query(book, 'brief')
    assert library_book_matches_query(book, 'humankind')
    assert library_book_matches_query(book, 'sapiens')


def test_library_book_matches_query_with_normalized_title():
    """Test that search matches normalized title."""
    book = {
        'title': 'The Lord of the Rings',
        'normalized_title': 'lord of the rings',
        'author': 'J.R.R. Tolkien',
        'description': 'Fantasy epic'
    }
    assert library_book_matches_query(book, 'rings')
    assert library_book_matches_query(book, 'lord')


def test_library_book_matches_query_empty_search():
    """Test that empty search matches everything."""
    book = {
        'title': 'Any Book',
        'author': 'Any Author'
    }
    assert library_book_matches_query(book, '')
    assert library_book_matches_query(book, None)


def test_library_book_matches_query_missing_fields():
    """Test that search works with missing fields."""
    book = {
        'title': 'Only Title'
    }
    assert library_book_matches_query(book, 'title')
    assert not library_book_matches_query(book, 'author')


def test_library_book_matches_query_non_dict():
    """Test that non-dict returns False."""
    assert not library_book_matches_query("not a dict", 'search')
    assert not library_book_matches_query(None, 'search')
    assert not library_book_matches_query(123, 'search')


def test_library_book_matches_query_none_values():
    """Test that None values in fields are handled."""
    book = {
        'title': 'Test Book',
        'author': None,
        'description': None
    }
    assert library_book_matches_query(book, 'test')
    assert not library_book_matches_query(book, 'unknown')
