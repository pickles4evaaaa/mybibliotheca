"""
Simplified Book Service - Decoupled Architecture
Separates book creation from user relationships for better persistence.
"""

import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from .domain.models import Book, Person, Publisher, Series, Category, BookContribution, ContributionType
from .infrastructure.kuzu_graph import get_graph_storage


@dataclass
class SimplifiedBook:
    """Simplified book model focused on core bibliographic data only."""
    title: str
    author: str  # Primary author as string for simplicity
    isbn13: Optional[str] = None
    isbn10: Optional[str] = None
    subtitle: Optional[str] = None
    description: Optional[str] = None
    publisher: Optional[str] = None
    published_date: Optional[str] = None
    page_count: Optional[int] = None
    language: str = "en"
    cover_url: Optional[str] = None
    series: Optional[str] = None
    series_volume: Optional[str] = None
    series_order: Optional[int] = None
    categories: List[str] = None
    google_books_id: Optional[str] = None
    openlibrary_id: Optional[str] = None
    average_rating: Optional[float] = None
    rating_count: Optional[int] = None


@dataclass 
class UserBookOwnership:
    """Simplified ownership relationship - just the essentials."""
    user_id: str
    book_id: str
    reading_status: str = "plan_to_read"
    ownership_status: str = "owned" 
    media_type: str = "physical"
    date_added: Optional[datetime] = None
    user_rating: Optional[float] = None
    personal_notes: Optional[str] = None
    location_id: Optional[str] = None
    custom_metadata: Dict[str, Any] = None


class SimplifiedBookService:
    """
    Simplified book service with clean separation:
    1. Create books as standalone global entities
    2. Create user relationships separately
    """
    
    def __init__(self):
        self.storage = get_graph_storage()
    
    def create_standalone_book(self, book_data: SimplifiedBook) -> Optional[str]:
        """
        Create a book as a standalone global entity.
        Returns book_id if successful, None if failed.
        """
        try:
            book_id = str(uuid.uuid4())
            
            print(f"📚 [SIMPLIFIED] Creating standalone book: {book_data.title}")
            
            # Prepare core book node data
            book_node_data = {
                'id': book_id,
                'title': book_data.title,
                'normalized_title': book_data.title.lower(),
                'subtitle': book_data.subtitle,
                'description': book_data.description,
                'published_date': book_data.published_date,
                'page_count': book_data.page_count,
                'language': book_data.language,
                'cover_url': book_data.cover_url,
                'isbn13': book_data.isbn13,
                'isbn10': book_data.isbn10,
                'google_books_id': book_data.google_books_id,
                'openlibrary_id': book_data.openlibrary_id,
                'average_rating': book_data.average_rating,
                'rating_count': book_data.rating_count,
                'series': book_data.series,
                'series_volume': book_data.series_volume,
                'series_order': book_data.series_order,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            
            # Remove None values
            book_node_data = {k: v for k, v in book_node_data.items() if v is not None}
            
            # 1. Create book node (SINGLE TRANSACTION)
            book_success = self.storage.store_node('Book', book_id, book_node_data)
            if not book_success:
                print(f"❌ [SIMPLIFIED] Failed to create book node")
                return None
            
            # 🔥 CRITICAL FIX: Force database commit/flush
            try:
                # Try to commit the transaction explicitly
                if hasattr(self.storage, 'commit'):
                    self.storage.commit()
                elif hasattr(self.storage, 'connection') and hasattr(self.storage.connection, 'commit'):
                    self.storage.connection.commit()
                print(f"💾 [SIMPLIFIED] Forced database commit after book creation")
            except Exception as commit_error:
                print(f"⚠️ [SIMPLIFIED] Could not force commit, relying on auto-commit: {commit_error}")
            
            print(f"✅ [SIMPLIFIED] Book node created: {book_id}")
            
            # 2. Create author relationship (SEPARATE TRANSACTION)
            if book_data.author:
                try:
                    author_id = str(uuid.uuid4())
                    author_data = {
                        'id': author_id,
                        'name': book_data.author,
                        'created_at': datetime.utcnow()
                    }
                    
                    author_success = self.storage.store_node('Person', author_id, author_data)
                    if author_success:
                        # Create AUTHORED relationship
                        authored_success = self.storage.create_relationship(
                            'Person', author_id, 'AUTHORED', 'Book', book_id,
                            {
                                'role': 'authored',
                                'order_index': 0,
                                'created_at': datetime.utcnow()
                            }
                        )
                        if authored_success:
                            print(f"✅ [SIMPLIFIED] Author relationship created: {book_data.author}")
                        else:
                            print(f"⚠️ [SIMPLIFIED] Book created but author relationship failed")
                    else:
                        print(f"⚠️ [SIMPLIFIED] Book created but author node failed")
                except Exception as e:
                    print(f"⚠️ [SIMPLIFIED] Book created but author processing failed: {e}")
            
            # 3. Create publisher relationship (SEPARATE TRANSACTION)
            if book_data.publisher:
                try:
                    publisher_id = str(uuid.uuid4())
                    publisher_data = {
                        'id': publisher_id,
                        'name': book_data.publisher,
                        'created_at': datetime.utcnow()
                    }
                    
                    publisher_success = self.storage.store_node('Publisher', publisher_id, publisher_data)
                    if publisher_success:
                        # Create PUBLISHED_BY relationship
                        # Convert publication_date to proper date format
                        pub_date = None
                        if book_data.published_date:
                            if isinstance(book_data.published_date, str):
                                try:
                                    # Try to parse the string as a date
                                    pub_date = datetime.strptime(book_data.published_date, '%Y-%m-%d').date()
                                except ValueError:
                                    try:
                                        # Try alternative formats
                                        pub_date = datetime.strptime(book_data.published_date, '%Y').date()
                                    except ValueError:
                                        print(f"⚠️ [SIMPLIFIED] Could not parse publication date: {book_data.published_date}")
                                        pub_date = None
                            elif hasattr(book_data.published_date, 'date'):
                                # It's already a date/datetime object
                                pub_date = book_data.published_date.date() if hasattr(book_data.published_date, 'date') else book_data.published_date
                            else:
                                pub_date = book_data.published_date
                        
                        published_success = self.storage.create_relationship(
                            'Book', book_id, 'PUBLISHED_BY', 'Publisher', publisher_id,
                            {
                                'publication_date': pub_date if pub_date else None,
                                'created_at': datetime.utcnow()
                            }
                        )
                        if published_success:
                            print(f"✅ [SIMPLIFIED] Publisher relationship created: {book_data.publisher}")
                        else:
                            print(f"⚠️ [SIMPLIFIED] Book created but publisher relationship failed")
                    else:
                        print(f"⚠️ [SIMPLIFIED] Book created but publisher node failed")
                except Exception as e:
                    print(f"⚠️ [SIMPLIFIED] Book created but publisher processing failed: {e}")
            
            # 4. Create category relationships (SEPARATE TRANSACTION)
            if book_data.categories:
                try:
                    for category_name in book_data.categories:
                        if not category_name.strip():
                            continue
                            
                        category_id = str(uuid.uuid4())
                        category_data = {
                            'id': category_id,
                            'name': category_name.strip(),
                            'book_count': 1,
                            'created_at': datetime.utcnow()
                        }
                        
                        category_success = self.storage.store_node('Category', category_id, category_data)
                        if category_success:
                            # Create CATEGORIZED_AS relationship
                            categorized_success = self.storage.create_relationship(
                                'Book', book_id, 'CATEGORIZED_AS', 'Category', category_id,
                                {
                                    'created_at': datetime.utcnow()
                                }
                            )
                            if categorized_success:
                                print(f"✅ [SIMPLIFIED] Category relationship created: {category_name}")
                            else:
                                print(f"⚠️ [SIMPLIFIED] Book created but category relationship failed: {category_name}")
                        else:
                            print(f"⚠️ [SIMPLIFIED] Book created but category node failed: {category_name}")
                except Exception as e:
                    print(f"⚠️ [SIMPLIFIED] Book created but category processing failed: {e}")
            
            print(f"🎉 [SIMPLIFIED] Book creation completed: {book_id}")
            return book_id
            
        except Exception as e:
            print(f"❌ [SIMPLIFIED] Failed to create standalone book: {e}")
            return None
    
    def create_user_ownership(self, ownership: UserBookOwnership) -> bool:
        """
        Create user ownership relationship - completely separate from book creation.
        Returns True if successful, False if failed.
        """
        try:
            print(f"🔗 [SIMPLIFIED] Creating ownership: User {ownership.user_id} -> Book {ownership.book_id}")
            
            # Ensure date_added is set
            if ownership.date_added is None:
                ownership.date_added = datetime.utcnow()
            
            # Prepare ownership relationship data
            ownership_data = {
                'reading_status': ownership.reading_status,
                'ownership_status': ownership.ownership_status,
                'media_type': ownership.media_type,
                'date_added': ownership.date_added,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            
            # Add optional fields if present
            if ownership.user_rating is not None:
                ownership_data['user_rating'] = ownership.user_rating
            if ownership.personal_notes:
                ownership_data['personal_notes'] = ownership.personal_notes
            if ownership.location_id:
                ownership_data['location_id'] = ownership.location_id
            if ownership.custom_metadata:
                import json
                ownership_data['custom_metadata'] = json.dumps(ownership.custom_metadata)
            
            # Create OWNS relationship (SINGLE TRANSACTION)
            success = self.storage.create_relationship(
                'User', ownership.user_id, 'OWNS', 'Book', ownership.book_id,
                ownership_data
            )
            
            if success:
                # 🔥 CRITICAL FIX: Force database commit/flush after ownership
                try:
                    if hasattr(self.storage, 'commit'):
                        self.storage.commit()
                    elif hasattr(self.storage, 'connection') and hasattr(self.storage.connection, 'commit'):
                        self.storage.connection.commit()
                    print(f"💾 [SIMPLIFIED] Ownership commit forced")
                except Exception as commit_error:
                    print(f"⚠️ [SIMPLIFIED] Could not force ownership commit: {commit_error}")
                
                print(f"✅ [SIMPLIFIED] Ownership created successfully")
                return True
            else:
                print(f"❌ [SIMPLIFIED] Failed to create ownership relationship")
                return False
                
        except Exception as e:
            print(f"❌ [SIMPLIFIED] Failed to create user ownership: {e}")
            return False
    
    def find_book_by_isbn(self, isbn: str) -> Optional[str]:
        """Find existing book by ISBN. Returns book_id if found."""
        try:
            # Normalize ISBN
            normalized_isbn = ''.join(filter(str.isdigit, isbn))
            
            # Search by ISBN13
            if len(normalized_isbn) == 13:
                query = "MATCH (b:Book {isbn13: $isbn}) RETURN b.id"
                result = self.storage.execute_cypher(query, {"isbn": normalized_isbn})
                if result:
                    return result[0]['col_0']
            
            # Search by ISBN10  
            elif len(normalized_isbn) == 10:
                query = "MATCH (b:Book {isbn10: $isbn}) RETURN b.id"
                result = self.storage.execute_cypher(query, {"isbn": normalized_isbn})
                if result:
                    return result[0]['col_0']
            
            return None
            
        except Exception as e:
            print(f"❌ [SIMPLIFIED] Error finding book by ISBN: {e}")
            return None
    
    def find_or_create_book(self, book_data: SimplifiedBook) -> Optional[str]:
        """
        Find existing book or create new one.
        Returns book_id if successful.
        """
        try:
            # Try to find existing book by ISBN
            if book_data.isbn13:
                existing_id = self.find_book_by_isbn(book_data.isbn13)
                if existing_id:
                    print(f"📚 [SIMPLIFIED] Found existing book by ISBN13: {existing_id}")
                    return existing_id
            
            if book_data.isbn10:
                existing_id = self.find_book_by_isbn(book_data.isbn10)
                if existing_id:
                    print(f"📚 [SIMPLIFIED] Found existing book by ISBN10: {existing_id}")
                    return existing_id
            
            # Book doesn't exist, create new one
            print(f"📚 [SIMPLIFIED] Book not found, creating new book")
            return self.create_standalone_book(book_data)
            
        except Exception as e:
            print(f"❌ [SIMPLIFIED] Error in find_or_create_book: {e}")
            return None
    
    def add_book_to_user_library(self, book_data: SimplifiedBook, user_id: str, 
                                reading_status: str = "plan_to_read",
                                ownership_status: str = "owned",
                                media_type: str = "physical",
                                user_rating: Optional[float] = None,
                                personal_notes: Optional[str] = None,
                                location_id: Optional[str] = None,
                                custom_metadata: Dict[str, Any] = None) -> bool:
        """
        Complete workflow: Find/create book + create user ownership.
        This is the main entry point for the simplified architecture.
        """
        try:
            print(f"🎯 [SIMPLIFIED] Starting add_book_to_user_library for: {book_data.title}")
            
            # Step 1: Find or create standalone book
            book_id = self.find_or_create_book(book_data)
            if not book_id:
                print(f"❌ [SIMPLIFIED] Failed to find/create book")
                return False
            
            # Step 2: Create user ownership relationship
            ownership = UserBookOwnership(
                user_id=user_id,
                book_id=book_id,
                reading_status=reading_status,
                ownership_status=ownership_status,
                media_type=media_type,
                date_added=datetime.utcnow(),
                user_rating=user_rating,
                personal_notes=personal_notes,
                location_id=location_id,
                custom_metadata=custom_metadata
            )
            
            ownership_success = self.create_user_ownership(ownership)
            if not ownership_success:
                print(f"❌ [SIMPLIFIED] Book created but ownership failed")
                return False
            
            # 🔥 CRITICAL FIX: Force final database commit/flush
            try:
                if hasattr(self.storage, 'commit'):
                    self.storage.commit()
                elif hasattr(self.storage, 'connection') and hasattr(self.storage.connection, 'commit'):
                    self.storage.connection.commit()
                print(f"💾 [SIMPLIFIED] Final database commit forced")
            except Exception as commit_error:
                print(f"⚠️ [SIMPLIFIED] Could not force final commit: {commit_error}")
            
            print(f"🎉 [SIMPLIFIED] Successfully added book to user library")
            return True
            
        except Exception as e:
            print(f"❌ [SIMPLIFIED] Failed to add book to user library: {e}")
            return False


# Convenience function for current routes
def create_simplified_book_service():
    """Factory function to create simplified book service."""
    return SimplifiedBookService()
