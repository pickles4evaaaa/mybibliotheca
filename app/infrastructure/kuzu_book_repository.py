"""Book repository implementation.

The compatibility module app.infrastructure.kuzu_repositories re-exports this
class.  The implementation is intentionally unchanged; this module only
separates book persistence from the other repository types.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..utils.safe_kuzu_manager import SafeKuzuManager, get_safe_kuzu_manager, safe_get_connection
from .kuzu_repositories import KuzuRepositoryAdapter


logger = logging.getLogger("app.infrastructure.kuzu_repositories")


class KuzuBookRepository:
    """Clean book repository using simplified Kuzu schema."""
    
    def __init__(self):
        # Lazy initialization - don't connect during startup
        self._safe_manager = None
    
    @property
    def safe_manager(self):
        """Lazy SafeKuzuManager connection - only connect when needed."""
        if self._safe_manager is None:
            self._safe_manager = get_safe_kuzu_manager()
        return self._safe_manager
    
    @property
    def db(self):
        """Backward compatibility: provide legacy db interface."""
        # DEPRECATED: Use safe_manager.get_connection() context manager instead
        # This property is only for backward compatibility during migration
        if not hasattr(self, '_db') or self._db is None:
            # Create a compatibility wrapper that provides the old interface
            self._db = KuzuRepositoryAdapter(self.safe_manager)
        return self._db
    
    async def create(self, book: Any) -> Optional[Any]:
        """Create a new book with relationships."""
        try:
            
            # Debug contributors
            contributors = getattr(book, 'contributors', [])
            for i, contrib in enumerate(contributors):
                person = getattr(contrib, 'person', None)
                person_name = getattr(person, 'name', 'unknown') if person else 'no person'
                contrib_type = getattr(contrib, 'contribution_type', 'unknown')
            
            # Debug categories
            categories = getattr(book, 'categories', [])
            raw_categories = getattr(book, 'raw_categories', None)
            
            # Debug publisher
            publisher = getattr(book, 'publisher', None)
            if publisher:
                publisher_name = getattr(publisher, 'name', 'unknown')
            else:
                publisher_name = 'unknown'
            
            if not getattr(book, 'id', None):
                if hasattr(book, 'id'):
                    book.id = str(uuid.uuid4())
            
            # Handle series field - it can be a Series object or a string
            series_value = None
            series_obj = getattr(book, 'series', None)
            if series_obj:
                if hasattr(series_obj, 'name'):
                    # Series object with name attribute
                    series_value = series_obj.name
                else:
                    # Assume it's already a string
                    series_value = str(series_obj)
            
            book_data = {
                'id': getattr(book, 'id', str(uuid.uuid4())),
                'title': getattr(book, 'title', ''),
                'normalized_title': getattr(book, 'normalized_title', None) or getattr(book, 'title', '').lower(),
                'isbn13': getattr(book, 'isbn13', ''),
                'isbn10': getattr(book, 'isbn10', ''),
                # Track ownership quantity (default 1)
                'quantity': getattr(book, 'quantity', 1) or 1,
                # Newly ensured fields that were previously not persisted
                'subtitle': getattr(book, 'subtitle', None),
                'asin': getattr(book, 'asin', None),
                'description': getattr(book, 'description', ''),
                'published_date': getattr(book, 'published_date', None),
                'page_count': getattr(book, 'page_count', 0),
                'language': getattr(book, 'language', 'en'),
                # Store NULL (None) rather than empty string when no cover yet
                'cover_url': (getattr(book, 'cover_url', None) or None),
                'google_books_id': getattr(book, 'google_books_id', None),
                'openlibrary_id': getattr(book, 'openlibrary_id', None),
                'average_rating': getattr(book, 'average_rating', 0.0),
                'rating_count': getattr(book, 'rating_count', 0),
                'media_type': getattr(book, 'media_type', None),
                'series': series_value,
                'series_volume': getattr(book, 'series_volume', None),
                'series_order': getattr(book, 'series_order', None),
                'custom_metadata': getattr(book, 'custom_metadata', None),
                # Use *_str variants so adapter casts to TIMESTAMP via timestamp($param)
                'created_at_str': getattr(book, 'created_at', datetime.now(timezone.utc)).isoformat() if hasattr(getattr(book, 'created_at', datetime.now(timezone.utc)), 'isoformat') else datetime.now(timezone.utc).isoformat(),
                'updated_at_str': getattr(book, 'updated_at', datetime.now(timezone.utc)).isoformat() if hasattr(getattr(book, 'updated_at', datetime.now(timezone.utc)), 'isoformat') else datetime.now(timezone.utc).isoformat()
            }
            
            # Create the book node first
            success = self.db.create_node('Book', book_data)
            if not success:
                logger.error(f"❌ Failed to create book node: {getattr(book, 'title', 'unknown')}")
                return None
            
            logger.info(f"✅ Created book node: {getattr(book, 'title', 'unknown')}")
            book_id = book_data['id']
            
            # Create author relationships
            contributors = getattr(book, 'contributors', [])
            if contributors:
                logger.info(f"🔗 Creating {len(contributors)} contributor relationships")
                for i, contribution in enumerate(contributors):
                    await self._create_contributor_relationship(book_id, contribution, i)
            
            # Create category relationships
            categories = getattr(book, 'categories', [])
            raw_categories = getattr(book, 'raw_categories', None)
            
            # Process raw_categories if available (from API data)
            if raw_categories:
                await self._create_category_relationships_from_raw(book_id, raw_categories)
            # Otherwise use existing categories
            elif categories:
                logger.info(f"🔗 Creating {len(categories)} category relationships")
                for category in categories:
                    await self._create_category_relationship(book_id, category)
            
            # Create publisher relationship if present
            publisher = getattr(book, 'publisher', None)
            if publisher:
                await self._create_publisher_relationship(book_id, publisher)
            
            logger.info(f"✅ Created book with all relationships: {getattr(book, 'title', 'unknown')}")
            return book
            
        except Exception as e:
            logger.error(f"❌ Failed to create book: {e}")
            return None
    
    async def _create_contributor_relationship(self, book_id: str, contribution: Any, order_index: int = 0):
        """Create a contributor relationship (AUTHORED, EDITED, etc.)."""
        try:
            person = getattr(contribution, 'person', None)
            if not person:
                logger.warning(f"⚠️ Contribution has no person: {contribution}")
                return
            
            # Create or find the person
            auto_fetch_metadata = getattr(contribution, 'auto_fetch_metadata', True)
            person_id = await self._ensure_person_exists(person, auto_fetch=auto_fetch_metadata)
            if not person_id:
                logger.warning(f"⚠️ Could not create/find person: {getattr(person, 'name', 'unknown')}")
                return
            
            # Determine relationship type and properties
            contribution_type = getattr(contribution, 'contribution_type', None)
            if contribution_type and hasattr(contribution_type, 'value'):
                contribution_str = contribution_type.value.upper()
            else:
                contribution_str = str(contribution_type).upper() if contribution_type else 'AUTHORED'
            
            # Map contribution types to relationship types
            rel_type_map = {
                'AUTHORED': 'AUTHORED',
                'EDITED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'TRANSLATED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'ILLUSTRATED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'NARRATED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'GAVE_FOREWORD': 'AUTHORED',  # Use AUTHORED relationship with role property
                'GAVE_INTRODUCTION': 'AUTHORED',  # Use AUTHORED relationship with role property
                'GAVE_AFTERWORD': 'AUTHORED',  # Use AUTHORED relationship with role property
                'COMPILED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'CONTRIBUTED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'CO_AUTHORED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'GHOST_WROTE': 'AUTHORED'  # Use AUTHORED relationship with role property
            }
            
            rel_type = rel_type_map.get(contribution_str, 'AUTHORED')
            
            # Create the relationship with properties
            role = contribution_str.lower()
            rel_props = {
                'role': role,
                'order_index': getattr(contribution, 'order', order_index)
            }
            
            logger.info(f"🔍 [DEBUG] Creating relationship with role: {role}, contribution_str: {contribution_str}")
            
            success = self.db.create_relationship(
                'Person', person_id, rel_type, 'Book', book_id, rel_props
            )
            
            if success:
                logger.info(f"✅ Created {rel_type} relationship: {getattr(person, 'name', 'unknown')} -> {book_id}")
            else:
                logger.error(f"❌ Failed to create {rel_type} relationship: {getattr(person, 'name', 'unknown')} -> {book_id}")
                
        except Exception as e:
            logger.error(f"❌ Failed to create contributor relationship: {e}")
    
    async def _ensure_person_exists(self, person: Any, *, auto_fetch: bool = True) -> Optional[str]:
        """Ensure a person exists in the database, create if necessary."""
        try:
            person_name = getattr(person, 'name', '')
            if not person_name:
                return None
            
            # Try to find existing person by name
            normalized_name = person_name.strip().lower()
            query = """
            MATCH (p:Person) 
            WHERE p.normalized_name = $normalized_name OR p.name = $name
            RETURN p.id
            LIMIT 1
            """
            
            results = self.db.query(query, {
                "normalized_name": normalized_name,
                "name": person_name
            })
            
            if results and (results[0].get('result') or results[0].get('col_0')):
                person_id = results[0].get('result') or results[0]['col_0']
                logger.debug(f"Found existing person: {person_name} (ID: {person_id})")
                return person_id
            
            # Create new person
            person_id = getattr(person, 'id', None) or str(uuid.uuid4())
            
            # Auto-fetch OpenLibrary metadata if available
            birth_year = getattr(person, 'birth_year', None)
            death_year = getattr(person, 'death_year', None)
            bio = getattr(person, 'bio', '')
            openlibrary_id = getattr(person, 'openlibrary_id', None)
            image_url = getattr(person, 'image_url', None)
            birth_place = getattr(person, 'birth_place', None)
            website = getattr(person, 'website', None)
            
            # Auto-fetch OpenLibrary metadata if not already provided
            logger.info(f"🔍 [DEBUG] Checking auto-fetch conditions for {person_name}: openlibrary_id={openlibrary_id}, bio='{bio}', birth_year={birth_year}, image_url={image_url}")
            if auto_fetch and not openlibrary_id and not bio and not birth_year and not image_url:
                try:
                    from ..utils import search_author_by_name, fetch_author_data
                    logger.info(f"🔍 Auto-fetching OpenLibrary metadata for: {person_name}")
                    search_result = search_author_by_name(person_name)
                    
                    if search_result and search_result.get('openlibrary_id'):
                        # Get detailed author data using the OpenLibrary ID
                        author_id = search_result['openlibrary_id']
                        detailed_author_data = fetch_author_data(author_id)
                        
                        if detailed_author_data:
                            # Use the same comprehensive parser as the person metadata refresh
                            from ..routes.people_routes import parse_comprehensive_openlibrary_data
                            
                            # Parse comprehensive data
                            updates = parse_comprehensive_openlibrary_data(detailed_author_data)
                            
                            # Apply all the comprehensive updates if not already provided
                            if not openlibrary_id and updates.get('openlibrary_id'):
                                openlibrary_id = updates['openlibrary_id']
                            if not bio and updates.get('bio'):
                                bio = updates['bio']
                            if not birth_year and updates.get('birth_year'):
                                birth_year = updates['birth_year']
                            if not death_year and updates.get('death_year'):
                                death_year = updates['death_year']
                            if not image_url and updates.get('image_url'):
                                image_url = updates['image_url']
                            if not birth_place and updates.get('birth_place'):
                                birth_place = updates['birth_place']
                            if not website and updates.get('website'):
                                website = updates['website']
                            
                            logger.info(f"✅ Auto-fetched comprehensive metadata for {person_name}: OpenLibrary ID {openlibrary_id}")
                            logger.info(f"📚 Applied fields: {list(updates.keys())}")
                        else:
                            logger.warning(f"⚠️ Could not fetch detailed data for OpenLibrary ID: {author_id}")
                    else:
                        logger.info(f"📝 No OpenLibrary data found for: {person_name}")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Failed to auto-fetch metadata for {person_name}: {e}")
                    # Continue with person creation even if metadata fetch fails
            
            # Only include properties that exist in the Person schema
            person_data = {
                'id': person_id,
                'name': person_name,
                'normalized_name': normalized_name,
                'birth_year': birth_year,
                'death_year': death_year,
                'birth_place': birth_place,
                'bio': bio,
                'website': website,
                'openlibrary_id': openlibrary_id,
                'image_url': image_url,
                'created_at_str': getattr(person, 'created_at', datetime.now(timezone.utc)).isoformat() if hasattr(getattr(person, 'created_at', datetime.now(timezone.utc)), 'isoformat') else datetime.now(timezone.utc).isoformat(),
                'updated_at_str': datetime.now(timezone.utc).isoformat()
            }
            # Filter out fields that don't exist in the current schema
            # birth_place and website are not in the current schema, but openlibrary_id and image_url are
            filtered_person_data = {
                'id': person_data['id'],
                'name': person_data['name'],
                'normalized_name': person_data['normalized_name'],
                'birth_year': person_data['birth_year'],
                'death_year': person_data['death_year'],
                'bio': person_data['bio'],
                'openlibrary_id': person_data['openlibrary_id'],
                'image_url': person_data['image_url'],
                'created_at_str': person_data['created_at_str']
            }
            
            success = self.db.create_node('Person', filtered_person_data)
            if success:
                logger.info(f"✅ Created new person: {person_name} (ID: {person_id})")
                return person_id
            else:
                logger.error(f"❌ Failed to create person: {person_name}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to ensure person exists: {e}")
            return None
    
    async def _create_category_relationships_from_raw(self, book_id: str, raw_categories: Any):
        """Create category relationships from raw category data (strings or list)."""
        try:
            # Handle different raw_categories formats
            if isinstance(raw_categories, str):
                # Split comma-separated string
                category_names = [cat.strip() for cat in raw_categories.split(',') if cat.strip()]
            elif isinstance(raw_categories, list):
                category_names = [str(cat).strip() for cat in raw_categories if str(cat).strip()]
            else:
                logger.warning(f"⚠️ Unknown raw_categories format: {type(raw_categories)}")
                return
            
            logger.info(f"🔗 Processing {len(category_names)} categories from raw data: {category_names}")
            
            for category_name in category_names:
                # Detect hierarchical category paths like "Fiction / Science Fiction / Space Opera"
                if ('/' in category_name) or ('>' in category_name):
                    try:
                        # Split on common separators and normalize whitespace
                        import re
                        parts = [p.strip() for p in re.split(r"[>/]", category_name) if p.strip()]
                        if not parts:
                            continue
                        leaf_category_id = await self._ensure_category_path_exists(parts)
                        if leaf_category_id:
                            # Link the book to the most specific (leaf) category
                            success = self.db.create_relationship(
                                'Book', book_id, 'CATEGORIZED_AS', 'Category', leaf_category_id, {}
                            )
                            if success:
                                logger.info(f"✅ Linked book {book_id} to leaf category path '{category_name}' (leaf id: {leaf_category_id})")
                            else:
                                logger.error(f"❌ Failed to link book {book_id} to leaf category id {leaf_category_id}")
                        else:
                            logger.warning(f"⚠️ Could not resolve category path for: {category_name}")
                    except Exception as e:
                        logger.error(f"❌ Failed processing hierarchical category '{category_name}': {e}")
                        # Fallback to flat handling
                        await self._create_category_relationship_by_name(book_id, category_name)
                else:
                    # Flat category name
                    await self._create_category_relationship_by_name(book_id, category_name)
                
        except Exception as e:
            logger.error(f"❌ Failed to create category relationships from raw data: {e}")

    async def _ensure_category_path_exists(self, parts: list[str]) -> Optional[str]:
        """Ensure a hierarchical category path exists; return the leaf category id.

        parts: e.g., ["Fiction", "Science Fiction", "Space Opera"]
        Creates (or finds) each Category with proper parent_id and PARENT_CATEGORY links.
        """
        try:
            parent_id: Optional[str] = None
            leaf_id: Optional[str] = None
            level = 0
            for name in parts:
                normalized_name = name.strip().lower()
                if not normalized_name:
                    continue
                # Find existing category matching name and parent context
                if parent_id is None:
                    query = (
                        "MATCH (c:Category) WHERE c.normalized_name = $normalized_name AND c.parent_id IS NULL "
                        "RETURN c.id LIMIT 1"
                    )
                    params = {"normalized_name": normalized_name}
                else:
                    query = (
                        "MATCH (c:Category) WHERE c.normalized_name = $normalized_name AND c.parent_id = $parent_id "
                        "RETURN c.id LIMIT 1"
                    )
                    params = {"normalized_name": normalized_name, "parent_id": parent_id}
                results = self.db.query(query, params)
                if results and (results[0].get('result') or results[0].get('col_0')):
                    category_id = results[0].get('result') or results[0]['col_0']
                else:
                    # Create new category node with parent link metadata
                    category_id = str(uuid.uuid4())
                    category_data = {
                        'id': category_id,
                        'name': name,
                        'normalized_name': normalized_name,
                        'description': '',
                        'parent_id': parent_id,
                        'level': level,
                        'color': '',
                        'icon': '',
                        'book_count': 0,
                        'user_book_count': 0,
                        'created_at_str': datetime.now(timezone.utc).isoformat(),
                        'updated_at_str': datetime.now(timezone.utc).isoformat()
                    }
                    created = self.db.create_node('Category', category_data)
                    if not created:
                        logger.error(f"❌ Failed to create category in path: {name} (level {level})")
                        return None
                    # If has parent, create explicit PARENT_CATEGORY relationship (parent -> child)
                    if parent_id:
                        self.db.create_relationship('Category', parent_id, 'PARENT_CATEGORY', 'Category', category_id, {})
                    logger.info(f"📁 Created category '{name}' (id={category_id}) under parent_id={parent_id}")
                # Advance to next level
                leaf_id = category_id
                parent_id = category_id
                level += 1
            return leaf_id
        except Exception as e:
            logger.error(f"❌ Failed ensuring category path exists for {parts}: {e}")
            return None
    
    async def _create_category_relationship_by_name(self, book_id: str, category_name: str):
        """Create a category relationship by category name (create category if needed)."""
        try:
            category_id = await self._ensure_category_exists(category_name)
            if not category_id:
                logger.warning(f"⚠️ Could not create/find category: {category_name}")
                return
            
            # Create the CATEGORIZED_AS relationship
            success = self.db.create_relationship(
                'Book', book_id, 'CATEGORIZED_AS', 'Category', category_id, {}
            )
            
            if success:
                logger.info(f"✅ Created CATEGORIZED_AS relationship: {book_id} -> {category_name}")
            else:
                logger.error(f"❌ Failed to create CATEGORIZED_AS relationship: {book_id} -> {category_name}")
                
        except Exception as e:
            logger.error(f"❌ Failed to create category relationship: {e}")
    
    async def _create_category_relationship(self, book_id: str, category: Any):
        """Create a category relationship with an existing category object."""
        try:
            category_id = getattr(category, 'id', None)
            category_name = getattr(category, 'name', '')
            
            if not category_id and category_name:
                # Try to find/create by name
                category_id = await self._ensure_category_exists(category_name)
            
            if not category_id:
                logger.warning(f"⚠️ Could not determine category ID for: {category}")
                return
            
            # Create the relationship
            success = self.db.create_relationship(
                'Book', book_id, 'CATEGORIZED_AS', 'Category', category_id, {}
            )
            
            if success:
                logger.info(f"✅ Created CATEGORIZED_AS relationship: {book_id} -> {category_name}")
            else:
                logger.error(f"❌ Failed to create CATEGORIZED_AS relationship: {book_id} -> {category_name}")
                
        except Exception as e:
            logger.error(f"❌ Failed to create category relationship: {e}")
    
    async def _ensure_category_exists(self, category_name: str) -> Optional[str]:
        """Ensure a category exists in the database, create if necessary."""
        try:
            if not category_name:
                return None
            
            # Normalize the category name
            normalized_name = category_name.strip().lower()
            
            # Try to find existing category
            query = """
            MATCH (c:Category) 
            WHERE c.normalized_name = $normalized_name OR c.name = $name
            RETURN c.id
            LIMIT 1
            """
            
            results = self.db.query(query, {
                "normalized_name": normalized_name,
                "name": category_name
            })
            
            if results and (results[0].get('result') or results[0].get('col_0')):
                category_id = results[0].get('result') or results[0]['col_0']
                logger.debug(f"Found existing category: {category_name} (ID: {category_id})")
                return category_id
            
            # Create new category
            category_id = str(uuid.uuid4())
            # Include all properties that exist in the Category schema
            category_data = {
                'id': category_id,
                'name': category_name,
                'normalized_name': normalized_name,
                'description': '',
                'level': 0,  # Default to root level
                'color': '',
                'icon': '',
                'book_count': 0,
                'user_book_count': 0,
                'created_at_str': datetime.now(timezone.utc).isoformat(),
                'updated_at_str': datetime.now(timezone.utc).isoformat()
            }
            # Note: parent_id and aliases are not included as they need special handling
            
            success = self.db.create_node('Category', category_data)
            if success:
                logger.info(f"✅ Created new category: {category_name} (ID: {category_id})")
                return category_id
            else:
                logger.error(f"❌ Failed to create category: {category_name}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to ensure category exists: {e}")
            return None
    
    async def _create_publisher_relationship(self, book_id: str, publisher: Any):
        """Create a publisher relationship."""
        try:
            publisher_id = await self._ensure_publisher_exists(publisher)
            if not publisher_id:
                logger.warning(f"⚠️ Could not create/find publisher: {publisher}")
                return
            
            # Create the PUBLISHED_BY relationship
            success = self.db.create_relationship(
                'Book', book_id, 'PUBLISHED_BY', 'Publisher', publisher_id, {}
            )
            
            if success:
                logger.info(f"✅ Created PUBLISHED_BY relationship: {book_id} -> {getattr(publisher, 'name', publisher)}")
            else:
                logger.error(f"❌ Failed to create PUBLISHED_BY relationship: {book_id} -> {getattr(publisher, 'name', publisher)}")
                
        except Exception as e:
            logger.error(f"❌ Failed to create publisher relationship: {e}")
    
    async def _ensure_publisher_exists(self, publisher: Any) -> Optional[str]:
        """Ensure a publisher exists in the database, create if necessary."""
        try:
            # Handle both string and object publishers
            if isinstance(publisher, str):
                publisher_name = publisher
                publisher_country = None
                publisher_founded = None
            else:
                publisher_name = getattr(publisher, 'name', '')
                publisher_country = getattr(publisher, 'country', None)
                publisher_founded = getattr(publisher, 'founded_year', None)
            
            if not publisher_name:
                return None
            
            # Try to find existing publisher
            query = """
            MATCH (p:Publisher {name: $name})
            RETURN p.id
            LIMIT 1
            """
            
            results = self.db.query(query, {"name": publisher_name})
            
            if results and (results[0].get('result') or results[0].get('col_0')):
                publisher_id = results[0].get('result') or results[0]['col_0']
                logger.debug(f"Found existing publisher: {publisher_name} (ID: {publisher_id})")
                return publisher_id
            
            # Create new publisher
            publisher_id = str(uuid.uuid4())
            # Only include properties that exist in the Publisher schema
            publisher_data = {
                'id': publisher_id,
                'name': publisher_name,
                'country': publisher_country or '',
                'founded_year': publisher_founded,
                'created_at_str': datetime.now(timezone.utc).isoformat()
            }
            # Note: Filtering out updated_at as it doesn't exist in DB schema
            
            success = self.db.create_node('Publisher', publisher_data)
            if success:
                logger.info(f"✅ Created new publisher: {publisher_name} (ID: {publisher_id})")
                return publisher_id
            else:
                logger.error(f"❌ Failed to create publisher: {publisher_name}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to ensure publisher exists: {e}")
            return None
    
    # ========================================
    # Missing Methods Required by kuzu_integration
    # ========================================
    
    async def get_by_id(self, book_id: str) -> Optional[Dict[str, Any]]:
        """Get a book by ID."""
        try:
            query = """
            MATCH (b:Book {id: $book_id})
            RETURN b
            LIMIT 1
            """
            
            results = self.db.query(query, {"book_id": book_id})
            logger.info(f"🔍 Query results for book {book_id}: {results}")
            
            if results and len(results) > 0:
                result = results[0]
                logger.info(f"🔍 First result structure: {result}")
                
                # Try different ways to access the book data - try 'result' first (single column)
                book_data = None
                if 'result' in result:
                    book_data = result['result']
                    logger.info(f"✅ Found book via 'result' key: {type(book_data)}")
                elif 'col_0' in result:
                    book_data = result['col_0']
                    logger.info(f"✅ Found book via 'col_0' key: {type(book_data)}")
                else:
                    # Fallback: return whatever we got
                    logger.warning(f"⚠️ Unexpected result structure for book {book_id}: {result}")
                    return result
                
                # Convert book data to dict
                if book_data is not None:
                    if hasattr(book_data, '__dict__'):
                        book_dict = dict(book_data)
                        logger.info(f"✅ Converted book object to dict with keys: {list(book_dict.keys())}")
                        return book_dict
                    elif isinstance(book_data, dict):
                        logger.info(f"✅ Book data is already dict with keys: {list(book_data.keys())}")
                        return book_data
                    else:
                        logger.warning(f"⚠️ Book data is unexpected type: {type(book_data)}")
                        return {'id': book_id, 'data': str(book_data)}
            
            logger.warning(f"⚠️ No results found for book {book_id}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get book by ID {book_id}: {e}")
            return None
    
    async def get_by_uid(self, book_uid: str) -> Optional[Dict[str, Any]]:
        """Get a book by UID (alias for get_by_id for backward compatibility)."""
        logger.info(f"🔍 get_by_uid called with: {book_uid}")
        result = await self.get_by_id(book_uid)
        logger.info(f"🔍 get_by_uid result: {result}")
        return result
    
    async def get_by_isbn(self, isbn: str) -> Optional[Dict[str, Any]]:
        """Get a book by ISBN (13 or 10)."""
        try:
            query = """
            MATCH (b:Book)
            WHERE b.isbn13 = $isbn OR b.isbn10 = $isbn
            RETURN b
            LIMIT 1
            """
            
            results = self.db.query(query, {"isbn": isbn})
            
            if results and len(results) > 0:
                result = results[0]
                
                # Try different ways to access the book data - try 'result' first (single column)
                book_data = None
                if 'result' in result:
                    book_data = result['result']
                elif 'col_0' in result:
                    book_data = result['col_0']
                else:
                    return result
                
                # Convert book data to dict
                if book_data is not None:
                    if hasattr(book_data, '__dict__'):
                        return dict(book_data)
                    elif isinstance(book_data, dict):
                        return book_data
                    else:
                        return {'isbn': isbn, 'data': str(book_data)}
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get book by ISBN {isbn}: {e}")
            return None
    
    async def search(self, query_text: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Search for books by title or author."""
        try:
            # Search by title
            query = """
            MATCH (b:Book)
            WHERE b.title CONTAINS $query_text OR b.normalized_title CONTAINS $query_text
            RETURN b
            LIMIT $limit
            """
            
            results = self.db.query(query, {
                "query_text": query_text.lower(),
                "limit": limit
            })
            
            books = []
            for result in results:
                # Handle both result formats for single column queries
                if 'result' in result:
                    books.append(dict(result['result']))
                elif 'col_0' in result:
                    books.append(dict(result['col_0']))
            
            logger.debug(f"Found {len(books)} books matching query: {query_text}")
            return books
            
        except Exception as e:
            logger.error(f"❌ Failed to search books: {e}")
            return []
    
    async def get_all(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Get all books with pagination."""
        try:
            query = f"""
            MATCH (b:Book)
            RETURN b
            SKIP {offset} LIMIT {limit}
            """
            
            results = self.db.query(query)
            
            books = []
            for result in results:
                # Handle both result formats for single column queries
                if 'result' in result:
                    books.append(dict(result['result']))
                elif 'col_0' in result:
                    books.append(dict(result['col_0']))
            
            logger.debug(f"Retrieved {len(books)} books")
            return books
            
        except Exception as e:
            logger.error(f"❌ Failed to get all books: {e}")
            return []
    
    async def get_book_authors(self, book_id: str) -> List[Dict[str, Any]]:
        """Get all contributors for a book with their roles."""
        try:
            query = """
            MATCH (p:Person)-[rel:AUTHORED]->(b:Book {id: $book_id})
            RETURN p.name as name, p.id as id, rel.role as role, rel.order_index as order_index
            ORDER BY rel.order_index ASC
            """
            
            results = self.db.query(query, {"book_id": book_id})
            
            contributors = []
            for result in results:
                if result.get('col_0'):  # name
                    contributors.append({
                        'name': result.get('col_0', ''),
                        'id': result.get('col_1', ''),
                        'role': result.get('col_2', 'authored'),  # Default to 'authored' if no role
                        'order_index': result.get('col_3', 0)
                    })
            
            logger.debug(f"Found {len(contributors)} contributors for book {book_id}")
            return contributors
            
        except Exception as e:
            logger.error(f"❌ Failed to get book authors: {e}")
            return []
    
    async def get_book_categories(self, book_id: str) -> List[Dict[str, Any]]:
        """Get all categories for a book."""
        try:
            query = """
            MATCH (b:Book {id: $book_id})-[:CATEGORIZED_AS]->(c:Category)
            RETURN c.name as name, c.id as id, c.description as description, 
                   c.color as color, c.icon as icon, c.aliases as aliases,
                   c.normalized_name as normalized_name, c.parent_id as parent_id,
                   c.level as level, c.book_count as book_count, c.user_book_count as user_book_count,
                   c.created_at as created_at, c.updated_at as updated_at
            ORDER BY c.name ASC
            """
            
            results = self.db.query(query, {"book_id": book_id})
            
            categories = []
            for result in results:
                if result.get('col_0'):  # name
                    categories.append({
                        'name': result.get('col_0', ''),
                        'id': result.get('col_1', ''),
                        'description': result.get('col_2', ''),
                        'color': result.get('col_3', ''),
                        'icon': result.get('col_4', ''),
                        'aliases': result.get('col_5', []),
                        'normalized_name': result.get('col_6', ''),
                        'parent_id': result.get('col_7', None),
                        'level': result.get('col_8', 0),
                        'book_count': result.get('col_9', 0),
                        'user_book_count': result.get('col_10', 0),
                        'created_at': result.get('col_11', None),
                        'updated_at': result.get('col_12', None)
                    })
            
            logger.debug(f"Found {len(categories)} categories for book {book_id}")
            return categories
            
        except Exception as e:
            logger.error(f"❌ Failed to get book categories: {e}")
            return []
    
    async def get_book_publisher(self, book_id: str) -> Optional[Dict[str, Any]]:
        """Get the publisher for a book."""
        try:
            query = """
            MATCH (b:Book {id: $book_id})-[:PUBLISHED_BY]->(p:Publisher)
            RETURN p.name as name, p.id as id, p.country as country, p.founded_year as founded_year
            LIMIT 1
            """
            
            results = self.db.query(query, {"book_id": book_id})
            
            if results and results[0].get('col_0'):
                return {
                    'name': results[0].get('col_0', ''),
                    'id': results[0].get('col_1', ''),
                    'country': results[0].get('col_2', ''),
                    'founded_year': results[0].get('col_3', None)
                }
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get book publisher: {e}")
            return None
    
    async def get_all_persons(self) -> List[Dict[str, Any]]:
        """Get all persons in the database with book counts."""
        try:
            query = """
            MATCH (p:Person)
            OPTIONAL MATCH (p)-[:AUTHORED]->(b:Book)
            RETURN p, COUNT(DISTINCT b) as book_count
            ORDER BY p.name ASC
            """
            
            logger.info(f"🔍 [DEBUG] Executing get_all_persons query")
            results = self.db.query(query)
            logger.info(f"🔍 [DEBUG] Query returned {len(results)} results")
            
            persons = []
            for i, result in enumerate(results):
                logger.info(f"🔍 [DEBUG] Result {i}: {result}")
                
                # Handle both result formats for two column queries
                person_data = None
                book_count = 0
                
                if 'col_0' in result and 'col_1' in result:
                    person_data = dict(result['col_0'])
                    book_count = result['col_1'] or 0
                    logger.info(f"🔍 [DEBUG] Using col_0/col_1 format: {person_data['name'] if 'name' in person_data else 'unknown'} with {book_count} books")
                elif 'result' in result:
                    # Fallback for single column format
                    person_data = dict(result['result'])
                    book_count = 0
                    logger.info(f"🔍 [DEBUG] Using result format: {person_data['name'] if 'name' in person_data else 'unknown'}")
                
                if person_data:
                    person_data['book_count'] = book_count
                    persons.append(person_data)
            
            logger.info(f"🔍 [DEBUG] Returning {len(persons)} persons")
            return persons
            
        except Exception as e:
            logger.error(f"❌ Failed to get all persons: {e}")
            return []
    
    async def get_all_categories(self) -> List[Dict[str, Any]]:
        """Get all categories in the database."""
        try:
            query = """
            MATCH (c:Category)
            RETURN c
            ORDER BY c.name ASC
            """
            
            logger.info(f"🔍 [DEBUG] Executing get_all_categories query")
            results = self.db.query(query)
            logger.info(f"🔍 [DEBUG] Categories query returned {len(results)} results")
            
            categories = []
            for i, result in enumerate(results):
                logger.info(f"🔍 [DEBUG] Category result {i}: {result}")
                
                # Handle both result formats for single column queries
                if 'result' in result:
                    category_data = dict(result['result'])
                    logger.info(f"🔍 [DEBUG] Using result format: {category_data['name'] if 'name' in category_data else 'unknown'}")
                    categories.append(category_data)
                elif 'col_0' in result:
                    category_data = dict(result['col_0'])
                    logger.info(f"🔍 [DEBUG] Using col_0 format: {category_data['name'] if 'name' in category_data else 'unknown'}")
                    categories.append(category_data)
            
            logger.info(f"🔍 [DEBUG] Returning {len(categories)} categories")
            return categories
            
        except Exception as e:
            logger.error(f"❌ Failed to get all categories: {e}")
            return []

    async def delete(self, book_id: str) -> bool:
        """Delete a book and all its relationships globally."""
        try:
            logger.info(f"🗑️ Starting global delete for book ID: {book_id}")
            
            # Use DETACH DELETE to remove the book and all its relationships
            delete_query = """
            MATCH (b:Book {id: $book_id})
            DETACH DELETE b
            """
            
            # Execute the deletion
            self.db.query(delete_query, {"book_id": book_id})
            logger.info(f"✅ Successfully deleted book and all relationships: {book_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to delete book {book_id}: {e}")
            return False

