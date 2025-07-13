"""
Kuzu Services Package

This package contains decomposed service classes for better maintainability:
- KuzuAsyncHelper: Async/sync utilities
- KuzuBookService: Core book operations
- KuzuCategoryService: Category management
- KuzuPersonService: Author/person management
- KuzuRelationshipService: User-book relationships
- KuzuSearchService: Search functionality
- KuzuServiceFacade: Backward compatibility facade
"""

try:
    from .kuzu_async_helper import KuzuAsyncHelper, run_async
    from .kuzu_service_facade import KuzuServiceFacade
    from .kuzu_user_service import KuzuUserService
    from .kuzu_custom_field_service import KuzuCustomFieldService
    from .kuzu_import_mapping_service import KuzuImportMappingService
    from .kuzu_person_service import KuzuPersonService

    # For backward compatibility, expose the main service
    KuzuBookService = KuzuServiceFacade
    
    # Create default service instances
    book_service = KuzuServiceFacade()
    user_service = KuzuUserService()
    custom_field_service = KuzuCustomFieldService()
    import_mapping_service = KuzuImportMappingService()
    person_service = KuzuPersonService()

    # Placeholder services for compatibility during migration
    class StubService:
        """Stub service for missing services."""
        def __getattr__(self, name):
            def stub_method(*args, **kwargs):
                print(f"Warning: {name} method called on stub service")
                return None
            return stub_method

    # Create stub service instances
    reading_log_service = StubService()
    direct_import_service = StubService()
    job_service = StubService()

    __all__ = [
        'KuzuAsyncHelper',
        'KuzuServiceFacade', 
        'KuzuBookService',     # Backward compatibility alias
        'KuzuUserService',     # User service
        'KuzuImportMappingService',  # Import mapping service
        'book_service',        # Default service instance
        'user_service',        # User service instance
        'person_service',      # Person service instance
        'run_async',
        # Stub services for compatibility
        'reading_log_service',
        'custom_field_service',
        'import_mapping_service',
        'direct_import_service',
        'job_service'
    ]
except ImportError as e:
    # Fallback imports
    __all__ = []
