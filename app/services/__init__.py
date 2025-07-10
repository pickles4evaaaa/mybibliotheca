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

    # For backward compatibility, expose the main service
    KuzuBookService = KuzuServiceFacade
    
    # Create default service instance for app.services.book_service import
    book_service = KuzuServiceFacade()

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
    custom_field_service = StubService()
    import_mapping_service = StubService()
    direct_import_service = StubService()
    job_service = StubService()

    # Create user_service - using stub for now to avoid circular imports
    # TODO: Implement proper KuzuUserService when needed
    user_service = StubService()

    __all__ = [
        'KuzuAsyncHelper',
        'KuzuServiceFacade', 
        'KuzuBookService',  # Backward compatibility alias
        'book_service',     # Default service instance
        'run_async',
        # Real services
        'user_service',
        # Stub services for compatibility
        'reading_log_service',
        'custom_field_service',
        'import_mapping_service',
        'direct_import_service',
        'job_service'
    ]
except ImportError as e:
    print(f"⚠️ Warning: Could not import all services: {e}")
    # Fallback imports
    __all__ = []
