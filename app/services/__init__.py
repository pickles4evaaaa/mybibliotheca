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
    from .kuzu_reading_log_service import KuzuReadingLogService

    # For backward compatibility, expose the main service
    KuzuBookService = KuzuServiceFacade
    
    # Service instances with lazy initialization
    _book_service = None
    _user_service = None
    _custom_field_service = None
    _import_mapping_service = None
    _person_service = None
    _reading_log_service = None
    
    def _get_book_service():
        """Get book service instance with lazy initialization."""
        global _book_service
        if _book_service is None:
            _book_service = KuzuServiceFacade()
        return _book_service
    
    def _get_user_service():
        """Get user service instance with lazy initialization."""
        global _user_service
        if _user_service is None:
            _user_service = KuzuUserService()
        return _user_service
    
    def _get_custom_field_service():
        """Get custom field service instance with lazy initialization."""
        global _custom_field_service
        if _custom_field_service is None:
            _custom_field_service = KuzuCustomFieldService()
        return _custom_field_service
    
    def _get_import_mapping_service():
        """Get import mapping service instance with lazy initialization."""
        global _import_mapping_service
        if _import_mapping_service is None:
            _import_mapping_service = KuzuImportMappingService()
        return _import_mapping_service
    
    def _get_person_service():
        """Get person service instance with lazy initialization."""
        global _person_service
        if _person_service is None:
            _person_service = KuzuPersonService()
        return _person_service
    
    def _get_reading_log_service():
        """Get reading log service instance with lazy initialization."""
        global _reading_log_service
        if _reading_log_service is None:
            _reading_log_service = KuzuReadingLogService()
        return _reading_log_service
    
    # Create property-like access using classes
    class _LazyService:
        """Lazy service that initializes on first access."""
        def __init__(self, service_getter):
            self._service_getter = service_getter
            self._service = None
        
        def __getattr__(self, name):
            if self._service is None:
                self._service = self._service_getter()
            return getattr(self._service, name)
        
        def __call__(self, *args, **kwargs):
            if self._service is None:
                self._service = self._service_getter()
            return self._service(*args, **kwargs)
    
    # Create lazy service instances
    book_service = _LazyService(_get_book_service)
    user_service = _LazyService(_get_user_service)
    custom_field_service = _LazyService(_get_custom_field_service)
    import_mapping_service = _LazyService(_get_import_mapping_service)
    person_service = _LazyService(_get_person_service)
    reading_log_service = _LazyService(_get_reading_log_service)

    # Placeholder services for compatibility during migration
    class StubService:
        """Stub service for missing services."""
        def __getattr__(self, name):
            def stub_method(*args, **kwargs):
                print(f"Warning: {name} method called on stub service")
                return None
            return stub_method

    # Create stub service instances
    direct_import_service = StubService()
    job_service = StubService()

    __all__ = [
        'KuzuAsyncHelper',
        'KuzuServiceFacade', 
        'KuzuBookService',     # Backward compatibility alias
        'KuzuUserService',     # User service
        'KuzuImportMappingService',  # Import mapping service
        'book_service',        # Lazy service instance
        'user_service',        # Lazy service instance
        'person_service',      # Lazy service instance
        'custom_field_service', # Lazy service instance
        'import_mapping_service', # Lazy service instance
        'run_async',
        # Stub services for compatibility
        'reading_log_service',
        'direct_import_service',
        'job_service'
    ]
except ImportError as e:
    # Fallback imports
    __all__ = []
