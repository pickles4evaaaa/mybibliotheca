"""
Legacy services module - now imports from the new services package.

This module is kept for backward compatibility with existing imports.
All service implementations have been moved to the services/ package.
"""

# Import all services from the new organized structure
from .services import (
    # Main service instances
    book_service,
    user_service,
    
    # Service classes for direct instantiation if needed
    KuzuServiceFacade,
    KuzuUserService,
    KuzuBookService,  # Alias for KuzuServiceFacade
    
    # Utility functions
    run_async,
    
    # Stub services for compatibility
    reading_log_service,
    custom_field_service,
    import_mapping_service,
    direct_import_service,
    job_service
)

# Export everything for backward compatibility
__all__ = [
    # Service instances (most commonly used)
    'book_service',
    'user_service', 
    'reading_log_service',
    'custom_field_service',
    'import_mapping_service',
    'direct_import_service',
    'job_service',
    
    # Service classes
    'KuzuServiceFacade',
    'KuzuUserService',
    'KuzuBookService',
    
    # Utilities
    'run_async'
]
