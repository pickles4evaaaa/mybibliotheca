"""
Placeholder models file for backward compatibility during Kuzu migration.

This file provides empty stubs to prevent import errors while the application
is being migrated to Kuzu graph database architecture. All actual data operations
should use the Kuzu-based services and domain models.
"""

from flask import current_app


class _NoOpDatabase:
    """No-op database object that doesn't perform any operations."""
    
    def __init__(self):
        pass
    
    @property
    def session(self):
        return _NoOpSession()
    
    def create_all(self):
        """No-op create_all - database creation disabled during Kuzu migration."""
        print("⚠️  SQLite database creation disabled - using Kuzu only")
        pass
    
    def drop_all(self):
        """No-op drop_all - database operations disabled during Kuzu migration."""
        pass


class _NoOpSession:
    """No-op session object that doesn't perform any operations."""
    
    def add(self, obj):
        """No-op add - database operations disabled during Kuzu migration."""
        print("⚠️  SQLite session.add() disabled - use Kuzu services instead")
        pass
    
    def commit(self):
        """No-op commit - database operations disabled during Kuzu migration."""
        print("⚠️  SQLite session.commit() disabled - use Kuzu services instead")
        pass
    
    def rollback(self):
        """No-op rollback - database operations disabled during Kuzu migration."""
        print("⚠️  SQLite session.rollback() disabled - use Kuzu services instead")
        pass
    
    def delete(self, obj):
        """No-op delete - database operations disabled during Kuzu migration."""
        print("⚠️  SQLite session.delete() disabled - use Kuzu services instead")
        pass


class _NoOpModel:
    """Base class for no-op models that don't perform any operations."""
    
    @classmethod
    def query(cls):
        return _NoOpQuery()
    
    def __init__(self, **kwargs):
        # Set attributes to prevent errors, but don't save anywhere
        for key, value in kwargs.items():
            setattr(self, key, value)


class _NoOpQuery:
    """No-op query object that returns empty results."""
    
    def filter_by(self, **kwargs):
        return self
    
    def filter(self, *args):
        return self
    
    def order_by(self, *args):
        return self
    
    def first(self):
        return None
    
    def all(self):
        return []
    
    def count(self):
        return 0
    
    def paginate(self, **kwargs):
        return _NoOpPagination()


class _NoOpPagination:
    """No-op pagination object."""
    
    @property
    def items(self):
        return []
    
    @property
    def total(self):
        return 0
    
    @property
    def pages(self):
        return 0
    
    @property
    def page(self):
        return 1
    
    @property
    def per_page(self):
        return 20
    
    @property
    def has_prev(self):
        return False
    
    @property
    def has_next(self):
        return False
    
    @property
    def prev_num(self):
        return None
    
    @property
    def next_num(self):
        return None


# Create no-op database instance
db = _NoOpDatabase()


class User(_NoOpModel):
    """No-op User model placeholder."""
    pass


class Book(_NoOpModel):
    """No-op Book model placeholder."""
    pass


class ReadingLog(_NoOpModel):
    """No-op ReadingLog model placeholder."""
    pass


# Note: Legacy SQLite models are disabled - use Kuzu services and domain models instead
# The migration is complete and app.services with app.domain.models should be used
