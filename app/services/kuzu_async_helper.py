"""
Kuzu Async Helper Utilities

Centralized async/sync wrapper utilities for Kuzu services.
Provides a clean, standardized way to handle async operations in a sync Flask context.
"""

import asyncio
import concurrent.futures
from typing import Any, Callable, TypeVar
from functools import wraps

T = TypeVar('T')


def run_async(coro_or_func) -> Any:
    """
    Run an async coroutine synchronously or convert an async function to sync.
    
    This is a more robust version of the async/sync bridge that handles
    various edge cases and provides better error handling.
    
    Usage:
    - run_async(async_method(args)) - runs a coroutine directly
    - run_async(async_function) - returns a sync wrapper function
    
    Args:
        coro_or_func: Either a coroutine object or an async function
        
    Returns:
        The result of the coroutine execution or a sync wrapper function
        
    Raises:
        TypeError: If the argument is neither a coroutine nor callable
    """
    # If it's a coroutine, run it directly
    if hasattr(coro_or_func, '__await__'):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        if loop.is_running():
            # We're in an already running loop, need to run in a separate thread
            with concurrent.futures.ThreadPoolExecutor() as executor:
                def run_in_new_loop():
                    new_loop = asyncio.new_event_loop()
                    try:
                        return new_loop.run_until_complete(coro_or_func)
                    finally:
                        new_loop.close()
                future = executor.submit(run_in_new_loop)
                return future.result()
        else:
            return loop.run_until_complete(coro_or_func)
    
    # If it's a callable (function), return a sync wrapper
    elif callable(coro_or_func):
        @wraps(coro_or_func)
        def wrapper(*args, **kwargs):
            coro = coro_or_func(*args, **kwargs)
            return run_async(coro)
        return wrapper
    
    # Fallback - shouldn't happen
    else:
        raise TypeError(f"Expected coroutine or callable, got {type(coro_or_func)}")


class KuzuAsyncHelper:
    """
    Helper class for managing async operations in Kuzu services.
    
    Provides utilities for:
    - Converting async methods to sync
    - Error handling in async contexts
    - Standardized async patterns
    """
    
    @staticmethod
    def async_to_sync(async_func: Callable[..., Any]) -> Callable[..., Any]:
        """
        Convert an async function to a sync function.
        
        Args:
            async_func: The async function to convert
            
        Returns:
            A sync wrapper function
        """
        return run_async(async_func)
    
    @staticmethod
    def safe_run_async(coro, default_value=None, log_errors=True):
        """
        Safely run an async coroutine with error handling.
        
        Args:
            coro: The coroutine to run
            default_value: Value to return if an error occurs
            log_errors: Whether to log errors
            
        Returns:
            The result of the coroutine or the default value
        """
        try:
            return run_async(coro)
        except Exception as e:
            if log_errors:
                print(f"Error in sync coroutine: {e}")
            return default_value
    
    @staticmethod
    def create_sync_wrapper(service_class):
        """
        Create sync wrapper methods for all async methods in a service class.
        
        This is useful for maintaining backward compatibility when converting
        services to async.
        
        Args:
            service_class: The service class to wrap
            
        Returns:
            The service class with added sync wrapper methods
        """
        for attr_name in dir(service_class):
            attr = getattr(service_class, attr_name)
            if (callable(attr) and 
                hasattr(attr, '__code__') and 
                asyncio.iscoroutinefunction(attr) and
                not attr_name.startswith('_')):
                
                # Create sync wrapper
                sync_name = f"{attr_name}_sync"
                if not hasattr(service_class, sync_name):
                    sync_wrapper = KuzuAsyncHelper.async_to_sync(attr)
                    setattr(service_class, sync_name, sync_wrapper)
        
        return service_class
