"""
Template context processors for making common objects available in templates.
"""

import os
from datetime import datetime, date
from flask import current_app, url_for
from flask_login import current_user
from app.debug_system import get_debug_manager


def _genre_url_for(terminology_preference, endpoint, **values):
    """Generate URLs for genre routes based on terminology preference."""
    # Map the generic endpoint to the specific blueprint endpoint
    if not endpoint.startswith('genres.'):
        # If it's a relative endpoint, assume it's for genres
        endpoint = f'genres.{endpoint}'
    
    # Generate the URL using the appropriate blueprint name based on preference
    if terminology_preference == 'category':
        # Use the categories blueprint registration for category preference
        endpoint = endpoint.replace('genres.', 'categories.')
    
    # Generate the URL - Flask will use the correct blueprint registration
    base_url = url_for(endpoint, **values)
    
    return base_url


def inject_debug_manager():
    """Make debug manager available in all templates."""
    try:
        debug_manager = get_debug_manager()
        
        # Test that the debug manager is working properly
        if debug_manager and hasattr(debug_manager, 'should_show_debug'):
            return {
                'debug_manager': debug_manager,
                'get_debug_manager': get_debug_manager  # Keep the old one for backward compatibility
            }
        else:
            raise Exception("Debug manager is not properly initialized")
            
    except Exception as e:
        # If debug manager fails, provide safe fallbacks
        
        # Create a dummy debug manager that always returns False
        class DummyDebugManager:
            def should_show_debug(self, user=None):
                return False
            def is_debug_enabled(self):
                return False
            def is_user_admin(self, user=None):
                return False
        
        dummy_manager = DummyDebugManager()
        return {
            'debug_manager': dummy_manager,
            'get_debug_manager': lambda: dummy_manager
        }


def inject_site_config():
    """Make site configuration available in all templates."""
    # Import here to avoid circular imports
    from app.admin import load_system_config
    
    try:
        # Load from config file first, fall back to environment variables
        system_config = load_system_config()
        site_name = system_config.get('site_name', os.getenv('SITE_NAME', 'MyBibliotheca'))
        server_timezone = system_config.get('server_timezone', os.getenv('TIMEZONE', 'UTC'))
        terminology_preference = system_config.get('terminology_preference', 'genre')
        background_config = system_config.get('background_config', {
            'type': 'default',
            'solid_color': '#667eea',
            'gradient_start': '#667eea',
            'gradient_end': '#764ba2',
            'gradient_direction': '135deg',
            'image_url': '',
            'image_position': 'cover'
        })
        reading_log_defaults = system_config.get('reading_log_defaults', {
            'default_pages_per_log': None,
            'default_minutes_per_log': None
        })
    except Exception:
        # Fallback to environment variables if config loading fails
        site_name = os.getenv('SITE_NAME', 'MyBibliotheca')
        server_timezone = os.getenv('TIMEZONE', 'UTC')
        terminology_preference = 'genre'
        background_config = {
            'type': 'default',
            'solid_color': '#667eea',
            'gradient_start': '#667eea',
            'gradient_end': '#764ba2',
            'gradient_direction': '135deg',
            'image_url': '',
            'image_position': 'cover'
        }
        reading_log_defaults = {
            'default_pages_per_log': None,
            'default_minutes_per_log': None
        }
    
    return {
        'site_name': site_name,
        'server_timezone': server_timezone,
        'terminology_preference': terminology_preference,
    'background_config': background_config,
    'reading_log_defaults': reading_log_defaults,
        # Helper functions for terminology
        'get_terminology': lambda: terminology_preference,
        'get_genre_term': lambda: 'Genre' if terminology_preference == 'genre' else 'Category',
        'get_genre_term_lower': lambda: 'genre' if terminology_preference == 'genre' else 'category',
        'get_genre_term_plural': lambda: 'Genres' if terminology_preference == 'genre' else 'Categories',
        'get_genre_term_plural_lower': lambda: 'genres' if terminology_preference == 'genre' else 'categories',
        # URL helper for genre routes
        'get_genre_url_prefix': lambda: 'genres' if terminology_preference == 'genre' else 'categories',
        # Dynamic URL generator for genre routes
        'genre_url_for': lambda endpoint, **values: _genre_url_for(terminology_preference, endpoint, **values)
    }


def inject_reading_streak():
    """Make current user's reading streak available in all templates."""
    try:
        if not current_user or not current_user.is_authenticated:
            return {'current_reading_streak': 0}
    except AttributeError:
        # current_user is None or not properly initialized
        return {'current_reading_streak': 0}
    
    try:
        # Import here to avoid circular imports
        from app.services import reading_log_service
        from app.routes.stats_routes import _calculate_current_streak, _generate_calendar_with_logs
        
        # Get recent reading logs for streak calculation
        current_month = datetime.now().month
        current_year = datetime.now().year
        
        # Get reading logs for current month and previous month to ensure we catch streaks
        result = reading_log_service.get_user_reading_logs_paginated_sync(str(current_user.id), page=1, per_page=100)
        all_logs = result.get('logs', []) if result else []
        
        if not all_logs:
            return {'current_reading_streak': 0}
        
        # Process logs to calculate streak
        processed_logs = []
        for log in all_logs:
            try:
                log_date_str = log.get('date')
                if not log_date_str:
                    continue
                    
                if isinstance(log_date_str, str):
                    log_date = datetime.strptime(log_date_str, '%Y-%m-%d').date()
                elif hasattr(log_date_str, 'date'):
                    log_date = log_date_str.date()
                else:
                    log_date = log_date_str
                
                # Only include recent logs (last 60 days for streak calculation)
                days_ago = (datetime.now().date() - log_date).days
                if days_ago <= 60:
                    processed_logs.append({
                        'day': log_date.day,
                        'log_date': log_date,
                        'activity_count': 1
                    })
            except:
                continue
        
        # Generate calendar data to use existing streak calculation logic
        calendar_data = _generate_calendar_with_logs(current_year, current_month, processed_logs)
        
        # Calculate current streak
        streak = _calculate_current_streak(calendar_data['days'])
        
        # Calculate their personal best streak from ALL reading history
        try:
            # Get all reading logs for this user (not just current month)
            all_logs_result = reading_log_service.get_user_reading_logs_paginated_sync(
                str(current_user.id), page=1, per_page=1000  # Get lots of logs
            )
            all_logs = all_logs_result.get('logs', []) if all_logs_result else []
            
            # Process all logs to find the longest historical streak
            all_processed_logs = []
            for log in all_logs:
                try:
                    log_date_str = log.get('date')
                    if not log_date_str:
                        continue
                        
                    if isinstance(log_date_str, str):
                        log_date = datetime.strptime(log_date_str, '%Y-%m-%d').date()
                    elif hasattr(log_date_str, 'date'):
                        log_date = log_date_str.date()
                    else:
                        log_date = log_date_str
                    
                    all_processed_logs.append({
                        'day': log_date.day,
                        'log_date': log_date,
                        'activity_count': 1
                    })
                except:
                    continue
            
            # Sort by date
            all_processed_logs.sort(key=lambda x: x['log_date'])
            
            # Calculate historical max streak
            personal_best_streak = 0
            current_historical_streak = 0
            last_date = None
            
            for log in all_processed_logs:
                if last_date is None:
                    current_historical_streak = 1
                else:
                    days_diff = (log['log_date'] - last_date).days
                    if days_diff == 1:
                        current_historical_streak += 1
                    else:
                        current_historical_streak = 1
                
                personal_best_streak = max(personal_best_streak, current_historical_streak)
                last_date = log['log_date']
            
        except Exception as e:
            # Fallback if historical calculation fails
            personal_best_streak = 0
        
        # Determine if they're exceeding their record
        is_exceeding_record = streak > personal_best_streak or personal_best_streak == 0
        
        # Simple color progression based on streak length relative to their personal best
        if streak == 0:
            streak_color_level = 0  # Green (getting started)
        elif personal_best_streak == 0 or streak > personal_best_streak:
            streak_color_level = 4  # Red (exceeding record or first time)
        elif streak >= (personal_best_streak * 0.75):
            streak_color_level = 3  # Orange-red (close to record)
        elif streak >= (personal_best_streak * 0.5):
            streak_color_level = 2  # Orange (halfway to record)
        elif streak >= (personal_best_streak * 0.25):
            streak_color_level = 1  # Yellow (building up)
        else:
            streak_color_level = 0  # Green (just starting)
        
        return {
            'current_reading_streak': streak,
            'streak_performance_level': streak_color_level,
            'is_exceeding_record': is_exceeding_record,
            'personal_best_streak': personal_best_streak
        }
        
    except Exception as e:
        # If anything fails, return 0 streak
        return {'current_reading_streak': 0}


def inject_datetime():
    """Make datetime and date available in templates."""
    return {
        'datetime': datetime,
        'date': date
    }


def register_context_processors(app):
    """Register all context processors with the Flask app."""
    app.context_processor(inject_debug_manager)
    app.context_processor(inject_site_config)
    app.context_processor(inject_reading_streak)
    app.context_processor(inject_datetime)
    # Helper for templates to resolve effective reading defaults quickly
    def _get_defaults(user_id=None):
        try:
            from app.utils.user_settings import get_effective_reading_defaults
            return get_effective_reading_defaults(user_id)
        except Exception:
            return (None, None)
    app.jinja_env.globals.update(get_effective_reading_defaults=_get_defaults)
    # Expose effective rows-per-page resolver
    def _get_rows(user_id=None):
        try:
            from app.utils.user_settings import get_effective_rows_per_page
            return get_effective_rows_per_page(user_id)
        except Exception:
            return None
    app.jinja_env.globals.update(get_effective_rows_per_page=_get_rows)
