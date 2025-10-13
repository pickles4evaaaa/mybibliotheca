"""
User-related utility functions for Bibliotheca
"""

from flask import current_app


def calculate_reading_streak(user_id, streak_offset=0):
    """
    Calculate reading streak for a specific user with foolproof logic.
    Now integrates with the reading log system when available.
    
    Args:
        user_id (str): The ID of the user
        streak_offset (int): The current streak offset from the user model
        
    Returns:
        int: The calculated reading streak
    """
    try:
        # Try to use the reading log service to calculate actual streak
        from app.services import reading_log_service
        from datetime import date, datetime, timedelta

        reading_dates = set()

        # Prefer optimized distinct-date query when available.
        try:
            raw_dates = reading_log_service.get_user_reading_dates_sync(user_id)
        except AttributeError:
            raw_dates = []
        except Exception as date_exc:
            current_app.logger.debug(f"Falling back to detailed logs for streak calculation (user={user_id}): {date_exc}")
            raw_dates = []

        for raw_date in raw_dates:
            if isinstance(raw_date, date):
                reading_dates.add(raw_date)
            elif isinstance(raw_date, datetime):
                reading_dates.add(raw_date.date())
            elif isinstance(raw_date, str):
                try:
                    reading_dates.add(date.fromisoformat(raw_date.split('T')[0]))
                except ValueError:
                    continue

        # Fallback to detailed logs if distinct-date query was unavailable or empty.
        if not reading_dates:
            logs = reading_log_service.get_user_reading_logs_sync(user_id, days_back=3650, limit=None)

            if not logs:
                current_app.logger.debug(f"No reading logs found for user {user_id}, returning streak offset: {streak_offset}")
                return streak_offset

            for log in logs:
                if 'date' not in log or log['date'] is None:
                    continue
                log_date = log['date']
                if isinstance(log_date, date):
                    reading_dates.add(log_date)
                elif isinstance(log_date, datetime):
                    reading_dates.add(log_date.date())
                elif isinstance(log_date, str):
                    try:
                        reading_dates.add(date.fromisoformat(log_date.split('T')[0]))
                    except (ValueError, TypeError):
                        continue
        
        if not reading_dates:
            current_app.logger.debug(f"No valid reading dates found for user {user_id}, returning streak offset: {streak_offset}")
            return streak_offset
        
        # Calculate current streak
        current_date = date.today()
        streak = 0
        
        # Check if user read today or yesterday (to account for different time zones)
        if current_date in reading_dates or (current_date - timedelta(days=1)) in reading_dates:
            # Start counting from today or yesterday
            check_date = current_date if current_date in reading_dates else current_date - timedelta(days=1)
            
            # Count consecutive days backwards
            while check_date in reading_dates:
                streak += 1
                check_date -= timedelta(days=1)
        
        # Add the streak offset to account for historical data
        total_streak = streak + streak_offset
        
        current_app.logger.debug(f"Calculated reading streak for user {user_id}: {streak} days from logs + {streak_offset} offset = {total_streak}")
        return total_streak
        
    except Exception as e:
        current_app.logger.error(f"Error calculating reading streak for user {user_id}: {e}")
        # Fall back to streak offset
        return streak_offset


def get_reading_streak(timezone=None):
    """
    Legacy function for backward compatibility
    Uses current user's streak calculation
    
    Args:
        timezone: Optional timezone (not currently used)
        
    Returns:
        int: The current user's reading streak
    """
    from flask_login import current_user
    if not current_user.is_authenticated:
        return 0
    return current_user.get_reading_streak()
