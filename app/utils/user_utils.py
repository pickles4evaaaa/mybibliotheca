"""
User-related utility functions for Bibliotheca
"""

from flask import current_app


def calculate_reading_streak(user_id, streak_offset=0):
    """
    Calculate reading streak for a specific user with foolproof logic.
    Currently returns the streak_offset until the reading log system is fully implemented.
    
    Args:
        user_id (str): The ID of the user
        streak_offset (int): The current streak offset from the user model
        
    Returns:
        int: The calculated reading streak
    """
    try:
        # TODO: Implement proper reading log system
        # For now, return the streak_offset as a fallback
        current_app.logger.debug(f"Reading log system not fully implemented, returning streak offset: {streak_offset}")
        return streak_offset
            
    except Exception as e:
        current_app.logger.error(f"Error calculating reading streak for user {user_id}: {e}")
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
