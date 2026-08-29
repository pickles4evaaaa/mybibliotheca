"""Session-state helpers for the onboarding wizard.

The facade module keeps the original imports and routes; this module only
organizes the existing session behavior.
"""

from __future__ import annotations

import logging
from typing import Dict

from flask import session


logger = logging.getLogger("app.onboarding_system")


def get_onboarding_data() -> Dict:
    """Get all onboarding data from session."""
    # Force session to be permanent for better persistence
    session.permanent = True
    
    data = session.get('onboarding_data', {})
    backup = session.get('onboarding_backup', {})
    
    # If main data is missing but backup exists, use backup and restore
    if not data and backup:
        session['onboarding_data'] = backup
        session.modified = True
        data = backup
    
    logger.info(f"🔍 SESSION DEBUG: get_onboarding_data returning: {data}")
    return data


def update_onboarding_data(data: Dict):
    """Update onboarding data in session."""
    logger.info(f"🔍 SESSION DEBUG: update_onboarding_data called with: {data}")
    
    # Force session to be permanent for better persistence
    session.permanent = True
    
    # Ensure onboarding_data exists in session
    if 'onboarding_data' not in session:
        session['onboarding_data'] = {}
    
    current_data = session['onboarding_data']
    logger.info(f"🔍 SESSION DEBUG: current_data before update: {current_data}")
    
    # Update the data
    current_data.update(data)
    session['onboarding_data'] = current_data
    session.modified = True
    
    # Also store in a backup location to help with debugging
    session['onboarding_backup'] = current_data.copy()
    
    logger.info(f"🔍 SESSION DEBUG: session after update: {dict(session)}")
    logger.info(f"🔍 SESSION DEBUG: onboarding_data in session: {session.get('onboarding_data', 'NOT_FOUND')}")


def set_onboarding_step(step: int):
    """Set current onboarding step in session."""
    logger.info(f"🔍 SESSION DEBUG: set_onboarding_step called with step: {step}")
    
    # Force session to be permanent for better persistence
    session.permanent = True
    session['onboarding_step'] = step
    session.modified = True
    
    logger.info(f"🔍 SESSION DEBUG: onboarding_step set to: {session.get('onboarding_step', 'NOT_FOUND')}")


def get_onboarding_step() -> int:
    """Get current onboarding step from session."""
    # Force session to be permanent for better persistence
    session.permanent = True
    return session.get('onboarding_step', 1)


def clear_onboarding_session():
    """Clear all onboarding data from session.
    
    Note: After onboarding completion, JavaScript timers in import progress templates
    may continue to make requests to onboarding routes. The authentication checks
    added to routes help prevent confusion and redirect users appropriately.
    """
    session.pop('onboarding_step', None)
    session.pop('onboarding_data', None)
    session.pop('onboarding_backup', None)
    session.pop('onboarding_import_task_id', None)
    session.modified = True
    logger.info(f"🔍 SESSION DEBUG: Onboarding session cleared")

