"""
Kuzu User Service

Handles user management operations using clean Kuzu architecture.
"""

import uuid
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone as dt_timezone
import logging

from flask import current_app

from ..domain.models import User, Location
from ..kuzu_integration import get_kuzu_service
from ..infrastructure.kuzu_repositories import KuzuUserRepository, KuzuLocationRepository
from .kuzu_async_helper import run_async

logger = logging.getLogger(__name__)


class KuzuUserService:
    """User service using clean Kuzu architecture."""
    
    def __init__(self):
        self.kuzu_service = get_kuzu_service()
        self.user_repo = KuzuUserRepository()
        self.location_repo = KuzuLocationRepository()
    
    def get_user_by_id_sync(self, user_id: str) -> Optional[User]:
        """Get user by ID (sync version for Flask-Login)."""
        try:
            user_data = run_async(self.kuzu_service.get_user(user_id))
            if user_data:
                # Cast to Dict[str, Any] for type safety
                user_data = dict(user_data)
                return User(
                    id=user_data['id'],
                    username=user_data['username'],
                    email=user_data['email'],
                    password_hash=user_data.get('password_hash', ''),
                    display_name=user_data.get('display_name'),
                    bio=user_data.get('bio'),
                    timezone=user_data.get('timezone', 'UTC'),
                    is_admin=user_data.get('is_admin', False),
                    is_active=user_data.get('is_active', True),
                    password_must_change=user_data.get('password_must_change', False),
                    failed_login_attempts=user_data.get('failed_login_attempts', 0),
                    share_current_reading=user_data.get('share_current_reading', True),
                    share_reading_activity=user_data.get('share_reading_activity', True),
                    share_library=user_data.get('share_library', False),
                    reading_streak_offset=user_data.get('reading_streak_offset', 0),
                    locked_until=user_data.get('locked_until'),
                    last_login=user_data.get('last_login'),
                    password_changed_at=user_data.get('password_changed_at'),
                    created_at=user_data.get('created_at') or datetime.now(dt_timezone.utc),
                    updated_at=user_data.get('updated_at') or datetime.now(dt_timezone.utc)
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error getting user {user_id}: {e}")
            return None
    
    async def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get user by ID (async version)."""
        try:
            user_data = await self.kuzu_service.get_user(user_id)
            if user_data:
                return User(
                    id=user_data['id'],
                    username=user_data['username'],
                    email=user_data['email'],
                    password_hash=user_data.get('password_hash', ''),
                    display_name=user_data.get('display_name'),
                    bio=user_data.get('bio'),
                    timezone=user_data.get('timezone', 'UTC'),
                    is_admin=user_data.get('is_admin', False),
                    is_active=user_data.get('is_active', True),
                    password_must_change=user_data.get('password_must_change', False),
                    failed_login_attempts=user_data.get('failed_login_attempts', 0),
                    share_current_reading=user_data.get('share_current_reading', True),
                    share_reading_activity=user_data.get('share_reading_activity', True),
                    share_library=user_data.get('share_library', False),
                    reading_streak_offset=user_data.get('reading_streak_offset', 0),
                    locked_until=user_data.get('locked_until'),
                    last_login=user_data.get('last_login'),
                    password_changed_at=user_data.get('password_changed_at'),
                    created_at=user_data.get('created_at') or datetime.now(dt_timezone.utc),
                    updated_at=user_data.get('updated_at') or datetime.now(dt_timezone.utc)
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error getting user {user_id}: {e}")
            return None
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username."""
        try:
            user_data = await self.kuzu_service.get_user_by_username(username)
            if user_data:
                user = User(
                    id=user_data['id'],
                    username=user_data['username'],
                    email=user_data['email'],
                    password_hash=user_data.get('password_hash', ''),
                    display_name=user_data.get('display_name'),
                    bio=user_data.get('bio'),
                    timezone=user_data.get('timezone', 'UTC'),
                    is_admin=user_data.get('is_admin', False),
                    is_active=user_data.get('is_active', True),
                    password_must_change=user_data.get('password_must_change', False),
                    failed_login_attempts=user_data.get('failed_login_attempts', 0),
                    share_current_reading=user_data.get('share_current_reading', True),
                    share_reading_activity=user_data.get('share_reading_activity', True),
                    share_library=user_data.get('share_library', False),
                    reading_streak_offset=user_data.get('reading_streak_offset', 0),
                    locked_until=user_data.get('locked_until'),
                    last_login=user_data.get('last_login'),
                    password_changed_at=user_data.get('password_changed_at'),
                    created_at=user_data.get('created_at') or datetime.now(dt_timezone.utc),
                    updated_at=user_data.get('updated_at') or datetime.now(dt_timezone.utc)
                )
                return user
            return None
        except Exception as e:
            current_app.logger.error(f"Error getting user by username {username}: {e}")
            return None
    
    def get_user_by_username_sync(self, username: str) -> Optional[User]:
        """Get user by username (sync version for form validation)."""
        return run_async(self.get_user_by_username(username))
    
    async def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email."""
        try:
            user_data = await self.kuzu_service.get_user_by_email(email)
            if user_data:
                return User(
                    id=user_data['id'],
                    username=user_data['username'],
                    email=user_data['email'],
                    password_hash=user_data.get('password_hash', ''),
                    display_name=user_data.get('display_name'),
                    bio=user_data.get('bio'),
                    timezone=user_data.get('timezone', 'UTC'),
                    is_admin=user_data.get('is_admin', False),
                    is_active=user_data.get('is_active', True),
                    created_at=user_data.get('created_at') or datetime.now(dt_timezone.utc)
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error getting user by email {email}: {e}")
            return None
    
    def get_user_by_email_sync(self, email: str) -> Optional[User]:
        """Get user by email (sync version for form validation)."""
        return run_async(self.get_user_by_email(email))
    
    async def get_user_by_username_or_email(self, username_or_email: str) -> Optional[User]:
        """Get user by username or email."""
        try:
            user_data = await self.kuzu_service.get_user_by_username_or_email(username_or_email)
            if user_data:
                return User(
                    id=user_data['id'],
                    username=user_data['username'],
                    email=user_data['email'],
                    password_hash=user_data.get('password_hash', ''),
                    display_name=user_data.get('display_name'),
                    bio=user_data.get('bio'),
                    timezone=user_data.get('timezone', 'UTC'),
                    is_admin=user_data.get('is_admin', False),
                    is_active=user_data.get('is_active', True),
                    created_at=user_data.get('created_at') or datetime.now(dt_timezone.utc)
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error getting user by username or email {username_or_email}: {e}")
            return None
    
    def get_user_by_username_or_email_sync(self, username_or_email: str) -> Optional[User]:
        """Get user by username or email (sync version for form validation)."""
        return run_async(self.get_user_by_username_or_email(username_or_email))
    
    async def create_user(self, username: str, email: str, password_hash: str, 
                         display_name: Optional[str] = None, is_admin: bool = False) -> Optional[User]:
        """Create a new user."""
        try:
            user_data = {
                'username': username,
                'email': email,
                'password_hash': password_hash,
                'display_name': display_name,
                'is_admin': is_admin,
                'is_active': True
            }
            
            created_user_data = await self.kuzu_service.create_user(user_data)
            if created_user_data:
                return User(
                    id=created_user_data['id'],
                    username=created_user_data['username'],
                    email=created_user_data['email'],
                    display_name=created_user_data.get('display_name'),
                    bio=created_user_data.get('bio'),
                    timezone=created_user_data.get('timezone', 'UTC'),
                    is_admin=created_user_data.get('is_admin', False),
                    is_active=created_user_data.get('is_active', True),
                    created_at=created_user_data.get('created_at') or datetime.now(dt_timezone.utc)
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error creating user {username}: {e}")
            return None

    def create_user_sync(self, username: str, email: str, password_hash: str, 
                        display_name: Optional[str] = None, is_admin: bool = False, 
                        is_active: bool = True, password_must_change: bool = False,
                        timezone: str = 'UTC', location: str = '') -> Optional[User]:
        """Create a new user (sync version for form validation and onboarding)."""
        try:
            print(f"🚀 [USER_SERVICE] ============ CREATE_USER_SYNC CALLED ============")
            print(f"🚀 [USER_SERVICE] Username: '{username}'")
            print(f"🚀 [USER_SERVICE] Email: '{email}'")
            print(f"🚀 [USER_SERVICE] Has password_hash: {bool(password_hash)}")
            print(f"🚀 [USER_SERVICE] Display name: '{display_name}'")
            print(f"🚀 [USER_SERVICE] Is admin: {is_admin}")
            print(f"🚀 [USER_SERVICE] Is active: {is_active}")
            print(f"🚀 [USER_SERVICE] Timezone: '{timezone}'")
            print(f"🚀 [USER_SERVICE] Location: '{location}'")
            logger.info(f"Creating user {username} with email {email}")
            
            user_data = {
                'username': username,
                'email': email,
                'password_hash': password_hash,
                'display_name': display_name,
                'is_admin': is_admin,
                'is_active': is_active,
                'password_must_change': password_must_change,
                'timezone': timezone,
                'bio': location  # Store location in bio field for now
            }
            print(f"🚀 [USER_SERVICE] User data prepared: {user_data}")
            
            # Use run_async to call the async method
            print(f"🚀 [USER_SERVICE] Calling kuzu_service.create_user via run_async...")
            created_user_data = run_async(self.kuzu_service.create_user(user_data))
            print(f"🚀 [USER_SERVICE] kuzu_service.create_user returned: {created_user_data}")
            
            if created_user_data:
                user = User(
                    id=created_user_data['id'],
                    username=created_user_data['username'],
                    email=created_user_data['email'],
                    display_name=created_user_data.get('display_name'),
                    bio=created_user_data.get('bio'),
                    timezone=created_user_data.get('timezone', 'UTC'),
                    is_admin=created_user_data.get('is_admin', False),
                    is_active=created_user_data.get('is_active', True),
                    password_must_change=created_user_data.get('password_must_change', False),
                    created_at=created_user_data.get('created_at') or datetime.now(dt_timezone.utc)
                )
                return user
            else:
                return None
        except Exception as e:
            logger.error(f"Error creating user {username}: {e}")
            import traceback
            traceback.print_exc()
            return None

    def get_user_count_sync(self) -> int:
        """Get total user count (sync version for compatibility)."""
        try:
            count = run_async(self.kuzu_service.get_user_count())
            return int(count) if count is not None else 0
        except Exception as e:
            current_app.logger.error(f"Error getting user count: {e}")
            return 0

    async def get_user_count(self) -> int:
        """Get total user count (async version)."""
        try:
            return await self.kuzu_service.get_user_count()
        except Exception as e:
            current_app.logger.error(f"Error getting user count: {e}")
            return 0

    async def get_all_users(self, limit: int = 1000) -> List[User]:
        """Get all users (async version)."""
        try:
            users_data = await self.kuzu_service.get_all_users(limit)
            users = []
            for user_data in users_data:
                user = User(
                    id=user_data['id'],
                    username=user_data['username'],
                    email=user_data['email'],
                    password_hash=user_data.get('password_hash', ''),
                    display_name=user_data.get('display_name'),
                    bio=user_data.get('bio'),
                    timezone=user_data.get('timezone', 'UTC'),
                    is_admin=user_data.get('is_admin', False),
                    is_active=user_data.get('is_active', True),
                    created_at=user_data.get('created_at') or datetime.now(dt_timezone.utc)
                )
                users.append(user)
            return users
        except Exception as e:
            current_app.logger.error(f"Error getting all users: {e}")
            return []

    def get_all_users_sync(self, limit: int = 1000) -> List[User]:
        """Get all users (sync version for form validation)."""
        return run_async(self.get_all_users(limit))

    async def update_user(self, user: User) -> Optional[User]:
        """Update an existing user (async version)."""
        try:
            user_data = {
                'id': user.id,
                'username': user.username,
                'email': user.email,
                'password_hash': user.password_hash,
                'display_name': user.display_name,
                'bio': user.bio,
                'timezone': user.timezone,
                'is_admin': user.is_admin,
                'is_active': user.is_active,
                'password_must_change': user.password_must_change,
                'failed_login_attempts': user.failed_login_attempts,
                'share_current_reading': user.share_current_reading,
                'share_reading_activity': user.share_reading_activity,
                'share_library': user.share_library,
                'reading_streak_offset': user.reading_streak_offset,
                'locked_until': user.locked_until,
                'last_login': user.last_login,
                'password_changed_at': user.password_changed_at,
                'updated_at': datetime.now(dt_timezone.utc)
            }
            
            updated_user_data = await self.kuzu_service.update_user(user.id or "", user_data)
            if updated_user_data:
                return User(
                    id=updated_user_data['id'],
                    username=updated_user_data['username'],
                    email=updated_user_data['email'],
                    password_hash=updated_user_data.get('password_hash', ''),
                    display_name=updated_user_data.get('display_name'),
                    bio=updated_user_data.get('bio'),
                    timezone=updated_user_data.get('timezone', 'UTC'),
                    is_admin=updated_user_data.get('is_admin', False),
                    is_active=updated_user_data.get('is_active', True),
                    password_must_change=updated_user_data.get('password_must_change', False),
                    failed_login_attempts=updated_user_data.get('failed_login_attempts', 0),
                    share_current_reading=updated_user_data.get('share_current_reading', True),
                    share_reading_activity=updated_user_data.get('share_reading_activity', True),
                    share_library=updated_user_data.get('share_library', False),
                    reading_streak_offset=updated_user_data.get('reading_streak_offset', 0),
                    locked_until=updated_user_data.get('locked_until'),
                    last_login=updated_user_data.get('last_login'),
                    password_changed_at=updated_user_data.get('password_changed_at'),
                    created_at=updated_user_data.get('created_at') or datetime.now(dt_timezone.utc),
                    updated_at=updated_user_data.get('updated_at') or datetime.now(dt_timezone.utc)
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error updating user {user.id}: {e}")
            return None

    def update_user_sync(self, user: User) -> Optional[User]:
        """Update an existing user (sync version for admin tools)."""
        return run_async(self.update_user(user))

    async def delete_user(self, user_id: str) -> bool:
        """Delete user (async)."""
        try:
            return await self.user_repo.delete(user_id)
        except Exception as e:
            current_app.logger.error(f"Error deleting user {user_id}: {e}")
            return False

    def delete_user_sync(self, user_id: str) -> bool:
        """Delete user (sync wrapper)."""
        return run_async(self.delete_user(user_id))

    async def get_admin_count(self) -> int:
        try:
            return await self.user_repo.count_admins()
        except Exception as e:
            current_app.logger.error(f"Error counting admins: {e}")
            return 0

    def get_admin_count_sync(self) -> int:
        return run_async(self.get_admin_count())

    async def create_user_location(self, user_id: str, name: str, description: Optional[str] = None,
                                  location_type: str = "home", is_default: bool = False) -> Optional[Location]:
        """Create a location for a user."""
        try:
            location = Location(
                id=str(uuid.uuid4()),
                user_id=user_id,
                name=name,
                description=description or f"Default location set during onboarding",
                location_type=location_type,
                is_default=is_default,
                is_active=True,
                created_at=datetime.now(dt_timezone.utc)
            )
            
            created_location = await self.location_repo.create(location, user_id)
            if created_location:
                logger.info(f"✅ Created location '{name}' for user {user_id}")
            
            return created_location
            
        except Exception as e:
            logger.error(f"Failed to create location for user {user_id}: {e}")
            return None
    
    async def get_user_locations(self, user_id: str) -> List[Location]:
        """Get all locations for a user."""
        return await self.location_repo.get_user_locations(user_id)

    # NOTE: Locations are now universal - use LocationService.get_default_location() directly
