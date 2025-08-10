"""
Safe Import Job Manager for MyBibliotheca

This module provides thread-safe, user-scoped import job management to replace
the dangerous global import_jobs dictionary.

Key Features:
- User isolation: Each user's jobs are completely separate
- Thread safety: Proper locking prevents race conditions
- Memory management: Automatic cleanup of old jobs
- Privacy protection: Users cannot access other users' data
"""

import threading
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, List, Any

logger = logging.getLogger(__name__)


class SafeImportJobManager:
    """
    Thread-safe import job manager with user isolation.
    
    Replaces the dangerous global import_jobs dictionary with proper
    user-scoped storage and thread safety mechanisms.
    """
    
    def __init__(self):
        self._jobs_by_user = {}  # user_id -> {task_id -> job_data}
        self._locks_by_user = {}  # user_id -> threading.RLock()
        self._global_lock = threading.RLock()
        self._creation_time = time.time()
        
        # Statistics for monitoring
        self._stats = {
            'jobs_created': 0,
            'jobs_completed': 0,
            'jobs_failed': 0,
            'jobs_cleaned_up': 0,
            'total_operations': 0
        }
        
        logger.info("SafeImportJobManager initialized")
    
    def _get_user_lock(self, user_id: str) -> threading.RLock:
        """Get or create a lock for a specific user."""
        with self._global_lock:
            if user_id not in self._locks_by_user:
                self._locks_by_user[user_id] = threading.RLock()
                logger.debug(f"Created new lock for user {user_id}")
            return self._locks_by_user[user_id]
    
    def _increment_stat(self, stat_name: str):
        """Thread-safe statistics increment."""
        with self._global_lock:
            self._stats[stat_name] += 1
            self._stats['total_operations'] += 1
    
    def create_job(self, user_id: str, task_id: str, job_data: dict) -> bool:
        """
        Create a new import job for a specific user.
        
        Args:
            user_id: ID of the user who owns this job
            task_id: Unique task identifier
            job_data: Dictionary containing job information
            
        Returns:
            bool: True if job was created successfully
        """
        if not user_id or not task_id:
            logger.error("Cannot create job: user_id and task_id are required")
            return False
            
        with self._get_user_lock(user_id):
            # Initialize user's job storage if needed
            if user_id not in self._jobs_by_user:
                self._jobs_by_user[user_id] = {}
                logger.debug(f"Initialized job storage for user {user_id}")
            
            # Check for duplicate task_id for this user
            if task_id in self._jobs_by_user[user_id]:
                logger.warning(f"Task {task_id} already exists for user {user_id}")
                return False
            
            # Store a copy of the job data to prevent external modification
            job_copy = job_data.copy()
            # Only set created_at if not already provided
            if 'created_at' not in job_copy:
                job_copy['created_at'] = datetime.now(timezone.utc).isoformat()
            job_copy['user_id'] = user_id  # Ensure user_id is always set
            
            self._jobs_by_user[user_id][task_id] = job_copy
            
            self._increment_stat('jobs_created')
            logger.info(f"Created job {task_id} for user {user_id}")
            
            return True
    
    def update_job(self, user_id: str, task_id: str, updates: dict) -> bool:
        """
        Update an existing import job.
        
        Args:
            user_id: ID of the user who owns this job
            task_id: Task identifier to update
            updates: Dictionary of fields to update
            
        Returns:
            bool: True if job was updated successfully
        """
        if not user_id or not task_id:
            logger.error("Cannot update job: user_id and task_id are required")
            return False
            
        with self._get_user_lock(user_id):
            user_jobs = self._jobs_by_user.get(user_id, {})
            
            if task_id not in user_jobs:
                logger.warning(f"Task {task_id} not found for user {user_id}")
                return False
            
            # Update the job data
            user_jobs[task_id].update(updates)
            user_jobs[task_id]['updated_at'] = datetime.now(timezone.utc).isoformat()
            
            # Update statistics based on status change
            if 'status' in updates:
                status = updates['status']
                if status == 'completed':
                    self._increment_stat('jobs_completed')
                elif status == 'failed':
                    self._increment_stat('jobs_failed')
            
            logger.debug(f"Updated job {task_id} for user {user_id}: {list(updates.keys())}")
            
            return True
    
    def get_job(self, user_id: str, task_id: str) -> Optional[dict]:
        """
        Get a specific import job for a user.
        
        Args:
            user_id: ID of the user who owns this job
            task_id: Task identifier to retrieve
            
        Returns:
            dict: Copy of job data, or None if not found
        """
        if not user_id or not task_id:
            logger.error("Cannot get job: user_id and task_id are required")
            return None
            
        with self._get_user_lock(user_id):
            user_jobs = self._jobs_by_user.get(user_id, {})
            job = user_jobs.get(task_id)
            
            if job:
                # Return a copy to prevent external modification
                return job.copy()
            
            logger.debug(f"Job {task_id} not found for user {user_id}")
            return None
    
    def get_user_jobs(self, user_id: str) -> Dict[str, dict]:
        """
        Get all import jobs for a specific user.
        
        Args:
            user_id: ID of the user
            
        Returns:
            dict: Dictionary of task_id -> job_data (copies)
        """
        if not user_id:
            logger.error("Cannot get user jobs: user_id is required")
            return {}
            
        with self._get_user_lock(user_id):
            user_jobs = self._jobs_by_user.get(user_id, {})
            
            # Return copies of all jobs to prevent external modification
            return {task_id: job.copy() for task_id, job in user_jobs.items()}
    
    def delete_job(self, user_id: str, task_id: str) -> bool:
        """
        Delete a specific import job.
        
        Args:
            user_id: ID of the user who owns this job
            task_id: Task identifier to delete
            
        Returns:
            bool: True if job was deleted successfully
        """
        if not user_id or not task_id:
            logger.error("Cannot delete job: user_id and task_id are required")
            return False
            
        with self._get_user_lock(user_id):
            user_jobs = self._jobs_by_user.get(user_id, {})
            
            if task_id in user_jobs:
                del user_jobs[task_id]
                logger.info(f"Deleted job {task_id} for user {user_id}")
                return True
            
            logger.warning(f"Job {task_id} not found for user {user_id} (cannot delete)")
            return False
    
    def cleanup_completed_jobs(self, user_id: str, max_age_hours: int = 24) -> int:
        """
        Clean up old completed/failed jobs to prevent memory leaks.
        
        Args:
            user_id: ID of the user to clean up jobs for
            max_age_hours: Maximum age in hours for completed jobs
            
        Returns:
            int: Number of jobs cleaned up
        """
        if not user_id:
            logger.error("Cannot cleanup jobs: user_id is required")
            return 0
            
        with self._get_user_lock(user_id):
            user_jobs = self._jobs_by_user.get(user_id, {})
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
            
            to_remove = []
            for task_id, job in user_jobs.items():
                if job.get('status') in ['completed', 'failed']:
                    # Parse job creation time
                    try:
                        job_time_str = job.get('created_at', '1970-01-01T00:00:00')
                        # Handle timezone info properly
                        if job_time_str.endswith('Z'):
                            job_time_str = job_time_str[:-1] + '+00:00'
                        
                        # Try parsing with timezone first
                        try:
                            job_time = datetime.fromisoformat(job_time_str)
                            # If no timezone info, assume UTC
                            if job_time.tzinfo is None:
                                job_time = job_time.replace(tzinfo=timezone.utc)
                        except ValueError:
                            # Fallback for timestamps without timezone
                            job_time = datetime.fromisoformat(job_time_str.split('+')[0].split('Z')[0])
                            job_time = job_time.replace(tzinfo=timezone.utc)
                        
                        if job_time < cutoff_time:
                            to_remove.append(task_id)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid timestamp in job {task_id}: {e}")
                        # Remove jobs with invalid timestamps
                        if job.get('status') in ['completed', 'failed']:
                            to_remove.append(task_id)
            
            # Remove old jobs
            for task_id in to_remove:
                del user_jobs[task_id]
            
            if to_remove:
                self._stats['jobs_cleaned_up'] += len(to_remove)
                logger.info(f"Cleaned up {len(to_remove)} old jobs for user {user_id}")
            
            return len(to_remove)
    
    def cleanup_all_users(self, max_age_hours: int = 24) -> int:
        """
        Clean up old jobs for all users.
        
        Args:
            max_age_hours: Maximum age in hours for completed jobs
            
        Returns:
            int: Total number of jobs cleaned up
        """
        total_cleaned = 0
        
        with self._global_lock:
            user_ids = list(self._jobs_by_user.keys())
        
        for user_id in user_ids:
            cleaned = self.cleanup_completed_jobs(user_id, max_age_hours)
            total_cleaned += cleaned
        
        if total_cleaned > 0:
            logger.info(f"Cleaned up {total_cleaned} total jobs across all users")
        
        return total_cleaned
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get manager statistics for monitoring.
        
        Returns:
            dict: Statistics about the import job manager
        """
        with self._global_lock:
            total_jobs = sum(len(jobs) for jobs in self._jobs_by_user.values())
            total_users = len(self._jobs_by_user)
            uptime_hours = (time.time() - self._creation_time) / 3600
            
            return {
                'uptime_hours': round(uptime_hours, 2),
                'total_users_with_jobs': total_users,
                'total_active_jobs': total_jobs,
                'jobs_by_user': {user_id: len(jobs) for user_id, jobs in self._jobs_by_user.items()},
                'operation_stats': self._stats.copy(),
                'memory_usage_estimate_kb': total_jobs * 2  # Rough estimate: 2KB per job
            }
    
    def get_jobs_for_admin_debug(self, requesting_user_id: str, include_user_data: bool = False) -> Dict[str, Any]:
        """
        Get job information for admin debugging.
        
        Args:
            requesting_user_id: ID of the user making the request (must be admin)
            include_user_data: Whether to include sensitive user data
            
        Returns:
            dict: Debug information (sanitized if not admin)
        """
        # Note: This method should only be called after verifying admin status
        # in the calling code for security
        
        with self._global_lock:
            if include_user_data:
                # Full data for admin users
                return {
                    'total_jobs': sum(len(jobs) for jobs in self._jobs_by_user.values()),
                    'users_with_jobs': len(self._jobs_by_user),
                    'jobs_by_user': {
                        user_id: {
                            task_id: {
                                'status': job.get('status', 'unknown'),
                                'created_at': job.get('created_at', 'unknown'),
                                'processed': job.get('processed', 0),
                                'total': job.get('total', 0)
                            }
                            for task_id, job in jobs.items()
                        }
                        for user_id, jobs in self._jobs_by_user.items()
                    },
                    'statistics': self.get_statistics()
                }
            else:
                # Sanitized data
                return {
                    'total_jobs': sum(len(jobs) for jobs in self._jobs_by_user.values()),
                    'users_with_jobs': len(self._jobs_by_user),
                    'your_jobs': len(self._jobs_by_user.get(requesting_user_id, {})),
                    'statistics': {
                        'total_operations': self._stats['total_operations'],
                        'uptime_hours': round((time.time() - self._creation_time) / 3600, 2)
                    }
                }


# Global instance - this will replace the dangerous import_jobs dictionary
safe_import_manager = SafeImportJobManager()


# Compatibility functions for gradual migration
def safe_create_import_job(user_id: str, task_id: str, job_data: dict) -> bool:
    """Create an import job safely with user isolation."""
    return safe_import_manager.create_job(user_id, task_id, job_data)


def safe_update_import_job(user_id: str, task_id: str, updates: dict) -> bool:
    """Update an import job safely with user isolation."""
    return safe_import_manager.update_job(user_id, task_id, updates)


def safe_get_import_job(user_id: str, task_id: str) -> Optional[dict]:
    """Get an import job safely with user isolation."""
    return safe_import_manager.get_job(user_id, task_id)


def safe_get_user_import_jobs(user_id: str) -> Dict[str, dict]:
    """Get all import jobs for a user safely."""
    return safe_import_manager.get_user_jobs(user_id)


def safe_delete_import_job(user_id: str, task_id: str) -> bool:
    """Delete an import job safely with user isolation."""
    return safe_import_manager.delete_job(user_id, task_id)
