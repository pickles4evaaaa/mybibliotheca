"""
Admin functionality for Bibliotheca multi-user platform
Provides admin-only decorators, middleware, and management functions
"""

from functools import wraps
from flask import Blueprint, render_template, redirect, url_for, flash, request, jsonify, abort
from flask_login import login_required, current_user
from .models import User, Book, ReadingLog, db
from .forms import UserProfileForm, AdminPasswordResetForm
from datetime import datetime, timedelta, timezone
from sqlalchemy import func

admin = Blueprint('admin', __name__, url_prefix='/admin')

def admin_required(f):
    """
    Decorator to require admin privileges for route access
    Usage: @admin_required
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('auth.login', next=request.url))
        
        if not current_user.is_admin:
            flash('Access denied. Admin privileges required.', 'error')
            abort(403)
        
        return f(*args, **kwargs)
    return decorated_function

def admin_or_self_required(user_id_param='user_id'):
    """
    Decorator to require admin privileges OR access to own user data
    Usage: @admin_or_self_required() or @admin_or_self_required('id')
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                flash('Please log in to access this page.', 'error')
                return redirect(url_for('auth.login', next=request.url))
            
            # Get the user_id from the route parameters
            target_user_id = kwargs.get(user_id_param)
            if target_user_id is None:
                target_user_id = request.view_args.get(user_id_param)
            
            # Allow if admin or accessing own data
            if current_user.is_admin or str(current_user.id) == str(target_user_id):
                return f(*args, **kwargs)
            
            flash('Access denied. Insufficient privileges.', 'error')
            abort(403)
        
        return decorated_function
    return decorator

@admin.route('/dashboard')
@login_required
@admin_required
def dashboard():
    """Admin dashboard with system overview"""
    # Get system statistics
    stats = get_system_stats()
    
    # Get recent user registrations (last 30 days)
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    recent_users = User.query.filter(User.created_at >= thirty_days_ago).order_by(User.created_at.desc()).limit(10).all()
    
    # Get recent book additions (last 30 days)  
    recent_books = Book.query.filter(Book.created_at >= thirty_days_ago).order_by(Book.created_at.desc()).limit(10).all()
    
    return render_template('admin/dashboard.html', 
                         title='Admin Dashboard',
                         stats=stats,
                         recent_users=recent_users,
                         recent_books=recent_books)

@admin.route('/users')
@login_required
@admin_required
def users():
    """User management interface"""
    page = request.args.get('page', 1, type=int)
    search = request.args.get('search', '', type=str)
    
    # Build query with optional search
    query = User.query
    if search:
        query = query.filter(
            db.or_(
                User.username.contains(search),
                User.email.contains(search)
            )
        )
    
    # Paginate results
    users = query.order_by(User.created_at.desc()).paginate(
        page=page, per_page=20, error_out=False
    )
    
    return render_template('admin/users.html',
                         title='User Management',
                         users=users,
                         search=search)

@admin.route('/users/<int:user_id>')
@login_required
@admin_required
def user_detail(user_id):
    """Individual user management"""
    user = User.query.get_or_404(user_id)
    
    # Get user statistics
    book_count = Book.query.filter_by(user_id=user.id).count()
    reading_count = ReadingLog.query.filter_by(user_id=user.id).count()
    
    # Get more detailed stats
    from sqlalchemy import extract
    current_year = datetime.now().year
    books_this_year = Book.query.filter(
        Book.user_id == user.id,
        extract('year', Book.created_at) == current_year
    ).count()
    
    logs_this_month = ReadingLog.query.filter(
        ReadingLog.user_id == user.id,
        ReadingLog.created_at >= datetime.now().replace(day=1)
    ).count()
    
    # Get recent activity
    recent_books = Book.query.filter_by(user_id=user.id).order_by(Book.created_at.desc()).limit(5).all()
    recent_logs = ReadingLog.query.filter_by(user_id=user.id).order_by(ReadingLog.created_at.desc()).limit(10).all()
    
    return render_template('admin/user_detail.html',
                         title=f'User: {user.username}',
                         user=user,
                         book_count=book_count,
                         reading_count=reading_count,
                         books_this_year=books_this_year,
                         logs_this_month=logs_this_month,
                         recent_books=recent_books,
                         recent_logs=recent_logs)

@admin.route('/users/<int:user_id>/toggle_admin', methods=['POST'])
@login_required
@admin_required
def toggle_admin(user_id):
    """Toggle admin status for a user"""
    user = User.query.get_or_404(user_id)
    
    # Prevent removing admin from the last admin
    if user.is_admin:
        admin_count = User.query.filter_by(is_admin=True).count()
        if admin_count <= 1:
            flash('Cannot remove admin privileges from the last admin user.', 'error')
            return redirect(url_for('admin.user_detail', user_id=user_id))
    
    # Toggle admin status
    user.is_admin = not user.is_admin
    db.session.commit()
    
    action = 'granted' if user.is_admin else 'removed'
    flash(f'Admin privileges {action} for user {user.username}.', 'success')
    
    return redirect(url_for('admin.user_detail', user_id=user_id))

@admin.route('/users/<int:user_id>/toggle_active', methods=['POST'])
@login_required
@admin_required
def toggle_active(user_id):
    """Toggle active status for a user"""
    user = User.query.get_or_404(user_id)
    
    # Prevent deactivating the current admin
    if user.id == current_user.id:
        flash('Cannot deactivate your own account.', 'error')
        return redirect(url_for('admin.user_detail', user_id=user_id))
    
    # Toggle active status
    user.is_active = not user.is_active
    db.session.commit()
    
    status = 'activated' if user.is_active else 'deactivated'
    flash(f'User {user.username} has been {status}.', 'success')
    
    return redirect(url_for('admin.user_detail', user_id=user_id))

@admin.route('/users/<int:user_id>/delete', methods=['POST'])
@login_required
@admin_required
def delete_user(user_id):
    """Delete a user and handle their data"""
    user = User.query.get_or_404(user_id)
    
    # Prevent deleting own account
    if user.id == current_user.id:
        flash('Cannot delete your own account.', 'error')
        return redirect(url_for('admin.user_detail', user_id=user_id))
    
    # Prevent deleting the last admin
    if user.is_admin:
        admin_count = User.query.filter_by(is_admin=True).count()
        if admin_count <= 1:
            flash('Cannot delete the last admin user.', 'error')
            return redirect(url_for('admin.user_detail', user_id=user_id))
    
    username = user.username
    
    # Delete user (cascades will handle books and logs)
    db.session.delete(user)
    db.session.commit()
    
    flash(f'User {username} and all associated data have been deleted.', 'success')
    return redirect(url_for('admin.users'))

@admin.route('/settings')
@login_required 
@admin_required
def settings():
    """Admin settings page"""
    return render_template('admin/settings.html', title='Admin Settings')

@admin.route('/api/stats')
@login_required
@admin_required
def api_stats():
    """API endpoint for dashboard statistics (for auto-refresh)"""
    stats = get_system_stats()
    return jsonify(stats)

@admin.route('/users/<int:user_id>/reset_password', methods=['GET', 'POST'])
@login_required
@admin_required
def reset_user_password(user_id):
    """Admin function to reset a user's password"""
    user = User.query.get_or_404(user_id)
    form = AdminPasswordResetForm()
    
    if form.validate_on_submit():
        try:
            user.set_password(form.new_password.data)
            # Set force password change if requested
            if form.force_change.data:
                user.password_must_change = True
            # Also unlock the account if it was locked
            user.unlock_account()
            db.session.commit()
            
            force_msg = " User will be required to change password on next login." if form.force_change.data else ""
            flash(f'Password reset successfully for user {user.username}.{force_msg}', 'success')
            return redirect(url_for('admin.user_detail', user_id=user.id))
        except ValueError as e:
            flash(str(e), 'error')
    
    return render_template('admin/reset_password.html', 
                         title=f'Reset Password - {user.username}',
                         form=form, 
                         user=user)

@admin.route('/users/<int:user_id>/unlock_account', methods=['POST'])
@login_required
@admin_required
def unlock_user_account(user_id):
    """Admin function to unlock a locked user account"""
    user = User.query.get_or_404(user_id)
    
    if user.is_locked():
        user.unlock_account()
        flash(f'Account unlocked for user {user.username}.', 'success')
    else:
        flash(f'User {user.username} account is not locked.', 'info')
    
    return redirect(url_for('admin.user_detail', user_id=user.id))

def get_system_stats():
    """Get system statistics for admin dashboard"""
    total_users = User.query.count()
    active_users = User.query.filter_by(is_active=True).count()
    admin_users = User.query.filter_by(is_admin=True).count()
    total_books = Book.query.count()
    
    # Users registered in last 30 days
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    new_users_30d = User.query.filter(User.created_at >= thirty_days_ago).count()
    
    # Books added in last 30 days
    new_books_30d = Book.query.filter(Book.created_at >= thirty_days_ago).count()
    
    # Most active users (by book count)
    top_users = db.session.query(
        User.username,
        func.count(Book.id).label('book_count')
    ).join(Book).group_by(User.id).order_by(func.count(Book.id).desc()).limit(5).all()
    
    # System health info (with fallback if psutil not available)
    system_info = {}
    try:
        import psutil
        disk_usage = psutil.disk_usage('/')
        memory = psutil.virtual_memory()
        
        system_info = {
            'disk_free_gb': round(disk_usage.free / (1024**3), 2),
            'disk_total_gb': round(disk_usage.total / (1024**3), 2),
            'disk_percent': round((disk_usage.used / disk_usage.total) * 100, 1),
            'memory_percent': memory.percent,
            'memory_available_gb': round(memory.available / (1024**3), 2)
        }
    except ImportError:
        # Fallback if psutil is not available
        system_info = {
            'disk_free_gb': 'N/A',
            'disk_total_gb': 'N/A', 
            'disk_percent': 'N/A',
            'memory_percent': 'N/A',
            'memory_available_gb': 'N/A'
        }
    
    return {
        'total_users': total_users,
        'active_users': active_users,
        'admin_users': admin_users,
        'total_books': total_books,
        'new_users_30d': new_users_30d,
        'new_books_30d': new_books_30d,
        'top_users': [{'username': user[0], 'book_count': user[1]} for user in top_users],
        'system': system_info
    }

def is_admin(user):
    """Helper function to check if user is admin"""
    return user.is_authenticated and user.is_admin

def promote_user_to_admin(user_id):
    """Promote a user to admin status"""
    user = User.query.get(user_id)
    if user:
        user.is_admin = True
        db.session.commit()
        return True
    return False

def demote_admin_user(user_id):
    """Demote an admin user (with safety checks)"""
    user = User.query.get(user_id)
    if user and user.is_admin:
        # Check if this is the last admin
        admin_count = User.query.filter_by(is_admin=True).count()
        if admin_count > 1:
            user.is_admin = False
            db.session.commit()
            return True
    return False

def unlock_user_account_by_id(user_id):
    """Helper function to unlock a user account"""
    user = User.query.get(user_id)
    if user:
        user.unlock_account()
        return True
    return False
