"""
Unified Cover Management System

This is the SINGLE source of truth for all cover-related operations.
All other code should delegate to this service.

Responsibilities:
- Cover URL validation and normalization
- Cover display logic with consistent fallbacks
- Cover form processing with proper preservation
- Cover fetching and caching
- Cover update operations

Design Principles:
- NEVER lose an existing cover unless explicitly requested
- Consistent fallback behavior across the entire application
- Single place to modify cover logic
- Clear separation of concerns
"""
from typing import Optional, Dict, Any, Union
from dataclasses import dataclass
from flask import url_for
import logging
import re

logger = logging.getLogger(__name__)


@dataclass
class CoverInfo:
    """Unified cover information structure."""
    url: Optional[str] = None
    has_cover: bool = False
    fallback_icon: str = "bi-book"
    fallback_image: Optional[str] = None
    source: Optional[str] = None  # 'user', 'google', 'openlibrary', etc.


class UnifiedCoverManager:
    """
    Single source of truth for ALL cover operations.
    
    This class handles:
    - Cover URL validation and sanitization
    - Display logic with consistent fallbacks
    - Form processing with cover preservation
    - Update operations that respect existing covers
    """
    
    def __init__(self):
        # URL validation patterns
        self.valid_url_pattern = re.compile(r'^https?://.+\..+')
        self.google_books_pattern = re.compile(r'books\.google\.')
        self.openlibrary_pattern = re.compile(r'covers\.openlibrary\.org')
    
    def validate_cover_url(self, url: Optional[str]) -> Optional[str]:
        """
        Validate and normalize a cover URL.
        
        Handles both external URLs (https://...) and cached covers (/covers/uuid.jpg).
        
        Returns:
            Normalized URL if valid, None if invalid
        """
        if not url or not isinstance(url, str):
            return None
            
        url = url.strip()
        if not url:
            return None
        
        # Check for cached covers (local files)
        if url.startswith('/covers/') or url.startswith('/static/covers/'):
            return url  # These are valid cached covers
            
        # Basic URL validation for external URLs
        if not self.valid_url_pattern.match(url):
            return None
            
        # Force HTTPS for external URLs
        if url.startswith('http://'):
            url = url.replace('http://', 'https://')
            
        return url
    
    def get_cover_info(self, book: Any) -> CoverInfo:
        """
        Get comprehensive cover information for a book.
        
        Args:
            book: Book object (can be dict or Book model)
            
        Returns:
            CoverInfo with all display information needed
        """
        # Extract cover URL from book object
        if hasattr(book, 'cover_url'):
            cover_url = getattr(book, 'cover_url')
        elif isinstance(book, dict):
            # Primary key is 'cover_url'; fall back to legacy 'cover'
            cover_url = book.get('cover_url') or book.get('cover')
            # Harmonize: if only legacy key present, mirror it so downstream code that mutates dicts
            # (e.g., templates expecting cover_url) can rely on consistent key without modifying original logic
            if cover_url and 'cover_url' not in book and 'cover' in book:
                try:
                    book['cover_url'] = cover_url  # non-fatal best-effort
                except Exception:
                    pass
        else:
            cover_url = None
            
        # Validate the cover URL
        validated_url = self.validate_cover_url(cover_url)
        
        # Determine source
        source = None
        if validated_url:
            if validated_url.startswith('/covers/') or validated_url.startswith('/static/covers/'):
                source = 'cached'
            elif self.google_books_pattern.search(validated_url):
                source = 'google'
            elif self.openlibrary_pattern.search(validated_url):
                source = 'openlibrary'
            else:
                source = 'user'
        
        # Get fallback image URL safely (only when in Flask context)
        fallback_image = None
        try:
            from flask import url_for, has_app_context
            if has_app_context():
                fallback_image = url_for('serve_static', filename='bookshelf.png')
        except (ImportError, RuntimeError):
            # No Flask context available, use None
            pass
        
        return CoverInfo(
            url=validated_url,
            has_cover=bool(validated_url),
            fallback_icon="bi-book",
            fallback_image=fallback_image,
            source=source
        )
    
    def should_update_cover(self, current_cover: Optional[str], new_cover: Optional[str]) -> bool:
        """
        Determine if a cover should be updated based on preservation rules.
        
        PRESERVATION RULES:
        1. If user provides a valid new URL -> UPDATE
        2. If user provides empty/invalid URL but we have existing cover -> PRESERVE (no update)
        3. If no existing cover and no valid new URL -> NO UPDATE
        
        Args:
            current_cover: Current cover URL
            new_cover: New cover URL from form
            
        Returns:
            True if cover should be updated, False if preserved
        """
        validated_new = self.validate_cover_url(new_cover)
        # Treat empty string same as None
        if isinstance(current_cover, str) and not current_cover.strip():
            current_cover = None
        has_current = bool(self.validate_cover_url(current_cover))
        
        # User provided a valid new URL - always update
        if validated_new:
            return True
            
        # No valid new URL provided
        if has_current:
            # We have existing cover and no valid new URL - preserve existing
            logger.info(f"[COVER] Preserving existing cover: {current_cover}")
            return False
        else:
            # No existing cover and no valid new URL - no change needed
            return False
    
    def process_cover_form_field(self, form_data: Dict[str, Any], current_book: Any) -> Dict[str, Any]:
        """
        Process cover_url field from form data with smart preservation.
        
        Args:
            form_data: Form data dictionary
            current_book: Current book object
            
        Returns:
            Dictionary of updates to apply (may be empty if cover preserved)
        """
        updates = {}
        
        if 'cover_url' not in form_data:
            # Cover field not in form - no change
            return updates
            
        current_cover = None
        if hasattr(current_book, 'cover_url'):
            current_cover = current_book.cover_url
        elif isinstance(current_book, dict):
            current_cover = current_book.get('cover_url')
            
        new_cover = form_data.get('cover_url')
        # Normalize blank submission to None for logic clarity
        if isinstance(new_cover, str) and not new_cover.strip():
            new_cover = None
        
        if self.should_update_cover(current_cover, new_cover):
            validated_new = self.validate_cover_url(new_cover)
            updates['cover_url'] = validated_new
            logger.info(f"[COVER] Updating cover: '{current_cover}' -> '{validated_new}'")
        else:
            logger.info(f"[COVER] Preserving existing cover: {current_cover}")
            
        return updates
    
    def render_cover_html(self, book: Any, css_classes: str = "", style: str = "", img_id: str = "") -> str:
        """
        Generate consistent cover HTML for templates.
        
        Args:
            book: Book object
            css_classes: Additional CSS classes for the image
            style: Inline styles for the image
            img_id: ID attribute for the image
            
        Returns:
            HTML string for cover display
        """
        cover_info = self.get_cover_info(book)
        
        if cover_info.has_cover:
            # Has valid cover - show image with fallback
            img_attrs = []
            if css_classes:
                img_attrs.append(f'class="{css_classes}"')
            if style:
                img_attrs.append(f'style="{style}"')
            if img_id:
                img_attrs.append(f'id="{img_id}"')
                
            attrs_str = ' ' + ' '.join(img_attrs) if img_attrs else ''
            
            # Get fallback URL safely
            fallback_url = '/static/bookshelf.png'  # Default fallback
            try:
                from flask import url_for, has_app_context
                if has_app_context():
                    fallback_url = url_for('serve_static', filename='bookshelf.png')
            except (ImportError, RuntimeError):
                pass
            
            return f'''<img src="{cover_info.url}" alt="{getattr(book, 'title', 'Book cover')}"
                           onerror="this.onerror=null;this.src='{fallback_url}';"{attrs_str}>'''
        else:
            # No cover - show placeholder
            container_attrs = []
            if css_classes:
                # Convert image classes to container classes
                container_classes = css_classes.replace('img-fluid', 'd-flex align-items-center justify-content-center')
                container_attrs.append(f'class="bg-light rounded shadow-sm {container_classes}"')
            else:
                container_attrs.append('class="bg-light rounded d-flex align-items-center justify-content-center shadow-sm"')
                
            if style:
                container_attrs.append(f'style="{style}"')
            if img_id:
                container_attrs.append(f'id="{img_id}"')
                
            attrs_str = ' ' + ' '.join(container_attrs) if container_attrs else ''
            
            return f'''<div{attrs_str}>
                           <i class="{cover_info.fallback_icon} text-muted" style="font-size: 6rem;"></i>
                       </div>'''
    
    def get_cover_form_input_html(self, book: Any) -> str:
        """
        Generate cover URL form input with proper preservation messaging.
        
        Args:
            book: Book object
            
        Returns:
            HTML for cover URL input field
        """
        cover_info = self.get_cover_info(book)
        
        if cover_info.has_cover and cover_info.url:
            placeholder = "Leave empty to keep current cover"
            url_display = cover_info.url[:50] + ('...' if len(cover_info.url) > 50 else '')
            help_text = f'''
            <div class="form-text">
                <small class="text-success">✅ Current cover: {url_display}</small>
                <br><small class="text-info">💡 Leave this field empty to keep the current cover, or enter a new URL to replace it</small>
            </div>'''
        else:
            placeholder = "https://..."
            help_text = '''
            <div class="form-text">
                <small class="text-muted">Enter a URL to add a cover image for this book</small>
            </div>'''
            
        return f'''
        <div class="mb-3">
            <label for="cover_url" class="form-label fw-medium">
                <i class="bi bi-image me-1"></i>Cover URL
            </label>
            <input type="url" class="form-control" id="cover_url" name="cover_url" 
                   placeholder="{placeholder}">
            {help_text}
        </div>'''


# Global instance - single source of truth
cover_manager = UnifiedCoverManager()
