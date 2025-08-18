"""
Template filters for unified cover management.
"""
from flask import Blueprint
from app.services.unified_cover_manager import cover_manager

def register_cover_filters(app):
    """Register cover-related template filters."""
    
    @app.template_filter('cover_info')
    def get_cover_info_filter(book):
        """Get cover info for a book."""
        return cover_manager.get_cover_info(book)
    
    @app.template_filter('has_cover')
    def has_cover_filter(book):
        """Check if book has a valid cover."""
        return cover_manager.get_cover_info(book).has_cover
    
    @app.template_filter('cover_html')
    def cover_html_filter(book, css_classes="", style="", img_id=""):
        """Generate cover HTML for a book."""
        return cover_manager.render_cover_html(book, css_classes, style, img_id)
