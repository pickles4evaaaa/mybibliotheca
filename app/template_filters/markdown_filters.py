"""
Template filters for markdown rendering.
"""
import mistune
from markupsafe import Markup, escape

# Create a markdown instance with safe defaults
# escape=True ensures user-provided HTML is escaped for security
markdown = mistune.create_markdown(
    escape=True,  # Escape HTML to prevent XSS
    plugins=['strikethrough', 'table', 'url']
)

def render_markdown(text):
    """
    Convert markdown text to HTML.
    
    Args:
        text: Markdown text to convert
        
    Returns:
        Safe HTML markup
    """
    if not text:
        return ''
    
    try:
        # Convert markdown to HTML (with HTML escaping enabled for security)
        html = markdown(text)
        # Return as safe markup so Jinja2 doesn't escape markdown-generated HTML
        return Markup(html)
    except Exception as e:
        # If markdown rendering fails, return the escaped text as-is
        return Markup(f'<p>{escape(text)}</p>')

def register_markdown_filters(app):
    """Register markdown-related template filters."""
    
    @app.template_filter('markdown')
    def markdown_filter(text):
        """Convert markdown text to HTML."""
        return render_markdown(text)
