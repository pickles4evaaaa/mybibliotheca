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
    except (ValueError, TypeError):
        # If markdown rendering fails (e.g., invalid input type), return the escaped text
        # ValueError: invalid markdown syntax
        # TypeError: wrong input type (not a string)
        # We gracefully fallback to plain text display
        return Markup(f'<p>{escape(text)}</p>')
