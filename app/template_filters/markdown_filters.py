"""
Template filters for markdown rendering.
"""
import logging
import mistune
from markupsafe import Markup, escape

logger = logging.getLogger(__name__)

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
        text: Markdown text to convert (should be a string)
        
    Returns:
        Safe HTML markup
    """
    if not text:
        return ''
    
    # Ensure text is a string
    if not isinstance(text, str):
        text = str(text)
    
    try:
        # Convert markdown to HTML (with HTML escaping enabled for security)
        html = markdown(text)
        # Return as safe markup so Jinja2 doesn't escape markdown-generated HTML
        return Markup(html)
    except Exception as e:
        # Mistune is very robust and rarely raises exceptions for valid strings
        # If it does fail (e.g., plugin issues), log and return escaped plain text
        # Note: Invalid markdown syntax doesn't raise exceptions - it renders as-is
        logger.warning(f"Markdown rendering failed: {type(e).__name__}: {e}")
        return Markup(f'<p>{escape(str(text))}</p>')
