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
    except Exception:
        # If markdown rendering fails, return the escaped text as-is in a paragraph
        # We don't log the exception as it's expected for invalid markdown
        return Markup(f'<p>{escape(text)}</p>')
