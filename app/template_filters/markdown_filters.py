"""
Template filters for markdown rendering.
"""
import mistune
from markupsafe import Markup

# Create a markdown instance with safe defaults
markdown = mistune.create_markdown(
    escape=False,  # We'll sanitize via Markup
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
        # Convert markdown to HTML
        html = markdown(text)
        # Return as safe markup so Jinja2 doesn't escape it
        return Markup(html)
    except Exception as e:
        # If markdown rendering fails, return the text as-is
        return Markup(f'<p>{text}</p>')

def register_markdown_filters(app):
    """Register markdown-related template filters."""
    
    @app.template_filter('markdown')
    def markdown_filter(text):
        """Convert markdown text to HTML."""
        return render_markdown(text)
