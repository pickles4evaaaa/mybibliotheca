#!/usr/bin/env python3
"""
Simple debug server to test the URL functionality
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, render_template_string

# Simple test to check URL generation
app = Flask(__name__)
app.config['SECRET_KEY'] = 'test'

@app.route('/test')
def test():
    template = """
    <!DOCTYPE html>
    <html>
    <head><title>URL Test</title></head>
    <body>
        <h1>URL Generation Test</h1>
        <button onclick="openBook('test123')">Test Book Click</button>
        
        <script>
        window.openBook = function(uid) {
            if (!uid) {
                console.error('No UID provided for book');
                return;
            }
            console.log('Opening book with UID:', uid);
            var baseUrl = "{{ url_for('view_book_enhanced', uid='PLACEHOLDER') }}";
            var bookUrl = baseUrl.replace('PLACEHOLDER', uid);
            console.log('Navigating to:', bookUrl);
            alert('Would navigate to: ' + bookUrl);
        };
        </script>
    </body>
    </html>
    """
    return render_template_string(template)

@app.route('/view_book_enhanced/<uid>')
def view_book_enhanced(uid):
    return f"Book page for UID: {uid}"

if __name__ == '__main__':
    app.run(debug=True, port=5055)
