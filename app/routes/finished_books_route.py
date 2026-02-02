from flask import Blueprint, render_template
from flask_login import login_required

# Blueprint Definition
bp = Blueprint('finished_books', __name__, url_prefix='/stats')

@bp.route('/finished-books')
@login_required
def finished_books_page():
    # Lädt das HTML Template
    return render_template('finished_books.html')