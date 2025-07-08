"""
Example of how to integrate the new routes structure with the existing application.
This shows the changes needed in the main application factory.
"""

def create_app(config_name='development'):
    """Application factory with new routes structure."""
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    
    # Initialize extensions
    db.init_app(app)
    login_manager.init_app(app)
    # ... other extensions
    
    # Register blueprints using new structure
    from app.routes import register_blueprints
    register_blueprints(app)
    
    # For backward compatibility, you might also register the old routes
    # during transition period:
    # from app.routes import bp as main_bp
    # app.register_blueprint(main_bp)
    
    return app

# Example of gradual migration approach
def create_app_with_gradual_migration(config_name='development'):
    """Application factory with gradual migration support."""
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    
    # Initialize extensions
    db.init_app(app)
    login_manager.init_app(app)
    
    # Phase 1: Register new modular routes
    from app.routes import register_blueprints
    register_blueprints(app)
    
    # Phase 2: Keep old routes as fallback (can be removed later)
    try:
        from app.routes import bp as legacy_bp
        app.register_blueprint(legacy_bp, url_prefix='/legacy')
    except ImportError:
        # Old routes.py has been removed
        pass
    
    # Phase 3: Add route aliases for backward compatibility
    @app.route('/people')
    def people_redirect():
        return redirect(url_for('people.people'))
    
    @app.route('/import-books')
    def import_redirect():
        return redirect(url_for('import.import_books'))
    
    return app
