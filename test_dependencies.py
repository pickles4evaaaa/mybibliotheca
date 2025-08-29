"""
Test to ensure MyBibliotheca doesn't accidentally import AI/ML libraries
"""
import sys
import pytest


def test_no_unwanted_ai_ml_imports():
    """Test that the application doesn't import unwanted AI/ML libraries"""
    
    # List of AI/ML libraries that should NOT be imported
    prohibited_modules = [
        'torch',
        'tensorflow', 'tf',
        'transformers',
        'huggingface_hub',
        'tokenizers',
        'numpy',  # While numpy itself isn't bad, it's not needed for a book app
        'pandas', # Same with pandas
        'sklearn', 'scikit-learn',
        'cv2', 'opencv',
        'PIL.ImageAI',  # AI-specific PIL modules
    ]
    
    # Import the main app to trigger any imports
    try:
        from app import create_app
        app = create_app()
    except ImportError as e:
        pytest.fail(f"Failed to import app: {e}")
    
    # Check which modules are loaded
    loaded_modules = list(sys.modules.keys())
    
    # Check for prohibited modules
    for module in prohibited_modules:
        if module in loaded_modules:
            pytest.fail(f"Prohibited AI/ML module '{module}' was imported")
        
        # Also check for submodules
        for loaded_module in loaded_modules:
            if loaded_module.startswith(f"{module}."):
                pytest.fail(f"Prohibited AI/ML submodule '{loaded_module}' was imported")


def test_only_approved_dependencies():
    """Test that only approved dependencies are in requirements"""
    
    approved_packages = {
        'flask', 'flask-sqlalchemy', 'flask-login', 'flask-wtf', 'flask-mail',
        'wtforms', 'werkzeug', 'requests', 'sqlalchemy', 'python-dotenv',
        'pillow', 'pytz', 'email-validator', 'gunicorn', 'psutil', 'scrypt',
        'cryptography', 'pytest', 'pytest-flask', 'pytest-cov'
    }
    
    # Read requirements.txt
    try:
        with open('requirements.txt', 'r') as f:
            requirements = f.read()
    except FileNotFoundError:
        pytest.fail("requirements.txt not found")
    
    # Parse package names (ignore versions and comments)
    for line in requirements.split('\n'):
        line = line.strip()
        if line and not line.startswith('#'):
            package_name = line.split('==')[0].split('>=')[0].split('<')[0].lower()
            if package_name not in approved_packages:
                pytest.fail(f"Unapproved package '{package_name}' found in requirements.txt")


if __name__ == "__main__":
    test_no_unwanted_ai_ml_imports()
    test_only_approved_dependencies()
    print("✅ All dependency tests passed")