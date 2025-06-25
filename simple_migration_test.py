#!/usr/bin/env python3
"""
Simple test to verify Kuzu migration basics
"""

import os
import sys

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

def test_imports():
    """Test basic imports work."""
    print("Testing imports...")
    try:
        # Test environment loading
        from dotenv import load_dotenv
        load_dotenv()
        print("✅ Environment loaded")
        
        # Test config
        import config
        print("✅ Config imported")
        
        # Test Kuzu graph
        from app.infrastructure.kuzu_graph import get_graph_storage
        storage = get_graph_storage()
        print("✅ Kuzu storage connected")
        
        # Test services
        from app.services import book_service, user_service
        print(f"✅ Book service: {type(book_service).__name__}")
        print(f"✅ User service: {type(user_service).__name__}")
        
        return True
    except Exception as e:
        print(f"❌ Import test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_syntax():
    """Test syntax of key files."""
    print("\nTesting syntax...")
    import py_compile
    
    files = [
        'app/__init__.py',
        'app/services.py', 
        'app/infrastructure/kuzu_repositories.py',
        'config.py'
    ]
    
    for file_path in files:
        try:
            py_compile.compile(file_path, doraise=True)
            print(f"✅ {file_path}")
        except Exception as e:
            print(f"❌ {file_path}: {e}")
            return False
    
    return True

if __name__ == "__main__":
    print("🧪 Simple Migration Test")
    print("=" * 30)
    
    os.environ.setdefault('SECRET_KEY', 'test-key')
    
    syntax_ok = test_syntax()
    imports_ok = test_imports()
    
    if syntax_ok and imports_ok:
        print("\n✅ Basic migration test PASSED!")
    else:
        print("\n❌ Basic migration test FAILED!")
