#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

# Set environment variables
os.environ.setdefault('SECRET_KEY', 'test-key-for-startup')
os.environ.setdefault('FLASK_ENV', 'development')

def test_basic_imports():
    print("🔍 Testing basic imports...")
    try:
        # Test config
        import config
        print("✅ Config imported")
        
        # Test Kuzu connection
        from app.infrastructure.kuzu_graph import get_graph_storage
        storage = get_graph_storage()
        print("✅ Kuzu storage connected")
        
        # Test services
        from app.services import book_service, user_service
        print(f"✅ Book service: {type(book_service).__name__}")
        print(f"✅ User service: {type(user_service).__name__}")
        
        return True
    except Exception as e:
        print(f"❌ Import failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if test_basic_imports():
        print("\n✅ Basic imports successful - ready to start app!")
    else:
        print("\n❌ Import issues detected")
