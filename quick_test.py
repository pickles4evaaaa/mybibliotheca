#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

print("🧪 Quick Migration Test")
print("=" * 30)

# Test 1: Basic imports
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ Environment loaded")
except Exception as e:
    print(f"❌ Environment failed: {e}")
    sys.exit(1)

# Test 2: Config
try:
    import config
    print("✅ Config imported")
except Exception as e:
    print(f"❌ Config failed: {e}")
    sys.exit(1)

# Test 3: Kuzu connection
try:
    from app.infrastructure.kuzu_graph import get_graph_storage
    storage = get_graph_storage()
    print("✅ Kuzu storage connected")
except Exception as e:
    print(f"❌ Kuzu connection failed: {e}")
    sys.exit(1)

# Test 4: Services
try:
    from app.services import book_service, user_service
    print(f"✅ Book service: {type(book_service).__name__}")
    print(f"✅ User service: {type(user_service).__name__}")
    
    # Check they're not Redis services
    if "Redis" in type(book_service).__name__:
        print("❌ Book service still using Redis!")
        sys.exit(1)
    if "Redis" in type(user_service).__name__:
        print("❌ User service still using Redis!")
        sys.exit(1)
        
except Exception as e:
    print(f"❌ Services failed: {e}")
    sys.exit(1)

# Test 5: App creation
try:
    os.environ['BIBLIOTHECA_VERBOSE_INIT'] = 'false'
    from app import create_app
    app = create_app()
    
    if hasattr(app, 'book_service'):
        print(f"✅ App book_service: {type(app.book_service).__name__}")
    else:
        print("❌ App missing book_service")
        sys.exit(1)
        
except Exception as e:
    print(f"❌ App creation failed: {e}")
    sys.exit(1)

print("\n🎉 Migration test PASSED!")
print("✅ Kuzu migration appears successful!")
