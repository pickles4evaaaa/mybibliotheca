#!/usr/bin/env python3
"""
Direct test of missing get_user_book_sync method
"""

import sys
import os

# Add the project root to the Python path
sys.path.insert(0, '/Users/jeremiah/Documents/Python Projects/bibliotheca')

def check_method_exists():
    """Check if get_user_book_sync method exists in KuzuBookService"""
    
    # Read the kuzu_services.py file directly
    kuzu_services_path = '/Users/jeremiah/Documents/Python Projects/bibliotheca/app/kuzu_services.py'
    
    if not os.path.exists(kuzu_services_path):
        print(f"❌ File not found: {kuzu_services_path}")
        return False
    
    with open(kuzu_services_path, 'r') as f:
        content = f.read()
    
    print("🔍 Searching for get_user_book_sync method...")
    
    if 'def get_user_book_sync(' in content:
        print("  ✅ get_user_book_sync method found in kuzu_services.py")
        
        # Extract the method definition
        lines = content.split('\n')
        in_method = False
        method_lines = []
        
        for line in lines:
            if 'def get_user_book_sync(' in line:
                in_method = True
                method_lines.append(line)
            elif in_method:
                if line.strip() and not line.startswith('    ') and not line.startswith('\t') and line.strip() != '':
                    # End of method
                    break
                method_lines.append(line)
        
        print("  📋 Method definition:")
        for line_num, line in enumerate(method_lines[:10], 1):  # Show first 10 lines
            print(f"    {line_num:2d}: {line}")
        
        if len(method_lines) > 10:
            print(f"    ... and {len(method_lines) - 10} more lines")
        
        return True
    else:
        print("  ❌ get_user_book_sync method NOT found in kuzu_services.py")
        
        # Check what methods are available
        print("  🔍 Searching for other get_ methods:")
        lines = content.split('\n')
        for line_num, line in enumerate(lines, 1):
            if 'def get_' in line and 'KuzuBookService' in content[:content.find(line)]:
                method_name = line.strip().split('def ')[1].split('(')[0]
                print(f"    - {method_name} (line {line_num})")
        
        return False

def check_services_py():
    """Check if get_user_book_sync method exists in services.py as well"""
    
    services_path = '/Users/jeremiah/Documents/Python Projects/bibliotheca/app/services.py'
    
    if not os.path.exists(services_path):
        print(f"❌ File not found: {services_path}")
        return False
    
    with open(services_path, 'r') as f:
        content = f.read()
    
    print("\n🔍 Searching for get_user_book_sync method in services.py...")
    
    if 'def get_user_book_sync(' in content:
        print("  ✅ get_user_book_sync method found in services.py")
        return True
    else:
        print("  ❌ get_user_book_sync method NOT found in services.py")
        return False

def check_import_errors():
    """Check where get_user_book_sync is being called from"""
    
    print("\n🔍 Searching for get_user_book_sync usage in project...")
    
    # Files to check
    files_to_check = [
        '/Users/jeremiah/Documents/Python Projects/bibliotheca/app/routes.py',
        '/Users/jeremiah/Documents/Python Projects/bibliotheca/app/advanced_migration_system.py',
        '/Users/jeremiah/Documents/Python Projects/bibliotheca/app/services.py',
        '/Users/jeremiah/Documents/Python Projects/bibliotheca/app/kuzu_services.py'
    ]
    
    found_usage = False
    
    for file_path in files_to_check:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.read()
            
            if 'get_user_book_sync' in content:
                print(f"  📄 Found usage in: {os.path.basename(file_path)}")
                
                lines = content.split('\n')
                for line_num, line in enumerate(lines, 1):
                    if 'get_user_book_sync' in line:
                        print(f"    Line {line_num}: {line.strip()}")
                        found_usage = True
    
    if not found_usage:
        print("  ✅ No usage of get_user_book_sync found - this might not be the issue")
    
    return found_usage

if __name__ == "__main__":
    print("🚀 Checking for missing get_user_book_sync method...\n")
    
    method_in_kuzu = check_method_exists()
    method_in_services = check_services_py()
    usage_found = check_import_errors()
    
    print(f"\n📊 Summary:")
    print(f"  get_user_book_sync in kuzu_services.py: {'✅' if method_in_kuzu else '❌'}")
    print(f"  get_user_book_sync in services.py: {'✅' if method_in_services else '❌'}")
    print(f"  Usage found in project: {'✅' if usage_found else '❌'}")
    
    if not method_in_kuzu and usage_found:
        print(f"\n⚠️  WARNING: Method is being called but doesn't exist in kuzu_services.py!")
        print(f"   This explains the import error: 'KuzuBookService' object has no attribute 'get_user_book_sync'")
    
    if method_in_services and not method_in_kuzu:
        print(f"\n💡 SOLUTION: Copy get_user_book_sync method from services.py to kuzu_services.py")
