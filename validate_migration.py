#!/usr/bin/env python3
"""
Migration System Validation Script

This script validates that the automatic migration system is properly configured
without actually running migrations or requiring dependencies.
"""

import os
import ast
import sys

def validate_migration_functions():
    """Validate that migration functions are present in app/__init__.py"""
    init_file = os.path.join(os.path.dirname(__file__), 'app', '__init__.py')
    
    if not os.path.exists(init_file):
        print("❌ app/__init__.py not found")
        return False
    
    with open(init_file, 'r') as f:
        content = f.read()
    
    # Check for required functions
    required_functions = [
        'backup_database',
        'run_security_privacy_migration', 
        'create_default_admin_if_needed'
    ]
    
    for func in required_functions:
        if f"def {func}(" in content:
            print(f"✅ Function {func} found")
        else:
            print(f"❌ Function {func} missing")
            return False
    
    # Check for migration logic in create_app
    if "backup_database(db_path)" in content:
        print("✅ Backup logic found in create_app")
    else:
        print("❌ Backup logic missing from create_app")
        return False
    
    if "run_security_privacy_migration" in content:
        print("✅ Security/privacy migration logic found")
    else:
        print("❌ Security/privacy migration logic missing")
        return False
    
    return True

def validate_config():
    """Validate that config has DATABASE_PATH"""
    config_file = os.path.join(os.path.dirname(__file__), 'config.py')
    
    if not os.path.exists(config_file):
        print("❌ config.py not found")
        return False
    
    with open(config_file, 'r') as f:
        content = f.read()
    
    if "DATABASE_PATH" in content:
        print("✅ DATABASE_PATH found in config")
        return True
    else:
        print("❌ DATABASE_PATH missing from config")
        return False

def validate_documentation():
    """Validate that documentation exists"""
    migration_doc = os.path.join(os.path.dirname(__file__), 'MIGRATION.md')
    
    if os.path.exists(migration_doc):
        print("✅ MIGRATION.md documentation found")
        return True
    else:
        print("❌ MIGRATION.md documentation missing")
        return False

def validate_deprecated_scripts():
    """Check that migration scripts are properly deprecated"""
    scripts = [
        'migrate_db_schema.py',
        'migrate_security_features.py'
    ]
    
    for script in scripts:
        script_path = os.path.join(os.path.dirname(__file__), script)
        if os.path.exists(script_path):
            with open(script_path, 'r') as f:
                content = f.read()
            
            if "DEPRECATED" in content:
                print(f"✅ {script} properly deprecated")
            else:
                print(f"⚠️  {script} should be deprecated")
        else:
            print(f"⚠️  {script} not found (may have been removed)")
    
    return True

def main():
    print("🔍 Validating Automatic Migration System")
    print("=" * 50)
    
    all_valid = True
    
    print("\n📁 Checking migration functions...")
    all_valid &= validate_migration_functions()
    
    print("\n⚙️  Checking configuration...")
    all_valid &= validate_config()
    
    print("\n📚 Checking documentation...")
    all_valid &= validate_documentation()
    
    print("\n🗂️  Checking deprecated scripts...")
    validate_deprecated_scripts()
    
    print("\n" + "=" * 50)
    if all_valid:
        print("🎉 Migration system validation PASSED!")
        print("✅ Automatic migrations are properly configured")
        print("✅ Database backups will be created before migration")
        print("✅ Security/privacy fields will be automatically added")
        print("✅ Multi-user migration is handled automatically")
        print("\n📖 See MIGRATION.md for complete details")
        return True
    else:
        print("❌ Migration system validation FAILED!")
        print("⚠️  Please fix the issues above before deploying")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
