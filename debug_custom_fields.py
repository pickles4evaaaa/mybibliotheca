#!/usr/bin/env python3
"""
Debug Custom Field Issues - Comprehensive Troubleshooting
"""

import os
import sys
import uuid
import kuzu
from datetime import datetime
from pathlib import Path

def debug_import_job_schema():
    """Debug ImportJob schema to see what fields are actually available."""
    print("🔍 Debugging ImportJob Schema...")
    
    try:
        # Connect to Kuzu directly
        db_path = 'data/kuzu'
        database = kuzu.Database(db_path)
        connection = kuzu.Connection(database)
        
        print("🔌 Connected to Kuzu database")
        
        # Try to describe the ImportJob table structure
        print("\n📋 Checking ImportJob table structure...")
        
        # Method 1: Try to get table info (if supported)
        try:
            result = connection.execute("CALL db_info()")
            while result.has_next():
                row = result.get_next()
                print(f"  DB Info: {row}")
        except Exception as e:
            print(f"  ❌ db_info() not available: {e}")
        
        # Method 2: Try to create a test ImportJob to see what fields are expected
        print("\n🧪 Testing ImportJob creation with minimal data...")
        test_job_id = f"debug_job_{uuid.uuid4()}"
        
        minimal_data = {
            "id": test_job_id,
            "task_id": "debug_task",
            "user_id": "debug_user",
            "status": "testing",
            "created_at": datetime.utcnow()
        }
        
        query = """
        CREATE (j:ImportJob {
            id: $id,
            task_id: $task_id,
            user_id: $user_id,
            status: $status,
            created_at: $created_at
        })
        """
        
        connection.execute(query, minimal_data)
        print("✅ Minimal ImportJob created successfully")
        
        # Method 3: Try to update with error_message field
        print("\n🔧 Testing error_message field update...")
        try:
            update_query = """
            MATCH (j:ImportJob {id: $job_id})
            SET j.error_message = $error_message
            """
            connection.execute(update_query, {
                "job_id": test_job_id,
                "error_message": "Test error message"
            })
            print("✅ error_message field update succeeded")
        except Exception as e:
            print(f"❌ error_message field update failed: {e}")
        
        # Method 4: Try to update with error_messages field (plural)
        print("\n🔧 Testing error_messages field update...")
        try:
            update_query = """
            MATCH (j:ImportJob {id: $job_id})
            SET j.error_messages = $error_messages
            """
            connection.execute(update_query, {
                "job_id": test_job_id,
                "error_messages": "Test error messages"
            })
            print("✅ error_messages field update succeeded")
        except Exception as e:
            print(f"❌ error_messages field update failed: {e}")
        
        # Method 5: Try to read back the job
        print("\n📖 Reading back the test job...")
        result = connection.execute("MATCH (j:ImportJob {id: $job_id}) RETURN j", {"job_id": test_job_id})
        if result.has_next():
            job_data = result.get_next()[0]
            job_dict = dict(job_data)
            print("✅ Job retrieved successfully:")
            for key, value in job_dict.items():
                print(f"  {key}: {value}")
        else:
            print("❌ Failed to retrieve job")
        
        # Method 6: Test all expected ImportJob fields
        print("\n🧪 Testing all ImportJob fields...")
        expected_fields = [
            "task_id", "user_id", "csv_file_path", "field_mappings", 
            "default_reading_status", "duplicate_handling", "custom_fields_enabled",
            "status", "processed", "total", "success", "errors", 
            "start_time", "end_time", "expires_at", "current_book",
            "error_message", "error_messages", "recent_activity", "job_data"
        ]
        
        test_job_id2 = f"debug_job_full_{uuid.uuid4()}"
        
        # Create job with all fields
        full_data = {
            "id": test_job_id2,
            "task_id": "full_test_task",
            "user_id": "full_test_user",
            "csv_file_path": "/test/path.csv",
            "field_mappings": "{}",
            "default_reading_status": "unread",
            "duplicate_handling": "skip",
            "custom_fields_enabled": True,
            "status": "running",
            "processed": 0,
            "total": 100,
            "success": 0,
            "errors": 0,
            "start_time": datetime.utcnow(),
            "end_time": None,
            "expires_at": datetime.utcnow(),
            "current_book": "Test Book",
            "error_message": "Single error",
            "error_messages": "Multiple errors",
            "recent_activity": "Activity log",
            "job_data": "{}",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        # Build dynamic query
        field_names = list(full_data.keys())
        field_assignments = [f"{field}: ${field}" for field in field_names]
        
        full_query = f"""
        CREATE (j:ImportJob {{
            {', '.join(field_assignments)}
        }})
        """
        
        try:
            connection.execute(full_query, full_data)
            print("✅ Full ImportJob created successfully with all fields")
        except Exception as e:
            print(f"❌ Full ImportJob creation failed: {e}")
            
            # Try to identify which field is causing the issue
            print("\n🔍 Testing fields individually...")
            for field in expected_fields:
                if field in full_data:
                    try:
                        test_query = f"""
                        CREATE (j:ImportJob {{
                            id: $id,
                            {field}: ${field}
                        }})
                        """
                        test_data = {
                            "id": f"field_test_{field}_{uuid.uuid4()}",
                            field: full_data[field]
                        }
                        connection.execute(test_query, test_data)
                        print(f"  ✅ {field}: OK")
                    except Exception as field_error:
                        print(f"  ❌ {field}: {field_error}")
        
        # Clean up test jobs
        print("\n🧹 Cleaning up test jobs...")
        try:
            connection.execute("MATCH (j:ImportJob) WHERE j.id STARTS WITH 'debug_job' OR j.id STARTS WITH 'field_test' DELETE j")
            print("✅ Test jobs cleaned up")
        except Exception as e:
            print(f"❌ Cleanup failed: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Schema debugging failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def debug_job_update_process():
    """Debug the job update process step by step."""
    print("\n🔍 Debugging Job Update Process...")
    
    try:
        # Connect to Kuzu directly
        db_path = 'data/kuzu'
        database = kuzu.Database(db_path)
        connection = kuzu.Connection(database)
        
        # Create a test job
        test_job_id = f"update_test_{uuid.uuid4()}"
        print(f"📝 Creating test job: {test_job_id}")
        
        initial_data = {
            "id": test_job_id,
            "task_id": "update_test_task",
            "user_id": "update_test_user",
            "status": "running",
            "processed": 0,
            "total": 10,
            "success": 0,
            "errors": 0,
            "created_at": datetime.utcnow()
        }
        
        # Create the job
        create_query = """
        CREATE (j:ImportJob {
            id: $id,
            task_id: $task_id,
            user_id: $user_id,
            status: $status,
            processed: $processed,
            total: $total,
            success: $success,
            errors: $errors,
            created_at: $created_at
        })
        """
        
        connection.execute(create_query, initial_data)
        print("✅ Test job created")
        
        # Test different update scenarios
        update_scenarios = [
            {
                "name": "Basic counters",
                "data": {"processed": 5, "success": 4, "errors": 1}
            },
            {
                "name": "Current book",
                "data": {"current_book": "Test Book Title"}
            },
            {
                "name": "Error message (singular)",
                "data": {"error_message": "Single error occurred"}
            },
            {
                "name": "Error messages (plural)",
                "data": {"error_messages": "Multiple errors occurred"}
            },
            {
                "name": "Combined update",
                "data": {
                    "processed": 10,
                    "success": 8,
                    "errors": 2,
                    "current_book": "Final Book",
                    "status": "completed"
                }
            }
        ]
        
        for scenario in update_scenarios:
            print(f"\n🧪 Testing: {scenario['name']}")
            
            try:
                # Build update query
                set_clauses = []
                params = {"job_id": test_job_id}
                
                for key, value in scenario['data'].items():
                    param_key = f"param_{key}"
                    set_clauses.append(f"j.{key} = ${param_key}")
                    params[param_key] = value
                
                update_query = f"""
                MATCH (j:ImportJob {{id: $job_id}})
                SET {', '.join(set_clauses)}
                """
                
                print(f"  📝 Query: {update_query}")
                print(f"  📝 Params: {params}")
                
                connection.execute(update_query, params)
                print(f"  ✅ {scenario['name']} update succeeded")
                
                # Verify the update
                result = connection.execute("MATCH (j:ImportJob {id: $job_id}) RETURN j", {"job_id": test_job_id})
                if result.has_next():
                    job_data = result.get_next()[0]
                    job_dict = dict(job_data)
                    print(f"  📊 Updated job state:")
                    for key, value in scenario['data'].items():
                        actual_value = job_dict.get(key)
                        if actual_value == value:
                            print(f"    ✅ {key}: {actual_value}")
                        else:
                            print(f"    ❌ {key}: expected {value}, got {actual_value}")
                
            except Exception as e:
                print(f"  ❌ {scenario['name']} update failed: {e}")
        
        # Clean up
        print("\n🧹 Cleaning up test job...")
        connection.execute("MATCH (j:ImportJob {id: $job_id}) DELETE j", {"job_id": test_job_id})
        print("✅ Test job cleaned up")
        
        return True
        
    except Exception as e:
        print(f"❌ Job update debugging failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def debug_schema_vs_code():
    """Compare the schema definition with what the code expects."""
    print("\n🔍 Debugging Schema vs Code Expectations...")
    
    # Fields that the code tries to update (from the error log)
    code_expected_fields = [
        "processed", "current_book", "success", "errors", "error_message", "error_messages"
    ]
    
    # Fields defined in our schema
    schema_defined_fields = [
        "id", "task_id", "user_id", "csv_file_path", "field_mappings", 
        "default_reading_status", "duplicate_handling", "custom_fields_enabled",
        "status", "processed", "total", "success", "errors", 
        "start_time", "end_time", "expires_at", "current_book",
        "error_message", "error_messages", "recent_activity", "job_data",
        "created_at", "updated_at"
    ]
    
    print("📋 Code expects these fields:")
    for field in code_expected_fields:
        if field in schema_defined_fields:
            print(f"  ✅ {field} (defined in schema)")
        else:
            print(f"  ❌ {field} (NOT in schema)")
    
    print("\n📋 Schema defines these fields:")
    for field in schema_defined_fields:
        print(f"  📝 {field}")
    
    print("\n🔍 Field mismatches:")
    missing_in_schema = set(code_expected_fields) - set(schema_defined_fields)
    if missing_in_schema:
        print("  ❌ Missing in schema:")
        for field in missing_in_schema:
            print(f"    - {field}")
    else:
        print("  ✅ All code-expected fields are in schema")
    
    return len(missing_in_schema) == 0

def debug_database_state():
    """Check the current state of the database."""
    print("\n🔍 Debugging Current Database State...")
    
    try:
        db_path = 'data/kuzu'
        database = kuzu.Database(db_path)
        connection = kuzu.Connection(database)
        
        # Check if ImportJob table exists and has any data
        print("📊 Checking ImportJob table...")
        
        try:
            result = connection.execute("MATCH (j:ImportJob) RETURN count(j) as job_count")
            if result.has_next():
                count = result.get_next()[0]
                print(f"  📈 ImportJob nodes in database: {count}")
            else:
                print("  ❌ No ImportJob count returned")
        except Exception as e:
            print(f"  ❌ ImportJob table query failed: {e}")
        
        # Try to get recent ImportJob entries
        print("\n📋 Recent ImportJob entries:")
        try:
            result = connection.execute("MATCH (j:ImportJob) RETURN j ORDER BY j.created_at DESC LIMIT 5")
            count = 0
            while result.has_next():
                job_data = result.get_next()[0]
                job_dict = dict(job_data)
                count += 1
                print(f"  📝 Job {count}:")
                print(f"    ID: {job_dict.get('id', 'N/A')}")
                print(f"    Status: {job_dict.get('status', 'N/A')}")
                print(f"    User: {job_dict.get('user_id', 'N/A')}")
                print(f"    Error Message: {job_dict.get('error_message', 'N/A')}")
                print(f"    Error Messages: {job_dict.get('error_messages', 'N/A')}")
            
            if count == 0:
                print("  📭 No ImportJob entries found")
                
        except Exception as e:
            print(f"  ❌ Recent jobs query failed: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database state debugging failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Starting Custom Field Debugging...\n")
    
    # Run all debugging steps
    tests = [
        ("ImportJob Schema", debug_import_job_schema),
        ("Job Update Process", debug_job_update_process), 
        ("Schema vs Code", debug_schema_vs_code),
        ("Database State", debug_database_state)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"🧪 {test_name}")
        print('='*60)
        
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results[test_name] = False
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 DEBUGGING SUMMARY")
    print('='*60)
    
    for test_name, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED" 
        print(f"  {test_name}: {status}")
    
    if all(results.values()):
        print("\n🎉 All debugging tests passed!")
        print("💡 The schema should be working correctly.")
    else:
        print("\n❌ Some debugging tests failed!")
        print("💡 Check the failed tests above for issues to fix.")
    
    print("\n🔍 Next steps:")
    print("  1. If schema tests pass but import still fails, check the actual import code")
    print("  2. Look for any schema recreation or force reset issues") 
    print("  3. Verify that the application is using the same database path")
    print("  4. Check for any caching or connection pooling issues")
