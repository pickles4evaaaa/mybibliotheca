#!/usr/bin/env python3
"""
Direct test to search for existing custom metadata fields without Flask dependencies
"""
import os
import kuzu
from pathlib import Path

def search_existing_metadata():
    """Search for existing custom metadata fields with 'test' or 'test1'"""
    print("🔍 Searching for existing custom metadata fields...")
    
    try:
        # Connect directly to Kuzu database
        database_path = os.getenv('KUZU_DB_PATH', 'data/kuzu')
        Path(database_path).parent.mkdir(parents=True, exist_ok=True)
        
        print(f"🔌 Connecting to database at: {database_path}")
        database = kuzu.Database(database_path)
        connection = kuzu.Connection(database)
        
        # Query 1: Look for CustomField nodes with name containing 'test'
        print("\n📋 Searching CustomField nodes...")
        query1 = """
        MATCH (cf:CustomField)
        WHERE cf.name CONTAINS 'test'
        RETURN cf.id, cf.name, cf.value, cf.field_type
        """
        
        try:
            result = connection.execute(query1)
            
            found_fields = []
            while result.has_next():
                row = result.get_next()
                field_id = row[0]
                field_name = row[1] 
                field_value = row[2]
                field_type = row[3] if len(row) > 3 else None
                found_fields.append({
                    'id': field_id,
                    'name': field_name,
                    'value': field_value,
                    'type': field_type
                })
                print(f"  🏷️  Field: {field_name} = {field_value} (ID: {field_id})")
            
            if not found_fields:
                print("  ❌ No CustomField nodes found with 'test' in name")
        except Exception as e:
            print(f"  ❌ Error querying CustomField nodes: {e}")
            found_fields = []
        
        # Query 2: Look for HAS_CUSTOM_FIELD relationships 
        print("\n🔗 Searching HAS_CUSTOM_FIELD relationships...")
        query2 = """
        MATCH (u:User)-[r:HAS_CUSTOM_FIELD]->(cf:CustomField)
        WHERE cf.name CONTAINS 'test' OR r.field_name CONTAINS 'test'
        RETURN u.username, u.id, cf.name, cf.value, r.book_id, r.field_name
        """
        
        try:
            result2 = connection.execute(query2)
            
            found_rels = []
            while result2.has_next():
                row = result2.get_next()
                username = row[0]
                user_id = row[1]
                cf_name = row[2]
                cf_value = row[3]
                book_id = row[4]
                rel_field_name = row[5]
                found_rels.append({
                    'username': username,
                    'user_id': user_id,
                    'cf_name': cf_name,
                    'cf_value': cf_value,
                    'book_id': book_id,
                    'rel_field_name': rel_field_name
                })
                book_short = book_id[:8] + "..." if book_id and len(book_id) > 8 else book_id
                print(f"  🔗 User: {username} -> Book: {book_short} -> Field: {cf_name} = {cf_value}")
            
            if not found_rels:
                print("  ❌ No HAS_CUSTOM_FIELD relationships found with 'test' fields")
        except Exception as e:
            print(f"  ❌ Error querying HAS_CUSTOM_FIELD relationships: {e}")
            found_rels = []
        
        # Query 3: Look for all CustomField nodes (to see what exists)
        print("\n📊 All CustomField nodes in database:")
        query3 = "MATCH (cf:CustomField) RETURN cf.id, cf.name, cf.value LIMIT 10"
        
        try:
            result3 = connection.execute(query3)
            
            all_fields = []
            while result3.has_next():
                row = result3.get_next()
                field_id = row[0]
                field_name = row[1]
                field_value = row[2]
                all_fields.append({
                    'id': field_id,
                    'name': field_name,
                    'value': field_value
                })
                field_id_short = field_id[:8] + "..." if field_id and len(field_id) > 8 else field_id
                print(f"  📋 {field_name} = {field_value} (ID: {field_id_short})")
            
            if not all_fields:
                print("  ❌ No CustomField nodes found in database at all")
        except Exception as e:
            print(f"  ❌ Error querying all CustomField nodes: {e}")
            all_fields = []
        
        # Query 4: Look for all HAS_CUSTOM_FIELD relationships
        print("\n🔗 All HAS_CUSTOM_FIELD relationships in database:")
        query4 = """
        MATCH (u:User)-[r:HAS_CUSTOM_FIELD]->(cf:CustomField)
        RETURN u.username, cf.name, cf.value, r.book_id
        LIMIT 10
        """
        
        try:
            result4 = connection.execute(query4)
            
            all_rels = []
            while result4.has_next():
                row = result4.get_next()
                username = row[0]
                cf_name = row[1]
                cf_value = row[2]
                book_id = row[3]
                all_rels.append({
                    'username': username,
                    'cf_name': cf_name,
                    'cf_value': cf_value,
                    'book_id': book_id
                })
                book_short = book_id[:8] + "..." if book_id and len(book_id) > 8 else book_id
                print(f"  🔗 {username} -> {book_short} -> {cf_name} = {cf_value}")
            
            if not all_rels:
                print("  ❌ No HAS_CUSTOM_FIELD relationships found in database at all")
        except Exception as e:
            print(f"  ❌ Error querying all HAS_CUSTOM_FIELD relationships: {e}")
            all_rels = []
        
        # Summary
        print(f"\n📈 Summary:")
        print(f"  CustomField nodes with 'test': {len(found_fields)}")
        print(f"  HAS_CUSTOM_FIELD relationships with 'test': {len(found_rels)}")
        print(f"  Total CustomField nodes: {len(all_fields)}")
        print(f"  Total HAS_CUSTOM_FIELD relationships: {len(all_rels)}")
        
        # Query 5: Check if tables exist
        print(f"\n🏗️ Database structure check:")
        try:
            # Check if CustomField table exists
            test_query = "MATCH (cf:CustomField) RETURN COUNT(cf) LIMIT 1"
            result = connection.execute(test_query)
            if result.has_next():
                count = result.get_next()[0]
                print(f"  ✅ CustomField table exists with {count} nodes")
            else:
                print(f"  ✅ CustomField table exists but is empty")
        except Exception as e:
            print(f"  ❌ CustomField table may not exist: {e}")
        
        try:
            # Check if HAS_CUSTOM_FIELD relationship table exists
            test_query2 = "MATCH ()-[r:HAS_CUSTOM_FIELD]->() RETURN COUNT(r) LIMIT 1"
            result = connection.execute(test_query2)
            if result.has_next():
                count = result.get_next()[0]
                print(f"  ✅ HAS_CUSTOM_FIELD relationship table exists with {count} relationships")
            else:
                print(f"  ✅ HAS_CUSTOM_FIELD relationship table exists but is empty")
        except Exception as e:
            print(f"  ❌ HAS_CUSTOM_FIELD relationship table may not exist: {e}")
        
        return {
            'test_fields': found_fields,
            'test_relationships': found_rels,
            'all_fields': all_fields,
            'all_relationships': all_rels
        }
        
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    search_existing_metadata()
