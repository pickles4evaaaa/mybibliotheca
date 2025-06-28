#!/usr/bin/env python3
"""
Simple test to search for existing custom metadata fields
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.infrastructure.kuzu_graph import get_graph_storage

def search_existing_metadata():
    """Search for existing custom metadata fields with 'test' or 'test1'"""
    print("🔍 Searching for existing custom metadata fields...")
    
    try:
        # Get graph storage
        graph_storage = get_graph_storage()
        
        # Query 1: Look for CustomField nodes with name containing 'test'
        print("\n📋 Searching CustomField nodes...")
        query1 = """
        MATCH (cf:CustomField)
        WHERE cf.name CONTAINS 'test'
        RETURN cf.id, cf.name, cf.value, cf.field_type
        """
        
        result = graph_storage.connection.execute(query1)
        
        found_fields = []
        while result.has_next():
            row = result.get_next()
            field_id = row[0]
            field_name = row[1] 
            field_value = row[2]
            field_type = row[3]
            found_fields.append({
                'id': field_id,
                'name': field_name,
                'value': field_value,
                'type': field_type
            })
            print(f"  🏷️  Field: {field_name} = {field_value} (ID: {field_id})")
        
        if not found_fields:
            print("  ❌ No CustomField nodes found with 'test' in name")
        
        # Query 2: Look for HAS_CUSTOM_FIELD relationships 
        print("\n🔗 Searching HAS_CUSTOM_FIELD relationships...")
        query2 = """
        MATCH (u:User)-[r:HAS_CUSTOM_FIELD]->(cf:CustomField)
        WHERE cf.name CONTAINS 'test' OR r.field_name CONTAINS 'test'
        RETURN u.username, u.id, cf.name, cf.value, r.book_id, r.field_name
        """
        
        result2 = graph_storage.connection.execute(query2)
        
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
            print(f"  🔗 User: {username} -> Book: {book_id[:8]}... -> Field: {cf_name} = {cf_value}")
        
        if not found_rels:
            print("  ❌ No HAS_CUSTOM_FIELD relationships found with 'test' fields")
        
        # Query 3: Look for all CustomField nodes (to see what exists)
        print("\n📊 All CustomField nodes in database:")
        query3 = "MATCH (cf:CustomField) RETURN cf.id, cf.name, cf.value LIMIT 10"
        
        result3 = graph_storage.connection.execute(query3)
        
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
            print(f"  📋 {field_name} = {field_value} (ID: {field_id[:8]}...)")
        
        if not all_fields:
            print("  ❌ No CustomField nodes found in database at all")
        
        # Query 4: Look for all HAS_CUSTOM_FIELD relationships
        print("\n🔗 All HAS_CUSTOM_FIELD relationships in database:")
        query4 = """
        MATCH (u:User)-[r:HAS_CUSTOM_FIELD]->(cf:CustomField)
        RETURN u.username, cf.name, cf.value, r.book_id
        LIMIT 10
        """
        
        result4 = graph_storage.connection.execute(query4)
        
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
            print(f"  🔗 {username} -> {book_id[:8]}... -> {cf_name} = {cf_value}")
        
        if not all_rels:
            print("  ❌ No HAS_CUSTOM_FIELD relationships found in database at all")
        
        # Summary
        print(f"\n📈 Summary:")
        print(f"  CustomField nodes with 'test': {len(found_fields)}")
        print(f"  HAS_CUSTOM_FIELD relationships with 'test': {len(found_rels)}")
        print(f"  Total CustomField nodes: {len(all_fields)}")
        print(f"  Total HAS_CUSTOM_FIELD relationships: {len(all_rels)}")
        
        return {
            'test_fields': found_fields,
            'test_relationships': found_rels,
            'all_fields': all_fields,
            'all_relationships': all_rels
        }
        
    except Exception as e:
        print(f"❌ Error searching metadata: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    search_existing_metadata()
