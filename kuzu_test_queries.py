#!/usr/bin/env python3
"""
Simple test to verify KuzuDB query behavior directly
"""

# Test what happens when we save and retrieve from KuzuDB directly
test_query = """
MATCH (u:User {id: "ee547875-a521-4ba8-8472-9707a0331bf7"}), (b:Book {id: "d9da4fb9-d708-42e8-83a4-36795fae70f0"})
CREATE (u)-[r:HAS_PERSONAL_METADATA {
    personal_custom_fields: '{"owned_copies": "5", "test_field": "test_value"}',
    created_at: datetime(),
    updated_at: datetime()
}]->(b)
"""

retrieve_query = """
MATCH (u:User {id: "ee547875-a521-4ba8-8472-9707a0331bf7"})-[r:HAS_PERSONAL_METADATA]->(b:Book {id: "d9da4fb9-d708-42e8-83a4-36795fae70f0"})
RETURN r.personal_custom_fields
"""

print("Test queries created. To test manually:")
print("1. Save query:")
print(test_query)
print("\n2. Retrieve query:")
print(retrieve_query)
print("\nThese can be run in KuzuDB directly to see the raw behavior.")
