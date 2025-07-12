#!/usr/bin/env python3
"""
Script to manually create custom field tables via SQL
"""

import subprocess
import sys

# SQL commands to create the tables
sql_commands = [
    """
    CREATE NODE TABLE IF NOT EXISTS GlobalMetadata (
        book_id STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (book_id)
    );
    """,
    """
    CREATE REL TABLE IF NOT EXISTS HAS_GLOBAL_METADATA (
        FROM Book TO GlobalMetadata,
        global_custom_fields STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
    """
]

print("🛠️ [MANUAL] Creating custom field tables...")

# Note: This is just for reference - we can't execute these directly 
# because of the database lock. The tables should be created automatically
# when the KuzuCustomFieldService is first initialized.

for i, sql in enumerate(sql_commands, 1):
    print(f"📝 [MANUAL] Table {i}: {sql.strip()}")

print("""
⚠️  [MANUAL] These tables should be created automatically when you:
1. Visit a book page (triggers custom field service initialization)
2. Try to save custom field values
3. The service will log: "📝 [CUSTOM_FIELDS] Created GlobalMetadata table"

If you don't see those logs, there might be an issue with the service initialization.
""")
