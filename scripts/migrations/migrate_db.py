#!/usr/bin/env python3
"""
Database migration script to add new metadata columns to existing Book table.
Run this script after updating the models to add the new fields.
"""

import sqlite3
import os

def migrate_database():
    # Default database path
    db_path = 'books.db'
    
    # Also check instance directory
    if not os.path.exists(db_path):
        db_path = 'instance/books.db'
    
    if not os.path.exists(db_path):
        print(f"Database file not found. Checked: books.db and instance/books.db")
        return
    
    print(f"Migrating database: {db_path}")
    
    # Connect directly to SQLite to add columns
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # List of new columns to add
    new_columns = [
        ('description', 'TEXT'),
        ('published_date', 'VARCHAR(50)'),
        ('page_count', 'INTEGER'),
        ('categories', 'VARCHAR(500)'),
        ('publisher', 'VARCHAR(255)'),
        ('language', 'VARCHAR(10)'),
        ('average_rating', 'REAL'),
        ('rating_count', 'INTEGER')
    ]
    
    # Check which columns already exist
    cursor.execute("PRAGMA table_info(book)")
    existing_columns = [row[1] for row in cursor.fetchall()]
    print(f"Existing columns: {existing_columns}")
    
    # Add missing columns
    for column_name, column_type in new_columns:
        if column_name not in existing_columns:
            try:
                cursor.execute(f"ALTER TABLE book ADD COLUMN {column_name} {column_type}")
                print(f"Added column: {column_name}")
            except sqlite3.Error as e:
                print(f"Error adding column {column_name}: {e}")
        else:
            print(f"Column {column_name} already exists, skipping...")
    
    conn.commit()
    conn.close()
    print("Database migration completed successfully!")

if __name__ == '__main__':
    migrate_database()
