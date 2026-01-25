import sqlite3
import uuid
import datetime
import os
import sys

# CONFIGURATION
# We mount the folder containing books.db to /data inside the container
DB_FILE = "/data/books.db"

def sync_library():
    if not os.path.exists(DB_FILE):
        print(f"Error: Database file '{DB_FILE}' not found.")
        sys.exit(1)

    print(f"Connecting to SQLite database at {DB_FILE}...")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. Get All Users
    users = [r[0] for r in cursor.execute("SELECT id FROM user").fetchall()]
    print(f"Found {len(users)} users.")

    # 2. Get the 'Master List' of books (One copy of every valid ISBN)
    # We ignore books without ISBNs to avoid duplicates of custom items
    print("Fetching master book list...")
    cursor.execute("SELECT * FROM book WHERE isbn IS NOT NULL AND isbn != '' GROUP BY isbn")
    source_books = cursor.fetchall()
    
    # Get column names dynamically so we don't break if schema changes
    col_names = [description[0] for description in cursor.description]
    col_idx = {name: index for index, name in enumerate(col_names)}

    books_added = 0

    for target_user_id in users:
        # Get list of ISBNs this specific user already owns
        user_isbns = set(r[0] for r in cursor.execute("SELECT isbn FROM book WHERE user_id = ?", (target_user_id,)).fetchall())
        
        for book_row in source_books:
            isbn = book_row[col_idx['isbn']]
            
            # If the user doesn't have this book, GIVE IT TO THEM
            if isbn not in user_isbns:
                
                # Prepare the new row data
                insert_cols = []
                insert_vals = []
                
                for col in col_names:
                    # Skip 'id' (Primary Key auto-increments)
                    if col == 'id': 
                        continue
                    
                    val = book_row[col_idx[col]]
                    
                    # --- DATA TRANSFORMATIONS ---
                    if col == 'user_id': 
                        val = target_user_id
                    elif col == 'uid': 
                        val = str(uuid.uuid4()) # Generate a fresh unique ID for the system
                    elif col == 'start_date': 
                        val = None # Reset progress
                    elif col == 'finish_date': 
                        val = None # Reset progress
                    elif col == 'want_to_read': 
                        val = 0 # Reset 'want to read' status
                    elif col == 'created_at': 
                        # CRITICAL FIX: Use space separator, not 'T'
                        # Format: YYYY-MM-DD HH:MM:SS.mmmmmm
                        val = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    # -----------------------------

                    insert_cols.append(col)
                    insert_vals.append(val)
                
                # Construct the INSERT query
                placeholders = ','.join(['?'] * len(insert_vals))
                columns_str = ','.join(insert_cols)
                query = f"INSERT INTO book ({columns_str}) VALUES ({placeholders})"
                
                cursor.execute(query, insert_vals)
                books_added += 1

    conn.commit()
    conn.close()
    print(f"Sync Complete. {books_added} new books were added across all users.")

if __name__ == "__main__":
    sync_library()