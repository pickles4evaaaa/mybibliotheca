# Bibliotheca Safe Sync

A utility for **MyBibliotheca** that automatically synchronizes book inventories between all users on the server.

### What it does
If you have multiple users (e.g., family members) sharing a MyBibliotheca instance, this script ensures that when **User A** adds a book, it automatically appears in **User B's** library.

* **Inventory Sync:** Ensures all users have all books.
* **Privacy Preserved:** "Read" status, "Start Date," and "Finish Date" are **NOT** synced. User B will see the new book as "Unread," allowing them to track their own progress.
* **Safety First:** Includes an activity monitor that prevents the sync from running if users are actively browsing the site (checks Docker logs for recent activity).

### Requirements
* MyBibliotheca instance running on **Docker**.
* Database must be **SQLite** (default for most setups).
* Host machine must have `bash` (Linux/macOS).

### Installation

1.  Clone this repository or copy the `safe_sync` folder to your server.
2.  Ensure `run_sync_safe.sh` is executable:
    ```bash
    chmod +x run_sync_safe.sh
    ```

### Configuration

You must edit `run_sync_safe.sh` to point to your specific data volume.

1.  **Find your Data Path:**
    Run this command to see where your Bibliotheca database lives on the host:
    ```bash
    docker inspect -f '{{ range .Mounts }}Source: {{.Source}}{{ "\n" }}{{ end }}' bibliotheca
    ```
    *(Replace `bibliotheca` with your container name if different)*

2.  **Edit the Script:**
    Open `run_sync_safe.sh` and update the `HOST_DATA_PATH` variable:
    ```bash
    # Example
    HOST_DATA_PATH="/var/lib/docker/volumes/my_library_data/_data"
    ```

### Usage

#### Manual Run (Test)
To run the sync immediately (bypassing the activity check):
```bash
./run_sync_safe.sh -override