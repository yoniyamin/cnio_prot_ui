# src/database/watcher_db.py
import sqlite3
from pathlib import Path
import os
from datetime import datetime
from src.utils import logger

class WatcherDB:
    def __init__(self, db_path="config/watchers.db"):
        script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        project_root = script_dir.parent.parent
        self.db_path = project_root / db_path
        self.db_dir = self.db_path.parent

        if not self.db_dir.exists():
            self.db_dir.mkdir(parents=True, exist_ok=True)

        # Initial connection just for table creation
        with sqlite3.connect(str(self.db_path)) as conn:
            self._create_tables(conn)

        logger.debug(f"Database initialized at: {self.db_path}")


    def _create_tables(self, conn):
        """Create the watchers and captured_files tables."""
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS watchers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    folder_path TEXT NOT NULL,
                    file_pattern TEXT NOT NULL,
                    job_type TEXT NOT NULL,
                    job_demands TEXT NOT NULL,
                    job_name_prefix TEXT NOT NULL,
                    creation_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    execution_time DATETIME,
                    status TEXT DEFAULT 'monitoring',
                    completion_time DATETIME
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS captured_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    watcher_id INTEGER NOT NULL,
                    file_name TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    capture_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (watcher_id) REFERENCES watchers(id)
                )
            """)
            conn.commit()
            print("Tables created successfully.")
        except sqlite3.Error as e:
            print(f"Failed to create tables: {e}")
            raise

    def add_watcher(self, folder_path, file_pattern, job_type, job_demands, job_name_prefix):
        """Add a new watcher configuration to the database."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                cursor = conn.execute("""
                    INSERT INTO watchers (folder_path, file_pattern, job_type, job_demands, job_name_prefix)
                    VALUES (?, ?, ?, ?, ?)
                """, (folder_path, file_pattern, job_type, job_demands, job_name_prefix))
                conn.commit()
                return cursor.lastrowid
            except sqlite3.Error as e:
                print(f"Failed to add watcher: {e}")
                raise

    def add_captured_file(self, job_id, watcher_id, file_name, file_path):
        """Add a captured file to the database."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                cursor = conn.execute("""
                    INSERT INTO captured_files (job_id, watcher_id, file_name, file_path)
                    VALUES (?, ?, ?, ?)
                """, (job_id, watcher_id, file_name, file_path))
                conn.commit()
                return cursor.lastrowid
            except sqlite3.Error as e:
                print(f"Failed to add captured file: {e}")
                raise

    def get_watchers(self, status=None):
        """Retrieve watcher configurations, optionally filtered by status."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                if status:
                    cursor = conn.execute("SELECT * FROM watchers WHERE status = ?", (status,))
                else:
                    cursor = conn.execute("SELECT * FROM watchers")
                return cursor.fetchall()
            except sqlite3.Error as e:
                print(f"Failed to retrieve watchers: {e}")
                raise

    def get_captured_files(self, watcher_id=None):
        """Retrieve captured files, optionally filtered by watcher_id."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                if watcher_id:
                    cursor = conn.execute("SELECT * FROM captured_files WHERE watcher_id = ?", (watcher_id,))
                else:
                    cursor = conn.execute("SELECT * FROM captured_files")
                return cursor.fetchall()
            except sqlite3.Error as e:
                print(f"Failed to retrieve captured files: {e}")
                raise

    def delete_test_watchers(self, prefix="test_"):
        """Delete watchers with a specific job_name_prefix (e.g., test ones)."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                cursor = conn.execute("""
                    DELETE FROM watchers WHERE job_name_prefix LIKE ?
                """, (f"{prefix}%",))
                conn.commit()
                print(f"Deleted {cursor.rowcount} test watchers with prefix '{prefix}'.")
            except sqlite3.Error as e:
                print(f"Failed to delete test watchers: {e}")
                raise

    def update_watcher_status(self, watcher_id, status):
        """Update the status and completion_time of a watcher."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                if status in ["completed", "cancelled"]:  # Add 'cancelled' here
                    conn.execute("""
                        UPDATE watchers SET status = ?, completion_time = CURRENT_TIMESTAMP WHERE id = ?
                    """, (status, watcher_id))
                else:
                    conn.execute("""
                        UPDATE watchers SET status = ? WHERE id = ?
                    """, (status, watcher_id))
                conn.commit()
                logger.info(
                    f"Updated watcher {watcher_id} status to '{status}'{' with completion time' if status in ['completed', 'cancelled'] else ''}."
                )
            except sqlite3.Error as e:
                logger.error(f"Failed to update watcher status: {e}")
                raise

    def update_execution_time(self, watcher_id, execution_time):
        """Update the execution time for a watcher."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                conn.execute("""
                    UPDATE watchers SET execution_time = ? WHERE id = ?
                """, (execution_time, watcher_id))
                conn.commit()
            except sqlite3.Error as e:
                print(f"Failed to update execution time: {e}")
                raise


    def close(self):
        """No persistent connection to close."""
        print("Database connection closed (no persistent connection).")
