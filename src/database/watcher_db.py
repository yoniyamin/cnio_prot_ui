# src/database/watcher_db.py
import sqlite3
from pathlib import Path
import os
from datetime import datetime
from src.logging_utils import get_logger
import threading

# Get a logger specific to this module
logger = get_logger(__name__)

# Singleton instance and lock
_watcher_db_instance = None
_watcher_db_lock = threading.Lock()

class WatcherDB:
    def __new__(cls, db_path="config/watchers.db"):
        global _watcher_db_instance
        if _watcher_db_instance is None:
            with _watcher_db_lock:
                if _watcher_db_instance is None:
                    _watcher_db_instance = super(WatcherDB, cls).__new__(cls)
                    _watcher_db_instance._initialized = False
        return _watcher_db_instance
        
    def __init__(self, db_path="config/watchers.db"):
        # Skip initialization if already done
        if hasattr(self, '_initialized') and self._initialized:
            return
            
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
        self._initialized = True

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
            
            # Modified the captured_files table to allow NULL in job_id
            conn.execute("""
                CREATE TABLE IF NOT EXISTS captured_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT,
                    watcher_id INTEGER NOT NULL,
                    file_name TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    capture_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (watcher_id) REFERENCES watchers(id)
                )
            """)
            conn.commit()
            logger.info("Tables created successfully.")
        except sqlite3.Error as e:
            logger.error(f"Failed to create tables: {e}", exc_info=True)
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
                watcher_id = cursor.lastrowid
                logger.info(f"Added new watcher ID {watcher_id} for {job_name_prefix} monitoring {folder_path}")
                return watcher_id
            except sqlite3.Error as e:
                logger.error(f"Failed to add watcher: {e}", exc_info=True)
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
                file_id = cursor.lastrowid
                logger.info(f"Captured file {file_name} (ID: {file_id}) for watcher {watcher_id}")
                return file_id
            except sqlite3.Error as e:
                logger.error(f"Failed to add captured file: {e}", exc_info=True)
                raise
                
    def update_captured_file_job_id(self, file_path, job_id):
        """Update the job_id for a captured file."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                conn.execute("""
                    UPDATE captured_files SET job_id = ? WHERE file_path = ?
                """, (job_id, file_path))
                conn.commit()
                logger.info(f"Updated job_id to {job_id} for file {file_path}")
                return True
            except sqlite3.Error as e:
                logger.error(f"Failed to update job_id for file: {e}", exc_info=True)
                return False

    def get_watchers(self, status=None):
        """Retrieve watcher configurations, optionally filtered by status."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                if status:
                    cursor = conn.execute("SELECT * FROM watchers WHERE status = ?", (status,))
                    logger.debug(f"Retrieved watchers with status: {status}")
                else:
                    cursor = conn.execute("SELECT * FROM watchers")
                    logger.debug(f"Retrieved all watchers")
                return cursor.fetchall()
            except sqlite3.Error as e:
                logger.error(f"Failed to retrieve watchers: {e}", exc_info=True)
                raise

    def get_captured_files(self, watcher_id=None):
        """Retrieve captured files, optionally filtered by watcher_id."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                if watcher_id:
                    cursor = conn.execute("SELECT * FROM captured_files WHERE watcher_id = ?", (watcher_id,))
                    logger.debug(f"Retrieved captured files for watcher: {watcher_id}")
                else:
                    cursor = conn.execute("SELECT * FROM captured_files")
                    logger.debug(f"Retrieved all captured files")
                return cursor.fetchall()
            except sqlite3.Error as e:
                logger.error(f"Failed to retrieve captured files: {e}", exc_info=True)
                raise

    def delete_test_watchers(self, prefix="test_"):
        """Delete watchers with a specific job_name_prefix (e.g., test ones)."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                cursor = conn.execute("""
                    DELETE FROM watchers WHERE job_name_prefix LIKE ?
                """, (f"{prefix}%",))
                conn.commit()
                logger.info(f"Deleted {cursor.rowcount} test watchers with prefix '{prefix}'.")
            except sqlite3.Error as e:
                logger.error(f"Failed to delete test watchers: {e}", exc_info=True)
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
                logger.error(f"Failed to update watcher status: {e}", exc_info=True)
                raise

    def update_execution_time(self, watcher_id, execution_time):
        """Update the execution time for a watcher."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                conn.execute("""
                    UPDATE watchers SET execution_time = ? WHERE id = ?
                """, (execution_time, watcher_id))
                conn.commit()
                logger.info(f"Updated execution time for watcher {watcher_id}: {execution_time}")
            except sqlite3.Error as e:
                logger.error(f"Failed to update execution time: {e}", exc_info=True)
                raise

    def get_interrupted_watchers(self):
        """Get watchers that were monitoring when the system was last shut down.
        These are candidates for restart during system recovery."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                cursor = conn.execute("""
                    SELECT * FROM watchers 
                    WHERE status = 'monitoring'
                    ORDER BY creation_time DESC
                """)
                result = cursor.fetchall()
                logger.info(f"Found {len(result)} interrupted watchers")
                return result
            except sqlite3.Error as e:
                logger.error(f"Failed to retrieve interrupted watchers: {e}", exc_info=True)
                return []

    def file_exists_in_watcher(self, watcher_id, file_name):
        """Check if a file has already been captured by a watcher"""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM captured_files 
                    WHERE watcher_id = ? AND file_name = ?
                """, (watcher_id, file_name))
                count = cursor.fetchone()[0]
                logger.debug(f"File existence check for {file_name} in watcher {watcher_id}: {'exists' if count > 0 else 'not found'}")
                return count > 0
            except sqlite3.Error as e:
                logger.error(f"Failed to check if file exists: {e}", exc_info=True)
                return False

    def get_captured_files_count(self, watcher_id):
        """Get the count of files captured by a specific watcher"""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM captured_files 
                    WHERE watcher_id = ?
                """, (watcher_id,))
                count = cursor.fetchone()[0]
                logger.debug(f"Watcher {watcher_id} has {count} captured files")
                return count
            except sqlite3.Error as e:
                logger.error(f"Failed to get captured files count: {e}", exc_info=True)
                return 0

    def close(self):
        """No persistent connection to close."""
        logger.debug("Database connection closed (no persistent connection).")
