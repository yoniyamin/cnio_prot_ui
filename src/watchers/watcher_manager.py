# src/watchers/watcher_manager.py
import threading
import time
from src.utils import logger, log_dir  # Import logger from utils instead of app
from src.watchers.file_watcher import FileWatcher


class WatcherManager:
    def __init__(self, db, job_queue_manager):
        self.db = db
        self.job_queue_manager = job_queue_manager
        self.watchers = {}
        self.threads = {}  # Store threads for proper management

    def load_watchers(self):
        """Load all watchers from the database."""
        watchers = self.db.get_watchers(status="monitoring")
        for watcher in watchers:
            watcher_id = watcher[0]
            self.watchers[watcher_id] = FileWatcher(watcher_id, self.db, self.job_queue_manager)

    def start_all(self):
        """Start all watchers in separate threads and store the threads."""
        for watcher_id, watcher in self.watchers.items():
            thread = threading.Thread(target=watcher.start, daemon=True)
            self.threads[watcher_id] = thread
            thread.start()
        # Give threads a moment to initialize
        time.sleep(1)  # Adjust this delay if needed based on your system

    def stop_all(self):
        """Stop all watchers and wait for their threads to finish."""
        for watcher in self.watchers.values():
            watcher.stop()
        # Wait for all threads to complete
        for thread in self.threads.values():
            if thread.is_alive():
                thread.join(timeout=5)  # Timeout to prevent hanging
        self.threads.clear()

    def create_single_watcher(self, watcher_id):
        """Create and return a single FileWatcher without starting it."""
        logger.info(f"Creating FileWatcher for ID: {watcher_id}")
        watcher = FileWatcher(watcher_id, self.db, self.job_queue_manager)
        self.watchers[watcher_id] = watcher
        return watcher

    def stop_watcher(self, watcher_id):
        """Stop a specific watcher and its thread."""
        logger.info(f"Attempting to stop watcher {watcher_id}")

        # Update status in database (already done in the API endpoint)
        # self.db.update_watcher_status(watcher_id, 'cancelled')

        # Find and stop the watcher instance
        if watcher_id in self.watchers:
            logger.info(f"Found watcher {watcher_id} in active watchers, stopping it")
            watcher = self.watchers[watcher_id]
            watcher.stop()  # Call the stop method on the FileWatcher

            # If there's a thread for this watcher, wait for it to finish
            if watcher_id in self.threads:
                logger.info(f"Joining thread for watcher {watcher_id}")
                self.threads[watcher_id].join(timeout=5)  # Wait up to 5 seconds
                del self.threads[watcher_id]

            # Remove the watcher from our tracking
            del self.watchers[watcher_id]
            return True

        # Even if not in our tracking, try to create and stop it
        # This is useful for watchers started in other processes/threads
        try:
            logger.info(f"Creating new FileWatcher for {watcher_id} to stop it")
            temp_watcher = FileWatcher(watcher_id, self.db, self.job_queue_manager)
            temp_watcher.stop()
            return True
        except Exception as e:
            logger.error(f"Error stopping watcher {watcher_id}: {str(e)}")
            return False