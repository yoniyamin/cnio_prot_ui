# src/watchers/watcher_manager.py
import threading
import time
import queue
from app import logger
from src.watchers.file_watcher import FileWatcher
from src.handlers.run_maxquant import MaxQuant_handler

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
        """Stop a specific watcher."""
        if watcher_id in self.watchers:
            logger.info(f"Stopping watcher {watcher_id}")
            self.watchers[watcher_id].stop()

            # If there's a thread for this watcher, wait for it to finish
            if watcher_id in self.threads:
                self.threads[watcher_id].join(timeout=5)
                del self.threads[watcher_id]

            # Remove from active watchers
            del self.watchers[watcher_id]
            return True
        return False