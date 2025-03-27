# src/watchers/watcher_manager.py
import threading
import time
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