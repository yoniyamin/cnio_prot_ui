# src/watchers/watcher_manager.py
import threading
import time
from src.logging_utils import get_logger
from src.watchers.file_watcher import FileWatcher
from src.core.event_bus import event_bus, EventType, JobEvent

logger = get_logger(__name__)


class WatcherManager:
    def __init__(self, db, job_queue_manager):
        self.db = db
        self.job_queue_manager = job_queue_manager
        self.watchers = {}
        self.threads = {}  # Store threads for proper management

        # Subscribe to watcher-related events
        event_bus.subscribe(EventType.WATCHER_CREATED, self._handle_watcher_created)
        event_bus.subscribe(EventType.WATCHER_STATUS_CHANGED, self._handle_watcher_status_changed)

        logger.info("WatcherManager initialized with event bus integration")

    def _handle_watcher_created(self, event: JobEvent):
        """Handle watcher creation events"""
        try:
            watcher_id = event.data.get('watcher_id')
            if watcher_id and watcher_id not in self.watchers:
                logger.info(f"Creating watcher {watcher_id} from event")
                watcher = self.create_single_watcher(watcher_id)
                if watcher:
                    thread = threading.Thread(target=watcher.start, daemon=True, name=f"Watcher-{watcher_id}")
                    self.threads[watcher_id] = thread
                    thread.start()
                    logger.info(f"Started watcher {watcher_id} from creation event")
        except Exception as e:
            logger.error(f"Error handling watcher created event: {e}", exc_info=True)

    def _handle_watcher_status_changed(self, event: JobEvent):
        """Handle watcher status change events"""
        try:
            watcher_id = event.data.get('watcher_id')
            new_status = event.data.get('new_status')

            if watcher_id in self.watchers:
                logger.info(f"Watcher {watcher_id} status changed to {new_status}")

                # If watcher is cancelled or completed, stop it
                if new_status in ['cancelled', 'completed']:
                    self.stop_watcher(watcher_id)

        except Exception as e:
            logger.error(f"Error handling watcher status changed event: {e}", exc_info=True)

    def load_watchers(self):
        """Load all active watchers from the database"""
        try:
            watchers = self.db.get_watchers(status="monitoring")
            logger.info(f"Loading {len(watchers)} active watchers from database")

            for watcher in watchers:
                watcher_id = watcher[0]
                try:
                    # Skip if already loaded
                    if watcher_id in self.watchers:
                        logger.info(f"Watcher {watcher_id} already loaded, skipping")
                        continue

                    logger.info(f"Loading watcher ID: {watcher_id}")
                    self.watchers[watcher_id] = FileWatcher(watcher_id, self.db, self.job_queue_manager)
                    logger.info(f"Successfully loaded watcher {watcher_id}")

                    # Publish watcher loaded event
                    event = JobEvent(
                        event_type=EventType.WATCHER_STATUS_CHANGED,
                        job_id="",
                        data={
                            'watcher_id': watcher_id,
                            'old_status': 'inactive',
                            'new_status': 'loaded'
                        },
                        timestamp=time.time()
                    )
                    event_bus.publish(event)

                except Exception as e:
                    logger.error(f"Error loading watcher {watcher_id}: {e}", exc_info=True)

            logger.info(f"Finished loading watchers. {len(self.watchers)} watchers active.")
        except Exception as e:
            logger.error(f"Error in load_watchers: {e}", exc_info=True)

    def start_all(self):
        """Start all watchers in separate threads"""
        logger.info(f"Starting all watchers ({len(self.watchers)})...")

        for watcher_id, watcher in self.watchers.items():
            try:
                # Check if thread already exists and is alive
                if watcher_id in self.threads and self.threads[watcher_id].is_alive():
                    logger.warning(f"Watcher {watcher_id} already has an active thread, skipping")
                    continue

                logger.info(f"Starting watcher {watcher_id} in new thread")
                thread = threading.Thread(target=watcher.start, daemon=True, name=f"Watcher-{watcher_id}")
                self.threads[watcher_id] = thread
                thread.start()
                logger.info(f"Thread started for watcher {watcher_id}")

                # Publish watcher started event
                event = JobEvent(
                    event_type=EventType.WATCHER_STATUS_CHANGED,
                    job_id="",
                    data={
                        'watcher_id': watcher_id,
                        'old_status': 'loaded',
                        'new_status': 'running'
                    },
                    timestamp=time.time()
                )
                event_bus.publish(event)

            except Exception as e:
                logger.error(f"Error starting watcher {watcher_id}: {e}", exc_info=True)

        # Brief pause for initialization
        time.sleep(1)

        # Check thread status
        active_count = 0
        for watcher_id, thread in self.threads.items():
            is_alive = thread.is_alive()
            logger.info(f"Watcher {watcher_id} thread is alive: {is_alive}")
            if is_alive:
                active_count += 1

        logger.info(f"Started {active_count} out of {len(self.threads)} watcher threads")

    def stop_all_watchers(self):
        """Stop all watchers and wait for their threads to finish"""
        logger.info("Stopping all watchers...")

        # First signal all watchers to stop
        for watcher_id, watcher in list(self.watchers.items()):
            try:
                logger.info(f"Stopping watcher {watcher_id}")
                watcher.stop()

                # Publish watcher stopped event
                event = JobEvent(
                    event_type=EventType.WATCHER_STATUS_CHANGED,
                    job_id="",
                    data={
                        'watcher_id': watcher_id,
                        'old_status': 'running',
                        'new_status': 'stopped'
                    },
                    timestamp=time.time()
                )
                event_bus.publish(event)

            except Exception as e:
                logger.error(f"Error stopping watcher {watcher_id}: {e}")

        # Wait for threads to complete
        logger.info("Waiting for watcher threads to complete...")
        for watcher_id, thread in list(self.threads.items()):
            try:
                if thread.is_alive():
                    logger.info(f"Joining thread for watcher {watcher_id}")
                    thread.join(timeout=5)

                    if thread.is_alive():
                        logger.warning(f"Thread for watcher {watcher_id} did not exit within timeout")
                    else:
                        logger.info(f"Thread for watcher {watcher_id} has exited")
            except Exception as e:
                logger.error(f"Error joining thread for watcher {watcher_id}: {e}")

        # Clear collections
        self.threads.clear()
        self.watchers.clear()
        logger.info("All watchers stopped")

    def create_single_watcher(self, watcher_id):
        """Create and return a single FileWatcher without starting it"""
        try:
            logger.info(f"Creating FileWatcher for ID: {watcher_id}")

            # Get watcher data from database
            watcher_data = next((w for w in self.db.get_watchers() if w[0] == watcher_id), None)
            if not watcher_data:
                logger.error(f"Watcher {watcher_id} not found in database")
                raise ValueError(f"Watcher ID {watcher_id} not found in database")

            logger.info(f"Watcher data from DB: {watcher_data}")

            # Create the watcher instance
            watcher = FileWatcher(watcher_id, self.db, self.job_queue_manager)
            self.watchers[watcher_id] = watcher

            # Publish watcher created event
            event = JobEvent(
                event_type=EventType.WATCHER_CREATED,
                job_id="",
                data={
                    'watcher_id': watcher_id,
                    'folder_path': watcher_data[1],
                    'job_type': watcher_data[3],
                    'status': watcher_data[8]
                },
                timestamp=time.time()
            )
            event_bus.publish(event)

            logger.info(f"Successfully created watcher {watcher_id}")
            return watcher
        except Exception as e:
            logger.error(f"Error creating single watcher {watcher_id}: {e}", exc_info=True)
            raise

    def stop_watcher(self, watcher_id):
        """Stop a specific watcher and its thread"""
        logger.info(f"Attempting to stop watcher {watcher_id}")

        # Find and stop the watcher instance
        if watcher_id in self.watchers:
            try:
                logger.info(f"Found watcher {watcher_id} in active watchers, stopping it")
                watcher = self.watchers[watcher_id]
                watcher.stop()

                # Wait for thread to finish
                if watcher_id in self.threads:
                    logger.info(f"Joining thread for watcher {watcher_id}")
                    self.threads[watcher_id].join(timeout=5)

                    if not self.threads[watcher_id].is_alive():
                        logger.info(f"Thread for watcher {watcher_id} has exited")
                        del self.threads[watcher_id]
                    else:
                        logger.warning(f"Thread for watcher {watcher_id} did not exit within timeout")

                # Remove from tracking
                del self.watchers[watcher_id]
                logger.info(f"Watcher {watcher_id} successfully stopped and removed")

                # Publish watcher stopped event
                event = JobEvent(
                    event_type=EventType.WATCHER_STATUS_CHANGED,
                    job_id="",
                    data={
                        'watcher_id': watcher_id,
                        'old_status': 'running',
                        'new_status': 'stopped'
                    },
                    timestamp=time.time()
                )
                event_bus.publish(event)

                return True
            except Exception as e:
                logger.error(f"Error stopping watcher {watcher_id}: {e}", exc_info=True)
                return False

        # Try to create and stop it (for watchers started elsewhere)
        try:
            logger.info(f"Creating temporary FileWatcher for {watcher_id} to stop it")
            temp_watcher = FileWatcher(watcher_id, self.db, self.job_queue_manager)
            temp_watcher.stop()
            logger.info(f"Successfully stopped temporary watcher {watcher_id}")
            return True
        except Exception as e:
            logger.error(f"Error stopping watcher {watcher_id}: {e}", exc_info=True)
            return False

    def force_rescan_watcher(self, watcher_id):
        """Force a specific watcher to rescan its directory"""
        logger.info(f"Forcing rescan for watcher {watcher_id}")

        try:
            # Use existing watcher if available
            if watcher_id in self.watchers:
                watcher = self.watchers[watcher_id]
                logger.info(f"Found watcher {watcher_id} in active watchers, forcing rescan")
                watcher.force_rescan()

                # Publish rescan event
                event = JobEvent(
                    event_type=EventType.WATCHER_STATUS_CHANGED,
                    job_id="",
                    data={
                        'watcher_id': watcher_id,
                        'action': 'rescan',
                        'timestamp': time.time()
                    },
                    timestamp=time.time()
                )
                event_bus.publish(event)

                logger.info(f"Completed forced rescan for watcher {watcher_id}")
                return True
            else:
                # Create temporary watcher for rescan
                logger.info(f"Watcher {watcher_id} not in active watchers, creating temporary instance")
                temp_watcher = FileWatcher(watcher_id, self.db, self.job_queue_manager)
                temp_watcher.force_rescan()
                logger.info(f"Completed forced rescan for temporary watcher {watcher_id}")
                return True
        except Exception as e:
            logger.error(f"Error forcing rescan for watcher {watcher_id}: {e}", exc_info=True)
            return False

    def get_watcher_status(self, watcher_id):
        """Get status information for a specific watcher"""
        try:
            # Check if watcher exists in memory
            in_memory = watcher_id in self.watchers
            thread_alive = watcher_id in self.threads and self.threads[watcher_id].is_alive()

            # Get database status
            watcher_data = next((w for w in self.db.get_watchers() if w[0] == watcher_id), None)
            db_status = watcher_data[8] if watcher_data else "not_found"

            return {
                'watcher_id': watcher_id,
                'in_memory': in_memory,
                'thread_alive': thread_alive,
                'db_status': db_status,
                'config': dict(zip(
                    ["id", "folder_path", "file_pattern", "job_type", "job_demands",
                     "job_name_prefix", "creation_time", "execution_time", "status", "completion_time"],
                    watcher_data
                )) if watcher_data else None
            }
        except Exception as e:
            logger.error(f"Error getting watcher status for {watcher_id}: {e}", exc_info=True)
            return {'error': str(e)}

    def get_all_watcher_statuses(self):
        """Get status information for all watchers"""
        try:
            all_watchers = self.db.get_watchers()
            statuses = []

            for watcher in all_watchers:
                watcher_id = watcher[0]
                status = self.get_watcher_status(watcher_id)
                statuses.append(status)

            return statuses
        except Exception as e:
            logger.error(f"Error getting all watcher statuses: {e}", exc_info=True)
            return []

    def cleanup(self):
        """Clean up the watcher manager"""
        logger.info("Cleaning up WatcherManager")

        # Stop all watchers
        self.stop_all_watchers()

        # Unsubscribe from events
        event_bus.unsubscribe(EventType.WATCHER_CREATED, self._handle_watcher_created)
        event_bus.unsubscribe(EventType.WATCHER_STATUS_CHANGED, self._handle_watcher_status_changed)

        logger.info("WatcherManager cleanup complete")

    # Alias for backwards compatibility
    def stop_all(self):
        """Alias for stop_all_watchers for backwards compatibility"""
        self.stop_all_watchers()