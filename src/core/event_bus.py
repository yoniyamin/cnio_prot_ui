# src/core/event_bus.py
from dataclasses import dataclass
from typing import Any, Dict, List, Callable, Optional
from threading import Lock, Event as ThreadEvent
from queue import Queue, Empty
import time
import threading
from enum import Enum
from src.logging_utils import get_logger

logger = get_logger(__name__)


class EventType(Enum):
    JOB_CREATED = "job_created"
    JOB_STATUS_CHANGED = "job_status_changed"
    JOB_PROGRESS_UPDATED = "job_progress_updated"
    JOB_ERROR = "job_error"
    JOB_COMPLETED = "job_completed"
    WATCHER_CREATED = "watcher_created"
    WATCHER_STATUS_CHANGED = "watcher_status_changed"
    FILE_CAPTURED = "file_captured"
    SYSTEM_RECOVERY = "system_recovery"


@dataclass
class JobEvent:
    event_type: EventType
    job_id: str
    data: Dict[str, Any]
    timestamp: float
    watcher_id: Optional[int] = None


class EventBus:
    """Thread-safe event bus for decoupled communication between components."""

    def __init__(self):
        self._subscribers: Dict[EventType, List[Callable]] = {}
        self._lock = Lock()
        self._event_queue = Queue()
        self._processing = True
        self._processor_thread = None
        self._start_processing()

    def _start_processing(self):
        """Start the event processing thread."""
        self._processor_thread = threading.Thread(
            target=self._process_events,
            name="EventBusProcessor",
            daemon=True
        )
        self._processor_thread.start()
        logger.info("Event bus processor started")

    def _process_events(self):
        """Process events from the queue in a separate thread."""
        while self._processing:
            try:
                # Get event with timeout to allow periodic checking of _processing flag
                event = self._event_queue.get(timeout=1.0)
                self._dispatch_event(event)
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error processing event: {e}", exc_info=True)

    def _dispatch_event(self, event: JobEvent):
        """Dispatch an event to all subscribers."""
        with self._lock:
            subscribers = self._subscribers.get(event.event_type, []).copy()

        for callback in subscribers:
            try:
                callback(event)
            except Exception as e:
                logger.error(f"Error in event callback for {event.event_type}: {e}", exc_info=True)

    def subscribe(self, event_type: EventType, callback: Callable[[JobEvent], None]):
        """Subscribe to an event type."""
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            self._subscribers[event_type].append(callback)
            logger.debug(f"Subscribed to {event_type}")

    def unsubscribe(self, event_type: EventType, callback: Callable[[JobEvent], None]):
        """Unsubscribe from an event type."""
        with self._lock:
            if event_type in self._subscribers:
                try:
                    self._subscribers[event_type].remove(callback)
                    logger.debug(f"Unsubscribed from {event_type}")
                except ValueError:
                    logger.warning(f"Callback not found in subscribers for {event_type}")

    def publish(self, event: JobEvent):
        """Publish an event (non-blocking)."""
        self._event_queue.put(event)
        logger.debug(f"Published event {event.event_type} for job {event.job_id}")

    def publish_sync(self, event: JobEvent):
        """Publish an event synchronously (blocking)."""
        self._dispatch_event(event)
        logger.debug(f"Published event {event.event_type} synchronously for job {event.job_id}")

    def shutdown(self):
        """Shutdown the event bus."""
        self._processing = False
        if self._processor_thread and self._processor_thread.is_alive():
            self._processor_thread.join(timeout=5.0)
        logger.info("Event bus shut down")


# Global event bus instance
event_bus = EventBus()