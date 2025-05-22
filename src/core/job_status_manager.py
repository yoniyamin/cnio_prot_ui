import threading
from datetime import datetime
from src.logging_utils import get_logger
from src.core.event_bus import event_bus, EventType, JobEvent
from src.database.jobs_db import JobsDB

logger = get_logger(__name__)

# Singleton instance and lock
_job_status_manager_instance = None
_job_status_manager_lock = threading.Lock()


class JobStatusManager:
    """
    Centralized class to manage job status changes and synchronization
    with integrated event bus support
    """

    def __new__(cls, jobs_db=None, job_queue_manager=None):
        global _job_status_manager_instance
        if _job_status_manager_instance is None:
            with _job_status_manager_lock:
                if _job_status_manager_instance is None:
                    _job_status_manager_instance = super(JobStatusManager, cls).__new__(cls)
                    _job_status_manager_instance._initialized = False
        return _job_status_manager_instance

    def __init__(self, jobs_db=None, job_queue_manager=None):
        # Skip initialization if already done
        if hasattr(self, '_initialized') and self._initialized:
            return

        if jobs_db is None or job_queue_manager is None:
            logger.warning("JobStatusManager initialized without required parameters - will be re-initialized later")
            return

        self.jobs_db = jobs_db
        self.job_queue_manager = job_queue_manager
        self.lock = threading.Lock()
        self._initialized = True

        # Subscribe to job status change events
        event_bus.subscribe(EventType.JOB_STATUS_CHANGED, self._handle_status_change_event)
        event_bus.subscribe(EventType.JOB_PROGRESS_UPDATED, self._handle_progress_update_event)

        logger.debug("JobStatusManager fully initialized with event bus integration")

    def _handle_status_change_event(self, event: JobEvent):
        """Handle job status change events from the event bus"""
        try:
            job_id = event.job_id
            new_status = event.data.get('new_status')

            # Update database with the new status
            self.jobs_db.update_job_status_by_id(job_id, new_status)
            logger.debug(f"Database updated for job {job_id} status change to {new_status}")

        except Exception as e:
            logger.error(f"Error handling status change event: {e}", exc_info=True)

    def _handle_progress_update_event(self, event: JobEvent):
        """Handle job progress update events from the event bus"""
        try:
            job_id = event.job_id
            progress = event.data.get('progress')

            if progress is not None:
                self.jobs_db.update_job_progress(job_id, progress)
                logger.debug(f"Database updated for job {job_id} progress: {progress}")

        except Exception as e:
            logger.error(f"Error handling progress update event: {e}", exc_info=True)

    def update_job_status(self, job_id, new_status, progress=None):
        """
        Update the status of a job in all places

        Args:
            job_id: The ID of the job to update
            new_status: The new status to set
            progress: Optional progress value (0-1)

        Returns:
            Boolean indicating success
        """
        # Check if we're fully initialized
        if not hasattr(self, '_initialized') or not self._initialized:
            logger.error("JobStatusManager not fully initialized")
            return False

        with self.lock:
            # Find the job object in the queue manager
            job = self._find_job_by_id(job_id)

            if job:
                # Get the previous status for the event
                previous_status = job.status

                # 1. Update the job object's status (this will trigger an event)
                self._update_job_object_status(job, new_status, progress)

                # 2. The event will handle database updates and queue movements
                logger.info(f"Job {job_id} status updated from {previous_status} to {new_status}")
                return True
            else:
                # If job not found in memory, still update the database
                logger.warning(f"Job object for ID {job_id} not found in memory, updating database only")
                return self._update_database_only(job_id, new_status, progress)

    def _update_job_object_status(self, job, new_status, progress=None):
        """Update job object status and trigger events"""
        old_status = job.status

        # Update progress first if provided
        if progress is not None:
            old_progress = job.progress
            if hasattr(job, 'update_progress') and progress > old_progress:
                try:
                    increment = progress - old_progress
                    job.update_progress(increment)
                except ValueError:
                    # Direct set if increment would exceed bounds
                    job.progress = progress
            else:
                job.progress = progress

            # Publish progress update event
            progress_event = JobEvent(
                event_type=EventType.JOB_PROGRESS_UPDATED,
                job_id=job.job_id,
                data={'progress': progress},
                timestamp=datetime.now().timestamp()
            )
            event_bus.publish(progress_event)

        # Update status (this should trigger the status change event)
        job.change_job_status(new_status)

        # Update timestamp if available
        if hasattr(job, 'time_stamp'):
            job.time_stamp = datetime.now()

        # Publish status change event
        status_event = JobEvent(
            event_type=EventType.JOB_STATUS_CHANGED,
            job_id=job.job_id,
            data={
                'old_status': old_status,
                'new_status': new_status,
                'timestamp': datetime.now().isoformat()
            },
            timestamp=datetime.now().timestamp()
        )
        event_bus.publish(status_event)

    def _update_database_only(self, job_id, new_status, progress):
        """Update database when job object is not in memory"""
        try:
            # Try to load the job from database and create an in-memory representation
            job_data = self.jobs_db.get_job_by_id(job_id)
            if job_data:
                # Create a Job object and add it to the appropriate queue
                from src.core.job import Job

                job_name = job_data.get('job_name', f"job_{job_id}")
                job_type = job_data.get('job_type', 'unknown')

                # Create a minimal Job object
                job = Job(
                    job_submitter="recovery",
                    job_demands=job_data.get('job_demands', {}),
                    job_type=job_type,
                    command=f"run_{job_type.lower()}",
                    job_name=job_name,
                    expected_files=job_data.get('expected_files', []),
                    local_folder=job_data.get('local_folder', ''),
                    is_simulation=job_data.get('is_simulation', False)
                )

                # Set the job_id manually after creation
                job.job_id = job_id
                job.status = new_status

                # Add to the appropriate queue based on the new status
                self._add_job_to_appropriate_queue(job, new_status)

                logger.info(f"Created in-memory job object for {job_id} with status {new_status}")

            # Update database regardless
            db_update_success = self.jobs_db.update_job_status_by_id(job_id, new_status)

            if not db_update_success:
                logger.error(f"Failed to update job status in database for job {job_id}")
                return False

            if progress is not None:
                self.jobs_db.update_job_progress(job_id, progress)

            # Publish event even for database-only updates
            status_event = JobEvent(
                event_type=EventType.JOB_STATUS_CHANGED,
                job_id=job_id,
                data={
                    'old_status': 'unknown',
                    'new_status': new_status,
                    'timestamp': datetime.now().isoformat(),
                    'source': 'database_only'
                },
                timestamp=datetime.now().timestamp()
            )
            event_bus.publish(status_event)

            return True
        except Exception as e:
            logger.error(f"Failed to update database for job {job_id}: {e}", exc_info=True)
            return False

    def _add_job_to_appropriate_queue(self, job, status):
        """Add job to the appropriate queue based on its status"""
        if status == 'waiting':
            self.job_queue_manager.add_job_to_set(
                job, self.job_queue_manager.waiting_jobs,
                self.job_queue_manager.waiting_jobs_lock
            )
        elif status == 'queued':
            self.job_queue_manager.add_job_to_set(
                job, self.job_queue_manager.queued_jobs,
                self.job_queue_manager.queued_jobs_lock
            )
            # Add job to the queue for processing
            self.job_queue_manager.job_queue.put(job)
        elif status == 'running':
            with self.job_queue_manager.lock:
                self.job_queue_manager.running_jobs[job.job_id] = job
        elif status in ['completed', 'errored', 'cancelled']:
            self.job_queue_manager.add_job_to_set(
                job, self.job_queue_manager.completed_jobs,
                self.job_queue_manager.completed_jobs_lock
            )
            # Handle cancellation if needed
            if status == 'cancelled' and hasattr(job, 'stop_queue'):
                try:
                    job.stop_queue.put("STOP")
                    logger.info(f"Sent stop signal to job {job.job_id}")
                except Exception as e:
                    logger.error(f"Error sending stop signal to job {job.job_id}: {e}")

    def _find_job_by_id(self, job_id):
        """Find a job object by ID across all queues"""
        return self.job_queue_manager._find_job_by_id(job_id)

    def bulk_update_jobs(self, job_updates):
        """
        Update multiple jobs in a single operation

        Args:
            job_updates: List of tuples (job_id, new_status, progress)
        """
        with self.lock:
            successful_updates = []
            failed_updates = []

            for job_id, new_status, progress in job_updates:
                try:
                    success = self.update_job_status(job_id, new_status, progress)
                    if success:
                        successful_updates.append(job_id)
                    else:
                        failed_updates.append(job_id)
                except Exception as e:
                    logger.error(f"Error updating job {job_id}: {e}", exc_info=True)
                    failed_updates.append(job_id)

            logger.info(f"Bulk update completed: {len(successful_updates)} successful, {len(failed_updates)} failed")
            return successful_updates, failed_updates

    def get_job_status(self, job_id):
        """Get current status of a job"""
        job = self._find_job_by_id(job_id)
        if job:
            return {
                'status': job.status,
                'progress': job.progress,
                'job_name': job.job_name,
                'job_type': job.job_type
            }
        else:
            # Fallback to database
            job_data = self.jobs_db.get_job_by_id(job_id)
            if job_data:
                return {
                    'status': job_data.get('status', 'unknown'),
                    'progress': job_data.get('progress', 0),
                    'job_name': job_data.get('job_name', ''),
                    'job_type': job_data.get('job_type', '')
                }
        return None

    def shutdown(self):
        """Shutdown the job status manager"""
        # Unsubscribe from events
        event_bus.unsubscribe(EventType.JOB_STATUS_CHANGED, self._handle_status_change_event)
        event_bus.unsubscribe(EventType.JOB_PROGRESS_UPDATED, self._handle_progress_update_event)
        logger.info("JobStatusManager shut down")


# Get the singleton instance
def get_job_status_manager():
    return JobStatusManager()


# Export for easier imports in other modules
job_status_manager = get_job_status_manager()