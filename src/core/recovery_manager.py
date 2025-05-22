# src/core/recovery_manager.py
import threading
import time
from datetime import datetime, timedelta
from src.logging_utils import get_logger
from src.core.event_bus import event_bus, EventType, JobEvent
from src.database.jobs_db import JobsDB
from src.utils.job_utils import error_job_safe

logger = get_logger(__name__)


class RecoveryManager:
    """Simple recovery manager that marks interrupted jobs as failed"""

    def __init__(self, job_queue_manager, watcher_manager):
        self.job_queue_manager = job_queue_manager
        self.watcher_manager = watcher_manager
        self.jobs_db = JobsDB()
        self.recovery_in_progress = False
        self.recovery_completed = False
        self.recovery_lock = threading.Lock()

        logger.info("RecoveryManager initialized (simplified)")

    def start_recovery(self):
        """Start the simple recovery process"""
        with self.recovery_lock:
            if self.recovery_in_progress:
                logger.warning("Recovery already in progress")
                return False

            if self.recovery_completed:
                logger.warning("Recovery already completed")
                return False

            self.recovery_in_progress = True

        # Publish recovery start event
        event = JobEvent(
            event_type=EventType.SYSTEM_RECOVERY,
            job_id="",
            data={'type': 'start', 'timestamp': datetime.now().isoformat()},
            timestamp=time.time()
        )
        event_bus.publish(event)

        # Start recovery in separate thread
        recovery_thread = threading.Thread(target=self._perform_recovery, daemon=True)
        recovery_thread.start()

        return True

    def _perform_recovery(self):
        """Perform the simplified recovery process"""
        start_time = time.time()
        logger.info("=== STARTING SIMPLIFIED RECOVERY ===")

        try:
            # Step 1: Find interrupted jobs
            interrupted_jobs = self._find_interrupted_jobs()
            logger.info(f"Found {len(interrupted_jobs)} interrupted jobs")

            # Step 2: Mark interrupted jobs as failed
            failed_count = self._fail_interrupted_jobs(interrupted_jobs)
            logger.info(f"Marked {failed_count} jobs as errored")

            # Step 3: Clean up any orphaned watchers (optional)
            self._cleanup_orphaned_watchers()

            recovery_time = time.time() - start_time
            logger.info(f"=== RECOVERY COMPLETED in {recovery_time:.2f}s ===")

            # Publish recovery complete event
            event = JobEvent(
                event_type=EventType.SYSTEM_RECOVERY,
                job_id="",
                data={
                    'type': 'complete',
                    'interrupted_jobs': len(interrupted_jobs),
                    'failed_jobs': failed_count,
                    'duration': recovery_time,
                    'timestamp': datetime.now().isoformat()
                },
                timestamp=time.time()
            )
            event_bus.publish(event)

        except Exception as e:
            logger.error(f"Error during recovery: {e}", exc_info=True)
            # Publish recovery error event
            event = JobEvent(
                event_type=EventType.SYSTEM_RECOVERY,
                job_id="",
                data={
                    'type': 'error',
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                },
                timestamp=time.time()
            )
            event_bus.publish(event)
        finally:
            self._mark_recovery_complete()

    def _find_interrupted_jobs(self):
        """Find jobs that were interrupted during shutdown"""
        try:
            # Get all jobs that were in running or queued state
            return self.jobs_db.get_interrupted_jobs()
        except Exception as e:
            logger.error(f"Error finding interrupted jobs: {e}", exc_info=True)
            return []

    def _fail_interrupted_jobs(self, interrupted_jobs):
        """Mark all interrupted jobs as errored"""
        failed_count = 0

        for job in interrupted_jobs:
            try:
                job_id = job['job_id']
                current_status = job['status']
                job_name = job.get('job_name', 'Unknown')

                logger.info(f"Marking job {job_id} ({job_name}) as errored (was {current_status})")

                # Mark job as errored with a clear message
                error_message = f"Job was {current_status} when system was shut down. Marked as failed on restart."

                # Update status through the safe utility function
                success = error_job_safe(job_id, error_message)

                if success:
                    failed_count += 1
                    logger.info(f"Successfully marked job {job_id} as errored")
                else:
                    logger.warning(f"Failed to update status for job {job_id}")

            except Exception as e:
                logger.error(f"Error failing job {job.get('job_id', 'unknown')}: {e}", exc_info=True)

        return failed_count

    def _cleanup_orphaned_watchers(self):
        """Clean up any orphaned watchers (watchers without active jobs)"""
        logger.info("Cleaning up orphaned watchers")

        try:
            # Get all watchers in monitoring state
            monitoring_watchers = self.watcher_manager.db.get_watchers(status='monitoring')

            for watcher in monitoring_watchers:
                watcher_id = watcher[0]

                # Check if this watcher has any non-completed jobs
                jobs = self.jobs_db.get_jobs_by_watcher_id(watcher_id)
                active_jobs = [j for j in jobs if j['status'] not in ['completed', 'errored', 'cancelled']]

                if not active_jobs:
                    # No active jobs, mark watcher as completed
                    logger.info(f"Marking orphaned watcher {watcher_id} as completed")
                    self.watcher_manager.db.update_watcher_status(watcher_id, 'completed')
                else:
                    logger.info(f"Watcher {watcher_id} has {len(active_jobs)} active jobs, keeping alive")

        except Exception as e:
            logger.error(f"Error cleaning up orphaned watchers: {e}", exc_info=True)

    def _mark_recovery_complete(self):
        """Mark recovery as completed"""
        with self.recovery_lock:
            self.recovery_in_progress = False
            self.recovery_completed = True
        logger.info("Recovery marked as completed")

    def get_recovery_status(self):
        """Get current recovery status"""
        with self.recovery_lock:
            return {
                'in_progress': self.recovery_in_progress,
                'completed': self.recovery_completed,
                'timestamp': datetime.now().isoformat()
            }

    def manually_restart_job(self, job_id):
        """Manually restart a failed job"""
        try:
            logger.info(f"Manually restarting job {job_id}")

            # Get job data from database
            job_data = self.jobs_db.get_job_by_id(job_id)
            if not job_data:
                logger.error(f"Job {job_id} not found in database")
                return False

            current_status = job_data.get('status')
            if current_status not in ['errored', 'cancelled']:
                logger.error(f"Job {job_id} cannot be restarted (current status: {current_status})")
                return False

            # Check if we need to restart the associated watcher
            watcher_id = job_data.get('watcher_id')
            if watcher_id:
                self._ensure_watcher_running(watcher_id)

            # Reset job to waiting status
            from src.utils.job_utils import update_job_status_safe
            success = update_job_status_safe(job_id, 'waiting')

            if success:
                logger.info(f"Successfully restarted job {job_id}")
                return True
            else:
                logger.error(f"Failed to restart job {job_id}")
                return False

        except Exception as e:
            logger.error(f"Error restarting job {job_id}: {e}", exc_info=True)
            return False

    def _ensure_watcher_running(self, watcher_id):
        """Ensure a watcher is running for manual job restart"""
        try:
            # Check if watcher is already running
            if self.watcher_manager and hasattr(self.watcher_manager, 'watchers'):
                if watcher_id in self.watcher_manager.watchers:
                    logger.info(f"Watcher {watcher_id} already running")
                    return True

            # Start the watcher
            watcher = self.watcher_manager.create_single_watcher(watcher_id)
            if watcher:
                watcher_thread = threading.Thread(target=watcher.start, daemon=True)
                watcher_thread.start()
                logger.info(f"Started watcher {watcher_id} for manual job restart")
                return True
            else:
                logger.warning(f"Failed to create watcher {watcher_id}")
                return False

        except Exception as e:
            logger.error(f"Error ensuring watcher {watcher_id} is running: {e}", exc_info=True)
            return False

    def get_recovery_statistics(self):
        """Get statistics about the last recovery"""
        try:
            # Count jobs by status
            all_jobs = self.jobs_db.get_all_jobs()

            stats = {
                'total_jobs': len(all_jobs),
                'completed': len([j for j in all_jobs if j['status'] == 'completed']),
                'errored': len([j for j in all_jobs if j['status'] == 'errored']),
                'running': len([j for j in all_jobs if j['status'] == 'running']),
                'waiting': len([j for j in all_jobs if j['status'] == 'waiting']),
                'queued': len([j for j in all_jobs if j['status'] == 'queued']),
                'cancelled': len([j for j in all_jobs if j['status'] == 'cancelled']),
                'recovery_status': self.get_recovery_status()
            }

            return stats

        except Exception as e:
            logger.error(f"Error getting recovery statistics: {e}", exc_info=True)
            return {'error': str(e)}

    def cleanup_old_jobs(self, older_than_days=7):
        """Clean up old completed/errored jobs from memory (not database)"""
        try:
            cutoff_time = datetime.now() - timedelta(days=older_than_days)
            cleaned_count = 0

            # Clean up from job queue manager sets
            with self.job_queue_manager.completed_jobs_lock:
                jobs_to_remove = []
                for job in self.job_queue_manager.completed_jobs:
                    if hasattr(job, 'time_stamp') and job.time_stamp:
                        if job.time_stamp < cutoff_time:
                            jobs_to_remove.append(job)

                for job in jobs_to_remove:
                    self.job_queue_manager.completed_jobs.remove(job)
                    cleaned_count += 1

            logger.info(f"Cleaned up {cleaned_count} old jobs from memory")
            return cleaned_count

        except Exception as e:
            logger.error(f"Error cleaning up old jobs: {e}", exc_info=True)
            return 0