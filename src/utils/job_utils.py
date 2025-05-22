import threading
from datetime import datetime
from src.logging_utils import get_logger
from src.core.event_bus import event_bus, EventType, JobEvent

logger = get_logger(__name__)

# Recovery tracking
_recovery_jobs = set()
_restarted_jobs = set()
_recovery_lock = threading.Lock()


def get_job_status_manager():
    """Get the job status manager instance"""
    from src.core.job_status_manager import get_job_status_manager
    return get_job_status_manager()


def update_job_status_safe(job_id, new_status, progress=None):
    """
    Safely update job status using the centralized manager

    Args:
        job_id: The ID of the job to update
        new_status: The new status to set
        progress: Optional progress value (0-1)

    Returns:
        Boolean indicating success
    """
    try:
        manager = get_job_status_manager()
        return manager.update_job_status(job_id, new_status, progress)
    except Exception as e:
        logger.error(f"Error updating job status safely: {e}", exc_info=True)
        return False


def update_job_progress_safe(job_id, progress):
    """
    Safely update job progress

    Args:
        job_id: The ID of the job to update
        progress: Progress value (0-1)

    Returns:
        Boolean indicating success
    """
    try:
        manager = get_job_status_manager()
        current = manager.get_job_status(job_id)
        if current:
            return manager.update_job_status(job_id, current['status'], progress)
        else:
            logger.warning(f"Could not find job {job_id} to update progress")
            return False
    except Exception as e:
        logger.error(f"Error updating job progress safely: {e}", exc_info=True)
        return False


def start_job_safe(job_id):
    """Safely start a job (transition to running)"""
    logger.info(f"Starting job {job_id}")
    return update_job_status_safe(job_id, 'running')


def queue_job_safe(job_id):
    """Safely queue a job (transition to queued)"""
    logger.info(f"Queuing job {job_id}")
    return update_job_status_safe(job_id, 'queued')


def complete_job_safe(job_id):
    """Safely complete a job"""
    logger.info(f"Completing job {job_id}")

    # Publish completion event
    event = JobEvent(
        event_type=EventType.JOB_COMPLETED,
        job_id=job_id,
        data={'completion_time': datetime.now().isoformat()},
        timestamp=datetime.now().timestamp()
    )
    event_bus.publish(event)

    return update_job_status_safe(job_id, 'completed')


def error_job_safe(job_id, error_message=None):
    """Safely mark a job as errored"""
    logger.error(f"Marking job {job_id} as errored: {error_message}")

    # Publish error event
    event = JobEvent(
        event_type=EventType.JOB_ERROR,
        job_id=job_id,
        data={
            'error_message': error_message or 'Unknown error',
            'error_time': datetime.now().isoformat()
        },
        timestamp=datetime.now().timestamp()
    )
    event_bus.publish(event)

    return update_job_status_safe(job_id, 'errored')


def cancel_job_safe(job_id):
    """Safely cancel a job"""
    logger.info(f"Cancelling job {job_id}")
    return update_job_status_safe(job_id, 'cancelled')


def run_job_safe(job_id):
    """Safely run a job (same as start_job_safe)"""
    return start_job_safe(job_id)


def track_recovery_job(job_id):
    """
    Track a job being processed during recovery
    """
    if not job_id:
        return
    with _recovery_lock:
        _recovery_jobs.add(job_id)
    logger.debug(f"Added job {job_id} to recovery tracking")


def track_restarted_job(job_id):
    """
    Track a job that has been restarted during recovery
    """
    if not job_id:
        return
    with _recovery_lock:
        _restarted_jobs.add(job_id)
    logger.debug(f"Added job {job_id} to restarted tracking")


def get_job_recovery_info():
    """
    Get information about job recovery for diagnostics
    """
    with _recovery_lock:
        return {
            'tracked_jobs': len(_recovery_jobs),
            'restarted_jobs': len(_restarted_jobs),
            'tracked_job_ids': list(_recovery_jobs),
            'restarted_job_ids': list(_restarted_jobs)
        }


def bulk_update_job_statuses(job_updates):
    """
    Update multiple jobs in a single operation

    Args:
        job_updates: List of tuples (job_id, new_status, progress)

    Returns:
        Tuple (successful_updates, failed_updates)
    """
    try:
        manager = get_job_status_manager()
        return manager.bulk_update_jobs(job_updates)
    except Exception as e:
        logger.error(f"Error in bulk job update: {e}", exc_info=True)
        return [], [u[0] for u in job_updates]


def get_job_status(job_id):
    """
    Get current status information for a job

    Args:
        job_id: The ID of the job

    Returns:
        Dictionary with job status information or None
    """
    try:
        manager = get_job_status_manager()
        return manager.get_job_status(job_id)
    except Exception as e:
        logger.error(f"Error getting job status: {e}", exc_info=True)
        return None


def wait_for_job_completion(job_id, timeout=300, check_interval=5):
    """
    Wait for a job to complete

    Args:
        job_id: The ID of the job to wait for
        timeout: Maximum time to wait in seconds
        check_interval: How often to check the status in seconds

    Returns:
        Final status of the job or None if timeout
    """
    import time
    start = time.time()
    while time.time() - start < timeout:
        status_info = get_job_status(job_id)
        if status_info and status_info.get('status') in ['completed', 'errored', 'cancelled']:
            logger.info(f"Job {job_id} finished with status: {status_info['status']}")
            return status_info['status']
        time.sleep(check_interval)
    logger.warning(f"Timeout waiting for job {job_id} to complete")
    return None


def restart_job_from_status(job_id, from_status='errored'):
    """
    Restart a job from a specific status

    Args:
        job_id: The ID of the job to restart
        from_status: The status the job should be in to restart

    Returns:
        Boolean indicating success
    """
    info = get_job_status(job_id)
    if not info or info.get('status') != from_status:
        logger.error(f"Cannot restart job {job_id}: expected status {from_status}, got {info.get('status') if info else 'N/A'}")
        return False
    success = queue_job_safe(job_id)
    if success:
        track_restarted_job(job_id)
    return success


