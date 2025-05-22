import logging
import os
from queue import Queue as ThreadQueue, Empty
import threading
from threading import Lock
from pathlib import Path
import time
from datetime import datetime
from src.core.job import Job
from src.database.watcher_db import WatcherDB
from src.handlers.run_maxquant import MaxQuantHandler
from src.handlers.diann_handler import DIANNHandler
from src.core.event_bus import event_bus, EventType, JobEvent
from src.logging_utils import get_logger

logger = get_logger(__name__)


class JobQueueManager:
    def __init__(self, db_path="config/watchers.db"):
        self.job_queue = ThreadQueue()
        self.lock = threading.Lock()
        self.active = True
        self.running_jobs = {}
        self.waiting_jobs = set()
        self.waiting_jobs_lock = Lock()
        self.queued_jobs = set()
        self.queued_jobs_lock = Lock()
        self.completed_jobs = set()
        self.completed_jobs_lock = Lock()
        self.db = WatcherDB(db_path)

        # Subscribe to relevant events
        self._setup_event_subscriptions()

        # Start the job processing thread
        self.processor_thread = threading.Thread(target=self.process_queue, daemon=True)
        self.processor_thread.start()
        logger.info("JobQueueManager initialized and processor started")

    def _setup_event_subscriptions(self):
        """Set up event bus subscriptions for this manager"""
        event_bus.subscribe(EventType.JOB_CREATED, self._handle_job_created)
        event_bus.subscribe(EventType.JOB_STATUS_CHANGED, self._handle_job_status_changed)
        event_bus.subscribe(EventType.FILE_CAPTURED, self._handle_file_captured)
        logger.debug("Event subscriptions set up for JobQueueManager")

    def _handle_job_created(self, event: JobEvent):
        """Handle job creation events"""
        try:
            logger.info(f"Handling job creation event for {event.job_id}")
            # The job should already be in the waiting queue
            # This event is mainly for logging and monitoring
        except Exception as e:
            logger.error(f"Error handling job created event: {e}", exc_info=True)

    def _handle_job_status_changed(self, event: JobEvent):
        """Handle job status change events"""
        try:
            job_id = event.job_id
            new_status = event.data.get('new_status')
            old_status = event.data.get('old_status')

            logger.info(f"Handling status change for job {job_id}: {old_status} -> {new_status}")

            # Find the job object
            job = self._find_job_by_id(job_id)
            if not job:
                logger.warning(f"Job {job_id} not found in memory for status change event")
                return

            # Move job between queues based on status change
            self._move_job_between_queues(job, old_status, new_status)

        except Exception as e:
            logger.error(f"Error handling job status changed event: {e}", exc_info=True)

    def _handle_file_captured(self, event: JobEvent):
        """Handle file capture events"""
        try:
            job_id = event.job_id
            file_name = event.data.get('file_name')

            logger.info(f"File captured for job {job_id}: {file_name}")

            # Check if all files are ready for this job
            job = self._find_job_by_id(job_id)
            if job:
                self._check_job_file_completion(job)

        except Exception as e:
            logger.error(f"Error handling file captured event: {e}", exc_info=True)

    def add_job_to_set(self, job, job_set, lock):
        """Thread-safe method to add job to a set"""
        with lock:
            job_set.add(job)

    def remove_job_from_set(self, job, job_set, lock):
        """Thread-safe method to remove job from a set"""
        with lock:
            job_set.discard(job)

    def add_job(self, job):
        """Add a job to the queue and publish creation event"""
        with self.lock:
            logger.info(f"Adding job {job.job_id} with status 'waiting'")
            job.change_job_status('waiting')
            self.add_job_to_set(job, self.waiting_jobs, self.waiting_jobs_lock)
            self.job_queue.put(job)

            # Publish job creation event
            event = JobEvent(
                event_type=EventType.JOB_CREATED,
                job_id=job.job_id,
                data={
                    'job_name': job.job_name,
                    'job_type': job.job_type,
                    'status': job.status,
                    'submitter': job.job_submitter
                },
                timestamp=time.time(),
                watcher_id=getattr(job, 'watcher_id', None)
            )
            event_bus.publish(event)

        # Start checking for existing files in a separate thread
        threading.Thread(target=self.check_existing_files, args=(job,), daemon=True).start()

    def _find_job_by_id(self, job_id):
        """Find a job object by ID across all queues"""
        # Check in running jobs
        if job_id in self.running_jobs:
            return self.running_jobs[job_id]

        # Check in other queues
        for job_set, lock in [
            (self.waiting_jobs, self.waiting_jobs_lock),
            (self.queued_jobs, self.queued_jobs_lock),
            (self.completed_jobs, self.completed_jobs_lock)
        ]:
            with lock:
                for job in job_set:
                    if job.job_id == job_id:
                        return job
        return None

    def _move_job_between_queues(self, job, old_status, new_status):
        """Move job between queues based on status change"""
        if old_status == new_status:
            return

        # Remove from old queue
        if old_status == 'waiting':
            self.remove_job_from_set(job, self.waiting_jobs, self.waiting_jobs_lock)
        elif old_status == 'queued':
            self.remove_job_from_set(job, self.queued_jobs, self.queued_jobs_lock)
        elif old_status == 'running':
            with self.lock:
                if job.job_id in self.running_jobs:
                    del self.running_jobs[job.job_id]
        elif old_status in ['completed', 'errored', 'cancelled']:
            self.remove_job_from_set(job, self.completed_jobs, self.completed_jobs_lock)

        # Add to new queue
        if new_status == 'waiting':
            self.add_job_to_set(job, self.waiting_jobs, self.waiting_jobs_lock)
        elif new_status == 'queued':
            self.add_job_to_set(job, self.queued_jobs, self.queued_jobs_lock)
            # Add to processing queue if not already there
            if old_status != 'queued':
                self.job_queue.put(job)
        elif new_status == 'running':
            with self.lock:
                self.running_jobs[job.job_id] = job
        elif new_status in ['completed', 'errored', 'cancelled']:
            self.add_job_to_set(job, self.completed_jobs, self.completed_jobs_lock)

    def is_file_ready(self, file_path, check_interval=5, retries=3, wait_time=300):
        """Check if a file is ready (not being written to)"""
        initial_size = Path(file_path).stat().st_size
        elapsed_time = 0
        while elapsed_time < wait_time:
            time.sleep(check_interval)
            elapsed_time += check_interval
            if Path(file_path).stat().st_size == initial_size:
                return True
        return False

    def _check_job_file_completion(self, job):
        """Check if all expected files for a job are ready"""
        if not job.get_list_expected_files():
            logger.info(f"All files ready for job {job.job_id}, transitioning to queued")
            self._transition_job_status(job, 'queued')
            return True
        return False

    def check_existing_files(self, job, recursive=True, wait_time=300):
        """Check for existing files and monitor their readiness"""
        logger.info(f"Checking files for job {job.job_id}: {job.get_list_expected_files()}")

        for file_name in list(job.get_list_expected_files()):
            local_file_path = Path(job.local_folder) / Path(file_name).name
            if local_file_path.exists() and self.is_file_ready(local_file_path, wait_time=wait_time):
                logger.info(f"File {file_name} is ready for job {job.job_id}")
                job.modify_expected_files(file_name, action="remove")

                # Publish file captured event
                event = JobEvent(
                    event_type=EventType.FILE_CAPTURED,
                    job_id=job.job_id,
                    data={
                        'file_name': file_name,
                        'file_path': str(local_file_path)
                    },
                    timestamp=time.time()
                )
                event_bus.publish(event)

        # Check if all files are ready
        self._check_job_file_completion(job)

    def _transition_job_status(self, job, new_status):
        """Transition job status and publish event"""
        old_status = job.status
        job.change_job_status(new_status)

        # Publish status change event
        event = JobEvent(
            event_type=EventType.JOB_STATUS_CHANGED,
            job_id=job.job_id,
            data={
                'old_status': old_status,
                'new_status': new_status,
                'timestamp': datetime.now().isoformat()
            },
            timestamp=time.time()
        )
        event_bus.publish(event)

    def run_job(self, job):
        """Execute a job"""
        logger.info(f"Running job {job.job_id}")

        # Transition to running status
        self._transition_job_status(job, 'running')

        try:
            if job.job_type == 'maxquant':
                self._run_maxquant_job(job)
            elif job.job_type == 'diann':
                self._run_diann_job(job)
            else:
                logger.error(f"Unknown job type: {job.job_type}")
                raise ValueError(f"Unknown job type: {job.job_type}")

        except Exception as e:
            logger.error(f"Job {job.job_id} failed: {e}", exc_info=True)
            self._transition_job_status(job, 'errored')

            # Publish error event
            error_event = JobEvent(
                event_type=EventType.JOB_ERROR,
                job_id=job.job_id,
                data={
                    'error': str(e),
                    'traceback': str(e.__traceback__) if hasattr(e, '__traceback__') else None
                },
                timestamp=time.time()
            )
            event_bus.publish(error_event)

    def _run_maxquant_job(self, job):
        """Run MaxQuant job with progress monitoring"""
        params = job.extras_dict
        job.progress_queue = ThreadQueue()
        job.stop_queue = ThreadQueue()

        handler = MaxQuantHandler(
            stop_queue=job.stop_queue,
            progress_queue=job.progress_queue,
            MQ_version=params['mq_version'],
            MQ_path=params['mq_path'],
            db_map=params.get('db_map', ''),
            fasta_folder=params['fasta_folder'],
            output_folder=params['output_folder'],
            conditions=params['conditions_file'],
            dbs=params['dbs'],
            protein_quantification=params.get('protein_quantification', 'Razor + Unique'),
            missed_cleavages=params.get('missed_cleavages', '2'),
            fixed_mods=params.get('fixed_mods', 'Carbamidomethyl (C)'),
            variable_mods=params.get('variable_mods', 'Oxidation (M), Acetyl (Protein N-term)'),
            enzymes=params.get('enzymes', 'Trypsin/P'),
            match_between_runs=params.get('match_between_runs', False),
            second_peptide=params.get('second_peptide', False),
            id_parse_rule=params.get('id_parse_rule', '>.*\\|(.*)\\|'),
            desc_parse_rule=params.get('desc_parse_rule', '>(*)'),
            andromeda_path=params.get('andromeda_path', 'C:\\Temp\\Andromeda'),
            mq_params_path=params.get('mq_params_path', ''),
            user_input_params=False,
            raw_folder=os.path.join(params['output_folder'], "raw_file_folder"),
            job_name=job.job_name,
            num_threads=params.get('num_threads', 16)
        )

        # Run handler in a separate thread
        handler_thread = threading.Thread(target=handler.run_MaxQuant_cli)
        handler_thread.start()

        # Monitor progress and stop signals
        while handler_thread.is_alive():
            try:
                message = job.progress_queue.get(timeout=1)
                self._process_job_message(job, message)
            except Empty:
                continue

        handler_thread.join()

        # Ensure job is marked as completed if not already
        if job.status not in ['completed', 'errored']:
            self._transition_job_status(job, 'completed')

    def _run_diann_job(self, job):
        """Run DIA-NN job with progress monitoring"""
        params = job.extras_dict
        job.progress_queue = ThreadQueue()
        job.stop_queue = ThreadQueue()

        # Extract output folder from params
        output_folder = params.get('output_folder')
        if not output_folder:
            raise ValueError("Missing required parameter: output_folder for DIA-NN job")

        # Extract the path to the DIA-NN executable (must be a str or PathLike)
        diann_exe = params.get('diann_path')
        if not isinstance(diann_exe, (str, os.PathLike)):
            raise ValueError("Missing or invalid parameter 'diann_path' for DIA-NN job")
        handler = DIANNHandler(
                   diann_exe,
                   job.progress_queue,
                   job.stop_queue,
                   output_folder
            )
        handler_thread = threading.Thread(target=handler.run_workflow)
        handler_thread.start()
        logger.info(f"Started DIA-NN handler thread for job {job.job_id}")

        # Monitor progress
        while handler_thread.is_alive():
            try:
                message = job.progress_queue.get(timeout=1)
                self._process_job_message(job, message)
            except Empty:
                continue

        handler_thread.join()

        # Ensure job is marked as completed if not already
        if job.status not in ['completed', 'errored']:
            self._transition_job_status(job, 'completed')

    def _process_job_message(self, job, message):
        """Process messages from job handlers"""
        if message == "COMPLETED" or "PROCESS COMPLETED" in message:
            self._transition_job_status(job, 'completed')
        elif message.startswith("ERROR"):
            job.error_flag = True
            self._transition_job_status(job, 'errored')
        elif message.startswith("STARTING"):
            # Already in running state, just log
            logger.info(f"Job {job.job_id} started processing")
        else:
            # Try to parse as progress
            try:
                progress = float(message)
                job.update_progress(progress / 100)  # Assuming progress is 0-100

                # Publish progress update event
                progress_event = JobEvent(
                    event_type=EventType.JOB_PROGRESS_UPDATED,
                    job_id=job.job_id,
                    data={'progress': job.progress},
                    timestamp=time.time()
                )
                event_bus.publish(progress_event)
            except ValueError:
                logger.debug(f"Job {job.job_id} message: {message}")

    def process_queue(self):
        """Main queue processing loop"""
        logger.info("JobQueueManager processor started")
        while self.active:
            try:
                if not self.job_queue.empty():
                    job = self.job_queue.get()
                    if job.status == 'queued':
                        self.run_job(job)
                    else:
                        # Re-queue if not ready
                        self.job_queue.put(job)
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error in queue processing: {e}", exc_info=True)
                time.sleep(5)  # Wait before retrying

    def stop_processing(self):
        """Stop the job queue manager"""
        self.active = False
        logger.info("JobQueueManager stopped")

        # Unsubscribe from events
        event_bus.unsubscribe(EventType.JOB_CREATED, self._handle_job_created)
        event_bus.unsubscribe(EventType.JOB_STATUS_CHANGED, self._handle_job_status_changed)
        event_bus.unsubscribe(EventType.FILE_CAPTURED, self._handle_file_captured)

    def get_job_stats(self):
        """Get statistics about jobs in the queue"""
        with self.waiting_jobs_lock, self.queued_jobs_lock, self.completed_jobs_lock:
            return {
                'waiting': len(self.waiting_jobs),
                'queued': len(self.queued_jobs),
                'running': len(self.running_jobs),
                'completed': len(self.completed_jobs)
            }