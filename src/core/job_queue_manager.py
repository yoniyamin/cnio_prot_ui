import logging
from queue import Queue as ThreadQueue
import threading
from threading import Lock
from pathlib import Path
import time
from src.core.job import Job # not needed at that point, but maybe in the future.
from src.database.watcher_db import WatcherDB

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


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

    def add_job_to_set(self, job, job_set, lock):
        with lock:
            job_set.add(job)

    def remove_job_from_set(self, job, job_set, lock):
        with lock:
            job_set.discard(job)

    def add_job(self, job):
        with self.lock:
            logger.info(f"Adding job {job.job_id} with status 'waiting'")
            job.change_job_status('waiting')
            self.add_job_to_set(job, self.waiting_jobs, self.waiting_jobs_lock)
            self.job_queue.put(job)
        threading.Thread(target=self.check_existing_files, args=(job,), daemon=True).start()

    def is_file_ready(self, file_path, check_interval=5, retries=3, wait_time=300):
        initial_size = Path(file_path).stat().st_size
        elapsed_time = 0
        while elapsed_time < wait_time:
            time.sleep(check_interval)
            elapsed_time += check_interval
            if Path(file_path).stat().st_size == initial_size:
                return True
        return False

    def check_existing_files(self, job, recursive=True, wait_time=300):
        logger.info(f"Checking files for job {job.job_id}: {job.get_list_expected_files()}")
        for file_name in list(job.get_list_expected_files()):
            local_file_path = Path(job.local_folder) / Path(file_name).name
            if local_file_path.exists() and self.is_file_ready(local_file_path, wait_time=wait_time):
                logger.info(f"File {file_name} is ready for job {job.job_id}")
                job.modify_expected_files(file_name, action="remove")

        if not job.get_list_expected_files():
            logger.info(f"All files ready for job {job.job_id}, setting status to 'queued'")
            with self.lock:
                job.change_job_status('queued')
                self.remove_job_from_set(job, self.waiting_jobs, self.waiting_jobs_lock)
                self.add_job_to_set(job, self.queued_jobs, self.queued_jobs_lock)

    def run_job(self, job):
        logger.info(f"Running job {job.job_id}")
        with self.lock:
            job.change_job_status('running')
            self.remove_job_from_set(job, self.queued_jobs, self.queued_jobs_lock)
            self.running_jobs[job.job_id] = job

        try:
            # Placeholder: Replace with your job execution logic
            logger.info(f"Executing {job.command} for {job.job_type}")
            time.sleep(2)  # Simulate work
            job.change_job_status('completed')
        except Exception as e:
            logger.error(f"Job {job.job_id} failed: {e}")
            job.change_job_status('errored')

        with self.lock:
            del self.running_jobs[job.job_id]
            self.add_job_to_set(job, self.completed_jobs, self.completed_jobs_lock)

    def process_queue(self):
        while self.active:
            if not self.job_queue.empty():
                job = self.job_queue.get()
                with self.lock:
                    if job.status == 'queued':
                        self.run_job(job)
                    else:
                        self.job_queue.put(job)
            time.sleep(1)

    def stop_processing(self):
        self.active = False
        logger.info("JobQueueManager stopped")