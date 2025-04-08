import fnmatch
from pathlib import Path
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from src.core.job import Job
import time

FILE_DELIMITER = ";"  # Delimiter for file patterns in the config

class FileEventHandler(FileSystemEventHandler):
    def __init__(self, watcher_id, db, config, job_queue_manager):
        self.watcher_id = watcher_id
        self.db = db
        self.config = config
        self.job_queue_manager = job_queue_manager
        self.patterns = self.config["file_pattern"].split(FILE_DELIMITER)
        # Flag to identify batch jobs (currently only MaxQuant)
        self.is_batch_job = self.config["job_type"] == 'maxquant'
        if self.is_batch_job:
            # For batch jobs, set expected files (specific filenames without wildcards)
            self.expected_files = set(
                pattern.strip() for pattern in self.patterns if not any(c in "*?[" for c in pattern)
            )
            self.captured_files = set()

    def on_created(self, event):
        """Handle file creation events based on job type."""
        if not event.is_directory and self.config["status"] == "monitoring":
            file_path = Path(event.src_path)
            # Check if the file matches any pattern
            if any(fnmatch.fnmatch(file_path.name, pattern.strip()) for pattern in self.patterns):
                print(f"Detected new file: {file_path}")
                if self.is_batch_job:
                    # Batch job: collect files and check for completion
                    self.captured_files.add(file_path.name)
                    self.db.add_captured_file(
                        job_id=None,  # Job not created yet
                        watcher_id=self.watcher_id,
                        file_name=file_path.name,
                        file_path=str(file_path)
                    )
                    self.check_completion()
                elif self.config["job_type"] != 'none':
                    # Per-file job: create a job immediately
                    self.queue_job(file_path)
                else:
                    # No-job watcher: log file without creating a job
                    self.db.add_captured_file(
                        job_id=None,
                        watcher_id=self.watcher_id,
                        file_name=file_path.name,
                        file_path=str(file_path)
                    )

    def queue_job(self, file_path):
        """Queue a job for a single file (for per-file job types)."""
        job_name = f"{self.config['job_name_prefix']}-{file_path.stem}-{self.watcher_id}"
        job = Job(
            job_submitter="file_watcher",
            job_demands=self.config["job_demands"],
            job_type=self.config["job_type"],
            command="run_" + self.config["job_type"].lower().replace("_handler", "_search"),
            expected_files=[str(file_path)],
            local_folder=str(file_path.parent),
            job_name=job_name,
            job_colour="blue",
            num_steps=1,
            args=[str(file_path)],
            kwargs={}
        )
        self.job_queue_manager.add_job(job)
        print(f"Queued job {job_name} (ID: {job.job_id}) for {file_path}")
        self.db.add_captured_file(
            job_id=job.job_id,
            watcher_id=self.watcher_id,
            file_name=file_path.name,
            file_path=str(file_path)
        )

    def create_maxquant_job(self):
        """Create a single job for all captured files (for MaxQuant)."""
        job_name = f"{self.config['job_name_prefix']}-{self.watcher_id}"
        # Get file paths from DB where job_id is None
        captured_file_paths = [
            row[4] for row in self.db.get_captured_files(self.watcher_id) if row[1] is None
        ]
        job = Job(
            job_submitter="file_watcher",
            job_demands=self.config["job_demands"],
            job_type=self.config["job_type"],
            command="run_maxquant_search",
            expected_files=captured_file_paths,
            local_folder=self.config["folder_path"],
            job_name=job_name,
            job_colour="blue",
            num_steps=1,
            args=captured_file_paths,
            kwargs={}
        )

        # Store watcher_id in job's extras_dict
        job.extras_dict['watcher_id'] = self.watcher_id

        self.job_queue_manager.add_job(job)
        print(f"Queued MaxQuant job {job_name} (ID: {job.job_id}) for watcher {self.watcher_id}")

        # Update job record in database to link to this watcher
        from src.database.jobs_db import JobsDB
        jobs_db = JobsDB()
        jobs_db.link_job_to_watcher(job.job_id, self.watcher_id)

        # Update captured files with the job ID
        for file_path in captured_file_paths:
            self.db.update_captured_file_job_id(file_path, job.job_id)

    def check_completion(self):
        """Check if all expected files are captured (for batch jobs only)."""
        if self.is_batch_job and self.expected_files and self.expected_files.issubset(self.captured_files):
            self.create_maxquant_job()
            self.db.update_watcher_status(self.watcher_id, "completed")

class FileWatcher:
    def __init__(self, watcher_id, db, job_queue_manager):
        self.watcher_id = watcher_id
        self.db = db
        self.config = dict(zip(
            ["id", "folder_path", "file_pattern", "job_type", "job_demands", "job_name_prefix", "creation_time",
             "execution_time", "status", "completion_time"],
            next(row for row in db.get_watchers() if row[0] == watcher_id)
        ))
        self.job_queue_manager = job_queue_manager
        self.event_handler = FileEventHandler(self.watcher_id, self.db, self.config, self.job_queue_manager)
        self.observer = Observer()

    def get_config(self):
        """Fetch configuration from the database based on watcher_id."""
        with self.db.conn:
            cursor = self.db.conn.execute("SELECT * FROM watchers WHERE id = ?", (self.watcher_id,))
            row = cursor.fetchone()
            if row:
                return {
                    "folder_path": row[1],
                    "file_pattern": row[2],
                    "job_type": row[3],
                    "job_demands": row[4],
                    "job_name_prefix": row[5]
                }
            else:
                raise ValueError(f"No watcher found with id {self.watcher_id}")

    def start(self):
        """Start watching the folder if status is 'monitoring'."""
        if self.config["status"] != "monitoring":
            print(f"Watcher {self.watcher_id} is {self.config['status']}, not starting.")
            return

        folder_path = Path(self.config["folder_path"])
        if not folder_path.exists():
            folder_path.mkdir(parents=True, exist_ok=True)
        self.observer.schedule(self.event_handler, str(folder_path), recursive=True)
        self.observer.start()
        print(f"Started watching {self.config['folder_path']} with pattern {self.config['file_pattern']}")
        try:
            while self.config["status"] == "monitoring":
                self.config = dict(zip(
                    ["id", "folder_path", "file_pattern", "job_type", "job_demands", "job_name_prefix", "creation_time",
                     "execution_time", "status", "completion_time"],
                    next(row for row in self.db.get_watchers() if row[0] == self.watcher_id)
                ))
                time.sleep(1)
            print(f"Watcher {self.watcher_id} status changed to {self.config['status']}, stopping.")
            self.stop()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Stop the watcher."""
        if self.observer.is_alive():
            self.observer.stop()
            self.observer.join()
        print(f"Stopped watching {self.config['folder_path']}")