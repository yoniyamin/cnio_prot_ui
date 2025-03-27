# src/watchers/file_watcher.py
import fnmatch
from pathlib import Path
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from src.core.job import Job
import time

FILE_DELIMITER = ";" # Use semicolon as the delimiter for file pattern.

class FileEventHandler(FileSystemEventHandler):
    def __init__(self, watcher_id, db, config, job_queue_manager):
        self.watcher_id = watcher_id
        self.db = db
        self.config = config
        self.job_queue_manager = job_queue_manager
        # Split file_pattern into a list if it contains semicolons
        self.patterns = self.config["file_pattern"].split(FILE_DELIMITER)
        self.expected_files = set(pattern.strip() for pattern in self.patterns if not any(c in "*?[" for c in pattern))

    def on_created(self, event):
        """Handle file creation events."""
        if not event.is_directory and self.config["status"] == "monitoring":
            file_path = Path(event.src_path)
            if any(fnmatch.fnmatch(file_path.name, pattern.strip()) for pattern in self.patterns):
                print(f"Detected new file: {file_path}")
                self.queue_job(file_path)
                self.check_completion()

    def queue_job(self, file_path):
        """Queue a job for the detected file and log it to the database."""
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
        print(f"Logged captured file: {file_path.name} with job ID {job.job_id}")

    def check_completion(self):
        """Check if all expected files have been captured and mark watcher as completed."""
        if not self.expected_files:
            return  # No exact files specified, so no completion check

        captured_files = {row[3] for row in self.db.get_captured_files(self.watcher_id)}  # file_name column
        if self.expected_files.issubset(captured_files):
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
        """Start watching the configured folder if not completed."""
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
        if self.observer.is_alive():  # Check if the observer thread is running
            self.observer.stop()
            self.observer.join()
        print(f"Stopped watching {self.config['folder_path']}")