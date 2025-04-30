import fnmatch
from pathlib import Path
from watchdog.events import FileSystemEventHandler, FileCreatedEvent
from watchdog.observers import Observer
from src.core.job import Job
import time
import logging
import os

logger = logging.getLogger(__name__)

FILE_DELIMITER = ";"  # Delimiter for file patterns in the config

class FileEventHandler(FileSystemEventHandler):
    def __init__(self, watcher_id, db, config, job_queue_manager):
        self.watcher_id = watcher_id
        self.db = db
        self.config = config
        self.job_queue_manager = job_queue_manager
        self.patterns = self.config["file_pattern"].split(FILE_DELIMITER)
        self.patterns = [pattern.strip() for pattern in self.patterns]  # Clean up patterns
        
        # Flag to identify batch jobs (currently only MaxQuant)
        self.is_batch_job = self.config["job_type"].lower() in ['maxquant', 'diann']
        
        logger.info(f"Initializing FileEventHandler for watcher {watcher_id} with patterns: {self.patterns}")
        logger.info(f"Is batch job: {self.is_batch_job}")
        
        if self.is_batch_job:
            # For batch jobs, set expected files (specific filenames without wildcards)
            self.expected_files = set(
                pattern for pattern in self.patterns if not any(c in "*?[" for c in pattern)
            )
            self.captured_files = set()
            logger.info(f"Expected files for batch job: {self.expected_files}")

    def on_created(self, event):
        """Handle file creation events based on job type."""
        if not event.is_directory and self.config["status"] == "monitoring":
            file_path = Path(event.src_path)
            file_name = file_path.name
            
            logger.info(f"File creation detected: {file_path}")
            logger.info(f"Checking against patterns: {self.patterns}")
            
            # Check if the file matches any pattern
            matches = False
            for pattern in self.patterns:
                if fnmatch.fnmatch(file_name, pattern):
                    matches = True
                    logger.info(f"File {file_name} matches pattern {pattern}")
                    break
            
            if matches:
                logger.info(f"File {file_path} matches a pattern, processing...")
                print(f"Detected new file: {file_path}")
                try:
                    if self.is_batch_job:
                        # Batch job: collect files and check for completion
                        self.captured_files.add(file_name)
                        logger.info(f"Added {file_name} to captured_files set. Current set: {self.captured_files}")
                        
                        # Add to database
                        try:
                            file_id = self.db.add_captured_file(
                                job_id=None,  # Job not created yet
                                watcher_id=self.watcher_id,
                                file_name=file_name,
                                file_path=str(file_path)
                            )
                            logger.info(f"Added file to database with ID: {file_id}")
                        except Exception as e:
                            logger.error(f"Error adding file to database: {str(e)}")
                        
                        # Check if all expected files are captured
                        self.check_completion()
                    elif self.config["job_type"].lower() != 'none':
                        # Per-file job: create a job immediately
                        self.queue_job(file_path)
                    else:
                        # No-job watcher: log file without creating a job
                        self.db.add_captured_file(
                            job_id=None,
                            watcher_id=self.watcher_id,
                            file_name=file_name,
                            file_path=str(file_path)
                        )
                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {str(e)}", exc_info=True)
            else:
                logger.info(f"File {file_name} does not match any pattern, ignoring")

    def scan_existing_files(self):
        """Scan the folder for existing files that match patterns."""
        folder_path = Path(self.config["folder_path"])
        logger.info(f"Scanning for existing files in {folder_path}")
        
        if not folder_path.exists():
            logger.warning(f"Folder {folder_path} does not exist, cannot scan")
            return
            
        found_files = []
        
        # Scan for files matching patterns
        for pattern in self.patterns:
            for file_path in folder_path.glob(pattern):
                if file_path.is_file():
                    found_files.append(file_path)
                    logger.info(f"Found existing file matching pattern {pattern}: {file_path}")
        
        # Process found files
        for file_path in found_files:
            # Create a fake event to process the file
            event = FileCreatedEvent(str(file_path))
            self.on_created(event)
            logger.info(f"Processed existing file: {file_path}")

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
        
        if not captured_file_paths:
            logger.warning(f"No captured files found for watcher {self.watcher_id}, cannot create MaxQuant job")
            return
            
        logger.info(f"Creating MaxQuant job with captured files: {captured_file_paths}")
        
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
        job.extras_dict = getattr(job, 'extras_dict', {})
        job.extras_dict['watcher_id'] = self.watcher_id

        self.job_queue_manager.add_job(job)
        logger.info(f"Queued MaxQuant job {job_name} (ID: {job.job_id}) for watcher {self.watcher_id}")
        print(f"Queued MaxQuant job {job_name} (ID: {job.job_id}) for watcher {self.watcher_id}")

        # Update job record in database to link to this watcher
        try:
            from src.database.jobs_db import JobsDB
            jobs_db = JobsDB()
            jobs_db.link_job_to_watcher(job.job_id, self.watcher_id)
        except Exception as e:
            logger.error(f"Error linking job to watcher: {str(e)}", exc_info=True)

        # Update captured files with the job ID
        for file_path in captured_file_paths:
            try:
                self.db.update_captured_file_job_id(file_path, job.job_id)
            except Exception as e:
                logger.error(f"Error updating captured file job ID: {str(e)}", exc_info=True)

    def create_diann_job(self):
        """Create a single job for all captured files (for DIA-NN)."""
        job_name = f"{self.config['job_name_prefix']}-{self.watcher_id}"
        # Get file paths from DB where job_id is None
        captured_file_paths = [
            row[4] for row in self.db.get_captured_files(self.watcher_id) if row[1] is None
        ]
        
        if not captured_file_paths:
            logger.warning(f"No captured files found for watcher {self.watcher_id}, cannot create DIA-NN job")
            return
            
        logger.info(f"Creating DIA-NN job with captured files: {captured_file_paths}")
        
        job = Job(
            job_submitter="file_watcher",
            job_demands=self.config["job_demands"],
            job_type=self.config["job_type"],
            command="run_diann_search",
            expected_files=captured_file_paths,
            local_folder=self.config["folder_path"],
            job_name=job_name,
            job_colour="blue",
            num_steps=1,
            args=captured_file_paths,
            kwargs={}
        )

        # Store watcher_id in job's extras_dict
        job.extras_dict = getattr(job, 'extras_dict', {})
        job.extras_dict['watcher_id'] = self.watcher_id
        
        # Parse job parameters from job_demands JSON
        import json
        try:
            job.extras_dict.update(json.loads(self.config["job_demands"]))
        except json.JSONDecodeError:
            logger.warning(f"Could not parse job demands for DIA-NN job {job_name}")
            print(f"Warning: Could not parse job demands for DIA-NN job {job_name}")

        self.job_queue_manager.add_job(job)
        logger.info(f"Queued DIA-NN job {job_name} (ID: {job.job_id}) for watcher {self.watcher_id}")
        print(f"Queued DIA-NN job {job_name} (ID: {job.job_id}) for watcher {self.watcher_id}")

        # Update job record in database to link to this watcher
        try:
            from src.database.jobs_db import JobsDB
            jobs_db = JobsDB()
            jobs_db.link_job_to_watcher(job.job_id, self.watcher_id)
        except Exception as e:
            logger.error(f"Error linking job to watcher: {str(e)}", exc_info=True)

        # Update captured files with the job ID
        for file_path in captured_file_paths:
            try:
                self.db.update_captured_file_job_id(file_path, job.job_id)
            except Exception as e:
                logger.error(f"Error updating captured file job ID: {str(e)}", exc_info=True)

    def check_completion(self, job_id=None):
        """Check if all expected files are captured (for batch jobs only)."""
        if not self.is_batch_job or not self.expected_files:
            logger.info(f"Not a batch job or no expected files defined, skipping completion check")
            return False
            
        logger.info(f"Checking completion for watcher {self.watcher_id}")
        logger.info(f"Expected files: {self.expected_files}")
        logger.info(f"Captured files: {self.captured_files}")
        
        if self.expected_files.issubset(self.captured_files):
            logger.info(f"All expected files captured for watcher {self.watcher_id}. Creating job...")
            if self.config["job_type"].lower() == 'maxquant':
                self.create_maxquant_job()
            elif self.config["job_type"].lower() == 'diann':
                self.create_diann_job()
            self.db.update_watcher_status(self.watcher_id, "completed")
            return True
        else:
            missing = self.expected_files - self.captured_files
            logger.info(f"Still waiting for {len(missing)} files: {missing}")
            return False

class FileWatcher:
    def __init__(self, watcher_id, db, job_queue_manager):
        self.watcher_id = watcher_id
        self.db = db
        
        # Get watcher configuration 
        watcher_row = next((row for row in db.get_watchers() if row[0] == watcher_id), None)
        if not watcher_row:
            raise ValueError(f"No watcher found with ID {watcher_id}")
            
        # Convert row to dictionary for easier access
        self.config = dict(zip(
            ["id", "folder_path", "file_pattern", "job_type", "job_demands", "job_name_prefix", "creation_time",
             "execution_time", "status", "completion_time"],
            watcher_row
        ))
        
        # Normalize job_type to lowercase for case-insensitive comparisons
        self.config["job_type"] = self.config["job_type"].lower() if self.config["job_type"] else ""
        
        self.job_queue_manager = job_queue_manager
        self.event_handler = FileEventHandler(self.watcher_id, self.db, self.config, self.job_queue_manager)
        self.observer = Observer()
        self.handler = self.event_handler  # Add this reference for direct access
        
        logger.info(f"Initialized FileWatcher for watcher {watcher_id} with config: {self.config}")

    def get_config(self):
        """Return the current configuration."""
        return self.config

    def start(self):
        """Start watching the folder if status is 'monitoring'."""
        logger.info(f"[DEBUG] FileWatcher.start() called for watcher_id={self.watcher_id}")
        
        if self.config["status"] != "monitoring":
            logger.info(f"[DEBUG] Watcher {self.watcher_id} is {self.config['status']}, not starting.")
            print(f"Watcher {self.watcher_id} is {self.config['status']}, not starting.")
            return

        folder_path = Path(self.config["folder_path"])
        logger.info(f"Setting up watcher for folder: {folder_path}")
        
        # Ensure folder exists
        if not folder_path.exists():
            try:
                folder_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created folder path for watcher {self.watcher_id}: {folder_path}")
            except Exception as e:
                logger.error(f"Error creating folder {folder_path}: {str(e)}")
                print(f"Error creating folder {folder_path}: {str(e)}")
                return
            
        # Process any existing files in the folder
        logger.info(f"Scanning for existing files before starting observer")
        self.event_handler.scan_existing_files()
            
        # Schedule the observer
        self.observer.schedule(self.event_handler, str(folder_path), recursive=True)
        self.observer.start()
        logger.info(f"Started watching {folder_path} with pattern {self.config['file_pattern']}")
        print(f"Started watching {self.config['folder_path']} with pattern {self.config['file_pattern']}")
        
        try:
            while self.config["status"] == "monitoring":
                # Refresh config from database to check for status changes
                watcher_row = next((row for row in self.db.get_watchers() if row[0] == self.watcher_id), None)
                if watcher_row:
                    self.config = dict(zip(
                        ["id", "folder_path", "file_pattern", "job_type", "job_demands", "job_name_prefix", "creation_time",
                         "execution_time", "status", "completion_time"],
                        watcher_row
                    ))
                time.sleep(1)
            logger.info(f"Watcher {self.watcher_id} status changed to {self.config['status']}, stopping.")
            print(f"Watcher {self.watcher_id} status changed to {self.config['status']}, stopping.")
            self.stop()
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            logger.error(f"Error in watcher {self.watcher_id}: {str(e)}", exc_info=True)
            self.stop()

    def stop(self):
        """Stop the watcher."""
        logger.info(f"Stopping watcher {self.watcher_id}")
        if self.observer.is_alive():
            self.observer.stop()
            self.observer.join()
        logger.info(f"Stopped watching {self.config['folder_path']}")
        print(f"Stopped watching {self.config['folder_path']}")

    def force_rescan(self):
        """Force a rescan of the watched folder."""
        logger.info(f"Forcing rescan for watcher {self.watcher_id}")
        self.event_handler.scan_existing_files()