import fnmatch
import json
import time
import threading
from pathlib import Path
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from src.core.job import Job
from src.core.event_bus import event_bus, EventType, JobEvent
from src.logging_utils import get_logger
from src.utils.job_utils import update_job_status_safe, queue_job_safe
from datetime import datetime

logger = get_logger(__name__)
FILE_DELIMITER = ";"  # Delimiter for file patterns in the config


class FileEventHandler(FileSystemEventHandler):
    def __init__(self, watcher_id, db, config, job_queue_manager):
        self.watcher_id = watcher_id
        self.db = db
        self.config = config
        self.job_queue_manager = job_queue_manager
        self.patterns = self.config["file_pattern"].split(FILE_DELIMITER)
        self.patterns = [pattern.strip() for pattern in self.patterns]

        # Flag to identify batch jobs
        self.is_batch_job = self.config["job_type"].lower() in ['maxquant', 'diann']

        # For batch jobs, track expected and captured files
        if self.is_batch_job:
            self.expected_files = set(
                pattern for pattern in self.patterns if not any(c in "*?[" for c in pattern)
            )
            self.captured_files = set()
            self.associated_jobs = {}  # job_id -> set of expected files

        # Subscribe to job creation events
        event_bus.subscribe(EventType.JOB_CREATED, self._handle_job_created)

        logger.info(f"FileEventHandler initialized for watcher {watcher_id}")
        logger.info(f"Patterns: {self.patterns}, Is batch job: {self.is_batch_job}")
        if self.is_batch_job:
            logger.info(f"Expected files: {self.expected_files}")

    def _handle_job_created(self, event: JobEvent):
        """Handle job creation events to track expected files per job"""
        try:
            if event.watcher_id == self.watcher_id:
                job_id = event.job_id
                job = self.job_queue_manager._find_job_by_id(job_id)
                if job:
                    expected_files = set(job.get_list_expected_files())
                    self.associated_jobs[job_id] = expected_files
                    self.expected_files.update(expected_files)
                    logger.info(f"Tracking {len(expected_files)} files for job {job_id}")
        except Exception as e:
            logger.error(f"Error handling job created event: {e}", exc_info=True)

    def on_created(self, event):
        """Handle file creation events"""
        if not event.is_directory:
            self._process_file_event(event.src_path, "created")

    def on_moved(self, event):
        """Handle file move events"""
        if not event.is_directory:
            self._process_file_event(event.dest_path, "moved")

    def on_modified(self, event):
        """Handle file modification events"""
        if not event.is_directory:
            if self._is_file_stable(event.src_path):
                self._process_file_event(event.src_path, "modified")

    def _process_file_event(self, file_path, event_type):
        """Process a file system event"""
        try:
            file_name = Path(file_path).name

            # Check if the watcher is monitoring
            if self.config["status"] != "monitoring":
                logger.debug(f"Watcher {self.watcher_id} not monitoring, ignoring file {file_name}")
                return

            # Check if file matches patterns
            if not self._matches_patterns(file_name):
                logger.debug(f"File {file_name} doesn't match patterns, ignoring")
                return

            # Check if already captured
            if file_path in getattr(self, 'captured_files', set()):
                logger.debug(f"File {file_name} already captured, skipping")
                return

            # Check if file exists and is stable
            if not Path(file_path).exists():
                logger.warning(f"File {file_path} no longer exists")
                return

            if not self._is_file_stable(file_path):
                logger.debug(f"File {file_name} not stable, will check later")
                return

            logger.info(f"Processing {event_type} file: {file_name}")

            # Handle based on job type
            if self.is_batch_job:
                self._handle_batch_file(file_path, file_name)
            elif self.config["job_type"].lower() != 'none':
                self._handle_individual_file(file_path)
            else:
                self._handle_no_job_file(file_path, file_name)

        except Exception as e:
            logger.error(f"Error processing file event for {file_path}: {e}", exc_info=True)

    def _matches_patterns(self, file_name):
        """Check if a file name matches any patterns"""
        for pattern in self.patterns:
            if not pattern:
                continue
            if pattern == file_name or fnmatch.fnmatch(file_name, pattern):
                return True
        return False

    def _is_file_stable(self, file_path, stability_time=2):
        """Check if a file is stable (not being written to)"""
        try:
            initial_size = Path(file_path).stat().st_size
            time.sleep(stability_time)
            if not Path(file_path).exists():
                return False
            final_size = Path(file_path).stat().st_size
            return initial_size == final_size
        except (OSError, IOError):
            return False

    def _handle_batch_file(self, file_path, file_name):
        """Handle file for batch jobs (MaxQuant, DIA-NN)"""
        # Check if already in database
        if self.db.file_exists_in_watcher(self.watcher_id, file_name):
            logger.info(f"File {file_name} already in database, skipping")
            # Still add to captured set for tracking
            self.captured_files.add(file_path)
            self.check_completion()
            return

        # Add to captured files
        self.captured_files.add(file_path)

        # Add to database
        try:
            file_id = self.db.add_captured_file(
                job_id=None,  # No job yet for batch processing
                watcher_id=self.watcher_id,
                file_name=file_name,
                file_path=file_path
            )
            logger.info(f"Added file {file_name} to database with ID: {file_id}")
        except Exception as e:
            logger.error(f"Error adding file to database: {e}", exc_info=True)

        # Publish file captured event
        event = JobEvent(
            event_type=EventType.FILE_CAPTURED,
            job_id="",  # No specific job yet
            data={
                'file_name': file_name,
                'file_path': file_path,
                'file_id': getattr(self, 'file_id', None),
                'watcher_id': self.watcher_id
            },
            timestamp=datetime.now().timestamp(),
            watcher_id=self.watcher_id
        )
        event_bus.publish(event)

        # Check if all expected files are captured
        self.check_completion()

    def _handle_individual_file(self, file_path):
        """Handle file for individual jobs"""
        # Check if job already exists for this file
        try:
            from src.database.jobs_db import JobsDB
            jobs_db = JobsDB()
            existing_jobs = jobs_db.get_jobs_by_watcher_id(self.watcher_id)

            # Check if file already processed
            for job in existing_jobs:
                job_files = jobs_db.get_files_for_job(job.get('job_id'))
                if any(str(file_path) == row.get('file_path') for row in job_files):
                    logger.info(f"File {file_path} already has a job, skipping")
                    return

            # Create new job
            self._create_individual_job(file_path)

        except Exception as e:
            logger.error(f"Error checking existing jobs: {e}", exc_info=True)
            # Create job anyway if there's an error
            self._create_individual_job(file_path)

    def _create_individual_job(self, file_path):
        """Create a job for a single file"""
        job_name = f"{self.config['job_name_prefix']}-{Path(file_path).stem}-{self.watcher_id}"

        job = Job(
            job_submitter="file_watcher",
            job_demands=self.config["job_demands"],
            job_type=self.config["job_type"],
            command="run_" + self.config["job_type"].lower().replace("_handler", "_search"),
            expected_files=[str(file_path)],
            local_folder=str(Path(file_path).parent),
            job_name=job_name,
            job_colour="blue",
            num_steps=1,
            args=[str(file_path)],
            kwargs={}
        )

        # Add job to queue manager (this publishes job creation event)
        self.job_queue_manager.add_job(job)
        logger.info(f"Created job {job_name} (ID: {job.job_id}) for {file_path}")

        # Add file to database with job association
        try:
            self.db.add_captured_file(
                job_id=job.job_id,
                watcher_id=self.watcher_id,
                file_name=Path(file_path).name,
                file_path=str(file_path)
            )
        except Exception as e:
            logger.error(f"Error adding file to database: {e}", exc_info=True)

    def _handle_no_job_file(self, file_path, file_name):
        """Handle file for no-job watchers"""
        logger.info(f"No-job watcher: adding file {file_name} to database")
        try:
            self.db.add_captured_file(
                job_id=None,
                watcher_id=self.watcher_id,
                file_name=file_name,
                file_path=str(file_path)
            )
        except Exception as e:
            logger.error(f"Error adding no-job file to database: {e}", exc_info=True)

    def scan_existing_files(self):
        """Scan for existing files that match patterns"""
        folder_path = Path(self.config["folder_path"])
        logger.info(f"Scanning existing files in {folder_path}")

        if not folder_path.exists():
            logger.warning(f"Folder {folder_path} doesn't exist, creating it")
            try:
                folder_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created folder {folder_path}")
            except Exception as e:
                logger.error(f"Error creating folder {folder_path}: {e}")
                return

        # Find matching files
        found_files = []
        for pattern in self.patterns:
            if not pattern:
                continue

            try:
                # For exact filenames
                if not any(c in "*?[" for c in pattern):
                    exact_file = folder_path / pattern
                    if exact_file.is_file():
                        found_files.append(exact_file)
                else:
                    # For wildcard patterns
                    matching_files = list(folder_path.glob(pattern))
                    found_files.extend([f for f in matching_files if f.is_file()])

                    # Fallback to manual fnmatch
                    if not matching_files:
                        for file_path in folder_path.iterdir():
                            if file_path.is_file() and fnmatch.fnmatch(file_path.name, pattern):
                                if file_path not in found_files:
                                    found_files.append(file_path)
            except Exception as e:
                logger.error(f"Error scanning pattern {pattern}: {e}")

        logger.info(f"Found {len(found_files)} files matching patterns")

        # Process found files
        for file_path in found_files:
            try:
                file_name = file_path.name

                # Check if already in database
                if not self.db.file_exists_in_watcher(self.watcher_id, file_name):
                    logger.info(f"Processing existing file: {file_path}")
                    # Add to database
                    self.db.add_captured_file(None, self.watcher_id, file_name, str(file_path))

                    # For batch jobs, add to captured set
                    if self.is_batch_job:
                        self.captured_files.add(str(file_path))
                else:
                    # Make sure it's in captured set for batch jobs
                    if self.is_batch_job and file_name not in self.captured_files:
                        self.captured_files.add(str(file_path))

            except Exception as e:
                logger.error(f"Error processing found file {file_path}: {e}")

        # Check completion for batch jobs
        if self.is_batch_job:
            self.check_completion()

    def check_completion(self, job_id=None):
        """Check if all expected files are captured for batch jobs"""
        if not self.is_batch_job or not self.expected_files:
            return False

        logger.info(f"Checking completion for watcher {self.watcher_id}")
        logger.info(f"Expected: {self.expected_files}")
        logger.info(f"Captured: {[Path(f).name for f in self.captured_files]}")

        # Convert captured file paths to names for comparison
        captured_names = {Path(f).name for f in self.captured_files}

        if self.expected_files.issubset(captured_names):
            logger.info(f"All expected files captured for watcher {self.watcher_id}")

            # Check if job already exists
            try:
                from src.database.jobs_db import JobsDB
                jobs_db = JobsDB()
                existing_jobs = jobs_db.get_jobs_by_watcher_id(self.watcher_id)

                if existing_jobs:
                    logger.info(f"Job already exists for watcher {self.watcher_id}")
                    # Update any waiting jobs to queued
                    for job in existing_jobs:
                        if job.get('status') == 'waiting':
                            job_id = job.get('job_id')
                            logger.info(f"Moving job {job_id} from waiting to queued")
                            queue_job_safe(job_id)
                            jobs_db.update_files_status(job_id, True)

                    # Update watcher status
                    self.db.update_watcher_status(self.watcher_id, "completed")
                    return True
            except Exception as e:
                logger.error(f"Error checking existing jobs: {e}", exc_info=True)

            # Create new job based on type
            if self.config["job_type"].lower() == 'maxquant':
                self._create_maxquant_job()
            elif self.config["job_type"].lower() == 'diann':
                self._create_diann_job()

            # Update watcher status
            self.db.update_watcher_status(self.watcher_id, "completed")
            return True
        else:
            missing = self.expected_files - captured_names
            logger.info(f"Still waiting for {len(missing)} files: {missing}")
            return False

    def _create_maxquant_job(self):
        """Create MaxQuant job for all captured files"""
        job_name = f"{self.config['job_name_prefix']}-{self.watcher_id}"

        # Get captured file paths
        captured_file_paths = [
            row[4] for row in self.db.get_captured_files(self.watcher_id) if row[1] is None
        ]

        if not captured_file_paths:
            logger.warning(f"No captured files for watcher {self.watcher_id}")
            return

        logger.info(f"Creating MaxQuant job with {len(captured_file_paths)} files")

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

        # Store watcher ID in extras
        job.extras_dict = getattr(job, 'extras_dict', {})
        job.extras_dict['watcher_id'] = self.watcher_id

        # Parse job demands
        try:
            job.extras_dict.update(json.loads(self.config["job_demands"]))
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"Could not parse job demands for MaxQuant job")

        # Add job to queue (publishes creation event)
        self.job_queue_manager.add_job(job)
        logger.info(f"Created MaxQuant job {job_name} (ID: {job.job_id})")

        # Link job to watcher in database
        try:
            from src.database.jobs_db import JobsDB
            jobs_db = JobsDB()
            jobs_db.link_job_to_watcher(job.job_id, self.watcher_id)

            # Update captured files with job ID
            for file_path in captured_file_paths:
                self.db.update_captured_file_job_id(file_path, job.job_id)
        except Exception as e:
            logger.error(f"Error linking job to watcher: {e}", exc_info=True)

    def _create_diann_job(self):
        """Create DIA-NN job for all captured files"""
        job_name = f"{self.config['job_name_prefix']}-{self.watcher_id}"

        # Get captured file paths
        captured_file_paths = [
            row[4] for row in self.db.get_captured_files(self.watcher_id) if row[1] is None
        ]

        if not captured_file_paths:
            logger.warning(f"No captured files for watcher {self.watcher_id}")
            return

        logger.info(f"Creating DIA-NN job with {len(captured_file_paths)} files")

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

        # Store watcher ID in extras
        job.extras_dict = getattr(job, 'extras_dict', {})
        job.extras_dict['watcher_id'] = self.watcher_id

        # Parse job demands
        try:
            job.extras_dict.update(json.loads(self.config["job_demands"]))
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"Could not parse job demands for DIA-NN job")

        # Add job to queue (publishes creation event)
        self.job_queue_manager.add_job(job)
        logger.info(f"Created DIA-NN job {job_name} (ID: {job.job_id})")

        # Link job to watcher in database
        try:
            from src.database.jobs_db import JobsDB
            jobs_db = JobsDB()
            jobs_db.link_job_to_watcher(job.job_id, self.watcher_id)

            # Update captured files with job ID
            for file_path in captured_file_paths:
                self.db.update_captured_file_job_id(file_path, job.job_id)
        except Exception as e:
            logger.error(f"Error linking job to watcher: {e}", exc_info=True)

    def cleanup(self):
        """Clean up the handler"""
        event_bus.unsubscribe(EventType.JOB_CREATED, self._handle_job_created)
        logger.info(f"FileEventHandler cleaned up for watcher {self.watcher_id}")


class FileWatcher:
    def __init__(self, watcher_id, db, job_queue_manager):
        self.watcher_id = watcher_id
        self.db = db
        self.job_queue_manager = job_queue_manager

        # Get watcher configuration
        watcher_row = next((row for row in db.get_watchers() if row[0] == watcher_id), None)
        if not watcher_row:
            raise ValueError(f"No watcher found with ID {watcher_id}")

        # Convert row to dictionary
        self.config = dict(zip(
            ["id", "folder_path", "file_pattern", "job_type", "job_demands",
             "job_name_prefix", "creation_time", "execution_time", "status", "completion_time"],
            watcher_row
        ))

        # Normalize job_type
        self.config["job_type"] = self.config["job_type"].lower() if self.config["job_type"] else ""

        # Initialize components
        self.event_handler = FileEventHandler(self.watcher_id, self.db, self.config, self.job_queue_manager)
        self.observer = Observer()
        self.handler = self.event_handler  # For direct access

        logger.info(f"FileWatcher initialized for watcher {watcher_id}")

    def get_config(self):
        """Return current configuration"""
        return self.config

    def start(self):
        """Start watching the folder"""
        logger.info(f"Starting watcher {self.watcher_id}")

        # Refresh config from database
        self._refresh_config()

        if self.config["status"] != "monitoring":
            logger.info(f"Watcher {self.watcher_id} status is {self.config['status']}, not starting")
            return

        folder_path = Path(self.config["folder_path"])
        logger.info(f"Setting up watcher for folder: {folder_path}")

        # Ensure folder exists
        if not folder_path.exists():
            try:
                folder_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created folder for watcher {self.watcher_id}: {folder_path}")
            except Exception as e:
                logger.error(f"Error creating folder {folder_path}: {e}")
                return

        # Stop existing observer if running
        if self.observer.is_alive():
            logger.warning(f"Observer for watcher {self.watcher_id} already running, stopping first")
            try:
                self.observer.stop()
                self.observer.join(timeout=5)
            except Exception as e:
                logger.error(f"Error stopping existing observer: {e}")
            self.observer = Observer()

        # Scan existing files
        logger.info(f"Scanning existing files for watcher {self.watcher_id}")
        self.event_handler.scan_existing_files()

        # Start observer
        try:
            self.observer.schedule(self.event_handler, str(folder_path), recursive=True)
            self.observer.start()
            logger.info(f"Started watching {folder_path}")

            # Monitor for status changes
            monitoring_start_time = time.time()
            while self.config["status"] == "monitoring":
                if time.time() - monitoring_start_time > 60:
                    logger.debug(f"Watcher {self.watcher_id} still monitoring")
                    monitoring_start_time = time.time()

                self._refresh_config()
                time.sleep(1)

            logger.info(f"Watcher {self.watcher_id} status changed to {self.config['status']}, stopping")
            self.stop()

        except KeyboardInterrupt:
            logger.info(f"Keyboard interrupt, stopping watcher {self.watcher_id}")
            self.stop()
        except Exception as e:
            logger.error(f"Error in watcher {self.watcher_id}: {e}", exc_info=True)
            self.stop()

    def _refresh_config(self):
        """Refresh watcher configuration from database"""
        try:
            watcher_row = next((row for row in self.db.get_watchers() if row[0] == self.watcher_id), None)
            if watcher_row:
                old_status = self.config.get("status")
                self.config = dict(zip(
                    ["id", "folder_path", "file_pattern", "job_type", "job_demands",
                     "job_name_prefix", "creation_time", "execution_time", "status", "completion_time"],
                    watcher_row
                ))

                if old_status and old_status != self.config["status"]:
                    logger.info(f"Watcher {self.watcher_id} status changed: {old_status} -> {self.config['status']}")

                # Update event handler config
                if hasattr(self, 'event_handler'):
                    self.event_handler.config = self.config
            else:
                logger.warning(f"Watcher {self.watcher_id} not found during refresh")
        except Exception as e:
            logger.error(f"Error refreshing watcher {self.watcher_id} config: {e}")

    def stop(self):
        """Stop the watcher"""
        logger.info(f"Stopping watcher {self.watcher_id}")
        if self.observer.is_alive():
            self.observer.stop()
            self.observer.join()

        # Clean up event handler
        if hasattr(self, 'event_handler'):
            self.event_handler.cleanup()

        logger.info(f"Stopped watcher {self.watcher_id}")

    def force_rescan(self):
        """Force a rescan of the watched folder"""
        logger.info(f"Forcing rescan for watcher {self.watcher_id}")
        if hasattr(self, 'handler'):
            self.handler.scan_existing_files()
        elif hasattr(self, 'event_handler'):
            self.event_handler.scan_existing_files()
        else:
            logger.error(f"No handler available for watcher {self.watcher_id}")