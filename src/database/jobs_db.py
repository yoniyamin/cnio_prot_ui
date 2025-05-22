# src/database/jobs_db.py
import sqlite3
from pathlib import Path
import os
from datetime import datetime
from src.logging_utils import get_logger
import threading

# Get a logger specific to this module
logger = get_logger(__name__)

# Singleton instance and lock
_jobs_db_instance = None
_jobs_db_lock = threading.Lock()

class JobsDB:
    def __new__(cls, db_path="config/jobs.db"):
        global _jobs_db_instance
        if _jobs_db_instance is None:
            with _jobs_db_lock:
                if _jobs_db_instance is None:
                    _jobs_db_instance = super(JobsDB, cls).__new__(cls)
                    _jobs_db_instance._initialized = False
        return _jobs_db_instance

    def __init__(self, db_path="config/jobs.db"):
        # Skip initialization if already done
        if hasattr(self, '_initialized') and self._initialized:
            return

        script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        project_root = script_dir.parent.parent
        self.db_path = project_root / db_path
        self.db_dir = self.db_path.parent

        if not self.db_dir.exists():
            self.db_dir.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(str(self.db_path)) as conn:
            self._create_tables(conn)
            self.ensure_job_columns()

        logger.debug(f"JobsDB initialized at: {self.db_path}")
        self._initialized = True

    def _create_tables(self, conn):
        """Create the jobs table (including watcher_name)."""
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    job_name TEXT NOT NULL,
                    job_submitter TEXT,
                    job_demands TEXT,
                    job_type TEXT NOT NULL,
                    local_folder TEXT,

                    watcher_name TEXT,      -- the watcher's name/prefix
                    watcher_id INTEGER,     -- optional, if you want a numeric FK
                    -- Or do a Foreign Key if you like:
                    -- FOREIGN KEY(watcher_id) REFERENCES watchers(id),

                    status TEXT DEFAULT 'waiting',
                    creation_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    completion_time DATETIME
                )
            """)
            conn.commit()
            logger.info("Jobs table created or exists")
        except sqlite3.Error as e:
            logger.error(f"Failed to create jobs table: {e}", exc_info=True)
            raise

    def add_job(
            self,
            job_id,
            job_name,
            job_type,
            job_submitter=None,
            job_demands=None,
            local_folder=None,
            watcher_name=None,
            watcher_id=None,
            expected_files=None,
            is_simulation=False
    ):
        """
        Insert a new Job record into the jobs table.
        :param job_id: The unique job ID (e.g., generated in Job __init__)
        :param job_name: A descriptive name (e.g., 'MaxQuantJob_12345')
        :param job_type: The job type string (e.g. 'maxquant')
        :param job_submitter: Username of whoever launched the job
        :param job_demands: JSON string representing job parameters or resources
        :param local_folder: Path to local folder used for the job
        :param watcher_name: (Optional) The watcher's name/prefix for reference
        :param watcher_id:   (Optional) Numeric watcher ID for reference
        :param expected_files: (Optional) JSON string of expected input files
        :param is_simulation: (Optional) Boolean flag indicating if this is a simulation job
        """
        # Convert expected_files to JSON string if it's a list/dictionary
        if expected_files and not isinstance(expected_files, str):
            import json
            expected_files = json.dumps(expected_files)

        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                # Make sure the is_simulation column exists
                self.ensure_job_columns()

                conn.execute(
                    """
                    INSERT INTO jobs
                        (job_id, job_name, job_type, job_submitter, job_demands,
                         local_folder, watcher_name, watcher_id, expected_files, is_simulation)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        job_name,
                        job_type,
                        job_submitter,
                        job_demands,
                        local_folder,
                        watcher_name,
                        watcher_id,
                        expected_files,
                        1 if is_simulation else 0
                    )
                )
                conn.commit()
                logger.info(f"Added new job record: {job_id} ({job_name}), type: {job_type}, watcher: {watcher_id}, simulation: {is_simulation}")
            except sqlite3.Error as e:
                logger.error(f"Failed to add job: {e}", exc_info=True)
                raise

    def get_job(self, job_id):
        """Retrieve a job row by job_id."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                cursor = conn.execute("""
                    SELECT * FROM jobs WHERE job_id = ?
                """, (job_id,))
                job = cursor.fetchone()
                logger.debug(f"Retrieved job {job_id}: {'found' if job else 'not found'}")
                return job
            except sqlite3.Error as e:
                logger.error(f"Failed to retrieve job: {e}", exc_info=True)
                raise

    def get_all_jobs(self):
        """Retrieve all jobs from the database with properly formatted fields."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM jobs 
                    ORDER BY creation_time DESC
                """)

                jobs = []
                for row in cursor.fetchall():
                    job_dict = dict(row)

                    # Handle job_demands field
                    job_demands = job_dict.get('job_demands')
                    if job_demands:
                        try:
                            import json
                            job_demands = json.loads(job_demands)
                        except json.JSONDecodeError:
                            # If not valid JSON, store as raw string
                            job_demands = {"raw_config": job_demands}
                    else:
                        job_demands = {}

                    jobs.append({
                        'job_id': job_dict.get('job_id'),
                        'job_name': job_dict.get('job_name', ''),
                        'job_type': job_dict.get('job_type', ''),
                        'status': job_dict.get('status', 'unknown'),
                        'progress': job_dict.get('progress', 0),  # Get progress from DB or default to 0
                        'creation_time': job_dict.get('creation_time'),
                        'completion_time': job_dict.get('completion_time'),
                        'job_submitter': job_dict.get('job_submitter', ''),
                        'local_folder': job_dict.get('local_folder', ''),
                        'watcher_id': job_dict.get('watcher_id'),
                        'watcher_name': job_dict.get('watcher_name', ''),
                        'job_demands': job_demands
                    })

                logger.debug(f"Retrieved {len(jobs)} jobs from database")
                return jobs
            except sqlite3.Error as e:
                logger.error(f"Failed to retrieve jobs: {e}", exc_info=True)
                return []

    # Add to jobs_db.py in the JobsDB class
    def get_watcher_id_for_job(self, job_id):
        """Get the watcher_id for a job."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.execute("SELECT watcher_id FROM jobs WHERE job_id = ?", (job_id,))
                result = cursor.fetchone()
                if result and result[0]:
                    logger.debug(f"Found watcher_id {result[0]} for job {job_id}")
                    return result[0]
                logger.debug(f"No watcher_id found for job {job_id}")
                return None
        except sqlite3.Error as e:
            logger.error(f"Database error getting watcher_id for job: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Error getting watcher_id for job: {e}", exc_info=True)
            return None

    def update_job_progress(self, job_id, progress):
        """Update the progress of a job in the database."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                # First check if we have a progress column
                cursor = conn.execute("PRAGMA table_info(jobs)")
                columns = [row[1] for row in cursor.fetchall()]

                # If progress column doesn't exist, add it
                if 'progress' not in columns:
                    conn.execute("ALTER TABLE jobs ADD COLUMN progress REAL DEFAULT 0")
                    logger.info("Added progress column to jobs table")
                    conn.commit()

                # Now update the progress
                conn.execute("""
                    UPDATE jobs SET progress = ? WHERE job_id = ?
                """, (progress, job_id))
                conn.commit()
                logger.debug(f"Updated progress for job {job_id} to {progress}")
                return True
        except sqlite3.Error as e:
            logger.error(f"Error updating job progress: {e}", exc_info=True)
            return False

    def ensure_job_columns(self):
        """Ensure the jobs table has all necessary columns."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                # Check if columns exist
                cursor = conn.execute("PRAGMA table_info(jobs)")
                columns = [row[1] for row in cursor.fetchall()]

                columns_to_add = []

                # Add progress column if it doesn't exist
                if 'progress' not in columns:
                    columns_to_add.append(('progress', 'REAL DEFAULT 0'))

                # Add expected_files column if it doesn't exist
                if 'expected_files' not in columns:
                    columns_to_add.append(('expected_files', 'TEXT'))

                # Add is_simulation column if it doesn't exist
                if 'is_simulation' not in columns:
                    columns_to_add.append(('is_simulation', 'INTEGER DEFAULT 0'))

                # Add update_time column if it doesn't exist
                if 'update_time' not in columns:
                    columns_to_add.append(('update_time', 'DATETIME'))

                # Execute ALTER TABLE statements for each missing column
                for col_name, col_type in columns_to_add:
                    try:
                        conn.execute(f"ALTER TABLE jobs ADD COLUMN {col_name} {col_type}")
                        logger.info(f"Added {col_name} column to jobs table")
                    except sqlite3.Error as e:
                        logger.error(f"Error adding column {col_name}: {e}", exc_info=True)

                if columns_to_add:
                    conn.commit()
                    logger.info(f"Added {len(columns_to_add)} new columns to jobs table")

                return True
        except sqlite3.Error as e:
            logger.error(f"Error ensuring job columns: {e}", exc_info=True)
            return False

    def update_job_status(self, job_id, new_status):
        """
        Centralized function to update job status in the database

        Args:
            job_id: The ID of the job to update
            new_status: The new status to set

        Returns:
            Boolean indicating success
        """
        return self.update_job_status_by_id(job_id, new_status)

    def update_job_status_by_id(self, job_id, new_status):
        """
        Update a job's status in the database by job_id.
        This is the instance method that actually performs the database update.

        Args:
            job_id: The ID of the job to update
            new_status: The new status to set

        Returns:
            Boolean indicating success
        """
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                from datetime import datetime
                current_time = datetime.now().isoformat()

                # First, ensure we have all required columns
                self.ensure_job_columns()

                # Check which columns exist in the table
                cursor = conn.execute("PRAGMA table_info(jobs)")
                columns = [row[1] for row in cursor.fetchall()]

                # Set completion_time if job is entering a terminal state
                if new_status in ['completed', 'errored', 'cancelled']:
                    if 'update_time' in columns:
                        conn.execute("""
                            UPDATE jobs 
                            SET status = ?, update_time = ?, completion_time = ? 
                            WHERE job_id = ?
                        """, (new_status, current_time, current_time, job_id))
                    else:
                        # Fallback if update_time column doesn't exist yet
                        conn.execute("""
                            UPDATE jobs 
                            SET status = ?, completion_time = ? 
                            WHERE job_id = ?
                        """, (new_status, current_time, job_id))
                else:
                    if 'update_time' in columns:
                        conn.execute("""
                            UPDATE jobs 
                            SET status = ?, update_time = ? 
                            WHERE job_id = ?
                        """, (new_status, current_time, job_id))
                    else:
                        # Fallback if update_time column doesn't exist yet
                        conn.execute("""
                            UPDATE jobs 
                            SET status = ?
                            WHERE job_id = ?
                        """, (new_status, job_id))

                conn.commit()

                # Check if any rows were affected
                affected_rows = conn.total_changes
                if affected_rows == 0:
                    logger.warning(f"No rows affected when updating job {job_id} status to {new_status}")
                    return False

                logger.info(f"Updated job {job_id} status to {new_status} in database")
                return True

        except sqlite3.Error as e:
            logger.error(f"Database error updating job status: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Error updating job status: {e}", exc_info=True)
            return False

    def link_job_to_watcher(self, job_id, watcher_id):
        """Link a job to a watcher in the database."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                # First check if the job exists
                cursor = conn.execute("SELECT id FROM jobs WHERE job_id = ?", (job_id,))
                job_record = cursor.fetchone()

                if job_record:
                    # Update the job with the watcher_id
                    conn.execute("""
                        UPDATE jobs SET watcher_id = ? WHERE job_id = ?
                    """, (watcher_id, job_id))
                    conn.commit()
                    logger.info(f"Linked job {job_id} to watcher {watcher_id}")
                    return True
                else:
                    logger.warning(f"Cannot link job {job_id} to watcher {watcher_id}: Job not found")
                    return False
        except sqlite3.Error as e:
            logger.error(f"Database error linking job to watcher: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Error linking job to watcher: {e}", exc_info=True)
            return False

    def update_files_status(self, job_id, files_ready):
        """Update job status when files are ready"""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                # If all files are ready, update the status to 'queued'
                if files_ready:
                    conn.execute("""
                        UPDATE jobs
                        SET status = 'queued'
                        WHERE job_id = ? AND status = 'waiting'
                    """, (job_id,))
                    conn.commit()
                    logger.info(f"Files ready for job {job_id}, status updated to 'queued'")
                return True
            except sqlite3.Error as e:
                logger.error(f"Failed to update files status: {e}", exc_info=True)
                return False

    def get_interrupted_jobs(self):
        """Get jobs that were in running/queued/waiting state when the system was last shut down.
        These are candidates for restart during system recovery."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM jobs 
                    WHERE status IN ('running', 'queued', 'waiting') 
                    ORDER BY creation_time DESC
                """)

                jobs = []
                for row in cursor.fetchall():
                    job_dict = dict(row)
                    job_demands = job_dict.get('job_demands')
                    if job_demands:
                        try:
                            import json
                            job_demands = json.loads(job_demands)
                        except:
                            job_demands = {"raw_config": job_demands}

                    jobs.append({
                        'job_id': job_dict.get('job_id'),
                        'job_name': job_dict.get('job_name', ''),
                        'status': job_dict.get('status', 'unknown'),
                        'output_folder': job_dict.get('local_folder', ''),
                        'local_folder': job_dict.get('local_folder', ''),
                        'job_type': job_dict.get('job_type', ''),
                        'watcher_id': job_dict.get('watcher_id'),
                        'job_demands': job_demands,
                        'expected_files': job_dict.get('expected_files'),
                        'is_simulation': job_dict.get('is_simulation', 0) == 1  # Convert to boolean
                    })

                logger.info(f"Found {len(jobs)} interrupted jobs to recover")
                return jobs
            except sqlite3.Error as e:
                logger.error(f"Failed to retrieve interrupted jobs: {e}", exc_info=True)
                return []

    def get_jobs_by_watcher_id(self, watcher_id):
        """Retrieve all jobs associated with a specific watcher_id."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM jobs 
                    WHERE watcher_id = ?
                    ORDER BY creation_time DESC
                """, (watcher_id,))
                jobs = []
                for row in cursor.fetchall():
                    job_dict = dict(row)
                    jobs.append({
                        'job_id': job_dict.get('job_id'),
                        'job_name': job_dict.get('job_name', ''),
                        'job_type': job_dict.get('job_type', ''),
                        'status': job_dict.get('status', 'unknown'),
                        'progress': 0,  # Default progress since not stored in DB
                        'creation_time': job_dict.get('creation_time'),
                        'completion_time': job_dict.get('completion_time'),
                        'job_submitter': job_dict.get('job_submitter', ''),
                        'local_folder': job_dict.get('local_folder', ''),
                        'watcher_id': job_dict.get('watcher_id'),
                        'watcher_name': job_dict.get('watcher_name', '')
                    })
                logger.debug(f"Retrieved {len(jobs)} jobs for watcher {watcher_id}")
                return jobs
            except sqlite3.Error as e:
                logger.error(f"Failed to retrieve jobs for watcher {watcher_id}: {e}", exc_info=True)
                return []

    def get_files_for_job(self, job_id):
        """
        Retrieve files associated with a specific job.
        Since there's no direct file association in the database schema,
        we'll attempt to look up the watcher_id and get files from there.

        :param job_id: The ID of the job to find files for
        :return: List of file dictionaries or an empty list
        """
        try:
            # First, see if there's an associated watcher
            watcher_id = self.get_watcher_id_for_job(job_id)

            if watcher_id:
                # We need to import WatcherDB within this method to avoid circular imports
                from src.database.watcher_db import WatcherDB
                watcher_db = WatcherDB()

                # Get files from the watcher database
                captured_files = watcher_db.get_captured_files(watcher_id)

                # Format the file data for API response
                file_list = []
                for f in captured_files:
                    # Only include files where the job_id matches or is empty
                    if f[1] == job_id or not f[1]:
                        file_list.append({
                            "id": f[0],
                            "job_id": job_id,
                            "watcher_id": watcher_id,
                            "file_name": f[3],
                            "file_path": f[4],
                            "capture_time": f[5],
                            "status": "captured"
                        })

                logger.debug(f"Retrieved {len(file_list)} files for job {job_id} via watcher {watcher_id}")
                return file_list

            # Otherwise, check if there's any job-specific file location we can use
            # Get the job details
            job = self.get_job(job_id)

            if job and job[6]:  # Check if local_folder exists
                local_folder = job[6]
                if os.path.isdir(local_folder):
                    # List files in the job's local folder
                    try:
                        files = []
                        for i, filename in enumerate(os.listdir(local_folder)):
                            file_path = os.path.join(local_folder, filename)
                            if os.path.isfile(file_path):
                                files.append({
                                    "id": i,
                                    "job_id": job_id,
                                    "file_name": filename,
                                    "file_path": file_path,
                                    "status": "local"  # These files weren't "captured" but exist locally
                                })
                        logger.debug(f"Retrieved {len(files)} local files for job {job_id} from folder {local_folder}")
                        return files
                    except Exception as e:
                        logger.error(f"Error listing files in job folder: {e}", exc_info=True)
                        # Continue to return empty list below

            # If no files found, return empty list
            logger.debug(f"No files found for job {job_id}")
            return []

        except Exception as e:
            logger.error(f"Error in get_files_for_job: {e}", exc_info=True)
            return []

    def get_job_demands(self, job_id):
        """
        Retrieve and parse the job_demands field for a specific job.
        This will attempt to parse the JSON string into a Python object.

        :param job_id: The ID of the job
        :return: Dict containing parsed job_demands, or None if not found/not parseable
        """
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.execute("""
                    SELECT job_demands FROM jobs WHERE job_id = ?
                """, (job_id,))
                result = cursor.fetchone()

                if result and result[0]:
                    job_demands_str = result[0]
                    try:
                        # Try to parse as JSON
                        import json
                        parsed = json.loads(job_demands_str)
                        logger.debug(f"Successfully parsed job_demands for job {job_id}")
                        return parsed
                    except json.JSONDecodeError:
                        # Return as raw string if not valid JSON
                        logger.warning(f"Job {job_id} has invalid JSON in job_demands field, returning as raw string")
                        return {"raw_config": job_demands_str}
                logger.debug(f"No job_demands found for job {job_id}")
                return None

        except sqlite3.Error as e:
            logger.error(f"Error getting job_demands for job {job_id}: {e}", exc_info=True)
            return None

    def get_jobs_by_watcher_id_and_status(self, watcher_id, status):
        """Retrieve all jobs associated with a specific watcher_id and having a specific status."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM jobs 
                    WHERE watcher_id = ? AND status = ?
                    ORDER BY creation_time DESC
                """, (watcher_id, status))
                jobs = []
                for row in cursor.fetchall():
                    job_dict = dict(row)
                    jobs.append({
                        'job_id': job_dict.get('job_id'),
                        'job_name': job_dict.get('job_name', ''),
                        'job_type': job_dict.get('job_type', ''),
                        'status': job_dict.get('status', 'unknown'),
                        'progress': job_dict.get('progress', 0),
                        'creation_time': job_dict.get('creation_time'),
                        'completion_time': job_dict.get('completion_time'),
                        'job_submitter': job_dict.get('job_submitter', ''),
                        'local_folder': job_dict.get('local_folder', ''),
                        'watcher_id': job_dict.get('watcher_id'),
                        'watcher_name': job_dict.get('watcher_name', '')
                    })
                logger.debug(f"Retrieved {len(jobs)} jobs with status '{status}' for watcher {watcher_id}")
                return jobs
            except sqlite3.Error as e:
                logger.error(f"Failed to retrieve jobs with status '{status}' for watcher {watcher_id}: {e}", exc_info=True)
                return []

    def get_job_by_id(self, job_id):
        """
        Retrieve a job by ID with a dictionary result format

        Args:
            job_id: The ID of the job to retrieve

        Returns:
            Dictionary with job details or None if not found
        """
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM jobs WHERE job_id = ?
                """, (job_id,))

                job = cursor.fetchone()

                if job:
                    # Convert to dictionary
                    job_dict = dict(job)

                    # Handle job_demands field
                    job_demands = job_dict.get('job_demands')
                    if job_demands:
                        try:
                            import json
                            job_demands = json.loads(job_demands)
                        except json.JSONDecodeError:
                            # If not valid JSON, store as raw string
                            job_demands = {"raw_config": job_demands}
                    else:
                        job_demands = {}

                    job_dict['job_demands'] = job_demands

                    # Convert is_simulation to boolean
                    job_dict['is_simulation'] = bool(job_dict.get('is_simulation', 0))

                    logger.debug(f"Retrieved job by ID {job_id}")
                    return job_dict

                logger.debug(f"Job {job_id} not found")
                return None

        except sqlite3.Error as e:
            logger.error(f"Database error retrieving job by ID: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Error retrieving job by ID: {e}", exc_info=True)
            return None

    def close(self):
        logger.debug("JobsDB connection closed (no persistent connection)")
        pass

def get_db_instance(db_type="jobs"):
    """
    Get appropriate database instance based on type.
    This is a helper function to maintain compatibility with code expecting
    this function to exist in the jobs_db module.

    Args:
        db_type: The type of database to get (default: "jobs")

    Returns:
        Database instance

    Raises:
        ValueError: If an unsupported database type is requested
    """
    if db_type == "jobs":
        return JobsDB()
    else:
        error_msg = f"jobs_db.get_db_instance only supports 'jobs' type, got: {db_type}"
        logger.error(error_msg)
        raise ValueError(error_msg)
