# src/database/jobs_db.py
import sqlite3
from pathlib import Path
import os
from datetime import datetime
from src.utils import logger

class JobsDB:
    def __init__(self, db_path="config/jobs.db"):
        script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        project_root = script_dir.parent.parent
        self.db_path = project_root / db_path
        self.db_dir = self.db_path.parent

        if not self.db_dir.exists():
            self.db_dir.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(str(self.db_path)) as conn:
            self._create_tables(conn)

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
        except sqlite3.Error as e:
            print(f"Failed to create jobs table: {e}")
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
            watcher_id=None
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
        """
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO jobs
                        (job_id, job_name, job_type, job_submitter, job_demands,
                         local_folder, watcher_name, watcher_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        job_name,
                        job_type,
                        job_submitter,
                        job_demands,
                        local_folder,
                        watcher_name,
                        watcher_id
                    )
                )
                conn.commit()
            except sqlite3.Error as e:
                print(f"Failed to add job: {e}")
                raise

    def get_job(self, job_id):
        """Retrieve a job row by job_id."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                cursor = conn.execute("""
                    SELECT * FROM jobs WHERE job_id = ?
                """, (job_id,))
                return cursor.fetchone()
            except sqlite3.Error as e:
                print(f"Failed to retrieve job: {e}")
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
                    # Convert db row to dictionary and format fields to match in-memory job structure
                    job_dict = dict(row)
                    jobs.append({
                        'job_id': job_dict.get('job_id'),
                        'job_name': job_dict.get('job_name', ''),
                        'job_type': job_dict.get('job_type', ''),
                        'status': job_dict.get('status', 'unknown'),
                        'progress': 0,  # Default progress since we don't store it in DB
                        'creation_time': job_dict.get('creation_time'),
                        'completion_time': job_dict.get('completion_time'),
                        'job_submitter': job_dict.get('job_submitter', ''),
                        'local_folder': job_dict.get('local_folder', ''),
                        'watcher_id': job_dict.get('watcher_id'),
                        'watcher_name': job_dict.get('watcher_name', '')
                    })

                return jobs
            except sqlite3.Error as e:
                print(f"Failed to retrieve jobs: {e}")
                return []

    # Add to jobs_db.py in the JobsDB class
    def get_watcher_id_for_job(self, job_id):
        """Get the watcher_id for a job."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.execute("SELECT watcher_id FROM jobs WHERE job_id = ?", (job_id,))
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0]
                return None
        except sqlite3.Error as e:
            logger.error(f"Database error getting watcher_id for job: {e}")
            return None
        except Exception as e:
            logger.error(f"Error getting watcher_id for job: {e}")
            return None

    def update_job_status(self, job_id, status):
        """Update status, possibly setting completion_time if done or errored."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                if status in ["completed", "cancelled", "errored"]:
                    conn.execute("""
                        UPDATE jobs
                        SET status = ?, completion_time = CURRENT_TIMESTAMP
                        WHERE job_id = ?
                    """, (status, job_id))
                else:
                    conn.execute("""
                        UPDATE jobs
                        SET status = ?
                        WHERE job_id = ?
                    """, (status, job_id))
                conn.commit()
            except sqlite3.Error as e:
                print(f"Failed to update job status: {e}")
                raise

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
            logger.error(f"Database error linking job to watcher: {e}")
            return False
        except Exception as e:
            logger.error(f"Error linking job to watcher: {e}")
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
                return True
            except sqlite3.Error as e:
                print(f"Failed to update files status: {e}")
                return False

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
                return jobs
            except sqlite3.Error as e:
                print(f"Failed to retrieve jobs for watcher {watcher_id}: {e}")
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
                        return files
                    except Exception as e:
                        print(f"Error listing files in job folder: {e}")
                        # Continue to return empty list below

            # If no files found, return empty list
            return []

        except Exception as e:
            print(f"Error in get_files_for_job: {e}")
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
                        return json.loads(job_demands_str)
                    except json.JSONDecodeError:
                        # Return as raw string if not valid JSON
                        return {"raw_config": job_demands_str}
                return None

        except sqlite3.Error as e:
            print(f"Error getting job_demands for job {job_id}: {e}")
            return None

    def close(self):
        pass
