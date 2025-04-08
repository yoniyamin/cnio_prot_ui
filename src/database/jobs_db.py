# src/database/jobs_db.py
import sqlite3
from pathlib import Path
import os
from datetime import datetime


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
        """Retrieve the watcher_id associated with a specific job."""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                cursor = conn.execute("""
                    SELECT watcher_id 
                    FROM jobs 
                    WHERE job_id = ?
                """, (job_id,))
                result = cursor.fetchone()
                return result[0] if result and result[0] is not None else None
            except sqlite3.Error as e:
                print(f"Failed to get watcher ID for job {job_id}: {e}")
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
        """Update a job record to associate it with a watcher"""
        with sqlite3.connect(str(self.db_path)) as conn:
            try:
                conn.execute("""
                    UPDATE jobs
                    SET watcher_id = ?
                    WHERE job_id = ?
                """, (watcher_id, job_id))
                conn.commit()
                print(f"Linked job {job_id} to watcher {watcher_id}")
                return True
            except sqlite3.Error as e:
                print(f"Failed to link job to watcher: {e}")
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


    def close(self):
        pass
