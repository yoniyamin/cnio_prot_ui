import uuid
from threading import Lock
from datetime import datetime

class Job:
    def __init__(self, job_submitter, job_demands, job_type, command, expected_files,
                 local_folder, job_name, job_colour=None, num_steps=None, args=None, kwargs=None):
        self.job_submitter = job_submitter
        self.job_demands = job_demands
        self.job_type = job_type
        self.command = command
        self.expected_files = set(expected_files)
        self.original_expected_files = set(expected_files)
        self.local_folder = local_folder
        self.job_name = job_name
        self.job_colour = job_colour or 'grey'
        self.num_steps = num_steps or 3
        self.args = args or []
        self.kwargs = kwargs or {}

        self.progress = 0
        self.status = 'waiting'  # Initial status
        self.job_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4()}"  # Timestamped ID
        self.removed = False
        self.cancelled = False
        self.error_flag = False
        self.time_stamp = None
        self.extras_dict = {}

        # Locks
        self.expected_files_lock = Lock()
        self.status_lock = Lock()

    def _format_timestamp(self):
        return self.time_stamp.isoformat() if self.time_stamp else None

    def to_dict(self):
        """Convert the job to a dictionary for serialization."""
        return {
            "job_submitter": self.job_submitter,
            "job_name": self.job_name,
            "job_id": self.job_id,
            "job_demands": self.job_demands,
            "job_type": self.job_type,
            "num_steps": self.num_steps,
            "progress": self.progress,
            "job_colour": self.job_colour,
            "command": self.command,
            "expected_files": list(self.original_expected_files),  # Convert set to list for JSON serialization
            "local_folder": self.local_folder,
            "status": self.status,
            "time_stamp": self._format_timestamp(),
            "extras_dict": self.extras_dict
        }

    def change_job_status(self, new_status):
        valid_statuses = {"waiting", "queued", "running", "completed", "cancelled", "errored"}
        if new_status not in valid_statuses:
            raise ValueError(f"Invalid job status: {new_status}")
        with self.status_lock:
            self.status = new_status

    def modify_expected_files(self, file_name, action="add"):
        with self.expected_files_lock:
            if action == "add":
                self.expected_files.add(file_name)
            elif action == "remove":
                self.expected_files.discard(file_name)
            else:
                raise ValueError("Action must be 'add' or 'remove'")

    def get_list_expected_files(self):
        return list(self.expected_files)

    def update_progress(self, increment):
        if not (0 <= self.progress + increment <= 1):
            raise ValueError("Progress must be between 0 and 1")
        self.progress += increment
