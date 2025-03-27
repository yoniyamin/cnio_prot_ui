import json
from pathlib import Path
from threading import Lock


class SharedQueueManager:
    def __init__(self, queue_file):
        """
        Manages shared queue operations.
        :param queue_file: Path to the shared queue file.
        """
        self.queue_file = Path(queue_file)
        self.lock = Lock()
        self.ensure_shared_queue_exists()

    def ensure_shared_queue_exists(self):
        """
        Ensures the shared queue file exists.
        """
        with self.lock:
            if not self.queue_file.exists():
                with open(self.queue_file, 'w') as json_queue:
                    json.dump({"version": 1, "jobs": []}, json_queue, indent=4)

    def get_shared_queue_safe(self):
        """
        Safely reads the shared queue from the file.
        :return: The shared queue data.
        """
        try:
            with self.lock:
                with open(self.queue_file, 'r') as json_queue:
                    return json.load(json_queue)
        except (json.JSONDecodeError, FileNotFoundError, IOError) as e:
            print(f"Error reading shared queue: {e}")
            return {"version": 1, "jobs": []}  # Return a default structure


    def write_shared_queue_safe(self, shared_queue, version):
        """
        Safely writes the shared queue data to the file.
        :param shared_queue: The updated shared queue data.
        :param version: The new version number.
        """
        with self.lock:
            with open(self.queue_file, 'w') as json_queue:
                json.dump({"version": version, "jobs": shared_queue}, json_queue, indent=4)

    def add_job(self, job):
        """
        Adds a job to the shared queue.
        :param job: The job to add (as a dictionary).
        """
        shared_queue = self.get_shared_queue_safe()
        jobs = shared_queue.get("jobs", [])
        version = shared_queue.get("version", 1)
        jobs.append(job.to_dict())
        self.write_shared_queue_safe(jobs, version + 1)

    def remove_job(self, job_id):
        """
        Removes a job from the shared queue.
        :param job_id: The ID of the job to remove.
        """
        shared_queue = self.get_shared_queue_safe()
        jobs = shared_queue.get("jobs", [])
        version = shared_queue.get("version", 1)
        updated_jobs = [j for j in jobs if j.get('job_id') != job_id]
        self.write_shared_queue_safe(updated_jobs, version + 1)

    def get_job_by_id(self, job_id):
        """
        Retrieves a job by its ID.
        :param job_id: The ID of the job to find.
        :return: The job dictionary or None if not found.
        """
        shared_queue = self.get_shared_queue_safe()
        jobs = shared_queue.get("jobs", [])
        return next((job for job in jobs if job.get('job_id') == job_id), None)

    def update_job_status(self, job_id, status):
        """
        Updates the status of a job in the shared queue.
        :param job_id: The ID of the job to update.
        :param status: The new status to set.
        """
        shared_queue = self.get_shared_queue_safe()
        jobs = shared_queue.get("jobs", [])
        version = shared_queue.get("version", 1)
        for job in jobs:
            if job.get('job_id') == job_id:
                job['status'] = status
                break
        self.write_shared_queue_safe(jobs, version + 1)

    def increment_job_progress(self, job_id, progress_increment):
        """
        Increment the progress of a job in the shared queue.
        :param job_id: The ID of the job.
        :param progress_increment: The amount to increment progress.
        """
        shared_queue = self.get_shared_queue_safe()
        jobs = shared_queue.get("jobs", [])
        version = shared_queue.get("version", 1)

        # Update progress for the specified job
        for job in jobs:
            if job.get('job_id') == job_id:
                current_progress = float(job.get('progress', 0))
                job['progress'] = current_progress + progress_increment
                break

        # Save updated shared queue
        self.write_shared_queue_safe(jobs, version + 1)

    def get_priority(self, job):
        """
        Determine if a job has the highest priority among similar-demand jobs in the shared queue.
        :param job: The job object.
        :return: True if the job can proceed based on its demand type and priority, False otherwise.
        """
        shared_queue = self.get_shared_queue_safe()
        jobs = shared_queue.get("jobs", [])

        # Filter jobs based on the current job's demand type
        if job.job_demands == 'heavy':
            # Only consider other 'heavy' jobs
            relevant_jobs = [j for j in jobs if j['job_demands'] == 'heavy']
        else:
            # Allow HPC jobs to proceed immediately
            return True

        # Sort the relevant jobs by timestamp
        sorted_queue = sorted(relevant_jobs, key=lambda x: x['time_stamp'])

        # Check if the current job is at the top of the queue
        for index, job_dict in enumerate(sorted_queue):
            if job_dict['job_id'] == job.job_id:
                job.priority = index
                return index == 0  # True if the job is at the top of the relevant queue

        return False

    def remove_user_jobs(self, username):
        """
        Removes all jobs submitted by the specified user.
        :param username: The username to filter jobs by.
        :return: True if successful, False otherwise.
        """
        max_retries = 3
        for _ in range(max_retries):
            shared_queue = self.get_shared_queue_safe()
            jobs = shared_queue.get("jobs", [])
            version = shared_queue.get("version", 1)

            updated_jobs = [j for j in jobs if j.get('job_submitter') != username]
            current_queue = self.get_shared_queue_safe()
            if current_queue.get("version") == version:
                self.write_shared_queue_safe(updated_jobs, version + 1)
                return True
        return False

