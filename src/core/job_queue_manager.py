import logging
import os
from queue import Queue as ThreadQueue, Empty
import threading
from threading import Lock
from pathlib import Path
import time
from src.core.job import Job # not needed at that point, but maybe in the future.
from src.database.watcher_db import WatcherDB
from src.handlers.run_maxquant import MaxQuantHandler

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

            # Update status in the database too
            from src.database.jobs_db import JobsDB
            jobs_db = JobsDB()
            jobs_db.update_job_status(job.job_id, 'queued')  # Update status
            jobs_db.update_files_status(job.job_id, True)  # Mark files as ready

            # Put the job back in the queue to ensure it gets processed
            self.job_queue.put(job)

    def run_job(self, job):
        logger.info(f"Running job {job.job_id}")
        with self.lock:
            job.change_job_status('running')
            self.remove_job_from_set(job, self.queued_jobs, self.queued_jobs_lock)
            self.running_jobs[job.job_id] = job

        try:
            if job.job_type == 'maxquant':
                params = job.extras_dict
                # Add progress and stop queues to the job
                job.progress_queue = ThreadQueue()
                job.stop_queue = ThreadQueue()

                handler = MaxQuantHandler(
                    stop_queue=job.stop_queue,
                    progress_queue=job.progress_queue,
                    MQ_version=params['mq_version'],
                    MQ_path=params['mq_path'],
                    db_map=params.get('db_map', ''),  # Provide default or fetch from config
                    fasta_folder=params['fasta_folder'],
                    output_folder=params['output_folder'],
                    conditions=params['conditions_file'],
                    dbs=params['dbs'],
                    protein_quantification=params.get('protein_quantification', 'Razor + Unique'),
                    missed_cleavages=params.get('missed_cleavages', '2'),
                    fixed_mods=params.get('fixed_mods', 'Carbamidomethyl (C)'),
                    variable_mods=params.get('variable_mods', 'Oxidation (M), Acetyl (Protein N-term)'),
                    enzymes=params.get('enzymes', 'Trypsin/P'),
                    match_between_runs=params.get('match_between_runs', False),
                    second_peptide=params.get('second_peptide', False),
                    id_parse_rule=params.get('id_parse_rule', '>.*\\|(.*)\\|'),
                    desc_parse_rule=params.get('desc_parse_rule', '>(*)'),
                    andromeda_path=params.get('andromeda_path', 'C:\\Temp\\Andromeda'),
                    mq_params_path=params.get('mq_params_path', ''),
                    user_input_params=False,
                    raw_folder=os.path.join(params['output_folder'], "raw_file_folder"),
                    job_name=job.job_name,
                    num_threads=params.get('num_threads', 16)
                )

                # Run handler in a separate thread
                handler_thread = threading.Thread(target=handler.run_MaxQuant_cli)
                handler_thread.start()

                # Monitor progress and stop signals
                while handler_thread.is_alive():
                    try:
                        message = job.progress_queue.get(timeout=1)
                        if message == "COMPLETED":
                            job.change_job_status('completed')
                        elif message.startswith("ERROR"):
                            job.change_job_status('errored')
                            job.error_flag = True
                        else:
                            try:
                                progress = float(message)
                                job.update_progress(progress / 100)  # Assuming progress is 0-100
                            except ValueError:
                                logger.debug(f"Progress message: {message}")
                    except Empty:
                        continue

                handler_thread.join()
                if job.status not in ['completed', 'errored']:
                    job.change_job_status('completed')
            elif job.job_type == 'diann':
                params = job.extras_dict
                # Add progress and stop queues to the job
                job.progress_queue = ThreadQueue()
                job.stop_queue = ThreadQueue()
                
                from src.handlers.diann_handler import DIANNHandler
                
                # Define progress callback
                def progress_callback(message):
                    job.progress_queue.put(message)

                    # Update job status based on message
                    if message.startswith("ERROR"):
                        job.change_job_status('errored')
                        job.error_flag = True
                        # Update database status
                        from src.database.jobs_db import JobsDB
                        jobs_db = JobsDB()
                        jobs_db.update_job_status(job.job_id, 'errored')
                    elif "PROCESS COMPLETED" in message:
                        job.change_job_status('completed')
                        # Update database status
                        from src.database.jobs_db import JobsDB
                        jobs_db = JobsDB()
                        jobs_db.update_job_status(job.job_id, 'completed')
                    elif message.startswith("STARTING"):
                        job.change_job_status('running')
                        # Update database status
                        from src.database.jobs_db import JobsDB
                        jobs_db = JobsDB()
                        jobs_db.update_job_status(job.job_id, 'running')

                    # Try to parse progress from the message
                    try:
                        # Find progress percentages in various formats
                        if "%" in message:
                            percent_str = message.split("%")[0].split()[-1]
                            progress = float(percent_str) / 100.0
                            job.update_progress(progress)

                            # Update progress in database
                            from src.database.jobs_db import JobsDB
                            jobs_db = JobsDB()
                            jobs_db.update_job_progress(job.job_id, progress)
                        elif "STEP COMPLETED" in message:
                            # Increase progress by a small amount for each completed step
                            current_progress = job.progress
                            # Make sure we don't exceed 1.0
                            new_progress = min(current_progress + 0.1, 0.95)
                            job.update_progress(new_progress - current_progress)

                            # Update progress in database
                            from src.database.jobs_db import JobsDB
                            jobs_db = JobsDB()
                            jobs_db.update_job_progress(job.job_id, job.progress)
                    except (ValueError, IndexError) as e:
                        logger.debug(f"Could not parse progress from message: {message} - {str(e)}")
                
                # Create the handler
                handler = DIANNHandler(
                    diann_exe=params['diann_path'],
                    fasta=params['fasta_file'],
                    conditions=params['conditions_file'],
                    op_folder=params['output_folder'],
                    msconvert_path=params.get('msconvert_path'),
                    progress_callback=progress_callback,
                    max_missed_cleavage=params.get('missed_cleavage', '1'),
                    max_var_mods=params.get('max_var_mods', '2'),
                    NtermMex_mod=params.get('mod_nterm_m_excision', True),
                    CCarb_mod=params.get('mod_c_carb', True),
                    OxM_mod=params.get('mod_ox_m', True),
                    AcNterm_mod=params.get('mod_ac_nterm', False),
                    Phospho_mod=params.get('mod_phospho', False),
                    KGG_mod=params.get('mod_k_gg', False),
                    peptide_length_range_min=params.get('peptide_length_min', '7'),
                    peptide_length_range_max=params.get('peptide_length_max', '30'),
                    precursor_charge_range_min=params.get('precursor_charge_min', '2'),
                    precursor_charge_range_max=params.get('precursor_charge_max', '4'),
                    precursor_min=params.get('precursor_min', '390'),
                    precursor_max=params.get('precursor_max', '1050'),
                    fragment_min=params.get('fragment_min', '200'),
                    fragment_max=params.get('fragment_max', '1800'),
                    threads=params.get('threads', '20'),
                    MBR=params.get('mbr', False)
                )
                
                # Run handler in a separate thread
                handler_thread = threading.Thread(target=handler.run_workflow)
                handler_thread.start()
                
                # Monitor progress and stop signals
                while handler_thread.is_alive():
                    try:
                        message = job.progress_queue.get(timeout=1)
                        logger.debug(f"DIA-NN job {job.job_id} message: {message}")
                    except Empty:
                        continue
                
                handler_thread.join()
                if job.status not in ['completed', 'errored']:
                    job.change_job_status('completed')
            else:
                logger.error(f"Unknown job type: {job.job_type}")
                raise ValueError(f"Unknown job type: {job.job_type}")

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