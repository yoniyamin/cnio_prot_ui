import os
from getpass import getuser
import json
import threading
import time
from flask import render_template, request, jsonify
from app_core import app, job_queue_manager
from src.utils import logger
from src.handlers.diann_handler import launch_diann_job
from src.core.file_utils import ensure_directory_exists, parse_conditions_file, get_raw_files_from_conditions
from src.database.watcher_db import WatcherDB
from src.database.jobs_db import JobsDB  # <-- The new DB class
from src.watchers.watcher_manager import WatcherManager
from src.core.job import Job

db_path = "config/watchers.db"  # existing watchers DB
watcher_db = WatcherDB(db_path=db_path)
jobs_db = JobsDB(db_path="config/jobs.db")  # new separate jobs DB

@app.route('/')
def home():
    logger.info("Route: home")
    return render_template('home.html')


@app.route('/maxquant', methods=['GET', 'POST'])
def maxquant():
    if request.method == 'GET':
        return render_template('maxquant.html')

    try:
        # Extract form data - General settings
        fasta_folder = request.form.get('fasta_folder')
        output_folder = request.form.get('output_folder')
        conditions_file = request.form.get('conditions_file')
        mq_path = request.form.get('mq_path')
        mq_version = request.form.get('mq_version')
        dbs = request.form.getlist('database_choices')
        job_name = request.form.get('job_name') or f"MaxQuantJob_{int(time.time())}"

        # Extract form data - Search parameters
        protein_quantification = request.form.get('protein_quantification', 'Razor + Unique')
        missed_cleavages = request.form.get('missed_cleavages', '2')
        fixed_mods = request.form.get('fixed_mods', 'Carbamidomethyl (C)')
        variable_mods = request.form.get('variable_mods', 'Oxidation (M), Acetyl (Protein N-term)')
        enzymes = request.form.get('enzymes', 'Trypsin/P')
        match_between_runs = request.form.get('match_between_runs') == 'on'
        second_peptide = request.form.get('second_peptide') == 'on'

        # Extract form data - Advanced parameters
        num_threads = request.form.get('num_threads', '16')
        id_parse_rule = request.form.get('id_parse_rule', '>.*\\|(.*)\\|')
        desc_parse_rule = request.form.get('desc_parse_rule', '>(.*)')
        andromeda_path = request.form.get('andromeda_path', 'C:\\Temp\\Andromeda')
        mq_params_path = request.form.get('mq_params_path', '')

        # Validate inputs
        if not all([fasta_folder, output_folder, conditions_file, mq_path, dbs]):
            return "Error: Missing required fields", 400
        if not os.path.isdir(fasta_folder):
            return f"Error: FASTA folder does not exist: {fasta_folder}", 400
        if not os.path.isfile(conditions_file):
            return f"Error: Conditions file does not exist: {conditions_file}", 400
        if not os.path.isfile(mq_path):
            return f"Error: MaxQuant executable does not exist: {mq_path}", 400

        # Create output directory if it doesn't exist
        ensure_directory_exists(output_folder)

        # Parse conditions file to get raw file list
        conditions_df = parse_conditions_file(conditions_file)
        if conditions_df is None:
            return "Error: Could not parse conditions file", 400

        # Get raw files from conditions dataframe
        raw_files = get_raw_files_from_conditions(conditions_df)
        if raw_files is None:
            return "Error: Could not extract raw files from conditions file", 400

        file_pattern = ";".join(raw_files)
        watch_folder = os.path.dirname(conditions_file)  # Adjust if raw files are in a different folder

        # Store job parameters
        job_params = {
            'fasta_folder': fasta_folder,
            'output_folder': output_folder,
            'conditions_file': conditions_file,
            'mq_path': mq_path,
            'mq_version': mq_version,
            'dbs': dbs,
            'job_name': job_name,
            'protein_quantification': protein_quantification,
            'missed_cleavages': missed_cleavages,
            'fixed_mods': fixed_mods,
            'variable_mods': variable_mods,
            'enzymes': enzymes,
            'match_between_runs': match_between_runs,
            'second_peptide': second_peptide,
            'num_threads': num_threads,
            'id_parse_rule': id_parse_rule,
            'desc_parse_rule': desc_parse_rule,
            'andromeda_path': andromeda_path,
            'mq_params_path': mq_params_path
        }

        # ------------------------------------------------------------------
        # Create the watcher first so we have a watcher_id
        watcher_data = {
            'folder_path': watch_folder,
            'file_pattern': file_pattern,
            'job_type': 'maxquant',
            'job_demands': json.dumps(job_params),
            'job_name_prefix': job_name
        }

        from app_core import watcher_db_path
        from src.watchers.watcher_manager import WatcherManager
        watcher_db = WatcherDB(watcher_db_path)

        # Create the watcher
        watcher_id = watcher_db.add_watcher(**watcher_data)
        logger.info(f"Created watcher (ID: {watcher_id}) for {job_name}")

        # ------------------------------------------------------------------
        # Now create the Job in "waiting" status and associate it with the watcher
        from getpass import getuser
        submitter_username = getuser()  # This captures the OS username

        new_job = Job(
            job_submitter=submitter_username,
            job_demands=json.dumps(job_params),
            job_type='maxquant',
            command="",
            expected_files=raw_files,
            local_folder=watch_folder,
            job_name=job_name
        )

        # Store all parameters in the job's extras_dict for in-memory reference
        if not hasattr(new_job, 'extras_dict'):
            new_job.extras_dict = {}
        new_job.extras_dict = job_params.copy()  # Store all parameters in extras_dict
        new_job.extras_dict['watcher_id'] = watcher_id

        # Insert the new job record into jobs_db
        from src.database.jobs_db import JobsDB
        jobs_db = JobsDB(db_path="config/jobs.db")
        jobs_db.add_job(
            job_id=new_job.job_id,
            job_name=new_job.job_name,
            job_type=new_job.job_type,
            job_submitter=new_job.job_submitter,
            job_demands=new_job.job_demands,
            local_folder=new_job.local_folder,
            watcher_name=job_name,
            watcher_id=watcher_id  # This is the key link between job and watcher
        )

        # Also add it to the JobQueueManager so it's in 'waiting'
        job_queue_manager.add_job(new_job)
        logger.info(f"Created job (ID: {new_job.job_id}) linked to watcher {watcher_id}")

        # Start watcher in the background
        watcher_manager = WatcherManager(watcher_db, job_queue_manager)
        watcher = watcher_manager.create_single_watcher(watcher_id)
        thread = threading.Thread(target=watcher.start, daemon=True)
        thread.start()
        logger.info(f"Started watcher {watcher_id} in thread")

        return f"""
        <h3>Watcher Created</h3>
        <p>A watcher (ID: {watcher_id}) has been set up for '{job_name}'.</p>
        <p>A MaxQuant job (ID: {new_job.job_id}) has been created in 'waiting' status.</p>
        <p>It will move to 'queued' once all files are captured.</p>
        <p>Track progress in the <a href='/job-monitor'>Job Monitor</a>.</p>
        """

    except Exception as e:
        logger.error(f"Error setting up MaxQuant watcher: {str(e)}", exc_info=True)
        return f"Error: {str(e)}", 500


@app.route('/diann', methods=['GET', 'POST'])
def diann():
    if request.method == 'GET':
        logger.info("Route: diann (GET)")
        return render_template('diann.html')

    logger.info("Route: diann (POST)")
    try:
        # Extract form data
        fasta_file = request.form.get('fasta_file')
        output_folder = request.form.get('output_folder')
        conditions_file = request.form.get('conditions_file')
        diann_path = request.form.get('diann_path')
        msconvert_path = request.form.get('msconvert_path')
        job_name = request.form.get('job_name') or "DIANNAnalysis"

        logger.info(f"DIA-NN job submitted: {job_name}")
        logger.debug(
            f"DIA-NN parameters: fasta={fasta_file}, output={output_folder}, conditions={conditions_file}, diann_path={diann_path}, msconvert_path={msconvert_path}")

        # Get parameter values
        missed_cleavage = request.form.get('missed_cleavage', '1')
        max_var_mods = request.form.get('max_var_mods', '2')
        mod_nterm_m_excision = request.form.get('mod_nterm_m_excision') == 'on'
        mod_c_carb = request.form.get('mod_c_carb') == 'on'
        mod_ox_m = request.form.get('mod_ox_m') == 'on'
        mod_ac_nterm = request.form.get('mod_ac_nterm') == 'on'
        mod_phospho = request.form.get('mod_phospho') == 'on'
        mod_k_gg = request.form.get('mod_k_gg') == 'on'
        mbr = request.form.get('mbr') == 'on'
        threads = request.form.get('threads', '20')

        # Get advanced parameter values
        peptide_length_min = request.form.get('peptide_length_min', '7')
        peptide_length_max = request.form.get('peptide_length_max', '30')
        precursor_charge_min = request.form.get('precursor_charge_min', '2')
        precursor_charge_max = request.form.get('precursor_charge_max', '4')
        precursor_min = request.form.get('precursor_min', '390')
        precursor_max = request.form.get('precursor_max', '1050')
        fragment_min = request.form.get('fragment_min', '200')
        fragment_max = request.form.get('fragment_max', '1800')

        # Validate key inputs
        if not all([fasta_file, output_folder, conditions_file, diann_path]):
            logger.warning(f"DIA-NN job {job_name} missing required fields")
            return "Error: Missing required fields", 400

        # Validate that files/folders exist
        if not os.path.isfile(fasta_file):
            logger.warning(f"DIA-NN job {job_name} FASTA file does not exist: {fasta_file}")
            return f"Error: FASTA file does not exist: {fasta_file}", 400

        if not os.path.isdir(output_folder):
            try:
                os.makedirs(output_folder, exist_ok=True)
                logger.info(f"Created output directory for DIA-NN job {job_name}: {output_folder}")
            except Exception as e:
                logger.error(f"Error creating output folder for DIA-NN job {job_name}: {str(e)}", exc_info=True)
                return f"Error: Could not create output folder: {str(e)}", 400

        if not os.path.isfile(conditions_file):
            logger.warning(f"DIA-NN job {job_name} conditions file does not exist: {conditions_file}")
            return f"Error: Conditions file does not exist: {conditions_file}", 400

        if not os.path.isfile(diann_path):
            logger.warning(f"DIA-NN job {job_name} executable does not exist: {diann_path}")
            return f"Error: DIA-NN executable does not exist: {diann_path}", 400

        if msconvert_path and not os.path.isfile(msconvert_path):
            logger.warning(f"DIA-NN job {job_name} MSConvert executable does not exist: {msconvert_path}")
            return f"Error: MSConvert executable does not exist: {msconvert_path}", 400

        # Create local output directory for job tracking
        local_output = os.path.join(app.config['UPLOAD_FOLDER'], job_name)
        ensure_directory_exists(local_output)
        logger.info(f"Created job tracking directory for DIA-NN job {job_name}: {local_output}")

        # Create job data structure
        job_data = {
            'job_name': job_name,
            'fasta_file': fasta_file,
            'output_folder': output_folder,
            'conditions_file': conditions_file,
            'diann_path': diann_path,
            'msconvert_path': msconvert_path,
            'missed_cleavage': missed_cleavage,
            'max_var_mods': max_var_mods,
            'mod_nterm_m_excision': mod_nterm_m_excision,
            'mod_c_carb': mod_c_carb,
            'mod_ox_m': mod_ox_m,
            'mod_ac_nterm': mod_ac_nterm,
            'mod_phospho': mod_phospho,
            'mod_k_gg': mod_k_gg,
            'mbr': mbr,
            'threads': threads,
            'peptide_length_min': peptide_length_min,
            'peptide_length_max': peptide_length_max,
            'precursor_charge_min': precursor_charge_min,
            'precursor_charge_max': precursor_charge_max,
            'precursor_min': precursor_min,
            'precursor_max': precursor_max,
            'fragment_min': fragment_min,
            'fragment_max': fragment_max
        }

        # Log job info to file
        with open(os.path.join(local_output, "job_info.json"), "w") as f:
            json.dump(job_data, f, indent=2)

        # Create status file to show job is queued
        with open(os.path.join(local_output, "status.txt"), "w") as f:
            f.write("queued")
        logger.info(f"DIA-NN job {job_name} status: queued")

        # Start DIA-NN job in a separate thread
        # Define progress callback
        def progress_callback(message):
            with open(os.path.join(local_output, "progress.log"), "a") as log:
                log.write(f"{message}\n")

            # Update status based on message content
            if message.startswith("ERROR"):
                logger.error(f"DIA-NN job {job_name}: {message}")
                with open(os.path.join(local_output, "status.txt"), "w") as status_file:
                    status_file.write(f"failed: {message}")
                logger.error(f"DIA-NN job {job_name} status: failed - {message}")
            elif message.startswith("PROCESS COMPLETED"):
                logger.info(f"DIA-NN job {job_name}: {message}")
                with open(os.path.join(local_output, "status.txt"), "w") as status_file:
                    status_file.write("complete")
                logger.info(f"DIA-NN job {job_name} status: complete")
            elif message.startswith("STARTING"):
                logger.info(f"DIA-NN job {job_name}: {message}")
                with open(os.path.join(local_output, "status.txt"), "w") as status_file:
                    status_file.write("running")
                logger.info(f"DIA-NN job {job_name} status: running")
            else:
                logger.debug(f"DIA-NN job {job_name}: {message}")

        thread = threading.Thread(
            target=launch_diann_job,
            args=(job_data, progress_callback)
        )
        thread.daemon = True
        thread.start()
        logger.info(f"Started DIA-NN job {job_name} in thread")

        return f"""
        <h3>Job Submitted Successfully</h3>
        <p>Your DIA-NN job '{job_name}' has been submitted.</p>
        <p>You can track its progress in the <a href='/job-monitor'>Job Monitor</a>.</p>
        """

    except Exception as e:
        logger.error(f"Error submitting DIA-NN job: {str(e)}", exc_info=True)
        return f"Error submitting DIA-NN job: {str(e)}", 500


@app.route('/spectronaut')
def spectronaut():
    logger.info("Route: spectronaut")
    return render_template('spectronaut.html')


@app.route('/quantms')
def quantms():
    logger.info("Route: quantms")
    return render_template('quantms.html')


@app.route('/gelbandido')
def gelbandido():
    logger.info("Route: gelbandido")
    return render_template('gelbandido.html')


@app.route('/dianalyzer')
def dianalyzer():
    logger.info("Route: dianalyzer")
    return render_template('dianalyzer.html')

def get_running_watchers():
    """Return only watchers whose status is 'running' or 'monitoring'."""
    running_watchers = watcher_db.get_watchers()
    # Adjust these checks to match how you store statuses
    return [w for w in running_watchers if w['status'].lower() in ('running', 'monitoring')]


@app.route('/job-monitor')
def job_monitor_dashboard():
    """
    Section 1: Dashboard
    Shows watchers with status='monitoring' or 'completed' and
    their associated jobs with status='running', 'waiting', 'completed', or 'errored'.
    """
    # Fetch watchers that are 'monitoring' or 'completed'
    running_watchers = []
    for status in ['monitoring', 'completed']:
        running_watchers.extend(watcher_db.get_watchers(status=status))

    running_pairs = []
    errored_pairs = []

    for watcher in running_watchers:
        watcher_id = watcher[0]

        # Get captured files count for each watcher
        captured_files = watcher_db.get_captured_files(watcher_id)
        captured_count = len(captured_files)

        # Calculate expected files count
        file_patterns = watcher[2].split(';')
        exact_patterns = [p.strip() for p in file_patterns if not any(c in "*?[" for c in p)]
        expected_count = len(exact_patterns) if exact_patterns else 0

        # Create a dictionary for the watcher to include additional fields
        watcher_dict = {
            'id': watcher[0],
            'folder_path': watcher[1],
            'file_pattern': watcher[2],
            'job_type': watcher[3],
            'job_demands': watcher[4],
            'job_name_prefix': watcher[5],
            'creation_time': watcher[6],
            'execution_time': watcher[7],
            'status': watcher[8],
            'completion_time': watcher[9],
            'captured_count': captured_count,
            'expected_count': expected_count
        }

        # Get jobs for this watcher
        associated_jobs = jobs_db.get_jobs_by_watcher_id(watcher_id)

        # Split jobs into running and errored
        for job in associated_jobs:
            if job['status'] in ['running', 'waiting', 'completed']:
                running_pairs.append({'watcher': watcher_dict, 'job': job})
            elif job['status'] == 'errored':
                errored_pairs.append({'watcher': watcher_dict, 'job': job})

    # Calculate statistics for running jobs
    running_stats = {
        'total': len(running_pairs),
        'running': sum(1 for pair in running_pairs if pair['job']['status'] == 'running'),
        'waiting': sum(1 for pair in running_pairs if pair['job']['status'] == 'waiting'),
        'completed': sum(1 for pair in running_pairs if pair['job']['status'] == 'completed')
    }

    # Calculate statistics for errored jobs
    errored_stats = {
        'total': len(errored_pairs)
    }

    logger.info(f'Found {len(running_pairs)} running pairs and {len(errored_pairs)} errored pairs')
    return render_template(
        'dashboard.html',
        running_pairs=running_pairs,
        errored_pairs=errored_pairs,
        running_stats=running_stats,
        errored_stats=errored_stats
    )

@app.route('/job-monitor/jobs')
def job_monitor_jobs():
    """
    Section 2: 'Jobs'
    (Renamed from /job-monitor/running-jobs to /job-monitor/jobs)
    Displays the full jobs table, how you already do it.
    """
    all_jobs = jobs_db.get_all_jobs()  # or whatever your method is named
    logger.info(f"Fetched {len(all_jobs) if all_jobs else 0} jobs for /job-monitor/jobs")
    return render_template('jobs.html', jobs=all_jobs)

@app.route('/job-monitor/watchers')
def job_monitor_watchers():
    """
    Section 3: 'Watchers'
    Displays the watchers table, how you already do it.
    """
    # If you want all watchers, just call get_watchers() with no status filter:
    all_watchers = watcher_db.get_watchers()
    return render_template('watchers.html', watchers=all_watchers)

@app.route('/config')
def config():
    logger.info("Route: config")
    return render_template('config.html')



@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
    response.headers['Access-Control-Allow-Methods'] = 'GET,PUT,POST,DELETE,OPTIONS'
    return response

