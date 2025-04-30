import os
import logging
import json
import threading
from datetime import datetime
from pathlib import Path
from flask import jsonify, render_template, request, send_file
from werkzeug.utils import secure_filename
from getpass import getuser
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
from queue import Queue as ThreadQueue, Empty
from src.watchers.file_watcher import FileEventHandler

# Initialize logger
logger = logging.getLogger(__name__)

# Initialize watcherDB
watcher_db = WatcherDB()

# Initialize jobsDB
jobs_db = JobsDB()

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
        
        # Debug: Check watcher status in DB after creation
        watcher_row = watcher_db.get_watchers()
        watcher_status = None
        for row in watcher_row:
            if row[0] == watcher_id:
                watcher_status = row[8]  # status column
                break
        logger.info(f"Watcher {watcher_id} status after creation: {watcher_status}")

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
            'fasta_file': fasta_file,
            'output_folder': output_folder,
            'conditions_file': conditions_file,
            'diann_path': diann_path,
            'msconvert_path': msconvert_path,
            'job_name': job_name,
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

        # ------------------------------------------------------------------
        # Create the watcher first so we have a watcher_id
        watcher_data = {
            'folder_path': watch_folder,
            'file_pattern': file_pattern,
            'job_type': 'diann',
            'job_demands': json.dumps(job_params),
            'job_name_prefix': job_name
        }

        from app_core import watcher_db_path
        from src.watchers.watcher_manager import WatcherManager
        watcher_db = WatcherDB(watcher_db_path)

        # Create the watcher
        watcher_id = watcher_db.add_watcher(**watcher_data)
        logger.info(f"Created watcher (ID: {watcher_id}) for {job_name}")
        
        # Debug: Check watcher status in DB after creation
        watcher_row = watcher_db.get_watchers()
        watcher_status = None
        for row in watcher_row:
            if row[0] == watcher_id:
                watcher_status = row[8]  # status column
                break
        logger.info(f"Watcher {watcher_id} status after creation: {watcher_status}")

        # ------------------------------------------------------------------
        # Now create the Job in "waiting" status and associate it with the watcher
        from getpass import getuser
        submitter_username = getuser()  # This captures the OS username

        new_job = Job(
            job_submitter=submitter_username,
            job_demands=json.dumps(job_params),
            job_type='diann',
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
        <p>A DIA-NN job (ID: {new_job.job_id}) has been created in 'waiting' status.</p>
        <p>It will move to 'queued' once all files are captured.</p>
        <p>Track progress in the <a href='/job-monitor'>Job Monitor</a>.</p>
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

@app.route('/test-job-submission', methods=['GET', 'POST'])
def test_job_submission():
    """
    Test route for simulating job submissions.
    GET - displays the job simulation form
    POST - processes the form and starts a simulated job
    """
    if request.method == 'GET':
        logger.info("Route: test-job-submission (GET)")
        # Return a simple form to select job type and simulation options
        return render_template('test_job_submission.html')
    
    # Handle POST request
    logger.info("Route: test-job-submission (POST)")
    try:
        # Get common parameters
        job_type = request.form.get('job_type')
        job_name = request.form.get('job_name') or f"Test_{job_type}_{int(time.time())}"
        simulate_error = request.form.get('simulate_error') == 'on'
        
        if not job_type:
            return "Error: Job type is required", 400
            
        # Create a temporary output directory for the test
        output_folder = os.path.join(app.config['UPLOAD_FOLDER'], f"test_{job_name}")
        ensure_directory_exists(output_folder)
        logger.info(f"Created test output directory: {output_folder}")
        
        # Get some basic simulation parameters
        if job_type == 'maxquant':
            result = simulate_maxquant_job(job_name, output_folder, simulate_error)
        elif job_type == 'diann':
            result = simulate_diann_job(job_name, output_folder, simulate_error)
        else:
            return f"Error: Unsupported job type: {job_type}", 400
            
        return result
    except Exception as e:
        logger.error(f"Error in test job submission: {str(e)}", exc_info=True)
        return f"Error: {str(e)}", 500

def simulate_maxquant_job(job_name, output_folder, simulate_error=False):
    """Simulate a MaxQuant job submission and execution"""
    try:
        # Prepare test data using absolute path under the simulation output folder
        test_data_dir = os.path.abspath(os.path.join(output_folder, "test_data"))
        ensure_directory_exists(test_data_dir)
        
        # Create a simple conditions file if it doesn't exist
        conditions_file = os.path.join(test_data_dir, "test_conditions.tsv")
        if not os.path.exists(conditions_file):
            with open(conditions_file, 'w') as f:
                f.write("Raw file\tExperiment\ntest_file1.raw\tExp1\ntest_file2.raw\tExp1\n")
        
        # Create a job params dictionary
        job_params = {
            'fasta_folder': test_data_dir,
            'output_folder': output_folder,
            'conditions_file': conditions_file,
            'mq_path': "C:/test/path/MaxQuantCmd.exe",  # Simulated path
            'mq_version': '2.1.4.0',
            'dbs': ['HUMAN'],
            'job_name': job_name,
            'protein_quantification': 'Razor + Unique',
            'missed_cleavages': '2',
            'fixed_mods': 'Carbamidomethyl (C)',
            'variable_mods': 'Oxidation (M), Acetyl (Protein N-term)',
            'enzymes': 'Trypsin/P',
            'match_between_runs': False,
            'second_peptide': False,
            'num_threads': 16,
            'id_parse_rule': '>.*\\|(.*)\\|',
            'desc_parse_rule': '>(.*)',
            'andromeda_path': 'C:\\Temp\\Andromeda',
            'mq_params_path': ''
        }
        
        # Get raw files from conditions file
        conditions_df = parse_conditions_file(conditions_file)
        raw_files = get_raw_files_from_conditions(conditions_df)
        file_pattern = ";".join(raw_files)
        watch_folder = os.path.dirname(conditions_file)
        
        # Make sure watch folder exists
        ensure_directory_exists(watch_folder)
        
        # Clear any existing files with the same names to avoid conflicts
        for file_name in raw_files:
            file_path = os.path.join(watch_folder, file_name)
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.info(f"Removed existing file to avoid conflicts: {file_path}")
                except Exception as e:
                    logger.warning(f"Could not remove existing file {file_path}: {str(e)}")
        
        # Create the watcher data with more detailed logging
        watcher_data = {
            'folder_path': watch_folder,
            'file_pattern': file_pattern,
            'job_type': 'maxquant',
            'job_demands': json.dumps(job_params),
            'job_name_prefix': job_name
        }
        
        logger.info(f"Creating watcher with folder: {watch_folder}, pattern: {file_pattern}")
        
        # Create the watcher
        watcher_id = watcher_db.add_watcher(**watcher_data)
        logger.info(f"Created test watcher (ID: {watcher_id}) for {job_name}")
        
        # Create the job with is_simulation=True
        new_job = Job(
            job_submitter="test_user",
            job_demands=json.dumps(job_params),
            job_type='maxquant',
            command="",
            expected_files=raw_files,
            local_folder=watch_folder,
            job_name=job_name,
            is_simulation=True  # Mark as simulation job
        )
        
        # Store parameters in the job's extras_dict
        new_job.extras_dict = job_params.copy()
        new_job.extras_dict['watcher_id'] = watcher_id
        new_job.extras_dict['is_simulation'] = True  # Also mark in extras dict for UI display
        
        # Add the job to the database
        jobs_db.add_job(
            job_id=new_job.job_id,
            job_name=new_job.job_name,
            job_type=new_job.job_type,
            job_submitter=new_job.job_submitter,
            job_demands=new_job.job_demands,
            local_folder=new_job.local_folder,
            watcher_name=job_name,
            watcher_id=watcher_id
        )
        
        # Define a progress callback function
        def progress_callback(message):
            logger.info(f"MaxQuant simulation progress: {message}")
            
            # Simulate an error if requested
            if simulate_error and "UPDATE: Simulated MaxQuant progress 3/5" in message:
                return "ERROR: Simulated error in MaxQuant execution"
            
            # Clean up files when the job completes
            if "PROCESS COMPLETED" in message:
                cleanup_files_thread = threading.Thread(
                    target=cleanup_test_files, 
                    args=(watch_folder, raw_files, 10)  # Wait 10 seconds before cleanup
                )
                cleanup_files_thread.daemon = True
                cleanup_files_thread.start()
                
                # Update the job status to completed
                new_job.change_job_status('completed')
                jobs_db.update_job_status(new_job.job_id, 'completed')
                logger.info(f"Job {new_job.job_id} ({job_name}) marked as completed")
        
        # Add the job to the queue manager
        job_queue_manager.add_job(new_job)
        
        # Start watcher in the background with more detailed logging
        from src.watchers.watcher_manager import WatcherManager
        watcher_manager = WatcherManager(watcher_db, job_queue_manager)
        watcher = watcher_manager.create_single_watcher(watcher_id)
        logger.info(f"Created watcher {watcher_id} with config: {watcher.config}")
        
        # Start the watcher thread
        watcher_thread = threading.Thread(target=watcher.start, daemon=True)
        watcher_thread.start()
        logger.info(f"Started watcher {watcher_id} in thread")
        
        # Wait a moment to ensure the watcher is ready
        time.sleep(1)
        
        # Create fake files with a slight delay to simulate files appearing
        file_creation_thread = threading.Thread(
            target=create_test_files_sequentially,
            args=(watch_folder, raw_files, new_job.job_id)
        )
        file_creation_thread.daemon = True
        file_creation_thread.start()
        
        # Add a synchronization event to know when all files have been created and processed
        files_ready_event = threading.Event()
        
        # Monitor for completed file creation and watcher processing
        def monitor_files_ready():
            # Wait for the file creation thread to finish (all files created)
            while file_creation_thread.is_alive():
                time.sleep(1)
            
            logger.info(f"All test files have been created for job {new_job.job_id}")
            
            # Now wait for the watcher to process all files
            # We'll check the jobs database to see if the job has all expected files
            max_wait_time = 30  # seconds
            start_time = time.time()
            
            while time.time() - start_time < max_wait_time:
                # Check if the job is ready to run (all files captured)
                job_data = jobs_db.get_job(new_job.job_id)
                if job_data and job_data.get('status') != 'waiting':
                    logger.info(f"Job {new_job.job_id} moved from 'waiting' state. Files are processed.")
                    files_ready_event.set()
                    return
                
                # Check directly with the watcher's handler for maxquant jobs
                if hasattr(watcher, 'handler') and isinstance(watcher.handler, FileEventHandler):
                    if watcher.handler.check_completion(new_job.job_id):
                        logger.info(f"Watcher handler confirms all files captured for job {new_job.job_id}")
                        files_ready_event.set()
                        return
                
                time.sleep(2)
            
            # If we reached here, we timed out waiting for files to be ready
            logger.warning(f"Timed out waiting for watcher to process all files for job {new_job.job_id}")
            files_ready_event.set()  # Set the event anyway to not block the job
        
        # Start the file monitoring thread
        monitor_thread = threading.Thread(target=monitor_files_ready)
        monitor_thread.daemon = True
        monitor_thread.start()
        
        # Add direct test for the watcher to scan for files (separate thread)
        def delayed_test_run():
            # Wait for all files to be created and processed by the watcher
            files_ready = files_ready_event.wait(timeout=40)  # Wait up to 40 seconds
            
            if not files_ready:
                logger.warning(f"Timed out waiting for files to be ready for job {new_job.job_id}")
            
            # Start the simulated MaxQuant job (this would normally be triggered by the watcher)
            from src.handlers.run_maxquant_sim import launch_maxquant_sim_job
            logger.info(f"Starting simulated MaxQuant job for {job_name} (job ID: {new_job.job_id})")
            
            # Create the queues for the simulation handler
            stop_queue = ThreadQueue()
            progress_queue = ThreadQueue()
            
            # Rename conditions_file to conditions to match handler's expected parameter name
            handler_params = job_params.copy()
            handler_params['conditions'] = handler_params.pop('conditions_file')
            
            # Launch MaxQuant simulation job
            maxquant_thread = threading.Thread(
                target=launch_maxquant_sim_job,
                args=(handler_params, progress_callback, stop_queue, progress_queue)
            )
            maxquant_thread.daemon = True
            maxquant_thread.start()
            
            # Check if job has moved to running state
            if new_job.status != 'running':
                logger.info(f"Updating job {new_job.job_id} ({job_name}) to 'running' state")
                new_job.change_job_status('running')
                jobs_db.update_job_status(new_job.job_id, 'running')
        
        # Start the delayed test runner
        test_thread = threading.Thread(target=delayed_test_run)
        test_thread.daemon = True
        test_thread.start()
        
        return f"""
        <h3>Test Job Submitted</h3>
        <p>MaxQuant test job '{job_name}' (ID: {new_job.job_id}) has been submitted.</p>
        <p>Fake files will be created one by one to demonstrate the watcher capture process.</p>
        <p>Files will be automatically deleted after job completion.</p>
        <p>Track progress in the <a href='/job-monitor/jobs?job_id={new_job.job_id}'>Job Monitor</a>.</p>
        <p><i>Note: First file will appear in about 5 seconds, and file processing will begin shortly after all files are created.</i></p>
        """
    
    except Exception as e:
        logger.error(f"Error in MaxQuant simulation: {str(e)}", exc_info=True)
        return f"Error: {str(e)}", 500

def simulate_diann_job(job_name, output_folder, simulate_error=False):
    """Simulate a DIA-NN job submission and execution"""
    try:
        # Prepare test data using absolute path under the simulation output folder
        test_data_dir = os.path.abspath(os.path.join(output_folder, "test_data"))
        ensure_directory_exists(test_data_dir)
        
        # Create a simple conditions file if it doesn't exist
        conditions_file = os.path.join(test_data_dir, "test_conditions.tsv")
        if not os.path.exists(conditions_file):
            with open(conditions_file, 'w') as f:
                f.write("Raw file\tExperiment\ntest_file1.raw\tExp1\ntest_file2.raw\tExp1\n")
        
        # Create a simple FASTA file if it doesn't exist
        fasta_file = os.path.join(test_data_dir, "test_database.fasta")
        if not os.path.exists(fasta_file):
            with open(fasta_file, 'w') as f:
                f.write(">sp|P12345|TEST_PROTEIN Test protein\nMKVFGRCELAAAMKRHGLDNYRGYSLGNWVCAAKFESNFNTQATNRNTDGSTDYGILQINSRWWCNDGRTPGSRNLCNIPCSALLSSS\n")
        
        # Create a job params dictionary
        job_params = {
            'fasta_file': fasta_file,
            'output_folder': output_folder,
            'conditions_file': conditions_file,
            'diann_path': "C:/test/path/DiaNN.exe",  # Simulated path
            'msconvert_path': "C:/test/path/msconvert.exe",  # Simulated path
            'job_name': job_name,
            'missed_cleavage': '1',
            'max_var_mods': '2',
            'mod_nterm_m_excision': True,
            'mod_c_carb': True,
            'mod_ox_m': True,
            'mod_ac_nterm': False,
            'mod_phospho': False,
            'mod_k_gg': False,
            'mbr': False,
            'threads': '20',
            'peptide_length_min': '7',
            'peptide_length_max': '30',
            'precursor_charge_min': '2',
            'precursor_charge_max': '4',
            'precursor_min': '390',
            'precursor_max': '1050',
            'fragment_min': '200',
            'fragment_max': '1800'
        }
        
        # Get raw files from conditions file
        conditions_df = parse_conditions_file(conditions_file)
        raw_files = get_raw_files_from_conditions(conditions_df)
        file_pattern = ";".join(raw_files)
        watch_folder = os.path.dirname(conditions_file)
        
        # Make sure watch folder exists
        ensure_directory_exists(watch_folder)
        
        # Clear any existing files with the same names to avoid conflicts
        for file_name in raw_files:
            file_path = os.path.join(watch_folder, file_name)
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.info(f"Removed existing file to avoid conflicts: {file_path}")
                except Exception as e:
                    logger.warning(f"Could not remove existing file {file_path}: {str(e)}")
        
        # Create the watcher data with detailed logging
        watcher_data = {
            'folder_path': watch_folder,
            'file_pattern': file_pattern,
            'job_type': 'diann',
            'job_demands': json.dumps(job_params),
            'job_name_prefix': job_name
        }
        
        logger.info(f"Creating watcher with folder: {watch_folder}, pattern: {file_pattern}")
        
        # Create the watcher
        watcher_id = watcher_db.add_watcher(**watcher_data)
        logger.info(f"Created test watcher (ID: {watcher_id}) for {job_name}")
        
        # Debug: Check watcher status in DB after creation
        watcher_row = watcher_db.get_watchers()
        watcher_status = None
        for row in watcher_row:
            if row[0] == watcher_id:
                watcher_status = row[8]  # status column
                break
        logger.info(f"Watcher {watcher_id} status after creation: {watcher_status}")
        
        # Create the job
        new_job = Job(
            job_submitter="test_user",
            job_demands=json.dumps(job_params),
            job_type='diann',
            command="",
            expected_files=raw_files,
            local_folder=watch_folder,
            job_name=job_name
        )
        
        # Store parameters in the job's extras_dict
        new_job.extras_dict = job_params.copy()
        new_job.extras_dict['watcher_id'] = watcher_id
        job_params['job_id'] = new_job.job_id  # Add job ID to params for status updates
        
        # Add the job to the database
        jobs_db.add_job(
            job_id=new_job.job_id,
            job_name=new_job.job_name,
            job_type=new_job.job_type,
            job_submitter=new_job.job_submitter,
            job_demands=new_job.job_demands,
            local_folder=new_job.local_folder,
            watcher_name=job_name,
            watcher_id=watcher_id
        )
        
        # Define a progress callback function
        def progress_callback(message):
            logger.info(f"DIA-NN simulation progress: {message}")
            
            # Handle status update messages
            if message.startswith("JOB_STATUS:"):
                parts = message.split(":")
                if len(parts) == 3:
                    job_id, status = parts[1], parts[2]
                    logger.info(f"Updating job {job_id} status to {status}")
                    jobs_db.update_job_status(job_id, status)
                return
            
            # Simulate an error if requested
            if simulate_error and "Building spectral library" in message:
                return "ERROR: Simulated error in DIA-NN execution"
            
            # Clean up files when the job completes
            if "PROCESS COMPLETED" in message or "completed successfully" in message:
                cleanup_files_thread = threading.Thread(
                    target=cleanup_test_files, 
                    args=(watch_folder, raw_files, 10)  # Wait 10 seconds before cleanup
                )
                cleanup_files_thread.daemon = True
                cleanup_files_thread.start()
                
                # Update the job status to completed
                new_job.change_job_status('completed')
                jobs_db.update_job_status(new_job.job_id, 'completed')
                logger.info(f"Job {new_job.job_id} ({job_name}) marked as completed")
        
        # Add the job to the queue manager
        job_queue_manager.add_job(new_job)
        
        # Start watcher in the background
        from src.watchers.watcher_manager import WatcherManager
        watcher_manager = WatcherManager(watcher_db, job_queue_manager)
        watcher = watcher_manager.create_single_watcher(watcher_id)
        logger.info(f"Created watcher {watcher_id} with config: {watcher.config}")
        
        # Start the watcher thread
        watcher_thread = threading.Thread(target=watcher.start, daemon=True)
        watcher_thread.start()
        logger.info(f"Started watcher {watcher_id} in thread")
        
        # Wait a moment to ensure the watcher is ready
        time.sleep(1)
        
        # Create fake files with a slight delay to simulate files appearing
        file_creation_thread = threading.Thread(
            target=create_test_files_sequentially,
            args=(watch_folder, raw_files, new_job.job_id)
        )
        file_creation_thread.daemon = True
        file_creation_thread.start()
        
        # Add direct test for the watcher to scan for files (separate thread)
        def delayed_test_run():
            # Let the files be created first
            time.sleep(10)
            
            # Start the simulated DIA-NN job (this would normally be triggered by the watcher)
            from src.handlers.diann_handler_sim import launch_diann_sim_job
            logger.info(f"Starting simulated DIA-NN job for {job_name} (job ID: {new_job.job_id})")
            
            # Launch DIA-NN simulation job
            launch_diann_sim_job(job_params, progress_callback)
            
            # Check if job has moved to running state
            if new_job.status != 'running':
                logger.info(f"Forcing job {new_job.job_id} ({job_name}) to 'running' state")
                new_job.change_job_status('running')
                jobs_db.update_job_status(new_job.job_id, 'running')
        
        # Start the delayed test runner
        test_thread = threading.Thread(target=delayed_test_run)
        test_thread.daemon = True
        test_thread.start()
        
        return f"""
        <h3>Test Job Submitted</h3>
        <p>DIA-NN test job '{job_name}' (ID: {new_job.job_id}) has been submitted.</p>
        <p>Fake files will be created one by one to demonstrate the watcher capture process.</p>
        <p>Files will be automatically deleted after job completion.</p>
        <p>Track progress in the <a href='/job-monitor/jobs?job_id={new_job.job_id}'>Job Monitor</a>.</p>
        <p><i>Note: First file will appear in about 5 seconds, and file processing will begin shortly after all files are created.</i></p>
        """
    
    except Exception as e:
        logger.error(f"Error in DIA-NN simulation: {str(e)}", exc_info=True)
        return f"Error: {str(e)}", 500

def create_test_files_sequentially(folder_path, file_names, job_id):
    """Create test files one by one with a delay to simulate file capture by the watcher"""
    logger.info(f"Starting sequential file creation for job {job_id}")
    
    # Wait a moment to ensure the watcher is ready to monitor files
    time.sleep(3)
    
    for i, file_name in enumerate(file_names):
        file_path = os.path.join(folder_path, file_name)
        logger.info(f"Creating test file {i+1}/{len(file_names)}: {file_path}")
        
        # Make sure the file doesn't exist before creating it (to avoid any issues)
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Removed existing test file before re-creating: {file_path}")
            except Exception as e:
                logger.error(f"Error removing existing file {file_path}: {str(e)}")
        
        # Create the file with sample content
        with open(file_path, 'w') as f:
            content = f"Test file content for {file_name}\nJob ID: {job_id}\nCreated at: {datetime.now().isoformat()}"
            f.write(content)
            f.flush()
            os.fsync(f.fileno())  # Ensure the file is written to disk
        
        # Make sure the file exists and log its size
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            logger.info(f"Verified file creation: {file_path} (Size: {file_size} bytes)")
        else:
            logger.error(f"Failed to create file: {file_path}")
        
        # Wait between file creations to let the watcher detect each file
        # Using a longer delay to ensure the watcher has time to process
        time.sleep(4)
    
    logger.info(f"Completed creating {len(file_names)} test files for job {job_id}")
    
    # Force a check for pending files by touching a trigger file
    trigger_file = os.path.join(folder_path, f"trigger_{job_id}.tmp")
    with open(trigger_file, 'w') as f:
        f.write(f"Trigger file to force watcher check. Created at: {datetime.now().isoformat()}")
    
    # Remove the trigger file after a brief delay
    time.sleep(1)
    try:
        if os.path.exists(trigger_file):
            os.remove(trigger_file)
    except Exception:
        pass

def cleanup_test_files(folder_path, file_names, delay_seconds=0):
    """Delete test files after a specified delay"""
    if delay_seconds > 0:
        logger.info(f"Waiting {delay_seconds} seconds before cleaning up test files")
        time.sleep(delay_seconds)
    
    logger.info(f"Starting cleanup of {len(file_names)} test files in {folder_path}")
    for file_name in file_names:
        file_path = os.path.join(folder_path, file_name)
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted test file: {file_path}")
            else:
                logger.info(f"File already deleted or not found: {file_path}")
        except Exception as e:
            logger.error(f"Error deleting file {file_path}: {str(e)}")
    
    logger.info(f"Completed cleanup of test files")

