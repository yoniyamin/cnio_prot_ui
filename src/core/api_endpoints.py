import os
import io
from flask import request, jsonify, send_file
from app_core import app, watcher_db_path, job_queue_manager, watcher_manager
from src.logging_utils import get_logger, log_dir
from src.database.watcher_db import WatcherDB
from src.utils.job_utils import update_job_status_safe

# Get module-specific logger
logger = get_logger(__name__)

# Initialize database connection
watcher_db = WatcherDB(watcher_db_path)

def get_db_instance(db_type):
    """Get appropriate database instance based on type"""
    global watcher_db
    
    if db_type == "jobs":
        from src.database.jobs_db import JobsDB
        db_instance = JobsDB(db_path="config/jobs.db")
        logger.debug(f"Created new JobsDB instance")
        return db_instance
    elif db_type == "watchers":
        logger.debug(f"Returning existing watcher_db instance")
        return watcher_db
    else:
        error_msg = f"Unknown database type: {db_type}"
        logger.error(error_msg)
        raise ValueError(error_msg)

@app.route('/api/logs')
def api_logs():
    try:
        # List all log files
        log_files = [f for f in os.listdir(log_dir) if f.startswith('ui_log_') and f.endswith('.log')]
        log_files.sort(reverse=True)  # Most recent first

        # Get the requested log file or use the most recent
        requested_log = request.args.get('file')
        if requested_log and requested_log in log_files:
            log_file = requested_log
        else:
            log_file = log_files[0] if log_files else None

        if not log_file:
            logger.warning("API request: logs - No log files found")
            return jsonify({"error": "No log files found"}), 404

        # Return the log file content
        log_path = os.path.join(log_dir, log_file)
        with open(log_path, 'r') as f:
            log_content = f.read()

        # Optionally filter by log level
        level = request.args.get('level', '').upper()
        if level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            filtered_lines = [line for line in log_content.splitlines()
                              if f" - {level} - " in line]
            log_content = '\n'.join(filtered_lines)
            logger.info(f"API request: logs - Filtered by level {level}, returning {len(filtered_lines)} lines")
        else:
            logger.info(f"API request: logs - Returning full log file {log_file}")

        # Return the log with proper headers for text display
        return log_content, 200, {'Content-Type': 'text/plain; charset=utf-8'}

    except Exception as e:
        logger.error(f"Error retrieving logs: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/watchers/<int:watcher_id>/rescan', methods=['POST'])
def api_force_watcher_rescan(watcher_id):
    """Force a watcher to rescan its folder for existing files"""
    try:
        logger.info(f"API request: force rescan for watcher {watcher_id}")
        
        # First check if the watcher exists and is active
        watcher_info = next((w for w in watcher_db.get_watchers() if w[0] == watcher_id), None)
        if not watcher_info:
            return jsonify({"error": f"Watcher {watcher_id} not found"}), 404
            
        # Check if watcher is monitoring
        if watcher_info[8] != 'monitoring':
            return jsonify({"error": f"Watcher {watcher_id} is not in monitoring state (current state: {watcher_info[8]})"}), 400
        
        # Get a fresh reference to the watcher_manager to avoid circular import issues
        from app_core import watcher_manager as current_watcher_manager
        
        # Use the existing watcher_manager to force a rescan
        if current_watcher_manager is not None and watcher_id in current_watcher_manager.watchers:
            # Use existing watcher
            current_watcher_manager.watchers[watcher_id].force_rescan()
            logger.info(f"Forced rescan for existing watcher {watcher_id}")
        else:
            if current_watcher_manager is None:
                logger.error("Global watcher_manager is not initialized, cannot force rescan")
                return jsonify({"error": "Watcher manager not initialized"}), 500
                
            # Create a temporary watcher if needed
            watcher = current_watcher_manager.create_single_watcher(watcher_id)
            watcher.force_rescan()
            logger.info(f"Forced rescan for newly created watcher {watcher_id}")
            
        return jsonify({"success": True, "message": f"Forced rescan for watcher {watcher_id}"})
    except Exception as e:
        logger.error(f"Error forcing rescan: {str(e)}", exc_info=True)
        return jsonify({"error": f"Error forcing rescan: {str(e)}"}), 500



@app.route('/api/download-log')
def api_download_log():
    try:
        # List all log files
        log_files = [f for f in os.listdir(log_dir) if f.startswith('ui_log_') and f.endswith('.log')]
        log_files.sort(reverse=True)  # Most recent first

        # Get the requested log file or use the most recent
        requested_log = request.args.get('file')
        if requested_log and requested_log in log_files:
            log_file = requested_log
        else:
            log_file = log_files[0] if log_files else None

        if not log_file:
            return jsonify({"error": "No log files found"}), 404

        # Return the log file for download
        log_path = os.path.join(log_dir, log_file)
        return send_file(
            log_path,
            mimetype='text/plain',
            as_attachment=True,
            download_name=log_file
        )

    except Exception as e:
        logger.error(f"Error downloading log: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/download-diann-example')
def download_diann_example():
    try:
        # For webview uses
        if request.args.get('webview') == 'true':
            return jsonify({
                "content": "Raw file\tReplicate\tExperiment\tCondition\n"
                           "file1.raw\t1\tExp1\tControl\n"
                           "file2.raw\t1\tExp1\tTreatment1\n"
                           "file3.raw\t1\tExp1\tTreatment2\n"
                           "file4.raw\t2\tExp1\tControl\n"
                           "file5.raw\t2\tExp1\tTreatment1\n"
                           "file6.raw\t2\tExp1\tTreatment2\n",
                "filename": "diann_conditions_example.tsv"
            })

        # For regular browser uses - existing code
        logger.info("API request: download-diann-example")
        # Create example TSV content
        example_content = (
            "Raw file\tReplicate\tExperiment\tCondition\n"
            "file1.raw\t1\tExp1\tControl\n"
            "file2.raw\t1\tExp1\tTreatment1\n"
            "file3.raw\t1\tExp1\tTreatment2\n"
            "file4.raw\t2\tExp1\tControl\n"
            "file5.raw\t2\tExp1\tTreatment1\n"
            "file6.raw\t2\tExp1\tTreatment2\n"
        )

        # Create a BytesIO object for the file
        buffer = io.BytesIO()
        buffer.write(example_content.encode('utf-8'))
        buffer.seek(0)

        return send_file(
            buffer,
            mimetype='text/tab-separated-values',
            as_attachment=True,
            download_name='diann_conditions_example.tsv'
        )
    except Exception as e:
        logger.error(f"Error creating example file: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/download-maxquant-example')
def download_maxquant_example():
    try:
        # For webview uses
        if request.args.get('webview') == 'true':
            return jsonify({
                "content": "Raw file\tExperiment\n"
                           "file1.raw\tExp1\n"
                           "file2.raw\tExp1\n"
                           "file3.raw\tExp1\n"
                           "file4.raw\tExp1\n"
                           "file5.raw\tExp2\n"
                           "file6.raw\tExp2\n",
                "filename": "maxquant_conditions_example.tsv"
            })

        # For regular browser uses - existing code
        logger.info("API request: download-maxquant-example")
        # Create example TSV content
        example_content = (
            "Raw file\tExperiment\n"
            "file1.raw\tExp1\n"
            "file2.raw\tExp1\n"
            "file3.raw\tExp1\n"
            "file4.raw\tExp1\n"
            "file5.raw\tExp1\n"
            "file6.raw\tExp1\n"
        )

        # Create a BytesIO object for the file
        buffer = io.BytesIO()
        buffer.write(example_content.encode('utf-8'))
        buffer.seek(0)

        return send_file(
            buffer,
            mimetype='text/tab-separated-values',
            as_attachment=True,
            download_name='maxquant_conditions_example.tsv'
        )
    except Exception as e:
        logger.error(f"Error creating example file: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

# Watcher API endpoints
@app.route('/api/watchers')
def api_get_watchers():
    """Get all watchers from the database with enhanced information"""
    try:
        logger.info("API request: get watchers")

        # Get status filter if provided
        status_param = request.args.get('status')

        # Handle multiple statuses separated by commas
        if status_param:
            statuses = status_param.split(',')
            watchers = []
            for status in statuses:
                watchers.extend(watcher_db.get_watchers(status=status.strip()))
        else:
            watchers = watcher_db.get_watchers()

        # Format the data for JSON response
        watcher_list = []
        for w in watchers:
            # Get captured files count for each watcher
            captured_files = watcher_db.get_captured_files(w[0])
            captured_count = len(captured_files)

            # Calculate expected files count if pattern doesn't have wildcards
            expected_count = 0
            file_patterns = w[2].split(';')
            exact_patterns = [p.strip() for p in file_patterns if not any(c in "*?[" for c in p)]
            expected_count = len(exact_patterns) if exact_patterns else 0

            watcher_list.append({
                "id": w[0],
                "folder_path": w[1],
                "file_pattern": w[2],
                "job_type": w[3],
                "job_demands": w[4],
                "job_name_prefix": w[5],
                "creation_time": w[6],
                "execution_time": w[7],
                "status": w[8],
                "completion_time": w[9],
                "captured_count": captured_count,
                "expected_count": expected_count
            })
        logger.info("Returning watchers data")
        return jsonify({"watchers": watcher_list})
    except Exception as e:
        logger.error(f"Error getting watchers: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/watchers/<int:watcher_id>/files')
def api_get_captured_files(watcher_id):
    """Get captured files for a specific watcher"""
    try:
        logger.info(f"API request: get captured files for watcher {watcher_id}")

        files = watcher_db.get_captured_files(watcher_id)

        # Format the data for JSON response
        file_list = []
        for f in files:
            file_list.append({
                "id": f[0],
                "job_id": f[1],
                "watcher_id": f[2],
                "file_name": f[3],
                "file_path": f[4],
                "capture_time": f[5]
            })

        # Get watcher details to check expected files
        watchers = watcher_db.get_watchers()
        watcher = next((w for w in watchers if w[0] == watcher_id), None)

        if watcher:
            # Get expected files based on file pattern if no wildcards
            file_patterns = watcher[2].split(';')
            expected_files = [p.strip() for p in file_patterns if not any(c in "*?[" for c in p)]

            # Find which expected files are not captured yet
            captured_filenames = [f["file_name"] for f in file_list]
            missing_files = []

            for expected in expected_files:
                if expected not in captured_filenames:
                    missing_files.append({
                        "id": None,
                        "job_id": None,
                        "watcher_id": watcher_id,
                        "file_name": expected,
                        "file_path": os.path.join(watcher[1], expected),
                        "capture_time": None,
                        "status": "pending"
                    })

            # Add missing files to the response
            file_list.extend(missing_files)

        return jsonify({"files": file_list})
    except Exception as e:
        logger.error(f"Error getting captured files: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/jobs/<job_id>')
def api_get_job(job_id):
    """Get details for a specific job"""
    try:
        logger.info(f"API request: get job details for job {job_id}")

        # First check if the job is in memory
        in_memory_job = None
        # Try to find the job in running, waiting, queued, and completed jobs
        for job_set, lock in [
            (job_queue_manager.running_jobs.values(), None),
            (job_queue_manager.waiting_jobs, job_queue_manager.waiting_jobs_lock),
            (job_queue_manager.queued_jobs, job_queue_manager.queued_jobs_lock),
            (job_queue_manager.completed_jobs, job_queue_manager.completed_jobs_lock)
        ]:
            if lock is not None and not lock.acquire(blocking=False):
                continue

            try:
                jobs = job_set if lock is None else list(job_set)
                for job in jobs:
                    if str(job.job_id) == str(job_id):
                        in_memory_job = job
                        break
            finally:
                if lock is not None:
                    lock.release()

            if in_memory_job:
                break

        # If found in memory, return it
        if in_memory_job:
            return jsonify({"job": in_memory_job.to_dict()})

        # If not found in memory, check the database
        from src.database.jobs_db import JobsDB
        jobs_db = JobsDB()
        job = jobs_db.get_job(job_id)

        if job:
            return jsonify({"job": job})
        else:
            return jsonify({"error": f"Job {job_id} not found"}), 404

    except Exception as e:
        logger.error(f"Error getting job {job_id}: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500
@app.route('/api/jobs')
def api_get_jobs():
    try:
        logger.info("API request: get jobs")

        # 1. Get in-memory jobs from the job queue manager
        in_memory_jobs = [
            job.to_dict()
            for job_set, lock in [
                (job_queue_manager.running_jobs.values(), None),
                (job_queue_manager.waiting_jobs, job_queue_manager.waiting_jobs_lock),
                (job_queue_manager.queued_jobs, job_queue_manager.queued_jobs_lock),
                (job_queue_manager.completed_jobs, job_queue_manager.completed_jobs_lock)
            ]
            for job in (job_set if lock is None else [job for job in job_set] if lock.acquire(blocking=False) else [])
        ]

        # 2. Get jobs from the database
        from src.database.jobs_db import JobsDB
        jobs_db = JobsDB()
        db_jobs = jobs_db.get_all_jobs()

        # 3. Filter to remove duplicates (prioritize in-memory jobs)
        # Find job_ids that are already in memory
        in_memory_job_ids = {job.get('job_id') for job in in_memory_jobs if job.get('job_id')}

        # Filter out database jobs that are already in memory
        unique_db_jobs = [
            job for job in db_jobs
            if job.get('job_id') and job.get('job_id') not in in_memory_job_ids
        ]

        # 4. Combine both lists
        all_jobs = in_memory_jobs + unique_db_jobs

        return jsonify({"jobs": all_jobs})
    except Exception as e:
        logger.error(f"Error getting jobs: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/watchers/<int:watcher_id>/update-status', methods=['POST'])
def api_update_watcher_status(watcher_id):
    try:
        data = request.json
        if not data or 'status' not in data:
            return jsonify({"error": "Missing status parameter"}), 400

        new_status = data['status']
        if new_status not in ['monitoring', 'completed', 'cancelled', 'paused']:
            return jsonify({"error": f"Invalid status: {new_status}"}), 400

        logger.info(f"API request: update watcher {watcher_id} status to {new_status}")
        watcher_db.update_watcher_status(watcher_id, new_status)
        # No need for separate completion_time update; handled in update_watcher_status

        return jsonify({"success": True, "message": f"Watcher {watcher_id} status updated to {new_status}"})
    except Exception as e:
        logger.error(f"Error updating watcher status: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/watchers', methods=['POST'])
def api_create_watcher():
    """Create a new file watcher"""
    try:
        data = request.json
        required_fields = ['folder_path', 'file_pattern', 'job_type', 'job_demands', 'job_name_prefix']

        # Validate required fields
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        logger.info(f"API request: create new watcher for {data['folder_path']}")

        # Ensure folder exists
        if not os.path.isdir(data['folder_path']):
            try:
                os.makedirs(data['folder_path'], exist_ok=True)
                logger.info(f"Created watcher directory: {data['folder_path']}")
            except Exception as e:
                logger.error(f"Error creating watcher directory: {str(e)}", exc_info=True)
                return jsonify({"error": f"Could not create directory: {str(e)}"}), 400

        watcher_id = watcher_db.add_watcher(
            folder_path=data['folder_path'],
            file_pattern=data['file_pattern'],
            job_type=data['job_type'],
            job_demands=data['job_demands'],
            job_name_prefix=data['job_name_prefix']
        )

        # Start the watcher in the WatcherManager
        try:
            # Get a fresh reference to the watcher_manager to avoid circular import issues
            from app_core import watcher_manager as current_watcher_manager
            
            # Create and start the watcher using the fresh reference
            if current_watcher_manager is not None:
                watcher = current_watcher_manager.create_single_watcher(watcher_id)
                
                # Start the watcher in a separate thread
                import threading
                thread = threading.Thread(target=watcher.start, daemon=True, 
                                        name=f"Watcher-{watcher_id}")
                current_watcher_manager.threads[watcher_id] = thread  # Store thread reference
                thread.start()
                
                logger.info(f"Started new watcher {watcher_id} in thread")
            else:
                logger.error(f"Global watcher_manager is not initialized, cannot start watcher {watcher_id}")
        except Exception as e:
            logger.error(f"Error starting watcher {watcher_id}: {str(e)}", exc_info=True)
            # Continue anyway since the watcher is created in the database

        return jsonify({"success": True, "watcher_id": watcher_id})
    except Exception as e:
        logger.error(f"Error creating watcher: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/jobs/<job_id>/stop', methods=['POST'])
def api_stop_job(job_id):
    """Stop a running job and its associated watcher - enhanced logging and part synchronous execution"""
    try:
        logger.info(f"JOB STOP: Beginning job stop for {job_id}")
        response = {"success": False, "actions": []}

        # Do essential operations synchronously to ensure they happen
        # 1. Update database record directly
        try:
            import sqlite3
            import os
            from datetime import datetime
            
            db_path = os.path.join("config", "jobs.db")
            logger.info(f"JOB STOP: Opening database at {db_path}")
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Check if update_time column exists
            cursor.execute("PRAGMA table_info(jobs)")
            columns = [row[1] for row in cursor.fetchall()]
            
            # Update the job status using appropriate SQL based on available columns
            current_time = datetime.now().isoformat()
            if 'update_time' in columns:
                cursor.execute(
                    "UPDATE jobs SET status = ?, update_time = ? WHERE job_id = ?",
                    ("cancelled", current_time, job_id)
                )
            else:
                # Fallback if update_time column doesn't exist
                cursor.execute(
                    "UPDATE jobs SET status = ? WHERE job_id = ?",
                    ("cancelled", job_id)
                )
            
            affected_rows = conn.total_changes
            conn.commit()
            
            logger.info(f"JOB STOP: Updated job {job_id} status to cancelled, affected rows: {affected_rows}")
            response["actions"].append(f"updated_db_rows_{affected_rows}")
            
            # Get watcher_id for this job
            cursor.execute("SELECT watcher_id FROM jobs WHERE job_id = ?", (job_id,))
            result = cursor.fetchone()
            watcher_id = result[0] if result else None
            conn.close()
            
            if watcher_id:
                logger.info(f"JOB STOP: Found watcher_id {watcher_id} for job {job_id}")
                response["actions"].append(f"found_watcher_{watcher_id}")
                
                # Update watcher status
                try:
                    # First try to update via the API
                    try:
                        from src.database.watcher_db import WatcherDB
                        from app_core import app, watcher_db_path, job_queue_manager, watcher_manager
                        watcher_db = WatcherDB(watcher_db_path)
                        watcher_db.update_watcher_status(int(watcher_id), 'cancelled')
                        logger.info(f"JOB STOP: Updated watcher {watcher_id} via API")
                        response["actions"].append("updated_watcher_api")
                    except Exception as e:
                        logger.error(f"JOB STOP: Error updating watcher via API: {str(e)}")
                        
                        # Fallback to direct SQL
                        watcher_db_path = os.path.join("config", "watchers.db")
                        conn = sqlite3.connect(watcher_db_path)
                        cursor = conn.cursor()
                        cursor.execute(
                            "UPDATE watchers SET status = ?, completion_time = ? WHERE id = ?",
                            ("cancelled", datetime.now().isoformat(), watcher_id)
                        )
                        affected_rows = conn.total_changes
                        conn.commit()
                        conn.close()
                        logger.info(f"JOB STOP: Updated watcher {watcher_id} via SQL, affected rows: {affected_rows}")
                        response["actions"].append(f"updated_watcher_sql_{affected_rows}")
                except Exception as e:
                    logger.error(f"JOB STOP: All watcher update methods failed: {str(e)}")
        except Exception as e:
            logger.error(f"JOB STOP: Database operation failed: {str(e)}")
            response["errors"] = [f"db_error: {str(e)}"]
        
        # 2. Now start a background thread for remaining actions
        import threading
        
        def background_actions():
            logger.info(f"JOB STOP BACKGROUND: Starting background actions for job {job_id}")
            
            # Create cancellation flag file
            try:
                import os
                flag_dir = os.path.join("temp", "cancellations")
                os.makedirs(flag_dir, exist_ok=True)
                flag_file = os.path.join(flag_dir, f"cancel_{job_id}.flag")
                
                with open(flag_file, 'w') as f:
                    f.write(f"Cancellation at {datetime.now().isoformat()}")
                
                logger.info(f"JOB STOP BACKGROUND: Created flag file at {flag_file}")
            except Exception as e:
                logger.error(f"JOB STOP BACKGROUND: Error creating flag file: {str(e)}")
            
            # Try to update in-memory job object
            try:
                from app_core import job_queue_manager
                if job_id in job_queue_manager.running_jobs:
                    job = job_queue_manager.running_jobs.get(job_id)
                    if job and hasattr(job, 'change_job_status'):
                        job.change_job_status('cancelled')
                        logger.info(f"JOB STOP BACKGROUND: Updated in-memory job status")
            except Exception as e:
                logger.error(f"JOB STOP BACKGROUND: Error updating in-memory job: {str(e)}")
            
            logger.info(f"JOB STOP BACKGROUND: Completed background actions for job {job_id}")
        
        # Start background thread
        bg_thread = threading.Thread(target=background_actions)
        bg_thread.daemon = True
        bg_thread.start()
        
        # Mark as success since we updated the database
        response["success"] = True
        logger.info(f"JOB STOP: Returning success response with actions: {response['actions']}")
        
        return response
        
    except Exception as e:
        logger.error(f"JOB STOP: Fatal error in stop endpoint: {str(e)}", exc_info=True)
        return {"error": str(e), "success": False}, 500


@app.route('/api/jobs/<job_id>/demands')
def api_get_job_demands(job_id):
    """Get the job_demands configuration for a specific job"""
    try:
        logger.info(f"API request: get job demands for job {job_id}")

        # First check for in-memory job
        in_memory_job = None
        for job_set, lock in [
            (job_queue_manager.running_jobs.values(), None),
            (job_queue_manager.waiting_jobs, job_queue_manager.waiting_jobs_lock),
            (job_queue_manager.queued_jobs, job_queue_manager.queued_jobs_lock),
            (job_queue_manager.completed_jobs, job_queue_manager.completed_jobs_lock)
        ]:
            if lock is not None and not lock.acquire(blocking=False):
                continue

            try:
                jobs = job_set if lock is None else list(job_set)
                for job in jobs:
                    if str(job.job_id) == str(job_id):
                        in_memory_job = job
                        break
            finally:
                if lock is not None:
                    lock.release()

            if in_memory_job:
                break

        # If found in memory and has job_demands
        if in_memory_job and hasattr(in_memory_job, 'job_demands'):
            job_demands = in_memory_job.job_demands
            if isinstance(job_demands, str):
                try:
                    import json
                    return jsonify({"demands": json.loads(job_demands)})
                except:
                    return jsonify({"demands": {"raw_config": job_demands}})
            elif isinstance(job_demands, dict):
                return jsonify({"demands": job_demands})

        # If not found in memory or no job_demands, check database
        from src.database.jobs_db import JobsDB
        jobs_db = JobsDB()
        job_demands = jobs_db.get_job_demands(job_id)

        if job_demands:
            return jsonify({"demands": job_demands})
        else:
            return jsonify({"demands": {}}), 404

    except Exception as e:
        logger.error(f"Error getting job demands: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/jobs/<job_id>/files')
def api_get_job_files(job_id):
    """Get files associated with a specific job by redirecting to the watcher's files endpoint"""
    try:
        logger.info(f"API request: get files for job {job_id}")

        # Get the watcher_id from the jobs database
        from src.database.jobs_db import JobsDB
        jobs_db = JobsDB()
        watcher_id = jobs_db.get_watcher_id_for_job(job_id)

        if watcher_id:
            logger.info(f"Found watcher ID {watcher_id} for job {job_id}, redirecting to watcher files API")
            # Simply call the watcher files API directly
            return api_get_captured_files(watcher_id)
        else:
            logger.info(f"No watcher ID found for job {job_id}")
            return jsonify({"files": []})

    except Exception as e:
        logger.error(f"Error getting job files: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/jobs/<job_id>/emergency-stop', methods=['POST'])
def api_emergency_stop_job(job_id):
    """Emergency endpoint to stop a job using direct database access and minimal dependencies"""
    try:
        import sqlite3
        import os
        import time
        import json
        from datetime import datetime
        
        response = {
            "success": False,
            "actions": [],
            "errors": []
        }
        
        # Log the request
        print(f"EMERGENCY: Received request to stop job {job_id}")
        response["actions"].append("request_received")
        
        # 1. Direct database update for jobs - absolute minimal approach
        try:
            db_path = os.path.join("config", "jobs.db")
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Check for update_time column
            cursor.execute("PRAGMA table_info(jobs)")
            columns = [row[1] for row in cursor.fetchall()]
            
            # Use appropriate SQL based on available columns
            current_time = datetime.now().isoformat()
            if 'update_time' in columns:
                cursor.execute(
                    "UPDATE jobs SET status = ?, update_time = ? WHERE job_id = ?",
                    ("cancelled", current_time, job_id)
                )
            else:
                # Fallback if update_time column doesn't exist
                cursor.execute(
                    "UPDATE jobs SET status = ? WHERE job_id = ?",
                    ("cancelled", job_id)
                )
                
            affected = conn.total_changes
            conn.commit()
            conn.close()
            
            response["actions"].append(f"updated_job_status_direct_sql_{affected}")
            print(f"EMERGENCY: Updated job {job_id} status to cancelled using direct SQL")
        except Exception as e:
            error_msg = str(e)
            response["errors"].append(f"db_update_error: {error_msg}")
            print(f"EMERGENCY: Error updating job status: {error_msg}")
        
        # 2. Create cancellation flag file - no database dependency
        try:
            # Create a physical file that the system can check
            flag_dir = os.path.join("temp", "cancellations")
            os.makedirs(flag_dir, exist_ok=True)
            flag_file = os.path.join(flag_dir, f"emergency_cancel_{job_id}.flag")
            with open(flag_file, 'w') as f:
                f.write(f"Emergency cancellation at {time.time()}")
            
            response["actions"].append("created_flag_file")
            print(f"EMERGENCY: Created cancellation flag file: {flag_file}")
        except Exception as e:
            error_msg = str(e)
            response["errors"].append(f"flag_file_error: {error_msg}")
            print(f"EMERGENCY: Error creating flag file: {error_msg}")
        
        # 3. Find and update watcher status directly
        try:
            # Get watcher_id directly from database
            db_path = os.path.join("config", "jobs.db")
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT watcher_id FROM jobs WHERE job_id = ?", (job_id,))
            result = cursor.fetchone()
            conn.close()
            
            watcher_id = result[0] if result else None
            
            if watcher_id:
                response["actions"].append(f"found_watcher_id_{watcher_id}")
                print(f"EMERGENCY: Found watcher_id {watcher_id} for job {job_id}")
                
                # Update watcher status directly
                watcher_db_path = os.path.join("config", "watchers.db")
                conn = sqlite3.connect(watcher_db_path)
                cursor = conn.cursor()
                
                cursor.execute(
                    "UPDATE watchers SET status = ?, completion_time = ? WHERE id = ?",
                    ("cancelled", datetime.now().isoformat(), watcher_id)
                )
                affected = conn.total_changes
                conn.commit()
                conn.close()
                
                response["actions"].append(f"updated_watcher_status_direct_sql_{affected}")
                print(f"EMERGENCY: Updated watcher {watcher_id} status to cancelled using direct SQL")
        except Exception as e:
            error_msg = str(e)
            response["errors"].append(f"watcher_update_error: {error_msg}")
            print(f"EMERGENCY: Error updating watcher: {error_msg}")
        
        # If we got here, at least the endpoint responded
        response["success"] = True
        return json.dumps(response), 200, {'Content-Type': 'application/json'}
    
    except Exception as e:
        # Use simplest possible error response with no dependencies
        return f"EMERGENCY STOP ERROR: {str(e)}", 500

@app.route('/api/jobs/<job_id>/direct-stop', methods=['POST'])
def api_direct_stop_job(job_id):
    """Direct endpoint to stop a job using only SQL commands"""
    try:
        # Log to both console and logger
        logger.info(f"DIRECT STOP: Starting emergency stop for job {job_id}")
        
        response = {
            "success": False,
            "actions": [],
            "errors": []
        }
        
        # 1. Update job status directly with SQL
        try:
            import sqlite3
            import os
            from datetime import datetime
            
            # Update job in jobs.db
            db_path = os.path.join("config", "jobs.db")
            logger.info(f"DIRECT STOP: Opening database at {db_path}")
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get current time for updates
            current_time = datetime.now().isoformat()
            
            # Check if update_time column exists
            cursor.execute("PRAGMA table_info(jobs)")
            columns = [row[1] for row in cursor.fetchall()]
            
            # Update the job status using appropriate SQL based on available columns
            if 'update_time' in columns:
                cursor.execute(
                    "UPDATE jobs SET status = ?, update_time = ? WHERE job_id = ?",
                    ("cancelled", current_time, job_id)
                )
            else:
                # Fallback if update_time column doesn't exist
                cursor.execute(
                    "UPDATE jobs SET status = ? WHERE job_id = ?",
                    ("cancelled", job_id)
                )
                
            affected_rows = conn.total_changes
            conn.commit()
            
            response["actions"].append(f"Updated job status, rows affected: {affected_rows}")
            logger.info(f"DIRECT STOP: Updated job {job_id} status to cancelled, affected rows: {affected_rows}")
            
            # Get watcher_id for this job
            cursor.execute("SELECT watcher_id FROM jobs WHERE job_id = ?", (job_id,))
            result = cursor.fetchone()
            watcher_id = result[0] if result else None
            conn.close()
            
            if watcher_id:
                response["actions"].append(f"Found watcher_id: {watcher_id}")
                logger.info(f"DIRECT STOP: Found watcher_id {watcher_id} for job {job_id}")
                
                # Update watcher in watchers.db
                watcher_db_path = os.path.join("config", "watchers.db")
                logger.info(f"DIRECT STOP: Opening watcher database at {watcher_db_path}")
                
                conn = sqlite3.connect(watcher_db_path)
                cursor = conn.cursor()
                
                # Update the watcher status
                cursor.execute(
                    "UPDATE watchers SET status = ?, completion_time = ? WHERE id = ?",
                    ("cancelled", current_time, watcher_id)
                )
                affected_rows = conn.total_changes
                conn.commit()
                conn.close()
                
                response["actions"].append(f"Updated watcher status, rows affected: {affected_rows}")
                logger.info(f"DIRECT STOP: Updated watcher {watcher_id} status to cancelled, affected rows: {affected_rows}")
            
            # Try to create a cancellation flag file
            try:
                flag_dir = os.path.join("temp", "emergency")
                os.makedirs(flag_dir, exist_ok=True)
                flag_file = os.path.join(flag_dir, f"cancel_{job_id}.flag")
                
                with open(flag_file, 'w') as f:
                    f.write(f"Cancellation requested at {current_time}")
                
                response["actions"].append(f"Created flag file at {flag_file}")
                logger.info(f"DIRECT STOP: Created cancellation flag file at {flag_file}")
            except Exception as e:
                logger.error(f"DIRECT STOP: Error creating flag file: {str(e)}")
                response["errors"].append(f"Flag file error: {str(e)}")
            
            # Mark as success
            response["success"] = True
            
        except Exception as e:
            error_msg = f"Database error: {str(e)}"
            logger.error(f"DIRECT STOP: {error_msg}")
            response["errors"].append(error_msg)
        
        # Log complete response
        logger.info(f"DIRECT STOP: Completed with response: {response}")
        
        return response
        
    except Exception as e:
        error_msg = f"Emergency stop failed: {str(e)}"
        logger.error(f"DIRECT STOP: {error_msg}", exc_info=True)
        return {"error": error_msg, "success": False}, 500