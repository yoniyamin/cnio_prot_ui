import os
import io
from flask import request, jsonify, send_file
from app_core import app, watcher_db_path, job_queue_manager
from src.utils import logger, log_dir
from src.database.watcher_db import WatcherDB

# Initialize database connection
watcher_db = WatcherDB(watcher_db_path)
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

        # Return the log with proper headers for text display
        return log_content, 200, {'Content-Type': 'text/plain; charset=utf-8'}

    except Exception as e:
        logger.error(f"Error retrieving logs: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


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
        # Here you would typically trigger the WatcherManager to load and start the new watcher
        try:
            # Import here to avoid circular imports
            from src.watchers.watcher_manager import WatcherManager
            # Create and start a single watcher
            watcher_manager = WatcherManager(watcher_db, job_queue_manager)
            watcher = watcher_manager.create_single_watcher(watcher_id)

            # Start the watcher in a separate thread
            import threading
            thread = threading.Thread(target=watcher.start, daemon=True)
            watcher_manager.threads[watcher_id] = thread  # Store thread reference
            thread.start()

            logger.info(f"Started new watcher {watcher_id} in thread")
        except Exception as e:
            logger.error(f"Error starting watcher {watcher_id}: {str(e)}", exc_info=True)
            # Continue anyway since the watcher is created in the database

        return jsonify({"success": True, "watcher_id": watcher_id})
    except Exception as e:
        logger.error(f"Error creating watcher: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/jobs/<job_id>/stop', methods=['POST'])
def api_stop_job(job_id):
    """Stop a running job and its associated watcher"""
    try:
        logger.info(f"API request: stop job {job_id}")

        # 1. Update job status in database
        from src.database.jobs_db import JobsDB
        jobs_db = JobsDB()
        jobs_db.update_job_status(job_id, 'cancelled')

        # 2. Get watcher_id from database
        watcher_id = jobs_db.get_watcher_id_for_job(job_id)

        logger.info(f"Found watcher_id: {watcher_id} for job {job_id}")

        # 3. Handle in-memory job cancellation
        job_found = False
        # ... (same code as before) ...

        # 4. Stop the associated watcher by updating status in DB
        if watcher_id:
            try:
                logger.info(f"Canceling watcher {watcher_id} associated with job {job_id}")

                # Update the watcher status in the database
                watcher_db.update_watcher_status(int(watcher_id), 'cancelled')
                logger.info(f"Watcher {watcher_id} status updated to cancelled in database")

                # The watcher thread should detect this status change within 1 second
                # (based on the status check in FileWatcher.start())

            except Exception as e:
                logger.error(f"Error canceling watcher {watcher_id}: {str(e)}", exc_info=True)

        return jsonify({
            "success": True,
            "message": f"Job {job_id} cancelled" +
                       (f" and watcher {watcher_id} status updated to cancelled" if watcher_id else "")
        })

    except Exception as e:
        logger.error(f"Error stopping job {job_id}: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


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