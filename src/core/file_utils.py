import os
import io
import json
import pandas as pd
from pathlib import Path
from src.utils import logger
import subprocess
import platform


def ensure_directory_exists(directory_path):
    """Ensure that a directory exists, create it if it doesn't"""
    if not os.path.exists(directory_path):
        try:
            os.makedirs(directory_path, exist_ok=True)
            logger.info(f"Created directory: {directory_path}")
            return True
        except Exception as e:
            logger.error(f"Error creating directory {directory_path}: {str(e)}", exc_info=True)
            return False
    return True


def parse_conditions_file(file_path):
    """Parse a conditions file (Excel, TSV, or TXT) and return a dataframe"""
    try:
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".xlsx":
            return pd.read_excel(file_path, sheet_name="Sheet1", dtype=str)
        elif ext in [".txt", ".tsv"]:
            return pd.read_csv(file_path, sep="\t", dtype=str)
        else:
            logger.error(f"Unsupported file extension: {ext}")
            return None
    except Exception as e:
        logger.error(f"Error parsing conditions file {file_path}: {str(e)}", exc_info=True)
        return None


def get_raw_files_from_conditions(conditions_df):
    """Extract raw file names from a conditions dataframe"""
    try:
        # Determine the column to use for raw files
        if 'Raw file' in conditions_df.columns:
            raw_file_column = 'Raw file'
        elif 'Raw file path' in conditions_df.columns:
            raw_file_column = 'Raw file path'
        else:
            logger.error(f"Conditions file missing required column. Available columns: {list(conditions_df.columns)}")
            return None

        # Extract file names (handling potential paths)
        raw_files = conditions_df[raw_file_column].apply(lambda x: os.path.basename(str(Path(x)))).tolist()
        return raw_files
    except Exception as e:
        logger.error(f"Error extracting raw files from conditions: {str(e)}", exc_info=True)
        return None


def read_job_status(job_dir, job_name):
    """Read job status from status file"""
    status_file = os.path.join(job_dir, job_name, "status.txt")
    if os.path.exists(status_file):
        try:
            with open(status_file, "r") as f:
                return f.read().strip()
        except Exception as e:
            logger.error(f"Error reading status file for job {job_name}: {str(e)}", exc_info=True)
    return "unknown"


def write_job_status(job_dir, job_name, status):
    """Write job status to status file"""
    try:
        job_status_dir = os.path.join(job_dir, job_name)
        os.makedirs(job_status_dir, exist_ok=True)

        status_file = os.path.join(job_status_dir, "status.txt")
        with open(status_file, "w") as f:
            f.write(status)
        logger.info(f"Job {job_name} status updated to: {status}")
        return True
    except Exception as e:
        logger.error(f"Error writing status for job {job_name}: {str(e)}", exc_info=True)
        return False


def append_job_log(job_dir, job_name, message):
    """Append a message to the job log file"""
    try:
        job_status_dir = os.path.join(job_dir, job_name)
        os.makedirs(job_status_dir, exist_ok=True)

        log_file = os.path.join(job_status_dir, "progress.log")
        with open(log_file, "a") as f:
            f.write(f"{message}\n")
        return True
    except Exception as e:
        logger.error(f"Error writing to log for job {job_name}: {str(e)}", exc_info=True)
        return False


def save_job_info(job_dir, job_name, job_data):
    """Save job information to a JSON file"""
    try:
        job_status_dir = os.path.join(job_dir, job_name)
        os.makedirs(job_status_dir, exist_ok=True)

        info_file = os.path.join(job_status_dir, "job_info.json")
        with open(info_file, "w") as f:
            json.dump(job_data, f, indent=2)
        logger.info(f"Job info saved for {job_name}")
        return True
    except Exception as e:
        logger.error(f"Error saving job info for {job_name}: {str(e)}", exc_info=True)
        return False


def read_job_info(job_dir, job_name):
    """Read job information from a JSON file"""
    info_file = os.path.join(job_dir, job_name, "job_info.json")
    if os.path.exists(info_file):
        try:
            with open(info_file, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading job info for {job_name}: {str(e)}", exc_info=True)
    return None


def get_log_files(log_dir):
    """Get list of log files from log directory"""
    try:
        if not os.path.exists(log_dir):
            return []

        log_files = [f for f in os.listdir(log_dir) if f.startswith('ui_log_') and f.endswith('.log')]
        log_files.sort(reverse=True)  # Most recent first
        return log_files
    except Exception as e:
        logger.error(f"Error getting log files: {str(e)}", exc_info=True)
        return []


def read_log_file(log_dir, log_file, level=None):
    """Read log file content with optional level filtering"""
    try:
        log_path = os.path.join(log_dir, log_file)
        if not os.path.exists(log_path):
            return None

        with open(log_path, 'r') as f:
            log_content = f.read()

        # Optionally filter by log level
        if level and level.upper() in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            filtered_lines = [line for line in log_content.splitlines()
                              if f" - {level.upper()} - " in line]
            log_content = '\n'.join(filtered_lines)

        return log_content
    except Exception as e:
        logger.error(f"Error reading log file {log_file}: {str(e)}", exc_info=True)
        return None


def get_active_log_path():
    """Get the path to the active log file"""
    try:
        # Get the project root directory (1 level up from src/file_utils.py)
        curr_dir = os.path.dirname(os.path.abspath(__file__))  # src directory
        project_root = os.path.dirname(curr_dir)  # Go up to project root

        # Path to logs directory in project root
        log_dir = os.path.join(project_root, 'logs')
        logger.info(f"Looking for logs in: {log_dir}")

        # Check if log directory exists
        if not os.path.exists(log_dir):
            logger.warning(f"Log directory not found: {log_dir}")
            return None

        # Get all log files
        log_files = get_log_files(log_dir)
        if not log_files:
            logger.warning("No log files found")
            return None

        # Return the full path to the most recent log file
        return os.path.join(log_dir, log_files[0])  # First one is the most recent due to sorting
    except Exception as e:
        logger.error(f"Error getting active log path: {str(e)}", exc_info=True)
        return None


def open_log_file():
    """Open the active log file in the default text editor"""
    try:
        # Get the active log file path
        active_log_path = get_active_log_path()
        if not active_log_path or not os.path.exists(active_log_path):
            logger.error("Active log file not found")
            return False

        # Open the file using the system's default application
        logger.info(f"Attempting to open log file: {active_log_path}")

        # Different methods to open files based on the platform
        system = platform.system()

        if system == 'Windows':
            os.startfile(active_log_path)
        elif system == 'Darwin':  # macOS
            subprocess.call(['open', active_log_path])
        else:  # Linux and other Unix-like
            subprocess.call(['xdg-open', active_log_path])

        logger.info(f"Log file opened: {active_log_path}")
        return True
    except Exception as e:
        logger.error(f"Error opening log file: {str(e)}", exc_info=True)
        return False