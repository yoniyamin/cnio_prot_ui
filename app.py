import os
import io
import json
import logging
import sys
import time
from datetime import datetime
import threading
import webview
from flask import Flask, render_template, request, redirect, url_for, jsonify, send_file
from src.handlers.run_maxquant import MaxQuant_handler
from src.handlers.diann_handler import DIANNHandler, launch_diann_job
try:
    import pystray
    from PIL import Image
    HAS_SYSTRAY = True
except ImportError:
    HAS_SYSTRAY = False
    print("pystray package not found, system tray functionality will be disabled")

# Setup logging
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)

log_filename = f"ui_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_filepath = os.path.join(log_dir, log_filename)

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filepath),
        logging.StreamHandler()  # Also log to console
    ]
)

logger = logging.getLogger('proteomics_ui')

# Initialize Flask app
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'

# Ensure upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Store window reference for API exposure
tray_icon = None
window = None


def create_icon():
    """Create an icon for the system tray"""
    # Try multiple potential locations for the icon file
    potential_icon_paths = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'favicon.ico'),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'icons', 'favicon.ico'),
        os.path.join('static', 'favicon.ico')
    ]

    for icon_path in potential_icon_paths:
        if os.path.exists(icon_path):
            logger.info(f"Using icon file from: {icon_path}")
            return Image.open(icon_path)

    # If no icon file is available, create a simple icon programmatically
    logger.warning("No icon file found, creating a default icon")
    image = Image.new('RGB', (64, 64), color=(73, 109, 137))
    return image


def setup_tray():
    global tray_icon
    logger.info("Setting up system tray icon")
    icon = create_icon()

    def do_restore():
        logger.info("Activating restore from tray")
        restore_window()

    def do_quit():
        logger.info("Activating quit from menu")
        quit_app()

    # Define the menu with 'Show' as the default action for double-click
    menu = pystray.Menu(
        pystray.MenuItem('Show', do_restore, default=True),  # Default action on double-click
        pystray.MenuItem('Quit', do_quit)
    )

    tray_icon = pystray.Icon(
        'Proteomics UI',
        icon,
        'Proteomics Core UI',
        menu=menu
    )

    tray_thread = threading.Thread(target=tray_icon.run)
    tray_thread.daemon = True
    tray_thread.start()
    logger.info("System tray setup complete")


def restore_window():
    logger.info("Restoring window from system tray")
    if window:
        try:
            # Make the window visible
            window.show()
            logger.info("Window show() called")

            # Attempt to restore using pywebview's API if available
            try:
                window.restore()  # Restore from minimized state
                logger.info("Window restore() called")
            except AttributeError:
                logger.debug("window.restore() not available, falling back to platform-specific method")

            # Ensure the window is brought to the foreground on Windows
            import platform
            if platform.system() == 'Windows':
                try:
                    import win32gui
                    import win32con

                    # Log all window titles and class names to identify the correct one
                    hwnds = []
                    def callback(hwnd, hwnds):
                        title = win32gui.GetWindowText(hwnd)
                        class_name = win32gui.GetClassName(hwnd)
                        logger.debug(f"Window title: {title}, class: {class_name}")
                        # Adjust this condition based on logged output
                        if "Proteomics Core UI" in title and "Chrome" in class_name:  # CEF window class might include "Chrome"
                            hwnds.append(hwnd)
                        return True

                    win32gui.EnumWindows(callback, hwnds)
                    if hwnds:
                        hwnd = hwnds[0]
                        win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)  # Restore from minimized
                        win32gui.SetForegroundWindow(hwnd)  # Bring to front
                        logger.info(f"Window activated using win32gui: {hwnd}")
                    else:
                        logger.warning("Could not find window handle matching criteria (No action Needed)")
                except Exception as we:
                    logger.warning(f"Windows-specific activation failed: {we}")
        except Exception as e:
            logger.error(f"Error restoring window: {str(e)}", exc_info=True)
    else:
        logger.warning("Cannot restore window - window reference not available")


def quit_app():
    """Quit the application"""
    logger.info("Quitting application from system tray")
    try:
        # First hide the window to prevent UI issues
        if window:
            try:
                window.hide()
                logger.info("Window hidden before shutdown")
            except:
                pass

        # Stop the tray icon
        if tray_icon:
            try:
                # Set a flag to prevent the tray's destroy callback from causing an infinite loop
                logger.info("Stopping system tray icon")
                tray_icon.stop()
            except Exception as e:
                logger.warning(f"Error stopping tray icon: {e}")

        # Close Flask server gracefully
        logger.info("Shutting down application")

        # Use a more graceful exit method that avoids the Chrome widget error
        def delayed_exit():
            # Give time for other resources to clean up
            time.sleep(0.5)
            # Use os._exit as a last resort
            os._exit(0)

        # Start the delayed exit in a separate thread
        threading.Thread(target=delayed_exit, daemon=True).start()

    except Exception as e:
        logger.error(f"Error during application shutdown: {str(e)}", exc_info=True)
        # If all else fails, force exit
        os._exit(0)


def on_closed():
    """Handle window close event"""
    logger.info("Window closed")
    if tray_icon:
        tray_icon.stop()
    sys.exit(0)


def on_minimize():
    """Handle window minimize event to send to system tray"""
    logger.info("Window minimize event detected")
    if not HAS_SYSTRAY:
        logger.warning("System tray functionality not available, window will be minimized normally")
        return False

    if window:
        logger.info("Hiding window to system tray")
        window.hide()
        logger.info("Window hidden to system tray")
        return True
    else:
        logger.warning("Window reference not available")
        return False

class Api:
    def __init__(self):
        """Initialize API with defaults directory"""
        self.defaults_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'defaults')
        os.makedirs(self.defaults_dir, exist_ok=True)
        logger.info(f"Initialized API with defaults directory: {self.defaults_dir}")

    def select_directory(self):
        """Open directory selection dialog and return the selected path"""
        logger.info("Opening directory selection dialog")
        result = window.create_file_dialog(webview.FOLDER_DIALOG)
        if result:
            logger.info(f"Selected directory: {result[0]}")
            return result[0]  # Returns the first selected directory
        logger.info("Directory selection canceled")
        return None

    def select_file(self, file_types=None):
        """Open file selection dialog with optional file type filter and return the selected path"""
        if not file_types:
            file_types = ()
            logger.info("Opening file selection dialog without filter")
        else:
            # Convert ".xlsx,.tsv,.txt" to a tuple of file extensions
            file_types = tuple(file_types.split(','))
            logger.info(f"Opening file selection dialog with filter: {file_types}")

        result = window.create_file_dialog(webview.OPEN_DIALOG, file_types=file_types)
        if result:
            logger.info(f"Selected file: {result[0]}")
            return result[0]  # Returns the first selected file
        logger.info("File selection canceled")
        return None

    def get_default_values(self, module):
        """Get all default values for a specific module"""
        try:
            defaults_file = os.path.join(self.defaults_dir, f"{module}_defaults.json")
            logger.info(f"Loading default values for module: {module}")

            if os.path.exists(defaults_file):
                with open(defaults_file, 'r') as f:
                    return json.load(f)
            else:
                logger.info(f"No defaults file found for module: {module}")
                return {}
        except Exception as e:
            logger.error(f"Error loading default values for module {module}: {str(e)}")
            return {}

    def save_example_file(self, content, default_filename):
        """Save example file to user-selected location"""
        logger.info(f"Saving example file with name {default_filename}")
        try:
            # Use the dialog to get save location
            save_path = window.create_file_dialog(
                webview.SAVE_DIALOG,
                directory='~',
                save_filename=default_filename
            )

            if not save_path:
                logger.info("Save dialog canceled")
                return {"success": False, "canceled": True}

            logger.info(f"Saving example file to {save_path}")
            with open(save_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return {"success": True, "path": save_path}
        except Exception as e:
            logger.error(f"Error saving example file: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}

    def show_window(self):
        """Bring the WebView window to front if minimized or hidden"""
        restore_window()
        return True


# API endpoints for file dialog operations
@app.route('/api/select-directory')
def api_select_directory():
    logger.info("API request: select-directory")
    result = window.create_file_dialog(webview.FOLDER_DIALOG)
    if result:
        logger.info(f"Directory selected: {result[0]}")
        return jsonify({"path": result[0]})
    logger.info("Directory selection canceled")
    return jsonify({"path": None})


@app.route('/api/select-file')
def api_select_file():
    file_types = request.args.get('types', '')
    logger.info(f"API request: select-file with types: {file_types}")

    try:
        result = window.create_file_dialog(webview.OPEN_DIALOG)

        if not result:
            logger.info("File selection canceled")
            return jsonify({"path": None})  # User canceled

        selected_file = result[0]
        logger.info(f"File selected: {selected_file}")

        # If specific file types were requested, validate the extension
        if file_types and file_types.strip():
            # Get allowed extensions
            allowed_extensions = [ext.strip().lower() for ext in file_types.split(',')]

            # Check if the selected file has an allowed extension
            file_ext = os.path.splitext(selected_file)[1].lower()

            if not any(file_ext == ext if ext.startswith('.') else file_ext == '.' + ext for ext in allowed_extensions):
                logger.warning(f"Selected file {selected_file} does not match required types: {file_types}")
                # Return an error if invalid extension, but also the path so the UI can show it
                return jsonify({
                    "path": selected_file,
                    "error": f"Selected file must be one of these types: {file_types}"
                })

        # All good
        return jsonify({"path": selected_file})

    except Exception as e:
        logger.error(f"Error in file dialog: {str(e)}", exc_info=True)
        return jsonify({"error": str(e), "path": None})


@app.route('/api/save-all-defaults', methods=['POST'])
def api_save_all_defaults():
    try:
        data = request.json
        if not data or 'module' not in data or 'defaults' not in data:
            logger.warning("API request: save-all-defaults with missing required data")
            return jsonify({"error": "Missing required data"}), 400

        module = data['module']
        defaults = data['defaults']
        logger.info(f"API request: save-all-defaults for module: {module}")

        # Get defaults directory
        defaults_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'defaults')
        os.makedirs(defaults_dir, exist_ok=True)

        # Save all defaults in a single operation
        defaults_file = os.path.join(defaults_dir, f"{module}_defaults.json")

        with open(defaults_file, 'w') as f:
            json.dump(defaults, f, indent=2)

        logger.info(f"Saved defaults for module {module} to {defaults_file}")
        return jsonify({"success": True, "message": "All defaults saved successfully"})
    except Exception as e:
        logger.error(f"Error saving defaults: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/get-defaults')
def api_get_defaults():
    module = request.args.get('module', '')
    if not module:
        logger.warning("API request: get-defaults without module parameter")
        return jsonify({"error": "Module parameter is required"}), 400

    try:
        logger.info(f"API request: get-defaults for module: {module}")
        defaults_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'defaults')
        defaults_file = os.path.join(defaults_dir, f"{module}_defaults.json")

        if os.path.exists(defaults_file):
            with open(defaults_file, 'r') as f:
                logger.info(f"Loaded defaults for module {module} from {defaults_file}")
                return jsonify(json.load(f))
        else:
            logger.info(f"No defaults file found for module: {module}")
            return jsonify({})
    except Exception as e:
        logger.error(f"Error getting defaults for module {module}: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


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


@app.route('/')
def home():
    logger.info("Route: home")
    return render_template('home.html')


@app.route('/maxquant', methods=['GET', 'POST'])
def maxquant():
    if request.method == 'GET':
        logger.info("Route: maxquant (GET)")
        return render_template('maxquant.html')

    logger.info("Route: maxquant (POST)")
    try:
        # Extract form data
        fasta_folder = request.form.get('fasta_folder')
        output_folder = request.form.get('output_folder')
        conditions_file = request.form.get('conditions_file')
        mq_path = request.form.get('mq_path')
        mq_version = request.form.get('mq_version')
        dbs = request.form.getlist('database_choices')
        job_name = request.form.get('job_name') or "MaxQuantJob"

        logger.info(f"MaxQuant job submitted: {job_name}")
        logger.debug(
            f"MaxQuant parameters: fasta={fasta_folder}, output={output_folder}, conditions={conditions_file}, mq_path={mq_path}, version={mq_version}, dbs={dbs}")

        # Validate key inputs
        if not all([fasta_folder, output_folder, conditions_file, mq_path, dbs]):
            logger.warning(f"MaxQuant job {job_name} missing required fields")
            return "Error: Missing required fields", 400

        # Validate that files/folders exist
        if not os.path.isdir(fasta_folder):
            logger.warning(f"MaxQuant job {job_name} FASTA folder does not exist: {fasta_folder}")
            return f"Error: FASTA folder does not exist: {fasta_folder}", 400

        if not os.path.isdir(output_folder):
            try:
                os.makedirs(output_folder, exist_ok=True)
                logger.info(f"Created output directory for MaxQuant job {job_name}: {output_folder}")
            except Exception as e:
                logger.error(f"Error creating output folder for MaxQuant job {job_name}: {str(e)}", exc_info=True)
                return f"Error: Could not create output folder: {str(e)}", 400

        if not os.path.isfile(conditions_file):
            logger.warning(f"MaxQuant job {job_name} conditions file does not exist: {conditions_file}")
            return f"Error: Conditions file does not exist: {conditions_file}", 400

        if not os.path.isfile(mq_path):
            logger.warning(f"MaxQuant job {job_name} executable does not exist: {mq_path}")
            return f"Error: MaxQuant executable does not exist: {mq_path}", 400

        # Create local output directory for job tracking
        local_output = os.path.join(app.config['UPLOAD_FOLDER'], job_name)
        os.makedirs(local_output, exist_ok=True)
        logger.info(f"Created job tracking directory for MaxQuant job {job_name}: {local_output}")

        # Log job info
        with open(os.path.join(local_output, "job_info.txt"), "w") as f:
            f.write(f"Job Name: {job_name}\n")
            f.write(f"FASTA Folder: {fasta_folder}\n")
            f.write(f"Output Folder: {output_folder}\n")
            f.write(f"Conditions File: {conditions_file}\n")
            f.write(f"MaxQuant Path: {mq_path}\n")
            f.write(f"MaxQuant Version: {mq_version}\n")
            f.write(f"Selected Databases: {', '.join(dbs)}\n")

        # Start MaxQuant job in a separate thread
        thread = threading.Thread(target=launch_maxquant_job,
                                  args=(mq_version, mq_path, conditions_file, dbs, output_folder, job_name))
        thread.daemon = True
        thread.start()
        logger.info(f"Started MaxQuant job {job_name} in thread")

        return f"""
        <h3>Job Submitted Successfully</h3>
        <p>Your MaxQuant job '{job_name}' has been submitted.</p>
        <p>You can track its progress in the <a href='/job-monitor'>Job Monitor</a>.</p>
        """

    except Exception as e:
        logger.error(f"Error submitting MaxQuant job: {str(e)}", exc_info=True)
        return f"Error submitting MaxQuant job: {str(e)}", 500


def launch_maxquant_job(mq_version, mq_path, conditions_file, dbs, output_folder, job_name):
    """Launch a MaxQuant job in a separate thread"""
    logger.info(f"Launching MaxQuant job '{job_name}'")
    logger.info(f"  Version: {mq_version}")
    logger.info(f"  Executable: {mq_path}")
    logger.info(f"  Conditions file: {conditions_file}")
    logger.info(f"  Databases: {dbs}")
    logger.info(f"  Output folder: {output_folder}")

    try:
        # Update status file to show job is running
        job_dir = os.path.join(app.config['UPLOAD_FOLDER'], job_name)
        with open(os.path.join(job_dir, "status.txt"), "w") as f:
            f.write("running")
        logger.info(f"MaxQuant job {job_name} status: running")

        # Here you'd create MaxQuant_handler(...).run_MaxQuant_cli()
        # For now it's just a placeholder
        import time
        time.sleep(5)  # Simulate a job running

        # Update status file to show job is complete
        with open(os.path.join(job_dir, "status.txt"), "w") as f:
            f.write("complete")
        logger.info(f"MaxQuant job {job_name} status: complete")

    except Exception as e:
        logger.error(f"Error in MaxQuant job {job_name}: {str(e)}", exc_info=True)

        # Update status file to show job failed
        with open(os.path.join(job_dir, "status.txt"), "w") as f:
            f.write(f"failed: {str(e)}")
        logger.error(f"MaxQuant job {job_name} status: failed")


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
        os.makedirs(local_output, exist_ok=True)
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


@app.route('/job-monitor')
def job_monitor():
    logger.info("Route: job-monitor")
    return render_template('job_monitor.html')


@app.route('/config')
def config():
    logger.info("Route: config")
    return render_template('config.html')


# Enhanced API endpoints for file watcher integration

@app.route('/api/watchers')
def api_get_watchers():
    """Get all watchers from the database with enhanced information"""
    try:
        logger.info("API request: get watchers")
        from src.database.watcher_db import WatcherDB

        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'watchers.db')
        db = WatcherDB(db_path)

        # Get status filter if provided
        status_param = request.args.get('status')

        # Handle multiple statuses separated by commas
        if status_param:
            statuses = status_param.split(',')
            watchers = []
            for status in statuses:
                watchers.extend(db.get_watchers(status=status.strip()))
        else:
            watchers = db.get_watchers()

        # Format the data for JSON response
        watcher_list = []
        for w in watchers:
            # Get captured files count for each watcher
            captured_files = db.get_captured_files(w[0])
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

        return jsonify({"watchers": watcher_list})
    except Exception as e:
        logger.error(f"Error getting watchers: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/watchers/<int:watcher_id>/files')
def api_get_captured_files(watcher_id):
    """Get captured files for a specific watcher"""
    try:
        logger.info(f"API request: get captured files for watcher {watcher_id}")
        from src.database.watcher_db import WatcherDB

        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'watchers.db')
        db = WatcherDB(db_path)

        files = db.get_captured_files(watcher_id)

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
        watchers = db.get_watchers()
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


@app.route('/api/watchers/<int:watcher_id>/update-status', methods=['POST'])
def api_update_watcher_status(watcher_id):
    """Update a watcher's status"""
    try:
        data = request.json
        if not data or 'status' not in data:
            return jsonify({"error": "Missing status parameter"}), 400

        new_status = data['status']
        if new_status not in ['monitoring', 'completed', 'cancelled', 'paused']:
            return jsonify({"error": f"Invalid status: {new_status}"}), 400

        logger.info(f"API request: update watcher {watcher_id} status to {new_status}")
        from src.database.watcher_db import WatcherDB

        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'watchers.db')
        db = WatcherDB(db_path)

        db.update_watcher_status(watcher_id, new_status)

        # Update completion time if status is completed or cancelled
        if new_status in ['completed', 'cancelled']:
            current_time = datetime.now().isoformat()
            db.conn.execute("UPDATE watchers SET completion_time = ? WHERE id = ?", (current_time, watcher_id))
            db.conn.commit()

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
        from src.database.watcher_db import WatcherDB

        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'watchers.db')
        db = WatcherDB(db_path)

        # Ensure folder exists
        if not os.path.isdir(data['folder_path']):
            try:
                os.makedirs(data['folder_path'], exist_ok=True)
                logger.info(f"Created watcher directory: {data['folder_path']}")
            except Exception as e:
                logger.error(f"Error creating watcher directory: {str(e)}", exc_info=True)
                return jsonify({"error": f"Could not create directory: {str(e)}"}), 400

        watcher_id = db.add_watcher(
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
            from src.core.job_queue_manager import JobQueueManager

            # Initialize job queue manager if not exists
            job_queue_manager = JobQueueManager(db_path=db_path)

            # Create and start a single watcher
            watcher_manager = WatcherManager(db, job_queue_manager)
            watcher = watcher_manager.create_single_watcher(watcher_id)

            # Start the watcher in a separate thread
            import threading
            thread = threading.Thread(target=watcher.start, daemon=True)
            thread.start()

            logger.info(f"Started new watcher {watcher_id} in thread")
        except Exception as e:
            logger.error(f"Error starting watcher {watcher_id}: {str(e)}", exc_info=True)
            # Continue anyway since the watcher is created in the database

        return jsonify({"success": True, "watcher_id": watcher_id})
    except Exception as e:
        logger.error(f"Error creating watcher: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/jobs')
def api_get_jobs():
    """Get running jobs from the queue manager"""
    try:
        logger.info("API request: get jobs")

        # This is a placeholder - in a real implementation, you would retrieve
        # jobs from your JobQueueManager or a job database
        from src.core.job_queue_manager import JobQueueManager

        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'watchers.db')
        job_queue_manager = JobQueueManager(db_path=db_path)

        # Convert job dictionary to JSON-serializable format
        jobs = []

        # Add running jobs
        for job_id, job in job_queue_manager.running_jobs.items():
            jobs.append(job.to_dict())

        # Add waiting jobs
        with job_queue_manager.waiting_jobs_lock:
            for job in job_queue_manager.waiting_jobs:
                jobs.append(job.to_dict())

        # Add queued jobs
        with job_queue_manager.queued_jobs_lock:
            for job in job_queue_manager.queued_jobs:
                jobs.append(job.to_dict())

        return jsonify({"jobs": jobs})
    except Exception as e:
        logger.error(f"Error getting jobs: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


def start_flask():
    app.run(debug=False, port=5000)


# Entry point
if __name__ == "__main__":
    try:
        logger.info("Starting Proteomics Core UI application")

        # Start Flask server
        flask_thread = threading.Thread(target=start_flask)
        flask_thread.daemon = True
        flask_thread.start()
        logger.info("Flask server started in background thread")

        # Create API instance first
        api = Api()
        logger.info("API instance created")

        # Create webview window
        window = webview.create_window(
            "Proteomics Core UI",
            "http://127.0.0.1:5000",
            min_size=(900, 650),
            js_api=api
        )
        logger.info("Webview window created")

        # Set event handlers for window
        window.events.closed += on_closed
        if HAS_SYSTRAY:
            setup_tray()
            logger.info("System tray setup complete")
            logger.info("minimized to try")
            window.events.minimized += on_minimize

        # Start the webview - this is blocking
        logger.info("Starting webview")
        webview.start()
        logger.info("Webview closed")

    except Exception as e:
        logger.critical(f"Fatal error starting application: {str(e)}", exc_info=True)



