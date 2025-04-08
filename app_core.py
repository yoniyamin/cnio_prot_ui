import os
import time
import webview
from flask import Flask, jsonify, request
import json
from src.utils import logger
from src.core.api_bridges import Api
from src.core.job_queue_manager import JobQueueManager
import base64
from src.core.file_utils import open_log_file

# Initialize Flask app
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'

# Ensure upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Load splash screen image
logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static/images', 'CNIOProteomicsUnitLogo.png')
with open(logo_path, 'rb') as f:
    img_data = base64.b64encode(f.read()).decode('utf-8')

splash_html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Loading...</title>
    <style>
        body {{
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            margin: 0;
            background-color: #111; /* Matches your app's dark theme */
        }}
        .logo {{
            width: 200px;
            height: auto;
            opacity: 0; /* Start invisible */
            animation: animateLogo 1.5s ease-in-out forwards;
        }}
        @keyframes animateLogo {{
            0% {{
                opacity: 0;
                transform: scale(0.5);
            }}
            100% {{
                opacity: 1;
                transform: scale(1);
            }}
        }}
    </style>
</head>
<body>
    <img id="logo" src="data:image/png;base64,{img_data}" alt="Logo" class="logo">
    <script>
        // Simply delay to ensure the animation has time to run
        setTimeout(function() {{
            // Signal that the splash screen has loaded
            window.pywebview.api.splash_loaded();
        }}, 1000);
    </script>
</body>
</html>
"""

# Initialize database path
watcher_db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'watchers.db')
job_queue_manager = JobQueueManager(db_path=watcher_db_path)

# Store window reference for API exposure
window = None
splash_window = None
flask_started = False
main_window = None
setup_handler_callback = None
main_window_setup_complete = False

# Config for app startup
app_config = {
    'maximized': True  # Default value, can be changed
}

# Variables to store window size/position before file dialog
window_size_before_dialog = None


# Function to destroy the splash window - exposed for app.py to use
def destroy_splash_window():
    global splash_window
    if splash_window:
        try:
            # Try to fully destroy the splash window
            splash_window.destroy()
            logger.info("Splash window destroyed")
        except Exception as e:
            logger.error(f"Error destroying splash window: {str(e)}")
        finally:
            # Clear the reference
            splash_window = None


# Create a single API class that will be exposed to JS
class AppAPI:
    def splash_loaded(self):
        """Called when splash screen is fully loaded"""
        global main_window, main_window_setup_complete

        # Only create the main window once
        if main_window_setup_complete:
            return True

        # Start Flask server if not already started
        if not flask_started:
            import threading
            flask_thread = threading.Thread(target=start_flask)
            flask_thread.daemon = True
            flask_thread.start()
            logger.info("Flask server started in background thread")

            # Let's give Flask a second to start up
            time.sleep(1)

        # Now create the main window
        main_window, api = create_main_window()

        # Store for global access
        global window
        window = main_window

        # Mark that we've created the main window
        main_window_setup_complete = True

        logger.info("Main window created after splash loaded")
        return True

    def show_window(self):
        """Bring the window to front"""
        global window
        if window:
            window.evaluate_js("console.log('Bringing window to front');")
            try:
                # The correct way to bring a window to front
                window.show()  # Make sure it's visible first
                if hasattr(window, 'bring_to_front'):
                    window.bring_to_front()  # Modern pywebview versions
                else:
                    # Fallback for older versions or different builds
                    # Focus the window to bring it to front
                    window.evaluate_js("window.focus();")
                return {"success": True}
            except Exception as e:
                logger.error(f"Error bringing window to front: {str(e)}")
                return {"success": False, "error": str(e)}
        return {"success": False, "error": "No window available"}


def start_flask():
    """Start the Flask server"""
    global flask_started
    try:
        app.run(debug=False, port=5000)
    finally:
        flask_started = True


def create_splash():
    """Create and show splash window only"""
    global splash_window

    api = AppAPI()

    splash_window = webview.create_window(
        'Loading...',
        html=splash_html,
        width=300,
        height=200,
        frameless=True,
        background_color='#111111',
        js_api=api
    )

    return splash_window


def create_main_window():
    """Create the main application window (hidden)"""
    api = Api()

    # Add the AppAPI methods to the API object for JS access
    app_api = AppAPI()
    for method_name in dir(app_api):
        if not method_name.startswith('_'):
            method = getattr(app_api, method_name)
            if callable(method):
                setattr(api, method_name, method)

    # Create the main window (hidden initially)
    main_window = webview.create_window(
        'Proteomics Core UI',
        'http://127.0.0.1:5000',  # Your Flask app URL
        min_size=(900, 650),
        background_color='#111111',
        frameless=False,
        hidden=True,  # Start hidden
        js_api=api,  # Your JS API, if used
        shadow=True
    )

    # Define what happens when main window is loaded
    def on_main_loaded():
        logger.info("Main window loaded, showing main window")
        main_window.show()  # Show the main window
        if app_config['maximized']:
            logger.info("Maximizing window as configured")
            main_window.maximize()

        # Call setup handler only once when the window is first loaded
        global setup_handler_callback
        if setup_handler_callback:
            setup_handler_callback(main_window.uid)

        # Only hide the splash window, not destroy it yet
        # It will be destroyed when the main window is closed
        global splash_window
        if splash_window:
            splash_window.hide()
            logger.info("Splash window hidden")

    # Attach the event handler - make sure to avoid duplicates
    if hasattr(main_window.events, 'loaded') and hasattr(main_window.events.loaded, '_functions'):
        for func in list(main_window.events.loaded._functions):
            main_window.events.loaded -= func

    main_window.events.loaded += on_main_loaded

    return main_window, api


def init_app(handler_callback=None, maximized=True):
    """Initialize the application with a properly sequenced splash and main window"""
    # Store the handler callback for later use
    global setup_handler_callback, app_config
    setup_handler_callback = handler_callback

    # Set maximized configuration
    app_config['maximized'] = maximized
    logger.info(f"Setting maximized mode: {maximized}")

    # First, create only the splash window with the API exposed
    splash = create_splash()

    # Return the splash window for now, the main_window will be created later
    return splash, None


# Save window size and position before showing file dialog
def save_window_size():
    global window, window_size_before_dialog
    if window:
        try:
            # Get current window size and position
            width, height = window.width, window.height
            x, y = window.x, window.y
            is_fullscreen = window.fullscreen
            is_maximized = window.maximized
            window_size_before_dialog = {
                'width': width,
                'height': height,
                'x': x,
                'y': y,
                'fullscreen': is_fullscreen,
                'maximized': is_maximized
            }
            logger.info(f"Saved window state: {window_size_before_dialog}")
            return True
        except Exception as e:
            logger.error(f"Error saving window size: {str(e)}")
    return False


# Restore window size and position after file dialog
def restore_window_size():
    global window, window_size_before_dialog
    if window and window_size_before_dialog:
        try:
            # First exit fullscreen if needed
            if window.fullscreen and not window_size_before_dialog['fullscreen']:
                window.toggle_fullscreen()

            # Restore maximized state if needed
            if window_size_before_dialog['maximized'] and not window.maximized:
                window.maximize()
            elif not window_size_before_dialog['maximized'] and window.maximized:
                window.restore()
                # After restoring from maximized, set the size and position
                window.resize(
                    window_size_before_dialog['width'],
                    window_size_before_dialog['height']
                )
                window.move(
                    window_size_before_dialog['x'],
                    window_size_before_dialog['y']
                )
            # Just restore position and size if not maximized
            elif not window_size_before_dialog['maximized']:
                window.resize(
                    window_size_before_dialog['width'],
                    window_size_before_dialog['height']
                )
                window.move(
                    window_size_before_dialog['x'],
                    window_size_before_dialog['y']
                )

            # Restore fullscreen state if needed (after handling maximized state)
            if window_size_before_dialog['fullscreen'] and not window.fullscreen:
                window.toggle_fullscreen()

            logger.info("Restored window state")
            return True
        except Exception as e:
            logger.error(f"Error restoring window size: {str(e)}")
    return False


# API endpoints for file dialog operations
@app.route('/api/select-directory')
def api_select_directory():
    logger.info("API request: select-directory")
    # Double-check that we have a valid window
    global window
    if window is None:
        # If the main window is not available, try to use any available window
        if len(webview.windows) > 0:
            window = webview.windows[0]
            logger.warning("Using fallback window for file dialog")

    if window:
        # Save window state before dialog
        save_window_size()

        result = window.create_file_dialog(webview.FOLDER_DIALOG)

        # Restore window state after dialog
        restore_window_size()

        if result:
            logger.info(f"Directory selected: {result[0]}")
            return jsonify({"path": result[0]})
        logger.info("Directory selection canceled")
        return jsonify({"path": None})
    else:
        logger.error("No window available for file dialog")
        return jsonify({"error": "No window available", "path": None}), 500


@app.route('/api/select-file')
def api_select_file():
    file_types = request.args.get('types', '')
    logger.info(f"API request: select-file with types: {file_types}")

    try:
        # Double-check that we have a valid window
        global window
        if window is None:
            # If the main window is not available, try to use any available window
            if len(webview.windows) > 0:
                window = webview.windows[0]
                logger.warning("Using fallback window for file dialog")

        if window is None:
            logger.error("No window available for file dialog")
            return jsonify({"error": "No window available", "path": None}), 500

        # Save window state before dialog
        save_window_size()

        result = window.create_file_dialog(webview.OPEN_DIALOG)

        # Restore window state after dialog
        restore_window_size()

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


# All other routes remain the same as before
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


@app.route('/api/open-logs')
def api_open_logs():
    """API endpoint to open the current log file"""
    try:
        logger.info("API request: open-logs")

        # Use the utility function to open the log file in the default text editor
        success = open_log_file()

        if success:
            logger.info("Log file successfully opened")
            return jsonify({
                "success": True,
                "message": "Log file opened in default text editor"
            })
        else:
            logger.error("Failed to open log file")
            return jsonify({
                "success": False,
                "error": "Failed to open log file - check application logs for details"
            }), 500
    except Exception as e:
        logger.error(f"Error in open-logs API: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

# Import routes to register them with the Flask app
# These imports must be after the app initialization to avoid circular imports
from src.core.routes import *
from src.core.api_endpoints import *