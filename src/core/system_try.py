import os
import sys
import time
import threading
from src.utils import logger
import datetime
import subprocess
import platform

# Check for pystray availability
try:
    import pystray
    from PIL import Image

    HAS_SYSTRAY = True
except ImportError:
    HAS_SYSTRAY = False
    print("pystray package not found, system tray functionality will be disabled")

# Global references
tray_icon = None


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


def get_active_log_file():
    """Get the path to the current active log file"""
    try:
        # Get the project root directory (2 levels up from the current file in src/core)
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up to src directory
        src_dir = os.path.dirname(curr_dir)
        # Go up to project root directory
        project_root = os.path.dirname(src_dir)

        # Path to logs directory in project root
        log_dir = os.path.join(project_root, 'logs')
        logger.info(f"Looking for logs in: {log_dir}")

        # Check if log directory exists
        if not os.path.exists(log_dir):
            logger.warning(f"Log directory not found: {log_dir}")
            return None

        # Get most recent log file
        log_files = [f for f in os.listdir(log_dir) if f.startswith('ui_log_') and f.endswith('.log')]
        if not log_files:
            logger.warning("No log files found")
            return None

        # Sort by creation time (most recent first)
        log_files.sort(key=lambda x: os.path.getctime(os.path.join(log_dir, x)), reverse=True)

        # Return the full path to the most recent log file
        return os.path.join(log_dir, log_files[0])
    except Exception as e:
        logger.error(f"Error retrieving active log file: {str(e)}", exc_info=True)
        return None


def open_log_file():
    """Open the active log file in the default text editor"""
    try:
        # Get the active log file path
        active_log_path = get_active_log_file()
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


def setup_tray(window_ref):
    """Set up the system tray icon and menu"""
    global tray_icon
    logger.info("Setting up system tray icon")
    icon = create_icon()

    def do_restore():
        logger.info("Activating restore from tray")
        restore_window(window_ref)

    def do_quit():
        logger.info("Activating quit from menu")
        quit_app(window_ref)

    def do_open_log():
        logger.info("Activating open log file from menu")
        success = open_log_file()
        if success:
            logger.info("Log file opened successfully")
        else:
            logger.error("Failed to open log file")

    # Define the menu with 'Show' as the default action for double-click
    menu = pystray.Menu(
        pystray.MenuItem('Show', do_restore, default=True),  # Default action on double-click
        pystray.MenuItem('Open Logs', do_open_log),  # New menu item for opening logs
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


def restore_window(window):
    """Restore the window from the system tray"""
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


def quit_app(window=None):
    """Quit the application"""
    global tray_icon

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
    global tray_icon

    logger.info("Window closed")
    if tray_icon:
        tray_icon.stop()
    sys.exit(0)


def on_minimize(window=None):
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