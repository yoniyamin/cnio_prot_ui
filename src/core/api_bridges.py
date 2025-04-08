import os
import json

import webview

from src.utils import logger
from src.core.system_try import restore_window


class Api:
    def __init__(self):
        """Initialize API with defaults directory"""
        self.defaults_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'defaults')
        os.makedirs(self.defaults_dir, exist_ok=True)
        logger.info(f"Initialized API with defaults directory: {self.defaults_dir}")

    def select_directory(self):
        """Open directory selection dialog and return the selected path"""
        from app_core import window

        logger.info("Opening directory selection dialog")
        result = window.create_file_dialog(webview.FOLDER_DIALOG)
        if result:
            logger.info(f"Selected directory: {result[0]}")
            return result[0]  # Returns the first selected directory
        logger.info("Directory selection canceled")
        return None

    def select_file(self, file_types=None):
        """Open file selection dialog with optional file type filter and return the selected path"""
        from app_core import window
        import webview

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
        from app_core import window
        import webview

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
        from app_core import window
        restore_window(window)
        return True

    # Window control methods
    def minimize_window(self):
        """Minimize the window"""
        window = self._get_current_window()
        if window:
            logger.info("Minimizing window")
            window.minimize()
            return True
        logger.error("No window available to minimize")
        return False

    def maximize_window(self):
        """Maximize the window or restore if already maximized"""
        window = self._get_current_window()
        if window:
            try:
                # In newer versions of pywebview
                if hasattr(window, 'toggle_maximize'):
                    logger.info("Toggling maximize state")
                    window.toggle_maximize()
                # Fallback to maximize (if available)
                elif hasattr(window, 'maximize'):
                    logger.info("Maximizing window")
                    window.maximize()
                # Last resort - use fullscreen
                else:
                    logger.info("Using fullscreen as fallback for maximize")
                    window.toggle_fullscreen()
                return True
            except Exception as e:
                logger.error(f"Error maximizing window: {str(e)}")
                return False
        logger.error("No window available to maximize")
        return False

    def toggle_fullscreen(self):
        """Toggle fullscreen mode"""
        window = self._get_current_window()
        if window:
            logger.info("Toggling fullscreen")
            window.toggle_fullscreen()
            return True
        logger.error("No window available to toggle fullscreen")
        return False

    def close_window(self):
        """Close the window"""
        window = self._get_current_window()
        if window:
            logger.info("Closing window")
            window.destroy()
            return True
        logger.error("No window available to close")
        return False

    def _get_current_window(self):
        """Helper method to get the current window"""
        if len(webview.windows) > 0:
            return webview.windows[0]
        return None