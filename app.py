import threading
import webview
import sys

# Import project modules
from app_core import start_flask, init_app, app, destroy_splash_window
from src.core.system_try import setup_tray, on_closed, on_minimize, HAS_SYSTRAY
from src.utils import logger

UI_DEBUG_MODE = True

# Track if tray has already been set up
tray_setup_complete = False
# Track if handlers have been set up for a window
handlers_setup_complete = set()


# Custom on_closed handler that ensures the application exits
def custom_on_closed():
    logger.info("Main window closed - destroying splash and exiting application")

    # Explicitly destroy the splash window
    destroy_splash_window()

    # Call the original on_closed handler
    on_closed()

    # Force the application to exit
    sys.exit(0)


# Entry point
if __name__ == "__main__":
    try:
        logger.info("Starting Proteomics Core UI application")


        # Create a global event handler that will be called when the main window is ready
        def setup_main_window_handlers(window_id):
            # Check if we've already set up handlers for this window
            if window_id in handlers_setup_complete:
                return True

            logger.info(f"Setting up handlers for main window ID: {window_id}")

            # Find the window by ID
            main_window = next((w for w in webview.windows if w.uid == window_id), None)

            if main_window:
                # Set up event handlers for the main window - use our custom handler
                main_window.events.closed += custom_on_closed

                # Only set up the system tray once
                global tray_setup_complete
                if HAS_SYSTRAY and not tray_setup_complete:
                    setup_tray(main_window)
                    tray_setup_complete = True
                    # Add minimize handler only after tray is set up
                    main_window.events.minimized += on_minimize

                # Mark this window as having its handlers set up
                handlers_setup_complete.add(window_id)
                return True
            else:
                logger.error(f"Could not find main window with ID: {window_id}")
                return False


        # Initialize the application with just the splash screen first
        splash, _ = init_app(setup_main_window_handlers)
        logger.info("Splash window created")

        if UI_DEBUG_MODE:
            logger.info("Starting webview with splash screen in debug mode")
            webview.start(debug=True)
        else:
            logger.info("Starting webview with splash screen")
            webview.start()

        logger.info("Webview closed")

    except Exception as e:
        logger.critical(f"Fatal error starting application: {str(e)}", exc_info=True)
        sys.exit(1)