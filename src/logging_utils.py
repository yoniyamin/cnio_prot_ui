import logging
import os
from datetime import datetime
import sys

# Set up logging directory relative to this file (src/utils.py)
script_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(script_dir, '..', 'logs')  # Adjust path to be at project root level
os.makedirs(log_dir, exist_ok=True)

# Create log filename with timestamp
log_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f"ui_log_{log_timestamp}.log"
log_filepath = os.path.join(log_dir, log_filename)

# Set default log levels
DEFAULT_CONSOLE_LEVEL = logging.INFO
DEFAULT_FILE_LEVEL = logging.DEBUG

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)  # Capture everything at the root level

# Create file handler
file_handler = logging.FileHandler(log_filepath)
file_handler.setLevel(DEFAULT_FILE_LEVEL)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Create console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(DEFAULT_CONSOLE_LEVEL)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# Add handlers to root logger if they haven't been added yet
if not root_logger.handlers:
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

# Create main application logger instance
logger = logging.getLogger('proteomics_ui')
logger.info(f"Logging initialized at {log_filepath}")

def get_logger(name):
    """
    Get a logger with the specified name. This ensures all loggers have the same configuration.
    
    Args:
        name: The name of the logger, typically __name__ from the calling module
        
    Returns:
        A configured logger instance
    """
    component_logger = logging.getLogger(name)
    # Logger inherits the handlers from the root logger
    return component_logger

def set_log_level(level, handler_type='all'):
    """
    Set the log level for a specific handler type or all handlers.
    
    Args:
        level: A logging level (e.g., logging.DEBUG, logging.INFO)
        handler_type: 'file', 'console', or 'all'
    """
    if handler_type in ('file', 'all'):
        for handler in root_logger.handlers:
            if isinstance(handler, logging.FileHandler):
                handler.setLevel(level)
                logger.info(f"File log level set to {logging.getLevelName(level)}")
    
    if handler_type in ('console', 'all'):
        for handler in root_logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                handler.setLevel(level)
                logger.info(f"Console log level set to {logging.getLevelName(level)}")