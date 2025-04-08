import logging
import os
from datetime import datetime

# Set up logging directory relative to this file (src/utils.py)
script_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(script_dir, '..', 'logs')  # Adjust path to be at project root level
os.makedirs(log_dir, exist_ok=True)

# Create log filename with timestamp
log_filename = f"ui_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_filepath = os.path.join(log_dir, log_filename)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filepath),
        logging.StreamHandler()  # Also log to console
    ]
)

# Create logger instance
logger = logging.getLogger('proteomics_ui')