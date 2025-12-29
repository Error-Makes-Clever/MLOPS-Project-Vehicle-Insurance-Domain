import logging
import os
from logging.handlers import RotatingFileHandler
from from_root import from_root
from datetime import datetime

# -------------------------------------------------------------------
# LOGGING CONFIGURATION CONSTANTS
# -------------------------------------------------------------------

# Directory where log files will be stored
LOG_DIR = 'logs'

# Create a timestamped log file name (helps in log versioning)
LOG_FILE = f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"

# Maximum size of a single log file (5 MB)
MAX_LOG_SIZE = 5 * 1024 * 1024  

# Number of backup log files to retain
BACKUP_COUNT = 3  

# -------------------------------------------------------------------
# CREATE LOG DIRECTORY PATH
# -------------------------------------------------------------------

# Get project root path using from_root()
log_dir_path = os.path.join(from_root(), LOG_DIR)

# Create logs directory if it does not exist
os.makedirs(log_dir_path, exist_ok=True)

# Full path of the log file
log_file_path = os.path.join(log_dir_path, LOG_FILE)

# -------------------------------------------------------------------
# LOGGER CONFIGURATION FUNCTION
# -------------------------------------------------------------------

def configure_logger():
    """
    Configures application-wide logging with:
    - Rotating file handler (persistent logs)
    - Console handler (real-time logs)
    """

    # Create root logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Log message format
    formatter = logging.Formatter(
        "[ %(asctime)s ] %(name)s - %(levelname)s - %(message)s"
    )

    # -----------------------------
    # File Handler (Persistent logs)
    # -----------------------------
    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=MAX_LOG_SIZE,
        backupCount=BACKUP_COUNT
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    # -----------------------------
    # Console Handler (Terminal output)
    # -----------------------------
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # Attach handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


# Initialize logger configuration
configure_logger()