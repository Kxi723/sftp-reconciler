import os
import logging
from pathlib import Path
from datetime import date, datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DATE = date.today()
DATE_TIME = datetime.now()
CURRENT_DATE_TIME = DATE_TIME.strftime("%d%m%Y_%H%M%S")

# Base Directory
SCRIPT_DIR = Path(__file__).parent

# Local Directory Paths
CSV_DIR = SCRIPT_DIR / "Evisibility_Folder"
SFTP_DIR = SCRIPT_DIR / "ExistInSFTP"
RESULT_DIR = SCRIPT_DIR / "MissUpload"
ERROR_DIR = SCRIPT_DIR / "Error"
LOG_DIR = SCRIPT_DIR

# Environment paths
NEW_FILE = os.getenv("THE_LATEST_CSV_FILE_PATH")
OLD_FILE = os.getenv("THE_SECOND_LATEST_CSV_FILE_PATH")

# Ensure Log directory exists
LOG_DIR.mkdir(parents=True, exist_ok=True)
RESULT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# Shared Logging Setup
def setup_logging():
    log_file_path = LOG_DIR / "activity.log"
    
    logging.basicConfig(
        filename=log_file_path,
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s: %(message)s',
        filemode='a'
    )
