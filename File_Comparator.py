"""
This script compares two lists of filename to identify missing files.
(a list of files from Excel should be uploaded
 v.s. the actual list of files uploaded to SFTP in the past 24hrs) 

Workflow:
    1. Read .txt files containing filename lists from both 'Excel' and 'SFTP' directories.
    2. Filter and format the SFTP filenames (removing date suffixes like `_DDMMYYYY123456.pdf`).
    3. Compare the two lists to output:
       - Files missing in SFTP (Present in Excel but not in SFTP)
       - Extra files in SFTP (Present in SFTP but not in Excel)
    4. Output the comparison results:
       - Missing files saved to the 'Result' directory with date&time.
       - Extra files saved to the 'Error' directory with date&time.

Directories:
    - Excel/   : Directory for the expected filename list (.txt)
    - SFTP/    : Directory for the actual uploaded SFTP filename list (.txt)
    - Result/  : Directory for the files missing from SFTP (.txt)
    - Error/   : Directory for the extra files found in SFTP (.txt)
    - Log/     : Directory for the log (.log)
"""

import datetime
import logging
import os
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
EXCEL_DIR = SCRIPT_DIR / "ShouldUpload"
SFTP_DIR = SCRIPT_DIR / "ExistInSFTP"
RESULT_DIR = SCRIPT_DIR / "MissUpload"
ERROR_DIR = SCRIPT_DIR / "Error"
LOG_DIR = SCRIPT_DIR / "Log"

# Get current date & time
DATE_TIME = datetime.datetime.now()
CURRENT_DATE_TIME = DATE_TIME.strftime("%d%m%Y_%H%M%S")

# Logging
logging.basicConfig(
    filename = LOG_DIR / f"{CURRENT_DATE_TIME}.log",
    level = logging.DEBUG,
    format = '%(asctime)s %(levelname)s: %(message)s',
    filemode = 'w'
)

# =============================================================================
# Functions
# =============================================================================

def read_file_list(dir_path: Path):
    """
    Iterate over all files in 'Excel' & 'SFTP' directory.
    Only one .txt file is accepted in each directory. Please replace with 
    latest file, no naming required.

    Return a list contains files name either files listed in Excel or 
    files listed from SFTP by generate Linux command 
    """

    logging.debug(f"Reading directory {dir_path}")

    # Ensure the path exists & is a directory
    if not dir_path.exists() or not dir_path.is_dir():
        raise FileNotFoundError("Directory not found")

    # list() constructor, reads .txt files only
    txt_files = list(dir_path.glob("*.txt"))

    if len(txt_files) > 1:
        raise SystemExit(f"Found {len(txt_files)} .txt files. Please keep only one .txt file you want")

    if not txt_files:
        raise FileNotFoundError("No .txt file found")

    # Only one file left
    file_path = txt_files[0]

    # Return ship_ref in list() format
    try:
        with open(file_path, 'r', encoding='utf-8') as ship_ref:

            # Get file modification timestamp and convert to Datetime object
            timestamp = os.path.getmtime(file_path)
            datestamp = datetime.datetime.fromtimestamp(timestamp)

            logging.debug(f"File readed: {file_path.name} | Last modified date: {datestamp}")

            # print("Last Modified Date: ", datestamp.date())
            # print("Today Date: ", DATE_TIME.date())

            # Remove '\n' in list()
            return ship_ref.read().splitlines()

    except Exception as e:
        raise SystemExit(f"Couldn't read {file_path.name} | Error {e}")


class FileComparator:
    """
    This script comparing shipment reference with internal format, figure
    out which files is missing in another server.
    """

    def __init__(self, file_listed_in_excel: list, file_uploaded_in_sftp: list):
        self.file_listed_in_excel = file_listed_in_excel
        self.file_uploaded_in_sftp = file_uploaded_in_sftp

        # Store cleaned ship_ref data
        self.ship_ref_in_sftp = []

        # Store comparison result
        self.file_missing_in_sftp = []
        self.extra_file_in_sftp = []
        self.filtering_count = 0

        # Ensure output directory exists before starting
        self.result_path = RESULT_DIR
        self.error_path = ERROR_DIR
        self.result_path.mkdir(parents=True, exist_ok=True)
        self.error_path.mkdir(parents=True, exist_ok=True)


    def clean_directory_path(self):
        """
        Iterates through all files listed in SFTP, remove directory paths 
        (/opt/sftp/...) and unnecessary suffixes like '_DDMMYYYY123456.pdf'
        """

        logging.debug("Cleaning directory path readed")

        for ship_ref in self.file_uploaded_in_sftp:

            # Remove the directory path to get the filename
            filename = Path(ship_ref).name

            # Use string slicing to remove the date suffix
            index = filename.find("_")

            if index != -1:
                self.filtering_count += 1
                self.ship_ref_in_sftp.append(filename[:index])

            # Add file name directly if no underscore is found
            else:
                self.ship_ref_in_sftp.append(filename)

        logging.debug(f"{self.filtering_count} files path have been cleaned")

        if not self.ship_ref_in_sftp:
            raise SystemExit("No file is stored in new list")


    def matching_process(self):
        """
        Compare excel_data and sftp_data to find missing and extra data.

        Excel_data is the base, use sftp_data deduct it
        """

        logging.debug(f"{len(set(self.file_listed_in_excel))} files listed in Excel")
        logging.debug(f"{len(set(self.ship_ref_in_sftp))} files uploaded in SFTP past 24hrs")

        logging.info("Comparison started")
        
        # Convert to sets for efficient O(1) lookups
        excel_set = set(self.file_listed_in_excel)
        sftp_set = set(self.ship_ref_in_sftp)

        # Files haven't uploaded to SFTP
        missing_files = [f for f in self.file_listed_in_excel if f not in sftp_set]
        # Use dictionary to remove duplicates (key is unique), then convert back to list
        self.file_missing_in_sftp = list(dict.fromkeys(missing_files))

        # Files not listed in Excel
        extra_files = [f for f in self.ship_ref_in_sftp if f not in excel_set]
        self.extra_file_in_sftp = list(dict.fromkeys(extra_files))

        logging.info("Comparison completed")

        logging.info(f"{len(self.file_missing_in_sftp)} SFTP's files failed to match Excel's files")
        print(f"Files missing from SFTP: {len(self.file_missing_in_sftp)} files")

        logging.info(f"{len(self.extra_file_in_sftp)} SFTP's files not listed in Excel")
        print(f"Extra files found in SFTP: {len(self.extra_file_in_sftp)} files")
        

    def upload_results(self):
        """
        Write result and rename with current date time.
        """

        # Define output path for both
        file_missing_path = self.result_path / f"{CURRENT_DATE_TIME}.txt"
        file_extra_path = self.error_path / f"{CURRENT_DATE_TIME}.txt"

        # Writing results
        with open(file_missing_path, 'w', encoding='utf-8') as file:
            for fileName in self.file_missing_in_sftp:
                file.write(f"{fileName}\n")
                
        with open(file_extra_path, 'w', encoding='utf-8') as file:
            for fileName in self.extra_file_in_sftp:
                file.write(f"{fileName}\n")

        logging.info(f"Results have been uploaded and renamed: {CURRENT_DATE_TIME}.txt")
        print(f"Results named: {CURRENT_DATE_TIME}.txt")


# -------------------------------------------------
# Main Entry Point
# -------------------------------------------------

if __name__ == "__main__":

    logging.info("Program started")

    try:
        # Read data from .txt files
        excel_list = read_file_list(EXCEL_DIR)
        sftp_list = read_file_list(SFTP_DIR)
        
        # Initialize and run the comparator
        comparator = FileComparator(excel_list, sftp_list)

        comparator.clean_directory_path()
        comparator.matching_process()
        comparator.upload_results()
        
    except FileNotFoundError as e:
        logging.error(e)
        print(e)

    except SystemExit as e:
        logging.error(e)
        print(e)

    except Exception as e:
        logging.error(e)
        print(e)

    finally:
        logging.info("Program ended")