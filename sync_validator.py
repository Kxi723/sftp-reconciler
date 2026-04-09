"""
This script compares expected shipment references (from CSV exports) 
against files actually uploaded to SFTP server.

Workflow:
    1. Read the latest CSV export (.txt) from 'csv_extractor.py'.
    2. Read the two latest SFTP files (.txt), compute the diff
       to obtain only newly uploaded filenames.
    3. Clean SFTP paths to extract bare shipment references.
    4. Merge new SFTP data with previously recorded pre-upload SFTP data.
    5. Merge new CSV data with file missed from last time.
    6. Compare CSV with SFTP.
    7. Export results with timestamp
"""

import logging
import os
from pathlib import Path
from datetime import datetime
from config import setup_logging, CURRENT_DATE_TIME, CSV_DIR, SFTP_DIR,\
RESULT_DIR, SURPLUS_DIR

# Initialize shared logging
setup_logging()

# =============================================================================
# Functions
# =============================================================================

class FileComparator:

    def __init__(self, csv_dir: Path = CSV_DIR, sftp_dir: Path = SFTP_DIR,
                result_dir: Path = RESULT_DIR, surplus_dir: Path = SURPLUS_DIR):

        self.csv_dir = csv_dir
        self.sftp_dir = sftp_dir
        self.result_dir = result_dir
        self.surplus_dir = surplus_dir
        self.result_list = []
        self.insequence_list = [] # Store data that havent upload at csv but in sftp
        self.latest_sftp_file = None  # Track latest SFTP file for post-processing rename


    def filter_parent_path(self, file_path: list) -> list:
        """
        Remove parent path and timestamped suffix,
        return a list of filename itself only
        """

        cleaned_data = []

        logging.debug("Cleaning data file path")
        for ship_ref in file_path:

            # Remove directory path to get file name
            filename = Path(ship_ref).name

            # Use string slicing to remove date suffix
            index = filename.find("_")

            if index != -1:
                cleaned_data.append(filename[:index])
            
            else:
                # Add file name directly if no '_' found
                cleaned_data.append(filename)

        if not cleaned_data:
            logging.warning("No data found in this file")

        return cleaned_data


    def read_latest_txt(self, dir_path: Path, sftp: bool = False) -> list:
        """
        CSV:
        Return the latest file path generated from csv_extractor.py

        SFTP:
        If only has one file, all data considered as new added.
        If the latest file has been processed, this program terminated.
        Return new SFTP data added.
        """

        logging.debug(f"Reading directory {dir_path}")
        file_type = "SFTP" if sftp else "csv"

        # Ensure the path exists & is a directory
        if not dir_path.exists() or not dir_path.is_dir():
            raise FileNotFoundError("Error accessing directory path provided")

        txt_files = list(dir_path.glob("*.txt"))
        logging.debug(f"Found {len(txt_files)} text files")

        # Nothing inside the directory
        if not txt_files:
            raise FileNotFoundError("Error finding any text file")

        # If only one file, everything is considered "new added"
        if sftp and len(txt_files) < 2:
            logging.warning("Only found 1 SFTP file, all data in past 10 days are considered as new uploaded")

        files_dict = {}
        for file in txt_files:

            # Get file modification timestamp and convert to Datetime object
            timestamp = os.path.getmtime(file)
            datestamp = datetime.fromtimestamp(timestamp)

            # Utilise setdefault() to ensure datestamp didnt overwrite
            files_dict.setdefault(file, datestamp)

        # Sort in descending order based on value(datestamp)
        files_sorted = sorted(files_dict.items(), key=lambda item: item[1], reverse=True)

        logging.info(f"Latest {file_type} file is: {files_sorted[0][0].name}")

        # For csv, return one list
        if not sftp:
            try:
                with open(files_sorted[0][0], 'r', encoding='utf-8') as data:
                    # Remove '\n' in list()
                    return data.read().splitlines()
                
            except Exception as e:
                raise SystemExit(f"Error reading {files_sorted[0][0].name} | {e}")

        # For sftp, get new data upload
        else:
            # Clear parent path & its extension
            file_name = files_sorted[0][0].stem

            # Extract to date & time
            date_n_time = file_name.split("_", 1)

            # If the file is renamed properly, its processed
            if len(date_n_time) == 2 and date_n_time[1].isdigit() and len(date_n_time[1]) == 6:
                raise SystemExit(f"Latest {file_type} has already been processed")

            try:
                with open(files_sorted[0][0], 'r', encoding='utf-8') as data:
                    new_list = self.filter_parent_path(data.read().splitlines())

            except Exception as e:
                raise SystemExit(f"Error reading {files_sorted[0][0].name} | {e}")            

            logging.info(f"Second {file_type} file is: {files_sorted[1][0].name}")

            try:
                with open(files_sorted[1][0], 'r', encoding='utf-8') as data:
                    old_list = self.filter_parent_path(data.read().splitlines())

            except Exception as e:
                raise SystemExit(f"Error reading {files_sorted[1][0].name} | {e}")

            new_data = [line for line in new_list if line not in set(old_list)]
            logging.info(f"Total {len(new_data)} new data uploaded")

            self.latest_sftp_file = files_sorted[0][0]
            return new_data


    def read_last_record(self, dir_path: Path, label: str = "") -> list:
        """
        Read and load data from the latest generated text file.

        Used to retrieve the previous result or surplus list so that
        still-relevant data can be carried forward.
        """

        logging.debug(f"Reading directory {dir_path}")
        txt_files = list(dir_path.glob("*.txt"))

        if not txt_files:
            logging.debug(f"No text file exists in this directory")
            return []

        latest = max(txt_files, key=os.path.getmtime)
        logging.debug(f"Latest {label} file is: {latest.name}")

        try:
            with open(latest, 'r', encoding='utf-8') as f:
                data = f.read().splitlines()
                logging.info(f"{len(data)} data loaded from {label} file")
                return data

        except Exception as e:
            logging.warning(f"Error reading {latest.name} | {e}")
            return []


    def display_result_in_terminal(self):
        """
        Display missing data from SFTP.
        """

        if not self.result_list:
            raise SystemExit("All data have been upload successfully")

        print("─" * 20)
        print(f"{'No':<4} {'Ship_Ref ':^16}")
        print("─" * 20)

        for index, file in enumerate(self.result_list, 1):
            print(f"{index:<4} {file:<16}")

        print("─" * 20)


    def export_result(self, output_data: list, path: Path, title: str = ""):
        """
        Export result to text file with timestamp.

        If latest output file is same as the current data, the system will
        not create a new text file. This reduce duplicate files created.
        """

        # Check latest .txt file to avoid redundant data export
        txt_files = list(path.glob("*.txt"))

        if txt_files:
            latest_txt = max(txt_files, key=os.path.getmtime)

            try:
                with open(latest_txt, 'r', encoding='utf-8') as file:
                    past_list = file.read().splitlines()

                if past_list == output_data:
                    logging.warning(f"{title} data is same as the latest file {latest_txt.name}, no file is created")
                    return

            except Exception as e:
                logging.warning(f"Error reading {latest_txt.name} | {e}")

        # Define output path
        result_file = path / f"{CURRENT_DATE_TIME}.txt"

        # Writing results
        with open(result_file, 'w', encoding='utf-8') as file:
            for ship_ref in output_data:
                file.write(f"{ship_ref}\n")

        logging.info(f"{title} data have been exported and renamed as {CURRENT_DATE_TIME}.txt")
        print(f"{title} data saved at {CURRENT_DATE_TIME}.txt")


    def mark_sftp_processed(self):
        """
        Rename the latest SFTP file with a full timestamp (HHMMSS)
        to mark it as processed, preventing repeated consumption
        of the same SFTP data.
        """

        if not self.latest_sftp_file or not self.latest_sftp_file.exists():
            return

        timestamp = f"{CURRENT_DATE_TIME}.txt"
        new_path = self.latest_sftp_file.parent / timestamp

        if new_path.exists():
            logging.warning(f"Cannot rename: {timestamp} already exists")
            return

        self.latest_sftp_file.rename(new_path)
        logging.info(f"SFTP file rename as: {timestamp}")
        print(f"SFTP file renamed: {self.latest_sftp_file.name} -> {timestamp}")


    def start(self):
        """
        1. Load the latest CSV reference list and cleaned SFTP uploads.
        2. Merge SFTP set with (pre-uploads found in SFTP).
        3. Identify files missing from SFTP, merge with still-missing
           files carried forward from previous file.
        4. Identify SFTP file not yet update in the CSV, merge with 
           previous (pre-upload) that still not updated.
        5. Display missing files in the terminal and export both lists.
        """

        # New data uploaded in SFTP
        sftp_data = self.read_latest_txt(self.sftp_dir, True)

        # Data that uploaded in SFTP but not recorded in csv
        pre_upload = self.read_last_record(self.surplus_dir, "surplus")

        # Use dictionary to remove duplicates (key is unique), then convert to list
        sftp_combined = list(dict.fromkeys(list(pre_upload) + list(sftp_data)))
        # Convert to sets for efficient O(1) lookups
        sftp_set = set(sftp_combined)

        # New data updated in csv
        csv_data = self.read_latest_txt(self.csv_dir, False)

        # Last missing data, check again
        last_missing_sftp = self.read_last_record(self.result_dir, "result")

        csv_combined = list(dict.fromkeys(list(csv_data) + list(last_missing_sftp)))
        csv_set = set(csv_combined)

        # Files haven't been upload to SFTP
        file_missing = [f for f in csv_combined if f not in sftp_set]
        self.result_list = list(dict.fromkeys(file_missing))
        logging.info(f"Total {len(self.result_list)} data haven't upload to SFTP")

        # Files hasnt recorded in csv
        wait_update = [f for f in sftp_combined if f not in csv_set]
        self.insequence_list = list(dict.fromkeys(wait_update))
        logging.info(f"Total {len(self.insequence_list)} data hasn't updated in csv")

        self.export_result(self.result_list, self.result_dir, "Results")
        self.export_result(self.insequence_list, self.surplus_dir, "Pre-uploads")

        self.display_result_in_terminal()

        # Lock: rename SFTP file to prevent reprocessing same data
        self.mark_sftp_processed()


# -------------------------------------------------
# Main Entry Point
# -------------------------------------------------

if __name__ == "__main__":

    logging.info("sync_validator.py program started")

    try:        
        comparator = FileComparator()
        comparator.start()
        
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
        logging.info("sync_validator.py program ended")