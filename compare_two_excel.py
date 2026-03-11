from pathlib import Path
import logging
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import date, datetime
from typing import Optional # can return as 'None'

# Load environment variables from .env file
load_dotenv()

# Logging
logging.basicConfig(
    # filename = LOG_DIR / f"{CURRENT_DATE_TIME}.log",
    level = logging.DEBUG,
    format = '%(asctime)s %(levelname)s: %(message)s',
    # filemode = 'w'
)


# -------------------------------------------------
# Configuration Constants
# -------------------------------------------------

NEW_FILE = os.getenv("THE_LATEST_CSV_FILE_PATH")
OLD_FILE = os.getenv("THE_SECOND_LATEST_CSV_FILE_PATH")




class NewShipmentFinder:
    """
    Compares two shipment records CSV files to identify newly added shipments
    within a specific date window.
    """

    def __init__(self, new_file: str = NEW_FILE, old_file: str = OLD_FILE,
                days_lookback: int = 31):
        self.new_file = new_file
        self.old_file = old_file
        self.days_lookback = days_lookback
        self.ship_ref_col = "Ship Ref"
        self.pod_col = "POD"


    def read_folder(self, dir_path:Path):
        if not dir_path.exists() or not dir_path.is_dir():
            print("wrong")
            return
        csv_files = list(dir_path.glob("*.csv"))
        if not csv_files:
            print("No .csv file found")
            return
        time_list= []
        correct_path = ''
        for i in csv_files:
            with open(i, 'r', encoding='utf-8') as f:
                timestamp = os.path.getmtime(i)
                datestamp = datetime.fromtimestamp(timestamp)
                time_list.append(datestamp)
                time_list.sort(reverse=True)
                if time_list[0] == datestamp:
                    correct_path = i
        dataFrame = pd.read_csv(correct_path)
        ShipRef_column = dataFrame.columns[9]
        POD_column = dataFrame.columns[29]

        ...



    # '->' is for type hinting, indicating the return type of the function
    def read_and_filter_csv(self, filepath: str) -> pd.DataFrame:
        """
        Reads CSV, filters data then return dataframe with past 30days record.
        """

        try:
            df = pd.read_csv(filepath, usecols=[self.ship_ref_col, self.pod_col])

        except FileNotFoundError:
            raise FileNotFoundError("File not found")
        
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
            return pd.DataFrame()

        # Convert date column to datetime
        df[self.pod_col] = pd.to_datetime(df[self.pod_col], errors="coerce")

        # Define date window
        today = pd.Timestamp(date.today())
        start_date = today - pd.Timedelta(days=self.days_lookback)

        # Apply date filter
        date_mask = (df[self.pod_col].dt.normalize() >= start_date) & \
                    (df[self.pod_col].dt.normalize() <= today)
        
        return df[date_mask].copy()

    def find_new_records(self) -> Optional[pd.DataFrame]:
        """Identifies records in the new file that don't exist in the old file."""
        new_df = self.read_and_filter_csv(self.new_file)
        old_df = self.read_and_filter_csv(self.old_file)

        if new_df.empty:
            print("New dataframe is empty or could not be read. Cannot proceed.")
            return None

        # Find Ship Refs in new that don't exist in old
        if old_df.empty:
            # If old file is missing or empty, everything in new is considered "added"
            added_records = new_df.copy()
        else:
            added_mask = ~new_df[self.ship_ref_col].isin(old_df[self.ship_ref_col])
            added_records = new_df[added_mask].copy()

        # Sort by Date (Descending) and Original Index (Ascending)
        added_records.index.name = "Original_Index"
        added_records = added_records.sort_values(
            by=[self.pod_col, "Original_Index"], 
            ascending=[False, True]
        ).reset_index(drop=True)
        
        added_records.index.name = "Index"

        self._print_results(added_records, date.today())
        return added_records

    def _print_results(self, added: pd.DataFrame, today: date):
        """Prints the summary and the newly added records."""
        print(f"\nToday's date : {today}")
        print(f"New matching rows (since last run): {len(added)}\n")

        if added.empty:
            print(f"No rows found for today's date in column '{self.pod_col}'.")
            return

        # Formatting output
        print(f"{'Index':<6} {self.ship_ref_col:<20} {self.pod_col}")
        print("─" * 60)
        for idx, row in added.iterrows():
            # Handle potential NaT (Not a Time) values before trying to get .date()
            pod_val = row[self.pod_col]
            pod_str = pod_val.date() if pd.notnull(pod_val) else "Invalid Date"
            print(f"{idx:<6} {row[self.ship_ref_col]:<20} {pod_str}")

if __name__ == "__main__":

    logging.info("Program started")

    try:
        finder = NewShipmentFinder()
        finder.find_new_records()

    except FileNotFoundError as e:
        logging.error(e)
        print(e)

    except SystemExit as e:
        logging.error(e)
        print(e)

    except Exception as e:
        logging.error(e)
        print(e)