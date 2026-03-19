from pathlib import Path
import logging
import pandas as pd
from datetime import datetime
import os
from config import setup_logging, DATE_TIME, DATE, NEW_FILE, OLD_FILE, CSV_DIR

setup_logging()

# =============================================================================
# Functions
# =============================================================================

class NewShipmentFinder:
    """
    Compares two shipment CSV files to identify new shipments added.
    """

    # If no files found, use default file for presentation
    def __init__(self, first_file: str = NEW_FILE, second_file: str = OLD_FILE,
                days_lookback: int = 31, dir_path: Path = CSV_DIR):

        self.first_file = first_file
        self.second_file = second_file
        self.days_lookback = days_lookback
        self.dir_path = dir_path
        self.demo_mode = False
        self.ship_ref_col = "Ship Ref"
        self.pod_col = "POD"


    def read_and_find_files(self) -> None :
        """
        Read all CSV & Excel files in directory provided, compare two
        latest files for finding new data updated. If less than two
        files validated, the system will read designated file for demo.

        This system is NOT workable with .xlsx files.
        """
        logging.debug(f"Reading directory {self.dir_path}")

        # Ensure the path exists & is a directory
        if not self.dir_path.exists() or not self.dir_path.is_dir():
            raise FileNotFoundError("Directory not found")

        csv_lists = list(self.dir_path.glob("*.csv"))
        excel_lists = list(self.dir_path.glob("*.xlsx"))

        # Nothing inside the directory
        if not csv_lists and not excel_lists:
            raise FileNotFoundError("Didn't find any .csv or .xlsx files")
        
        # Prioritise for .csv files
        elif csv_lists: 
            logging.debug("Reading .csv files")

            files_dict = {}

            for file in csv_lists:

                try:
                    # Read only header row
                    columns = pd.read_csv(file, nrows=0).columns
                
                    # Skip invalid .csv file
                    if self.ship_ref_col not in columns or self.pod_col not in columns:
                        logging.debug(f"File not considered {file.name}")
                        continue

                except Exception as e:
                    logging.debug(f"Error reading {file.name} | {e}")
                    continue

                # Get file modification timestamp and convert to Datetime object
                timestamp = os.path.getmtime(file)
                datestamp = datetime.fromtimestamp(timestamp)

                # Utilise setdefault() to ensure datestamp didnt overwrite
                files_dict.setdefault(file, datestamp)

            if len(files_dict) < 2:
                logging.info(f"{len(files_dict)} valid .csv file is insufficient for further action")
                logging.info("The system will utilise designated file for demo")
                self.demo_mode = True

            else:
                logging.debug(f"{len(files_dict)} .csv files validated")

                # Sort in descending order based on value(datestamp)
                files_dict = sorted(files_dict.items(), key=lambda item: item[1], reverse=True)
                self.first_file = files_dict[0][0]
                self.second_file = files_dict[1][0]

                logging.info(f"Latest csv file found {files_dict[0][0].name}")
                logging.info(f"Second csv file found {files_dict[1][0].name}")

        # No Function for .xlsx files
        else:
            logging.debug("Finding two latest .xlsx files")
            raise SystemExit("This program currently doesn't support for .xlsx files")


    def csv_filter_by_date(self, file_path: str) -> pd.DataFrame:
        """
        Filters past 30days data and return DataFrame.
        If using designated files, the date is already been set.
        """

        try:
            df = pd.read_csv(
                file_path, 
                usecols=[self.ship_ref_col, self.pod_col],
                dtype={self.ship_ref_col: str}
            )

            logging.info(f"Extracting data from {Path(file_path).name}")

        except FileNotFoundError:
            raise FileNotFoundError(f"File not found {Path(file_path).name}")

        except Exception as e:
            raise SystemExit(f"Error reading {Path(file_path).name} | {e}")

        # Convert date data to datetime
        # "Coerce" return invalid date data as NaT
        df[self.pod_col] = pd.to_datetime(df[self.pod_col], errors="coerce")

        # Get date range
        today_date = pd.Timestamp(DATE) if not self.demo_mode else pd.Timestamp("2026-03-11")
        start_date = today_date - pd.Timedelta(days=self.days_lookback)

        # Apply date filter, data needed will be store as True value
        date_mask = (df[self.pod_col].dt.normalize() >= start_date) & \
                    (df[self.pod_col].dt.normalize() <= today_date)
        
        logging.debug(f"Retrieve past 30days data")
        
        # Copy True value only
        return df[date_mask].copy()


    def find_new_records(self):
        """
        Identify Ship Ref in new file that don't exist in the old file.
        """

        new_df = self.csv_filter_by_date(self.first_file)
        old_df = self.csv_filter_by_date(self.second_file)

        if new_df.empty:
            raise SystemExit("New dataframe is empty.")

        # If old file is missing or empty, everything is considered "new added"
        if old_df.empty:
            added_df = new_df.copy()
            logging.warning("All data in past 30days are new added")

        # '~' means NOT, all data not in old_df means new data
        else:
            added_mask = ~new_df[self.ship_ref_col].isin(old_df[self.ship_ref_col])
            added_df = new_df[added_mask].copy()

            if added_df.empty:
                logging.info("No new ship_ref added")

            else:
                logging.info(f"Found {len(added_df)} new ship_ref added")

        added_df.index.name = "new_index"

        # Sort by date in descending order and index
        added_df = added_df.sort_values(
            by=[self.pod_col, "new_index"], 
            ascending=[False, True]
        ).reset_index(drop=True)

        self.display_result_in_terminal(added_df)
        self.write_result_in_txt(added_df)


    def write_result_in_txt(self, new_data: pd.DataFrame):
        """
        Export the new Ship Ref to .txt file for futher comparison.

        If last created .txt file havent been compared, the system will not
        create a new .txt file. This reduce duplicate files created.
        """

        date_str = DATE_TIME.strftime("%d%m%Y")
        
        output_file_name = f"{date_str}.txt"
        output_path = self.dir_path / output_file_name

        # Ensure not creating same .txt file
        if output_path.exists():
            logging.warning(f"Output file {output_file_name} had been generated")
            print(f"File {output_file_name} had been generated. No new file created")

        else:
            # Check latest .txt file to avoid redundant data export
            txt_files = list(self.dir_path.glob("*.txt"))

            if txt_files:
                latest_txt = max(txt_files, key=os.path.getmtime)

                try:
                    # Read past data, single column with no header
                    past_df = pd.read_csv(latest_txt, header=None, dtype=str)
                    past_list = past_df[0].tolist()

                    current_list = new_data[self.ship_ref_col].astype(str).tolist()

                    if past_list == current_list:
                        logging.warning(f"Result same as latest file {latest_txt.name}, no file created")
                        print(f"Data matches the latest file {latest_txt.name}, no file created")
                        return

                except pd.errors.EmptyDataError as e:
                    logging.warning(e)

                except Exception as e:
                    logging.warning(f"Could not read {latest_txt.name} | {e}")

            # Save as text format without index and header
            new_data[self.ship_ref_col].to_csv(output_path, index=False, header=False)
            logging.info(f"Exported result to {output_path.name}")


    def display_result_in_terminal(self, new_data: pd.DataFrame):
        """
        Display new Ship Ref added from older data.
        """
        print(f"Today's date: {DATE}")

        if new_data.empty:
            raise SystemExit("No new ship ref is added.")

        # Formatting output
        print("─" * 33)
        print(f"{'No':<4} {self.ship_ref_col:^17} {self.pod_col:^10}")
        print("─" * 33)

        for index, row in new_data.iterrows():

            # Handle potential NaT values before .date()
            pod_val = row[self.pod_col]
            pod_str = pod_val.date() if pd.notnull(pod_val) else "Invalid Date"

            print(f"{index+1:<4} {row[self.ship_ref_col]:<17} {pod_str}")

        print("─" * 33)

# -------------------------------------------------
# Main Entry Point
# -------------------------------------------------

if __name__ == "__main__":
    logging.info("csv_extractor.py program started")

    try:
        finder = NewShipmentFinder()
        finder.read_and_find_files()
        finder.find_new_records()

    except FileNotFoundError as e:
        logging.error(e)

    except SystemExit as e:
        logging.error(e)

    except Exception as e:
        logging.error(e)
 
    finally:
        logging.info("csv_extractor.py program ended")
