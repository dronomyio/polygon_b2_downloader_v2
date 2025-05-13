import argparse
import time
from datetime import datetime, timedelta

# Allow discoverer.main to be run as a script for testing, need to adjust path for shared modules
import sys
import os
# Add project root to sys.path to allow `from src.shared...` if discoverer/main.py is run directly
# This is a common pattern for making submodules runnable.
# Assuming this file is src/discoverer/main.py, project root is two levels up from its directory.
# PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# if PROJECT_ROOT not in sys.path:
# sys.path.append(PROJECT_ROOT)
# For package execution (python -m src.discoverer.main), relative imports are fine.

from src.shared.config import load_config, logger
from src.shared.db_manager import DBManager, STATUS_PENDING
from src.shared.polygon_client import PolygonClient

class Discoverer:
    def __init__(self, config):
        self.config = config
        self.polygon_client = PolygonClient(aws_access_key_id=config["POLYGON_API_KEY"])
        self.db_manager = DBManager(database_url=config["DATABASE_URL"])
        logger.info("Discoverer initialized.")

    def run(self, mode: str, start_date_str: str = None, end_date_str: str = None, specific_dates_str: str = None):
        logger.info(f"Discoverer running in mode: {mode}")
        files_to_process = []

        if mode == "historical":
            # For historical, list all available US stocks daily files
            # User wants *only* historical. This implies downloading everything available once.
            # The list_us_stocks_daily_files can take start_date and end_date.
            # If not specified, it should fetch all.
            logger.info(f"Historical mode: Discovering all US stocks daily files. Start: {start_date_str}, End: {end_date_str}")
            files_to_process = self.polygon_client.list_us_stocks_daily_files(start_date=start_date_str, end_date=end_date_str)
        
        elif mode == "daily":
            # For daily, discover yesterday's file
            yesterday = datetime.now() - timedelta(days=1)
            date_str = yesterday.strftime("%Y-%m-%d")
            year_str = yesterday.strftime("%Y")
            s3_key = f"us_stocks_sip/day_aggs_v1/{year_str}/{date_str}.csv.gz"
            logger.info(f"Daily mode: Discovering file for {date_str}: {s3_key}")
            # We don't list for daily, we construct the expected key.
            # We can optionally verify its existence with a head_object if Polygon's S3 supports it cheaply,
            # but for now, let's assume the file *should* exist and let the worker handle download failure.
            files_to_process.append(s3_key)

        elif mode == "on-demand":
            if not specific_dates_str:
                logger.error("On-demand mode requires --dates to be specified.")
                return
            dates_list = [d.strip() for d in specific_dates_str.split(",")]
            logger.info(f"On-demand mode: Discovering files for dates: {dates_list}")
            for date_str in dates_list:
                try:
                    target_date = datetime.strptime(date_str, "%Y-%m-%d")
                    year_str = target_date.strftime("%Y")
                    s3_key = f"us_stocks_sip/day_aggs_v1/{year_str}/{date_str}.csv.gz"
                    files_to_process.append(s3_key)
                except ValueError:
                    logger.error(f"Invalid date format for on-demand: {date_str}. Please use YYYY-MM-DD. Skipping.")
            logger.info(f"Constructed {len(files_to_process)} S3 keys for on-demand processing.")

        else:
            logger.error(f"Unknown discoverer mode: {mode}")
            return

        if not files_to_process:
            logger.info("No files discovered to process for the given mode/parameters.")
            return

        added_count = 0
        skipped_count = 0
        for file_key in files_to_process:
            # Check if task already exists and its status. 
            # We might only add if it doesn't exist or if it failed permanently and we want to retry (policy decision).
            # For now, add_task handles uniqueness: it won't add if file_key already exists.
            existing_task = self.db_manager.get_task_by_file_key(file_key)
            if existing_task:
                logger.info(f"File key {file_key} already exists in DB with status 	{existing_task["status"]}	. Skipping add.")
                skipped_count +=1
                continue
            
            if self.db_manager.add_task(file_key):
                added_count += 1
            else:
                # This case should ideally be caught by the check above, but as a safeguard.
                logger.warning(f"Failed to add task for {file_key} to DB (might already exist or DB error).")
                skipped_count +=1
        
        logger.info(f"Discoverer finished. Added {added_count} new tasks to the database. Skipped {skipped_count} (already existed or error). Total discovered: {len(files_to_process)}.")

def main():
    parser = argparse.ArgumentParser(description="Discoverer for Polygon.io flat files.")
    parser.add_argument("mode", choices=["historical", "daily", "on-demand"], 
                        help="Discovery mode: 'historical' for all past data (optionally within date range), 'daily' for yesterday's data, 'on-demand' for specific dates.")
    parser.add_argument("--start_date", type=str, help="Start date for historical mode (YYYY-MM-DD). Optional.")
    parser.add_argument("--end_date", type=str, help="End date for historical mode (YYYY-MM-DD). Optional.")
    parser.add_argument("--dates", type=str, help="Comma-separated list of dates for on-demand mode (YYYY-MM-DD,YYYY-MM-DD).")

    args = parser.parse_args()

    try:
        app_config = load_config() # Loads config and sets up logging from shared.config
    except (FileNotFoundError, ValueError) as e:
        # Logging might not be set up if load_config fails early
        print(f"FATAL: Configuration error: {e}", file=sys.stderr)
        sys.exit(1)

    discoverer = Discoverer(config=app_config)
    discoverer.run(mode=args.mode, 
                   start_date_str=args.start_date, 
                   end_date_str=args.end_date, 
                   specific_dates_str=args.dates)

if __name__ == "__main__":
    # This allows running: python -m src.discoverer.main historical --start_date YYYY-MM-DD
    main()

