import argparse
import time
from datetime import datetime, timedelta
import sys
import os

from src.shared.config import load_config, logger
from src.shared.db_manager import DBManager, STATUS_PENDING
from src.shared.polygon_client import PolygonClient

class Discoverer:
    def __init__(self, config):
        self.config = config
        self.polygon_client = PolygonClient(
            polygon_s3_access_key_id=config["POLYGON_S3_ACCESS_KEY_ID"],
            polygon_s3_secret_access_key=config["POLYGON_S3_SECRET_ACCESS_KEY"]
        )
        self.db_manager = DBManager(database_url=config["DATABASE_URL"])
        logger.info("Discoverer initialized with dedicated Polygon S3 credentials.")

    def run(self, mode: str, start_date_str: str = None, end_date_str: str = None, specific_dates_str: str = None):
        logger.info(f"Discoverer running in mode: {mode} for US Options Day Aggregates.")
        files_to_process = []
        s3_path_prefix = "us_options_opra/day_aggs_v1"

        if mode == "historical":
            logger.info(f"Historical mode: Discovering all US options daily files from {s3_path_prefix}. Start: {start_date_str}, End: {end_date_str}")
            files_to_process = self.polygon_client.list_us_options_daily_files(start_date=start_date_str, end_date=end_date_str)
        
        elif mode == "daily":
            yesterday = datetime.now() - timedelta(days=1)
            date_str = yesterday.strftime("%Y-%m-%d")
            year_str = yesterday.strftime("%Y")
            s3_key = f"{s3_path_prefix}/{year_str}/{date_str}.csv.gz"
            logger.info(f"Daily mode: Discovering file for {date_str}: {s3_key}")
            files_to_process.append(s3_key)

        elif mode == "on-demand":
            if not specific_dates_str:
                logger.error("On-demand mode requires --dates to be specified.")
                return
            dates_list = [d.strip() for d in specific_dates_str.split(",")]
            logger.info(f"On-demand mode: Discovering files for dates: {dates_list} from {s3_path_prefix}")
            for date_str in dates_list:
                try:
                    target_date = datetime.strptime(date_str, "%Y-%m-%d")
                    year_str = target_date.strftime("%Y")
                    s3_key = f"{s3_path_prefix}/{year_str}/{date_str}.csv.gz"
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
            existing_task = self.db_manager.get_task_by_file_key(file_key)
            if existing_task:
                logger.info(f"File key {file_key} already exists in DB with status '{existing_task['status']}'. Skipping add.")
                skipped_count +=1
                continue
            
            if self.db_manager.add_task(file_key):
                added_count += 1
            else:
                logger.warning(f"Failed to add task for {file_key} to DB (might already exist or DB error).")
                skipped_count +=1
        
        logger.info(f"Discoverer finished. Added {added_count} new tasks to the database. Skipped {skipped_count} (already existed or error). Total discovered: {len(files_to_process)}.")

def main():
    parser = argparse.ArgumentParser(description="Discoverer for Polygon.io US Options Day Aggregates flat files.")
    parser.add_argument("mode", choices=["historical", "daily", "on-demand"], 
                        help="Discovery mode: 'historical' for all past data (optionally within date range), 'daily' for yesterday's data, 'on-demand' for specific dates.")
    parser.add_argument("--start_date", type=str, help="Start date for historical mode (YYYY-MM-DD). Optional.")
    parser.add_argument("--end_date", type=str, help="End date for historical mode (YYYY-MM-DD). Optional.")
    parser.add_argument("--dates", type=str, help="Comma-separated list of dates for on-demand mode (YYYY-MM-DD,YYYY-MM-DD).")

    args = parser.parse_args()

    try:
        app_config = load_config()
    except (FileNotFoundError, ValueError) as e:
        print(f"FATAL: Configuration error: {e}", file=sys.stderr)
        sys.exit(1)

    discoverer = Discoverer(config=app_config)
    discoverer.run(mode=args.mode, 
                   start_date_str=args.start_date, 
                   end_date_str=args.end_date, 
                   specific_dates_str=args.dates)

if __name__ == "__main__":
    main()

