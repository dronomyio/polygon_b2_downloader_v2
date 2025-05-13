import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import os
# import logging # Logging is now handled by the shared config
from datetime import datetime, timedelta

# Use logger from shared config
from .config import logger # Relative import for shared.config

class PolygonClient:
    """Client to interact with Polygon.io S3-compatible flat file storage."""
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str = "manus_ai_v2", region_name: str = "us-east-1", endpoint_url: str = "https://files.polygon.io"):
        """
        Initializes the Polygon S3 client.
        Polygon.io uses the API key as the AWS_ACCESS_KEY_ID.
        The AWS_SECRET_ACCESS_KEY can be any non-empty string for Polygon's S3 access.
        """
        if not aws_access_key_id:
            logger.error("Polygon API Key (AWS_ACCESS_KEY_ID) is required for PolygonClient.")
            raise ValueError("Polygon API Key (AWS_ACCESS_KEY_ID) is required for PolygonClient.")
        
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key, # Can be any non-empty string
            region_name=region_name,
            endpoint_url=endpoint_url,
            config=Config(signature_version="s3v4")
        )
        self.bucket_name = "flatfiles" # Polygon's S3 bucket name for flat files
        logger.info(f"PolygonClient initialized for bucket '{self.bucket_name}' at endpoint {endpoint_url}")

    def list_us_stocks_daily_files(self, start_date: str = None, end_date: str = None) -> list[str]:
        """
        Lists available "US stocks daily" files (day_aggs_v1) from Polygon.io.
        File path format: us_stocks_sip/day_aggs_v1/YYYY/YYYY-MM-DD.csv.gz

        Args:
            start_date (str, optional): YYYY-MM-DD format. Filters files from this date onwards.
            end_date (str, optional): YYYY-MM-DD format. Filters files up to this date.

        Returns:
            list[str]: A list of S3 object keys for the daily stock aggregate files.
        """
        prefix = "us_stocks_sip/day_aggs_v1/"
        all_files = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        try:
            logger.info(f"Listing files from Polygon bucket '{self.bucket_name}' with prefix '{prefix}'. Start: {start_date}, End: {end_date}")
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        if not key.endswith(".csv.gz") or not key.startswith(prefix):
                            continue # Skip non-matching files early
                        
                        try:
                            # Extract date from key: us_stocks_sip/day_aggs_v1/YYYY/YYYY-MM-DD.csv.gz
                            date_str = key.split("/")[-1].replace(".csv.gz", "")
                            file_date = datetime.strptime(date_str, "%Y-%m-%d").date() # Use .date() for comparison

                            # Apply date filters
                            if start_date:
                                if file_date < datetime.strptime(start_date, "%Y-%m-%d").date():
                                    continue
                            if end_date:
                                if file_date > datetime.strptime(end_date, "%Y-%m-%d").date():
                                    continue
                            all_files.append(key)
                        except ValueError:
                            logger.warning(f"Could not parse date from Polygon file key: {key}. Skipping.")
                            continue
            logger.info(f"Found {len(all_files)} files in Polygon path '{prefix}' matching criteria.")
            all_files.sort() # Sort chronologically by name
            return all_files
        except ClientError as e:
            logger.error(f"ClientError listing files from Polygon.io S3 bucket '{self.bucket_name}' with prefix '{prefix}': {e}")
            return [] # Return empty list on error
        except Exception as e:
            logger.error(f"Unexpected error listing files from Polygon.io S3 bucket '{self.bucket_name}': {e}")
            return []

    def download_file(self, s3_key: str, local_download_dir: str) -> str | None:
        """
        Downloads a specific file from Polygon.io S3 to a local directory.

        Args:
            s3_key (str): The S3 object key of the file to download.
            local_download_dir (str): The local directory to save the downloaded file.

        Returns:
            str | None: The full path to the downloaded file, or None if download failed.
        """
        if not os.path.exists(local_download_dir):
            try:
                os.makedirs(local_download_dir)
                logger.info(f"Created local download directory: {local_download_dir}")
            except OSError as e:
                logger.error(f"Error creating download directory {local_download_dir}: {e}")
                return None

        file_name = os.path.basename(s3_key)
        local_file_path = os.path.join(local_download_dir, file_name)

        try:
            logger.info(f"Attempting to download s3://{self.bucket_name}/{s3_key} to {local_file_path}")
            self.s3_client.download_file(self.bucket_name, s3_key, local_file_path)
            logger.info(f"Successfully downloaded {s3_key} to {local_file_path}")
            return local_file_path
        except ClientError as e:
            logger.error(f"ClientError downloading file {s3_key} from Polygon.io S3: {e}")
            if os.path.exists(local_file_path):
                 try:
                     os.remove(local_file_path) # Clean up partially downloaded file
                 except OSError as remove_e:
                     logger.error(f"Error removing partially downloaded file {local_file_path}: {remove_e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error downloading file {s3_key} from Polygon.io S3: {e}")
            if os.path.exists(local_file_path):
                 try:
                     os.remove(local_file_path)
                 except OSError as remove_e:
                     logger.error(f"Error removing partially downloaded file {local_file_path} after unexpected error: {remove_e}")
            return None

# Example usage (for testing this module directly)
if __name__ == "__main__":
    # This test requires POLYGON_API_KEY to be set in the environment or a .env file
    # and the config.py to be in the same directory or accessible in PYTHONPATH
    # For this new structure, config.py is in src.shared so direct run needs careful path setup or running as module.
    # python -m src.shared.polygon_client
    print("Testing PolygonClient (v2)...")
    try:
        # Attempt to load config to setup logging and get API key
        from .config import load_config, APP_CONFIG # If APP_CONFIG is set globally by load_config
        
        # If config.py doesn't set a global APP_CONFIG, load it explicitly
        try:
            app_config = APP_CONFIG
        except NameError:
            app_config = load_config()

        polygon_api_key = app_config.get("POLYGON_API_KEY")
        
        if not polygon_api_key:
            logger.error("POLYGON_API_KEY not found in config. Please set it up in .env")
        else:
            client = PolygonClient(aws_access_key_id=polygon_api_key)
            
            # Test listing files for a small recent range
            today = datetime.now()
            # Look back a few days to increase chance of finding files for testing
            # Polygon data might have a delay.
            start_test_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
            end_test_date = (today - timedelta(days=1)).strftime("%Y-%m-%d") # up to yesterday
            
            logger.info(f"Listing US stocks daily files from {start_test_date} to {end_test_date}...")
            files = client.list_us_stocks_daily_files(start_date=start_test_date, end_date=end_test_date)
            
            if files:
                logger.info(f"Found {len(files)} files:")
                for f_key in files[:3]: # Print first 3
                    logger.info(f"  {f_key}")
                
                # Test downloading the first file found
                file_to_download = files[0]
                logger.info(f"Attempting to download: {file_to_download}")
                # Create a temporary directory for this test relative to this script's project root
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                download_dir_test = os.path.join(project_root, "temp_polygon_downloads_test")
                
                downloaded_file_path = client.download_file(file_to_download, download_dir_test)
                if downloaded_file_path and os.path.exists(downloaded_file_path):
                    logger.info(f"Successfully downloaded to: {downloaded_file_path}")
                    logger.info(f"File size: {os.path.getsize(downloaded_file_path)} bytes")
                    # Clean up test file and dir
                    # os.remove(downloaded_file_path)
                    # if not os.listdir(download_dir_test):
                    #     os.rmdir(download_dir_test)
                else:
                    logger.error(f"Failed to download {file_to_download}")
            else:
                logger.info("No files found in the specified date range for US stocks daily data for testing.")

    except ImportError as ie:
        print(f"ImportError during PolygonClient test: {ie}. Ensure you run as a module if needed (e.g., python -m src.shared.polygon_client) and .env is in project root.")
    except Exception as e:
        logger.error(f"An error occurred during PolygonClient test: {e}")
        import traceback
        traceback.print_exc()

