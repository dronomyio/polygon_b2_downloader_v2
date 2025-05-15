import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError
import os
from datetime import datetime, timedelta

from .config import logger # Relative import for shared.config

class PolygonClient:
    """Client to interact with Polygon.io S3-compatible flat file storage."""
    def __init__(self, polygon_s3_access_key_id: str, polygon_s3_secret_access_key: str, region_name: str = "us-east-1", endpoint_url: str = "https://files.polygon.io"):
        """
        Initializes the Polygon S3 client using dedicated S3 credentials.

        Args:
            polygon_s3_access_key_id (str): Your Polygon.io S3 Access Key ID.
            polygon_s3_secret_access_key (str): Your Polygon.io S3 Secret Access Key.
            region_name (str, optional): The AWS region. Defaults to "us-east-1".
            endpoint_url (str, optional): The S3 endpoint for Polygon.io. Defaults to "https://files.polygon.io".
        """
        if not polygon_s3_access_key_id or not polygon_s3_secret_access_key:
            logger.error("Polygon S3 Access Key ID and Secret Access Key are required for PolygonClient.")
            raise ValueError("Polygon S3 Access Key ID and Secret Access Key are required for PolygonClient.")
        
        s3_config = Config(
            signature_version="s3v4",
            connect_timeout=15,  # seconds
            read_timeout=30      # seconds
        )

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=polygon_s3_access_key_id,
            aws_secret_access_key=polygon_s3_secret_access_key,
            region_name=region_name,
            endpoint_url=endpoint_url,
            config=s3_config 
        )
        self.bucket_name = "flatfiles"
        logger.info(f"PolygonClient initialized for bucket 	{self.bucket_name}	 at endpoint {endpoint_url} using dedicated S3 credentials and timeouts (Connect: 15s, Read: 30s).")

    def list_us_stocks_daily_files(self, start_date: str = None, end_date: str = None) -> list[str]:
        logger.debug(f"Entering list_us_stocks_daily_files. Start: {start_date}, End: {end_date}")
        prefix = "us_stocks_sip/day_aggs_v1/"
        all_files = []
        
        logger.debug(f"Creating paginator for list_objects_v2 for bucket 	{self.bucket_name}	 and prefix 	{prefix}	")
        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_count = 0
        try:
            logger.info(f"Listing files from Polygon bucket 	{self.bucket_name}	 with prefix 	{prefix}	. Start: {start_date}, End: {end_date}")
            logger.debug("Starting pagination loop...")
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                page_count += 1
                logger.debug(f"Processing page {page_count} from S3 listing.")
                if "Contents" in page:
                    logger.debug(f"Page {page_count} contains 	{len(page['Contents'])}	 objects.")
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        logger.debug(f"Processing S3 key: {key}")
                        if not key.endswith(".csv.gz") or not key.startswith(prefix):
                            logger.debug(f"Skipping key {key} as it does not match suffix/prefix criteria.")
                            continue
                        
                        try:
                            date_str = key.split("/")[-1].replace(".csv.gz", "")
                            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                            logger.debug(f"Parsed date {file_date} from key {key}")

                            if start_date and file_date < datetime.strptime(start_date, "%Y-%m-%d").date():
                                logger.debug(f"Skipping key {key} (date {file_date}) as it is before start_date {start_date}.")
                                continue
                            if end_date and file_date > datetime.strptime(end_date, "%Y-%m-%d").date():
                                logger.debug(f"Skipping key {key} (date {file_date}) as it is after end_date {end_date}.")
                                continue
                            all_files.append(key)
                            logger.debug(f"Added key {key} to list of files.")
                        except ValueError:
                            logger.warning(f"Could not parse date from Polygon file key: {key}. Skipping.")
                            continue
                else:
                    logger.debug(f"Page {page_count} does not contain 'Contents'.")
            logger.debug("Finished pagination loop.")
            logger.info(f"Found {len(all_files)} files in Polygon path 	{prefix}	 matching criteria after processing {page_count} pages.")
            all_files.sort()
            return all_files
        except ConnectTimeoutError as cte:
            logger.error(f"ConnectTimeoutError listing files from Polygon.io S3 after {page_count} pages: {cte}")
            return []
        except ReadTimeoutError as rte:
            logger.error(f"ReadTimeoutError listing files from Polygon.io S3 after {page_count} pages: {rte}")
            return []
        except ClientError as e:
            logger.error(f"ClientError listing files from Polygon.io S3 bucket 	{self.bucket_name}	 with prefix 	{prefix}	 after {page_count} pages: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error listing files from Polygon.io S3 bucket 	{self.bucket_name}	 after {page_count} pages: {e}", exc_info=True)
            return []

    def download_file(self, s3_key: str, local_download_dir: str) -> str | None:
        logger.debug(f"Entering download_file for s3_key: {s3_key}, local_download_dir: {local_download_dir}")
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
        except ConnectTimeoutError as cte:
            logger.error(f"ConnectTimeoutError downloading file {s3_key} from Polygon.io S3: {cte}")
            if os.path.exists(local_file_path):
                try: 
                    os.remove(local_file_path)
                except OSError as re:
                    logger.error(f"Error removing partially downloaded file {local_file_path} after ConnectTimeoutError: {re}")
            return None
        except ReadTimeoutError as rte:
            logger.error(f"ReadTimeoutError downloading file {s3_key} from Polygon.io S3: {rte}")
            if os.path.exists(local_file_path):
                try:
                    os.remove(local_file_path)
                except OSError as re:
                    logger.error(f"Error removing partially downloaded file {local_file_path} after ReadTimeoutError: {re}")
            return None
        except ClientError as e:
            logger.error(f"ClientError downloading file {s3_key} from Polygon.io S3: {e}")
            if os.path.exists(local_file_path):
                try:
                    os.remove(local_file_path)
                except OSError as re:
                    logger.error(f"Error removing partially downloaded file {local_file_path} after ClientError: {re}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error downloading file {s3_key} from Polygon.io S3: {e}", exc_info=True)
            if os.path.exists(local_file_path):
                try:
                    os.remove(local_file_path)
                except OSError as re:
                    logger.error(f"Error removing partially downloaded file {local_file_path} after unexpected error: {re}")
            return None

if __name__ == "__main__":
    print("Testing PolygonClient (v2 - with dedicated S3 keys, timeouts, and enhanced debug logging)...")
    try:
        from src.shared.config import load_config 
        app_config = load_config()
        logger.info(f"Test run: LOG_LEVEL is set to: {app_config.get('LOG_LEVEL')}")

        polygon_s3_id = app_config.get("POLYGON_S3_ACCESS_KEY_ID")
        polygon_s3_secret = app_config.get("POLYGON_S3_SECRET_ACCESS_KEY")
        
        if not polygon_s3_id or not polygon_s3_secret:
            logger.error("POLYGON_S3_ACCESS_KEY_ID and POLYGON_S3_SECRET_ACCESS_KEY not found in config. Please set them up in .env for testing.")
        else:
            client = PolygonClient(polygon_s3_access_key_id=polygon_s3_id, polygon_s3_secret_access_key=polygon_s3_secret)
            
            today = datetime.now()
            start_test_date = (today - timedelta(days=7)).strftime("%Y-%m-%d") 
            end_test_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
            
            logger.info(f"Listing US stocks daily files from {start_test_date} to {end_test_date} for __main__ test...")
            files = client.list_us_stocks_daily_files(start_date=start_test_date, end_date=end_test_date)
            
            if files:
                logger.info(f"Found {len(files)} files in __main__ test:")
                for f_key in files[:3]: logger.info(f"  {f_key}")
                
                if len(files) > 0:
                    file_to_download = files[0]
                    logger.info(f"Attempting to download: {file_to_download} in __main__ test")
                    current_script_path = os.path.abspath(__file__)
                    project_root_for_test = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
                    download_dir_test = os.path.join(project_root_for_test, "temp_polygon_downloads_test_s3keys_stocks_debug")
                    
                    downloaded_file_path = client.download_file(file_to_download, download_dir_test)
                    if downloaded_file_path and os.path.exists(downloaded_file_path):
                        logger.info(f"Successfully downloaded to: {downloaded_file_path} in __main__ test")
                        logger.info(f"File size: {os.path.getsize(downloaded_file_path)} bytes")
                    else:
                        logger.error(f"Failed to download {file_to_download} in __main__ test")
            else:
                logger.info(f"No files found in the specified date range for US stocks daily data for testing ({start_test_date} to {end_test_date}) in __main__ test. This might be normal or indicate an issue with access/credentials if files are expected.")

    except ImportError as ie:
        print(f"ImportError during PolygonClient test: {ie}. Ensure you run as a module (e.g., python -m src.shared.polygon_client from project root) and .env is in project root.")
    except Exception as e:
        logger.error(f"An error occurred during PolygonClient test: {e}", exc_info=True)

