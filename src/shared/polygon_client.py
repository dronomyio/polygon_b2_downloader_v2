import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
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
        
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=polygon_s3_access_key_id,
            aws_secret_access_key=polygon_s3_secret_access_key,
            region_name=region_name,
            endpoint_url=endpoint_url,
            config=Config(signature_version="s3v4") # Polygon might require v4 signatures
        )
        self.bucket_name = "flatfiles" # Polygon's S3 bucket name for flat files
        logger.info(f"PolygonClient initialized for bucket 	{self.bucket_name}	 at endpoint {endpoint_url} using dedicated S3 credentials.")

    def list_us_options_daily_files(self, start_date: str = None, end_date: str = None) -> list[str]:
        """
        Lists available "US options daily" files (day_aggs_v1) from Polygon.io.
        File path format: us_options_opra/day_aggs_v1/YYYY/YYYY-MM-DD.csv.gz
        """
        prefix = "us_options_opra/day_aggs_v1/"
        all_files = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        try:
            logger.info(f"Listing files from Polygon bucket 	{self.bucket_name}	 with prefix 	{prefix}	. Start: {start_date}, End: {end_date}")
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        if not key.endswith(".csv.gz") or not key.startswith(prefix):
                            continue
                        
                        try:
                            date_str = key.split("/")[-1].replace(".csv.gz", "")
                            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()

                            if start_date and file_date < datetime.strptime(start_date, "%Y-%m-%d").date():
                                continue
                            if end_date and file_date > datetime.strptime(end_date, "%Y-%m-%d").date():
                                continue
                            all_files.append(key)
                        except ValueError:
                            logger.warning(f"Could not parse date from Polygon file key: {key}. Skipping.")
                            continue
            logger.info(f"Found {len(all_files)} files in Polygon path 	{prefix}	 matching criteria.")
            all_files.sort()
            return all_files
        except ClientError as e:
            logger.error(f"ClientError listing files from Polygon.io S3 bucket 	{self.bucket_name}	 with prefix 	{prefix}	: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error listing files from Polygon.io S3 bucket 	{self.bucket_name}	: {e}")
            return []

    def download_file(self, s3_key: str, local_download_dir: str) -> str | None:
        """
        Downloads a specific file from Polygon.io S3 to a local directory.
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
                 try: os.remove(local_file_path)
                 except OSError as re: logger.error(f"Error removing partially downloaded file {local_file_path}: {re}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error downloading file {s3_key} from Polygon.io S3: {e}")
            if os.path.exists(local_file_path):
                 try: os.remove(local_file_path)
                 except OSError as re: logger.error(f"Error removing partially downloaded file {local_file_path} after unexpected error: {re}")
            return None

if __name__ == "__main__":
    print("Testing PolygonClient (v2 - with dedicated S3 keys)...")
    try:
        from .config import load_config
        app_config = load_config()

        polygon_s3_id = app_config.get("POLYGON_S3_ACCESS_KEY_ID")
        polygon_s3_secret = app_config.get("POLYGON_S3_SECRET_ACCESS_KEY")
        
        if not polygon_s3_id or not polygon_s3_secret:
            logger.error("POLYGON_S3_ACCESS_KEY_ID and POLYGON_S3_SECRET_ACCESS_KEY not found in config. Please set them up in .env for testing.")
        else:
            client = PolygonClient(polygon_s3_access_key_id=polygon_s3_id, polygon_s3_secret_access_key=polygon_s3_secret)
            
            today = datetime.now()
            start_test_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
            end_test_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
            
            logger.info(f"Listing US options daily files from {start_test_date} to {end_test_date}...")
            files = client.list_us_options_daily_files(start_date=start_test_date, end_date=end_test_date)
            
            if files:
                logger.info(f"Found {len(files)} files:")
                for f_key in files[:3]: logger.info(f"  {f_key}")
                
                file_to_download = files[0]
                logger.info(f"Attempting to download: {file_to_download}")
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                download_dir_test = os.path.join(project_root, "temp_polygon_downloads_test_s3keys")
                
                downloaded_file_path = client.download_file(file_to_download, download_dir_test)
                if downloaded_file_path and os.path.exists(downloaded_file_path):
                    logger.info(f"Successfully downloaded to: {downloaded_file_path}")
                    logger.info(f"File size: {os.path.getsize(downloaded_file_path)} bytes")
                else:
                    logger.error(f"Failed to download {file_to_download}")
            else:
                logger.info("No files found in the specified date range for US options daily data for testing.")

    except ImportError as ie:
        print(f"ImportError during PolygonClient test: {ie}. Ensure you run as a module (e.g., python -m src.shared.polygon_client) and .env is in project root.")
    except Exception as e:
        logger.error(f"An error occurred during PolygonClient test: {e}")
        import traceback
        traceback.print_exc()

