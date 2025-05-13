import boto3
from botocore.exceptions import ClientError
import os
# import logging # Logging is now handled by the shared config

# Use logger from shared config
from .config import logger # Relative import for shared.config

class B2Client:
    """Client to interact with Backblaze B2 S3-compatible storage."""
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, bucket_name: str, endpoint_url: str, region_name: str = None):
        """
        Initializes the Backblaze B2 S3 client.

        Args:
            aws_access_key_id (str): Your Backblaze B2 Key ID.
            aws_secret_access_key (str): Your Backblaze B2 Application Key.
            bucket_name (str): The name of your B2 bucket.
            endpoint_url (str): The S3 endpoint URL for your B2 bucket region.
            region_name (str, optional): The region of your B2 bucket. 
                                         Often not strictly needed if endpoint_url is specific enough,
                                         but can be derived from endpoint_url (e.g., us-west-000).
                                         If not provided, it will try to infer or default.
        """
        if not all([aws_access_key_id, aws_secret_access_key, bucket_name, endpoint_url]):
            msg = "B2Client requires Key ID, Application Key, Bucket Name, and Endpoint URL."
            logger.error(msg)
            raise ValueError(msg)

        # Attempt to infer region from endpoint if not provided, e.g. s3.us-west-000.backblazeb2.com -> us-west-000
        if not region_name and "." in endpoint_url:
            try:
                # Example: s3.us-west-000.backblazeb2.com -> parts[1] = us-west-000
                # Example: s3.eu-central-003.backblazeb2.com -> parts[1] = eu-central-003
                # This simple split might need adjustment if B2 changes endpoint naming conventions.
                domain_parts = endpoint_url.replace("https://", "").replace("http://", "").split(".")
                if len(domain_parts) > 2 and domain_parts[0] == "s3": # e.g. ["s3", "us-west-000", "backblazeb2", "com"]
                    region_name = domain_parts[1]
                    logger.info(f"Inferred B2 region_name as 	{region_name}	 from endpoint_url 	{endpoint_url}	")
                else:
                    logger.warning(f"Could not reliably infer region from B2 endpoint_url: {endpoint_url}. Using default or letting boto3 handle.")
                    # Let boto3 try to figure it out or rely on a default if endpoint is specific enough.
                    # region_name = "us-east-1" # A common boto3 default if not specified
            except Exception as e:
                logger.warning(f"Error inferring region from B2 endpoint_url {endpoint_url}: {e}. Using default or letting boto3 handle.")
                # region_name = "us-east-1"

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url,
            region_name=region_name # B2 might ignore this if endpoint_url is specific, but good to provide if known
        )
        self.bucket_name = bucket_name
        logger.info(f"B2Client initialized for bucket 	{self.bucket_name}	 at endpoint {endpoint_url} (Region: {region_name or 'Default'})")

    def upload_file(self, local_file_path: str, s3_object_key: str) -> bool:
        """
        Uploads a local file to the Backblaze B2 bucket.

        Args:
            local_file_path (str): The path to the local file to upload.
            s3_object_key (str): The desired object key (path) in the B2 bucket.
                                 This should be the same as the Polygon S3 key to maintain structure.

        Returns:
            bool: True if upload was successful, False otherwise.
        """
        if not os.path.exists(local_file_path):
            logger.error(f"Local file not found for B2 upload: {local_file_path}")
            return False
        
        try:
            logger.info(f"Attempting to upload {local_file_path} to B2 s3://{self.bucket_name}/{s3_object_key}")
            self.s3_client.upload_file(local_file_path, self.bucket_name, s3_object_key)
            logger.info(f"Successfully uploaded {local_file_path} to B2 {self.bucket_name}/{s3_object_key}")
            return True
        except ClientError as e:
            logger.error(f"ClientError uploading file {local_file_path} to B2 bucket {self.bucket_name} as {s3_object_key}: {e}")
            return False
        except FileNotFoundError:
            # This case should ideally be caught by os.path.exists, but as a safeguard:
            logger.error(f"Local file {local_file_path} disappeared before B2 upload could start.")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading file {local_file_path} to B2: {e}")
            return False

    def file_exists(self, s3_object_key: str) -> bool:
        """
        Checks if a file (object) exists in the Backblaze B2 bucket.

        Args:
            s3_object_key (str): The object key to check in the B2 bucket.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_object_key)
            logger.debug(f"File {s3_object_key} exists in B2 bucket {self.bucket_name}.")
            return True
        except ClientError as e:
            # Error codes can vary. "404", "NoSuchKey" are common for not found.
            if e.response.get("Error", {}).get("Code") in ["404", "NoSuchKey", "NotFound"]:
                logger.debug(f"File {s3_object_key} does not exist in B2 bucket {self.bucket_name}.")
                return False
            else:
                logger.error(f"ClientError checking for file {s3_object_key} in B2 bucket {self.bucket_name}: {e}. Assuming not found due to error.")
                return False # Treat as not existing if check fails for reasons other than explicit not found
        except Exception as e:
            logger.error(f"Unexpected error checking for file {s3_object_key} in B2: {e}. Assuming not found.")
            return False

# Example usage (for testing this module directly)
if __name__ == "__main__":
    # python -m src.shared.b2_client
    print("Testing B2Client (v2)...")
    try:
        from .config import load_config, APP_CONFIG
        try:
            app_config = APP_CONFIG
        except NameError:
            app_config = load_config()

        b2_key_id = app_config.get("B2_KEY_ID")
        b2_app_key = app_config.get("B2_APPLICATION_KEY")
        b2_bucket = app_config.get("B2_BUCKET_NAME")
        b2_endpoint = app_config.get("B2_ENDPOINT_URL")

        if not all([b2_key_id, b2_app_key, b2_bucket, b2_endpoint]):
            logger.error("B2 configuration not found in .env. Please set B2_KEY_ID, B2_APPLICATION_KEY, B2_BUCKET_NAME, and B2_ENDPOINT_URL.")
        else:
            b2_client = B2Client(
                aws_access_key_id=b2_key_id,
                aws_secret_access_key=b2_app_key,
                bucket_name=b2_bucket,
                endpoint_url=b2_endpoint
            )

            # Create a dummy file for testing upload
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            dummy_file_dir_test = os.path.join(project_root, "temp_b2_uploads_test")
            if not os.path.exists(dummy_file_dir_test):
                os.makedirs(dummy_file_dir_test)
            
            dummy_file_name = "test_b2_upload.txt"
            dummy_local_path = os.path.join(dummy_file_dir_test, dummy_file_name)
            dummy_s3_key = f"test_data_v2/{dummy_file_name}" # Use a distinct path for v2 tests
            
            with open(dummy_local_path, "w") as f:
                f.write("This is a test file for B2 upload (v2).")
            logger.info(f"Created dummy file for B2 test: {dummy_local_path}")

            # Test file_exists (should be false initially if not cleaned up from previous test)
            logger.info(f"Checking if {dummy_s3_key} exists in B2 bucket {b2_bucket}...")
            exists = b2_client.file_exists(dummy_s3_key)
            logger.info(f"File exists in B2: {exists}")

            if not exists:
                # Test upload
                logger.info(f"Attempting to upload {dummy_local_path} to B2 as {dummy_s3_key}...")
                upload_success = b2_client.upload_file(dummy_local_path, dummy_s3_key)
                logger.info(f"B2 upload successful: {upload_success}")

                if upload_success:
                    # Test file_exists again (should be true)
                    logger.info(f"Checking again if {dummy_s3_key} exists in B2...")
                    exists_after_upload = b2_client.file_exists(dummy_s3_key)
                    logger.info(f"File exists in B2 after upload: {exists_after_upload}")
                    if exists_after_upload:
                        logger.info(f"B2 Test successful: {dummy_s3_key} uploaded and verified.")
                        # Note: To fully test, you might want to add a delete S3 object call here
                        # For now, manual cleanup in B2 console might be needed for test files.
                    else:
                        logger.error("B2 Test failed: File not found after supposedly successful upload.")
            else:
                logger.info(f"Skipping B2 upload test as file {dummy_s3_key} already reported as existing. Clean up B2 bucket and retry test if needed.")

            # Clean up local dummy file
            # if os.path.exists(dummy_local_path):
            #     os.remove(dummy_local_path)
            #     logger.info(f"Removed local dummy file: {dummy_local_path}")
            # if os.path.exists(dummy_file_dir_test) and not os.listdir(dummy_file_dir_test):
            #     os.rmdir(dummy_file_dir_test)

    except ImportError as ie:
        print(f"ImportError during B2Client test: {ie}. Ensure you run as a module (e.g., python -m src.shared.b2_client) and .env is in project root.")    
    except Exception as e:
        logger.error(f"An error occurred during B2Client test: {e}")
        import traceback
        traceback.print_exc()

