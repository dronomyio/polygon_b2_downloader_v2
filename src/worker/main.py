import argparse
import time
import os
import signal # For graceful shutdown
import sys

# Adjust path for direct script execution if needed (similar to discoverer/main.py)
# PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# if PROJECT_ROOT not in sys.path:
#     sys.path.append(PROJECT_ROOT)

from src.shared.config import load_config, logger
from src.shared.db_manager import DBManager, STATUS_PROCESSING, STATUS_DOWNLOADED, STATUS_FAILED_DOWNLOAD, STATUS_UPLOADED_TO_B2, STATUS_FAILED_UPLOAD, STATUS_PERMANENT_FAILURE, MAX_RETRIES
from src.shared.polygon_client import PolygonClient
from src.shared.b2_client import B2Client

# Global flag for graceful shutdown
shutdown_flag = False

def signal_handler(signum, frame):
    global shutdown_flag
    logger.info(f"Signal {signum} received, initiating graceful shutdown for worker...")
    shutdown_flag = True

class Worker:
    def __init__(self, config):
        self.config = config
        self.worker_id = config["WORKER_ID"]
        self.db_manager = DBManager(database_url=config["DATABASE_URL"])
        self.polygon_client = PolygonClient(aws_access_key_id=config["POLYGON_API_KEY"])
        self.b2_client = B2Client(
            aws_access_key_id=config["B2_KEY_ID"],
            aws_secret_access_key=config["B2_APPLICATION_KEY"],
            bucket_name=config["B2_BUCKET_NAME"],
            endpoint_url=config["B2_ENDPOINT_URL"]
        )
        # Ensure temporary download directory for this worker exists
        # It might be good to make this worker-specific if multiple workers run on the same filesystem
        # For Docker, each container has its own filesystem, so a common name is fine.
        self.local_temp_dir = os.path.join(config.get("PROJECT_ROOT", "/app"), "temp_worker_downloads") # /app is common WORKDIR
        if not os.path.exists(self.local_temp_dir):
            try:
                os.makedirs(self.local_temp_dir)
                logger.info(f"Worker {self.worker_id} created temp download dir: {self.local_temp_dir}")
            except OSError as e:
                logger.error(f"Worker {self.worker_id} failed to create temp download dir {self.local_temp_dir}: {e}")
                raise # Critical failure
        logger.info(f"Worker {self.worker_id} initialized. Temp dir: {self.local_temp_dir}")

    def _process_single_task(self, task: dict) -> None:
        task_id = task["id"]
        file_key = task["file_key"]
        retry_count = task["retry_count"] # This is the current attempt number (1-based after get_pending_task)

        logger.info(f"Worker {self.worker_id} processing task ID {task_id} (file: {file_key}, attempt: {retry_count}).")

        # 1. Download from Polygon
        logger.info(f"Worker {self.worker_id} downloading {file_key} from Polygon for task {task_id}.")
        downloaded_file_path = self.polygon_client.download_file(s3_key=file_key, local_download_dir=self.local_temp_dir)

        if not downloaded_file_path:
            logger.error(f"Worker {self.worker_id} failed to download {file_key} for task {task_id}.")
            if retry_count >= MAX_RETRIES:
                self.db_manager.update_task_status(task_id, STATUS_PERMANENT_FAILURE, error_msg=f"Download failed after {MAX_RETRIES} retries.", worker_id_to_clear=self.worker_id)
            else:
                self.db_manager.update_task_status(task_id, STATUS_FAILED_DOWNLOAD, error_msg="Download failed.", worker_id_to_clear=self.worker_id)
            return

        self.db_manager.update_task_status(task_id, STATUS_DOWNLOADED, error_msg=None) # Keep worker_id for upload step
        logger.info(f"Worker {self.worker_id} successfully downloaded {file_key} to {downloaded_file_path} for task {task_id}.")

        # 2. Upload to Backblaze B2
        logger.info(f"Worker {self.worker_id} uploading {downloaded_file_path} to B2 as {file_key} for task {task_id}.")
        upload_success = self.b2_client.upload_file(local_file_path=downloaded_file_path, s3_object_key=file_key)

        if not upload_success:
            logger.error(f"Worker {self.worker_id} failed to upload {file_key} to B2 for task {task_id}.")
            if retry_count >= MAX_RETRIES:
                self.db_manager.update_task_status(task_id, STATUS_PERMANENT_FAILURE, error_msg=f"Upload to B2 failed after {MAX_RETRIES} retries.", worker_id_to_clear=self.worker_id)
            else:
                self.db_manager.update_task_status(task_id, STATUS_FAILED_UPLOAD, error_msg="Upload to B2 failed.", worker_id_to_clear=self.worker_id)
        else:
            self.db_manager.update_task_status(task_id, STATUS_UPLOADED_TO_B2, error_msg=None, worker_id_to_clear=self.worker_id)
            logger.info(f"Worker {self.worker_id} successfully uploaded {file_key} to B2 for task {task_id}.")

        # 3. Cleanup local file
        if os.path.exists(downloaded_file_path):
            try:
                os.remove(downloaded_file_path)
                logger.info(f"Worker {self.worker_id} cleaned up local file: {downloaded_file_path}")
            except OSError as e:
                logger.error(f"Worker {self.worker_id} failed to clean up local file {downloaded_file_path}: {e}")

    def run_once(self) -> bool:
        """Processes one task if available. Returns True if a task was processed or attempted, False if no task found."""
        task = self.db_manager.get_pending_task(worker_id=self.worker_id)
        if task:
            try:
                self._process_single_task(task)
            except Exception as e:
                logger.error(f"Worker {self.worker_id} encountered an unhandled exception while processing task ID {task.get("id", "N/A")}: {e}", exc_info=True)
                # Release the task so it can be retried if the error was transient or due to worker crash
                # Ensure retry_count was incremented by get_pending_task
                current_retry_count = task.get("retry_count", 0)
                if current_retry_count >= MAX_RETRIES:
                    self.db_manager.update_task_status(task["id"], STATUS_PERMANENT_FAILURE, error_msg=f"Unhandled exception: {e}", worker_id_to_clear=self.worker_id)
                else:
                    # Release with a generic failed status, or a specific one if identifiable
                    self.db_manager.release_task(task["id"], new_status=STATUS_FAILED_DOWNLOAD, error_msg=f"Unhandled exception: {e}")
            return True
        else:
            # logger.debug(f"Worker {self.worker_id} found no pending tasks.")
            return False

    def loop(self, poll_interval_seconds: int = 10):
        """Continuously polls for and processes tasks until shutdown is signaled."""
        logger.info(f"Worker {self.worker_id} starting main loop. Polling interval: {poll_interval_seconds}s.")
        while not shutdown_flag:
            processed_task = self.run_once()
            if not processed_task:
                # No task found, wait before polling again
                # logger.debug(f"Worker {self.worker_id} sleeping for {poll_interval_seconds}s as no task was found.")
                for _ in range(poll_interval_seconds):
                    if shutdown_flag: break
                    time.sleep(1)
            else:
                # Task was processed, could potentially check for new task immediately or with shorter delay
                # For simplicity, we use the same poll_interval logic or just continue the loop
                pass 
        logger.info(f"Worker {self.worker_id} has shut down.")

def main():
    parser = argparse.ArgumentParser(description="Worker for Polygon.io to B2 data transfer.")
    parser.add_argument("--run_once", action="store_true", help="Run one task processing cycle and exit.")
    parser.add_argument("--poll_interval", type=int, default=10, help="Polling interval in seconds for continuous mode.")
    args = parser.parse_args()

    # Setup signal handling for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        app_config = load_config()
        # Add project root to config for worker to use if needed (e.g. for local_temp_dir)
        app_config["PROJECT_ROOT"] = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    except (FileNotFoundError, ValueError) as e:
        print(f"FATAL: Configuration error: {e}", file=sys.stderr)
        sys.exit(1)

    worker = Worker(config=app_config)

    if args.run_once:
        logger.info(f"Worker {worker.worker_id} starting in run_once mode.")
        worker.run_once()
        logger.info(f"Worker {worker.worker_id} run_once mode complete.")
    else:
        worker.loop(poll_interval_seconds=args.poll_interval)

if __name__ == "__main__":
    # python -m src.worker.main
    main()

