from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, DateTime, UniqueConstraint, Index, text
from sqlalchemy.sql import func # For server_default=func.now()
from sqlalchemy.exc import IntegrityError, OperationalError
import datetime
import os
import logging
import time

# Assuming config.py is in the same directory or accessible via PYTHONPATH
# For relative imports within the package structure (src.shared.config)
from .config import load_config, logger # Use . for relative import

# Define status constants
STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_DOWNLOADED = "downloaded"
STATUS_UPLOADED_TO_B2 = "completed" # Final success state
STATUS_FAILED_DOWNLOAD = "failed_download"
STATUS_FAILED_UPLOAD = "failed_upload"
STATUS_PERMANENT_FAILURE = "permanent_failure" # After max retries

MAX_RETRIES = 2 # As per user requirement

class DBManager:
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url) #, echo=True) # echo for debugging SQL
        self.metadata = MetaData()
        self.files_table = Table(
            "files_to_process", self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("file_key", String, nullable=False, unique=True), # S3 object key
            Column("status", String, nullable=False, default=STATUS_PENDING, index=True),
            Column("worker_id", String, nullable=True, index=True), # ID of the worker processing this file
            Column("discovered_at", DateTime, nullable=False, server_default=func.now()),
            Column("last_attempted_at", DateTime, nullable=True, onupdate=func.now()),
            Column("completed_at", DateTime, nullable=True),
            Column("retry_count", Integer, nullable=False, default=0),
            Column("error_message", String, nullable=True),
            Index("idx_status_retry_count", "status", "retry_count") # For querying pending tasks with retries
        )
        self._create_tables_if_not_exist()

    def _create_tables_if_not_exist(self):
        try:
            self.metadata.create_all(self.engine)
            logger.info("Database tables checked/created successfully.")
        except OperationalError as e:
            logger.error(f"Operational error during table creation (database might not be accessible or path invalid): {e}", exc_info=True)
            raise

    def add_task(self, file_key: str) -> bool:
        """Adds a new file task to the database with 'pending' status if it doesn't already exist."""
        stmt = self.files_table.insert().values(
            file_key=file_key,
            status=STATUS_PENDING,
            retry_count=0
        )
        try:
            with self.engine.connect() as connection:
                with connection.begin() as trans:
                    connection.execute(stmt)
                    # trans.commit() # Context manager handles commit on successful exit
            logger.info(f"Task added successfully for file_key: {file_key}")
            return True
        except IntegrityError: # Handles unique constraint violation for file_key
            logger.warning(f"Task for file_key: {file_key} already exists or another unique constraint failed.")
            return False
        except Exception as e:
            logger.error(f"Error adding task for file_key {file_key}: {e}", exc_info=True)
            return False

    def get_pending_task(self, worker_id: str) -> dict | None:
        """
        Atomically fetches a pending task and marks it as 'processing' by the given worker_id.
        Prioritizes tasks that are PENDING or FAILED (download/upload) within retry limits.
        Returns the task (dict) or None if no suitable task is found.
        """
        with self.engine.connect() as connection:
            for attempt_num in range(5): # Try a few times to find and claim a task
                try:
                    with connection.begin() as trans: # Start transaction for both SELECT and UPDATE
                        # 1. Find a candidate task (read-only within this transaction)
                        stmt_select_candidate = (
                            self.files_table.select()
                            .where(
                                (self.files_table.c.status == STATUS_PENDING) |
                                (
                                    (self.files_table.c.status.in_([STATUS_FAILED_DOWNLOAD, STATUS_FAILED_UPLOAD])) &
                                    (self.files_table.c.retry_count < MAX_RETRIES)
                                )
                            )
                            .where(self.files_table.c.worker_id.is_(None))
                            .order_by(self.files_table.c.retry_count, self.files_table.c.discovered_at)
                            .limit(1)
                            .with_for_update(skip_locked=True) # Attempt to lock the row for update if DB supports
                        )
                        
                        candidate_row_proxy = connection.execute(stmt_select_candidate).first()

                        if not candidate_row_proxy:
                            if attempt_num == 0: # Only log if no tasks found on first try
                                logger.debug(f"No suitable pending tasks found for worker {worker_id} on attempt {attempt_num + 1}.")
                            # trans.commit() # No task found, commit the (empty) transaction
                            return None # No tasks available

                        task_id = candidate_row_proxy.id
                        current_retry_count = candidate_row_proxy.retry_count
                        file_key_for_logging = candidate_row_proxy.file_key

                        # 2. Attempt to claim the task (UPDATE)
                        stmt_claim = (
                            self.files_table.update()
                            .where(self.files_table.c.id == task_id)
                            # .where(self.files_table.c.worker_id.is_(None)) # Already handled by with_for_update or implicitly by select
                            .values(
                                status=STATUS_PROCESSING,
                                worker_id=worker_id,
                                last_attempted_at=func.now(),
                                retry_count=current_retry_count + 1 
                            )
                        )
                        result = connection.execute(stmt_claim)

                        if result.rowcount == 1:
                            # Successfully claimed, now fetch the full task details (still within same transaction)
                            claimed_task_stmt = self.files_table.select().where(self.files_table.c.id == task_id)
                            claimed_task_data = connection.execute(claimed_task_stmt).mappings().first()
                            # trans.commit() # Context manager handles commit on successful exit of 'with connection.begin()' block
                            logger.info(f"Worker {worker_id} claimed task ID {task_id} (file: {claimed_task_data['file_key'] if claimed_task_data else 'N/A'}).")
                            return dict(claimed_task_data) if claimed_task_data else None
                        else:
                            # This case should be less likely if with_for_update works as expected or if the select and update are tight.
                            # If rowcount is 0, it means the task was modified/claimed by another worker between our select and update.
                            logger.debug(f"Worker {worker_id} failed to claim task ID {task_id} (file: {file_key_for_logging}), likely claimed by another. Retrying fetch (attempt {attempt_num + 1}).")
                            # trans.rollback() # Context manager handles rollback on exception or if we explicitly raise one
                            # No explicit rollback needed here, the loop will continue or exit
                
                except OperationalError as oe: # Catch specific DB operational errors like lock timeouts if applicable
                    logger.warning(f"Database operational error during task claim for worker {worker_id}: {oe}. Retrying fetch (attempt {attempt_num + 1}).", exc_info=True)
                    # 'with connection.begin()' handles rollback on exception.
                except Exception as e:
                    logger.error(f"Unexpected error during task claim for worker {worker_id}: {e}", exc_info=True)
                    # 'with connection.begin()' handles rollback on exception.
                    return None # Stop trying on unexpected errors

                time.sleep(0.1 * (attempt_num + 1)) # Small, increasing delay before retrying the outer loop

            logger.info(f"Worker {worker_id} could not claim a task after multiple attempts.")
            return None

    def update_task_status(self, task_id: int, new_status: str, error_msg: str | None = None, worker_id_to_clear: str | None = None):
        """Updates the status of a task. Optionally clears worker_id if task is completed or failed permanently."""
        values_to_update = {"status": new_status}
        if error_msg is not None: # Ensure empty string error messages are also set
            values_to_update["error_message"] = error_msg
        
        if new_status == STATUS_UPLOADED_TO_B2:
            values_to_update["completed_at"] = func.now()
            values_to_update["worker_id"] = None # Clear worker_id on completion
        elif new_status == STATUS_PERMANENT_FAILURE: # Explicitly clear worker_id on permanent failure
             values_to_update["worker_id"] = None
        elif worker_id_to_clear and (new_status.startswith("failed_")):
            # Clear worker_id if specified for a failed attempt that is not yet permanent, allowing retry by others
            values_to_update["worker_id"] = None

        stmt = self.files_table.update().where(self.files_table.c.id == task_id).values(**values_to_update)
        try:
            with self.engine.connect() as connection:
                with connection.begin() as trans:
                    connection.execute(stmt)
                    # trans.commit() # Context manager handles commit
            logger.info(f"Task ID {task_id} status updated to {new_status}.")
            return True
        except Exception as e:
            logger.error(f"Error updating status for task ID {task_id}: {e}", exc_info=True)
            return False

    def release_task(self, task_id: int, new_status: str = STATUS_PENDING, error_msg: str | None = None):
        """Releases a task by setting its worker_id to None and updating status (e.g., back to pending or a failed state)."""
        values_to_update = {"status": new_status, "worker_id": None}
        if error_msg is not None:
            values_to_update["error_message"] = error_msg
        
        stmt = self.files_table.update().where(self.files_table.c.id == task_id).values(**values_to_update)
        try:
            with self.engine.connect() as connection:
                with connection.begin() as trans:
                    connection.execute(stmt)
                    # trans.commit() # Context manager handles commit
            logger.info(f"Task ID {task_id} released. Status set to {new_status}.")
            return True
        except Exception as e:
            logger.error(f"Error releasing task ID {task_id}: {e}", exc_info=True)
            return False

    def get_task_by_file_key(self, file_key: str) -> dict | None:
        """Retrieves a task by its file_key."""
        stmt = self.files_table.select().where(self.files_table.c.file_key == file_key)
        with self.engine.connect() as connection:
            # For a simple read, an explicit transaction is not strictly necessary
            # unless you need a specific isolation level or consistency guarantee
            # across multiple reads, which is not the case here.
            result_proxy = connection.execute(stmt)
            mapping = result_proxy.mappings().first()
        return dict(mapping) if mapping else None

# Example Usage (for testing this module directly)
if __name__ == "__main__":
    print("Testing DBManager...")
    try:
        app_config = load_config() # Loads config and sets up logging
        # Override for local testing if DATABASE_URL is not set for in-memory or specific file
        db_url_for_test = app_config.get("DATABASE_URL", "sqlite:///./test_db_manager.sqlite")
        if "test_db_manager.sqlite" in db_url_for_test and os.path.exists("./test_db_manager.sqlite"):
             os.remove("./test_db_manager.sqlite") # Clean start for test
        
        db_manager = DBManager(database_url=db_url_for_test)
        logger.info(f"DBManager initialized for testing with DB: {db_url_for_test}.")

        logger.info("Testing add_task...")
        db_manager.add_task("test_file_1.csv.gz")
        db_manager.add_task("test_file_2.csv.gz")
        db_manager.add_task("test_file_1.csv.gz") # Test duplicate

        logger.info("Testing get_task_by_file_key...")
        task = db_manager.get_task_by_file_key("test_file_1.csv.gz")
        assert task is not None, "Failed to retrieve task test_file_1.csv.gz"
        logger.info(f"Retrieved task: {task}")

        logger.info("Testing get_pending_task for worker1...")
        worker1_task = db_manager.get_pending_task(worker_id="worker1")
        assert worker1_task is not None, "Worker1 found no pending tasks when one was expected."
        logger.info(f"Worker1 got task: {worker1_task}")
        task_id_w1 = worker1_task["id"]
        assert worker1_task["status"] == STATUS_PROCESSING
        assert worker1_task["worker_id"] == "worker1"
        assert worker1_task["retry_count"] == 1

        logger.info(f"Worker1 updating task {task_id_w1} to downloaded...")
        db_manager.update_task_status(task_id=task_id_w1, new_status=STATUS_DOWNLOADED)
        updated_task_w1 = db_manager.get_task_by_file_key(worker1_task["file_key"])
        assert updated_task_w1["status"] == STATUS_DOWNLOADED
        logger.info(f"Task {task_id_w1} after download update: {updated_task_w1}")

        logger.info(f"Worker1 updating task {task_id_w1} to completed...")
        db_manager.update_task_status(task_id=task_id_w1, new_status=STATUS_UPLOADED_TO_B2, worker_id_to_clear="worker1")
        updated_task_w1_completed = db_manager.get_task_by_file_key(worker1_task["file_key"])
        assert updated_task_w1_completed["status"] == STATUS_UPLOADED_TO_B2
        assert updated_task_w1_completed["worker_id"] is None
        logger.info(f"Task {task_id_w1} after completed update: {updated_task_w1_completed}")

        logger.info("Testing get_pending_task for worker2 (should get test_file_2)...")
        worker2_task = db_manager.get_pending_task(worker_id="worker2")
        assert worker2_task is not None, "Worker2 found no pending tasks when one was expected."
        assert worker2_task["file_key"] == "test_file_2.csv.gz"
        logger.info(f"Worker2 got task: {worker2_task}")
        task_id_w2 = worker2_task["id"]
        
        logger.info(f"Worker2 simulating failure for task {task_id_w2} and releasing...")
        db_manager.release_task(task_id=task_id_w2, new_status=STATUS_FAILED_DOWNLOAD, error_msg="Simulated download error by worker2")
        failed_task_w2 = db_manager.get_task_by_file_key(worker2_task["file_key"])
        assert failed_task_w2["status"] == STATUS_FAILED_DOWNLOAD
        assert failed_task_w2["worker_id"] is None
        logger.info(f"Task {task_id_w2} after release by worker2: {failed_task_w2}")
        
        logger.info(f"Worker3 attempting to get the failed task {task_id_w2} for retry...")
        worker3_task = db_manager.get_pending_task(worker_id="worker3")
        assert worker3_task is not None and worker3_task["id"] == task_id_w2, "Worker3 failed to get task for retry"
        assert worker3_task["retry_count"] == failed_task_w2["retry_count"] + 1 # Retry count should increment
        logger.info(f"Worker3 successfully got task {task_id_w2} for retry. Retry count: {worker3_task['retry_count']}") # Corrected line
        
        # Simulate MAX_RETRIES leading to permanent failure
        logger.info(f"Simulating MAX_RETRIES for task {task_id_w2}...")
        db_manager.update_task_status(task_id_w2, STATUS_FAILED_DOWNLOAD, worker_id_to_clear="worker3") # Release it
        for i in range(MAX_RETRIES - worker3_task['retry_count'] + 1): # Corrected line
            next_worker_id = f"retry_worker_{i}"
            retry_task = db_manager.get_pending_task(worker_id=next_worker_id)
            if retry_task and retry_task["id"] == task_id_w2:
                # Corrected line 273
                logger.info(f"{next_worker_id} got task {task_id_w2} for retry {retry_task['retry_count']}")
                if retry_task['retry_count'] > MAX_RETRIES: # Worker would check this # Corrected line
                    db_manager.update_task_status(task_id_w2, STATUS_PERMANENT_FAILURE, error_msg="Max retries exceeded", worker_id_to_clear=next_worker_id)
                    logger.info(f"Task {task_id_w2} marked as PERMANENT_FAILURE.")
                    break
                else:
                    db_manager.update_task_status(task_id_w2, STATUS_FAILED_DOWNLOAD, worker_id_to_clear=next_worker_id) # Simulate another failure
            else:
                logger.error("Failed to re-acquire task for retry simulation or wrong task acquired.")
                break
        
        final_status_task_w2 = db_manager.get_task_by_file_key(worker2_task["file_key"])
        assert final_status_task_w2["status"] == STATUS_PERMANENT_FAILURE, "Task was not marked as permanent failure after max retries."

        logger.info("DBManager tests completed.")
        if "test_db_manager.sqlite" in db_url_for_test and os.path.exists("./test_db_manager.sqlite"):
             os.remove("./test_db_manager.sqlite") # Clean up test DB

    except Exception as e:
        logger.error(f"Error during DBManager test: {e}", exc_info=True)
        print(f"DBManager test FAILED: {e}")


