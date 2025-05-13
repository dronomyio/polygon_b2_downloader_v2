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

# Load configuration to get DATABASE_URL and WORKER_ID
# This should ideally be loaded once and passed around, or accessed via a global APP_CONFIG
# For simplicity in this module, we might load it here or expect it to be pre-loaded.
# Let's assume config is loaded by the main application and passed or db_manager is initialized with it.

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
            logger.error(f"Operational error during table creation (database might not be accessible or path invalid): {e}")
            # This could happen if the directory for SQLite doesn't exist and config.py didn't create it.
            # Or if DB server is down for other DB types.
            raise

    def add_task(self, file_key: str) -> bool:
        """Adds a new file task to the database with 'pending' status if it doesn't already exist."""
        stmt = self.files_table.insert().values(
            file_key=file_key,
            status=STATUS_PENDING,
            # discovered_at is server_default
            retry_count=0
        )
        try:
            with self.engine.connect() as connection:
                connection.execute(stmt)
                connection.commit()
            logger.info(f"Task added successfully for file_key: {file_key}")
            return True
        except IntegrityError: # Handles unique constraint violation for file_key
            logger.warning(f"Task for file_key: {file_key} already exists or another unique constraint failed.")
            return False
        except Exception as e:
            logger.error(f"Error adding task for file_key {file_key}: {e}")
            return False

    def get_pending_task(self, worker_id: str) -> dict | None:
        """
        Atomically fetches a pending task and marks it as 'processing' by the given worker_id.
        Prioritizes tasks that are PENDING or FAILED (download/upload) within retry limits.
        Returns the task (dict) or None if no suitable task is found.
        """
        # SQLite doesn't have SELECT ... FOR UPDATE SKIP LOCKED easily.
        # We need to simulate atomicity. One common way is to try to update a row
        # and see if it succeeds, then select it.
        # This query tries to find a task that is either pending or failed but still within retry limits.
        # It orders by last_attempted_at to try older tasks first (or those never attempted).
        # The LIMIT 1 and specific WHERE conditions are crucial.

        # We'll try to claim a task in a loop with a small delay to handle potential races, though less likely with SQLite's write lock.
        # For SQLite, a simpler approach: select a candidate, then try to update it with the worker_id.
        # If the update affects 1 row, we've claimed it.

        # Candidate selection: (status = PENDING OR (status IN (FAILED_DOWNLOAD, FAILED_UPLOAD) AND retry_count < MAX_RETRIES)) AND worker_id IS NULL
        # Order by discovered_at for pending, last_attempted_at for retries.
        # This logic can get complex. Let's simplify for SQLite: find one, try to claim.

        # Simpler SQLite strategy: Iterate through potential tasks and try to claim one.
        # This is not perfectly atomic across multiple processes without external locking if they query very fast.
        # However, SQLite's default transaction behavior (SERIALIZABLE) helps a lot.
        # A more robust way for SQLite in high concurrency would be to have workers pick from distinct ID ranges or use a more complex locking scheme.
        # Given the context, let's try a common pattern:

        with self.engine.connect() as connection:
            for _ in range(5): # Try a few times to find and claim a task
                # Find a candidate task
                # Prioritize tasks that have never been processed or have fewer retries
                # And are not currently being processed by another worker
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
                )
                
                candidate_row = connection.execute(stmt_select_candidate).first()

                if not candidate_row:
                    # logger.debug(f"No suitable pending tasks found for worker {worker_id}.")
                    return None # No tasks available

                task_id = candidate_row.id
                current_retry_count = candidate_row.retry_count

                # Try to claim this task
                stmt_claim = (
                    self.files_table.update()
                    .where(self.files_table.c.id == task_id)
                    .where(self.files_table.c.worker_id.is_(None)) # Ensure it's still not claimed
                    .values(
                        status=STATUS_PROCESSING,
                        worker_id=worker_id,
                        last_attempted_at=func.now(), # Mark attempt time
                        retry_count=current_retry_count + 1 # Increment retry count on this attempt
                    )
                )
                
                trans = connection.begin()
                try:
                    result = connection.execute(stmt_claim)
                    if result.rowcount == 1:
                        # Successfully claimed
                        # Now fetch the full row data for the claimed task
                        claimed_task_stmt = self.files_table.select().where(self.files_table.c.id == task_id)
                        claimed_task = connection.execute(claimed_task_stmt).mappings().first()
                        trans.commit()
                        logger.info(f"Worker {worker_id} claimed task ID {task_id} (file: {claimed_task.file_key if claimed_task else 'N/A'}).")
                        return dict(claimed_task) if claimed_task else None
                    else:
                        # Task was claimed by another worker in the meantime, or status changed
                        trans.rollback() # Rollback the attempt
                        logger.debug(f"Worker {worker_id} failed to claim task ID {task_id}, likely claimed by another. Retrying fetch.")
                        time.sleep(0.1) # Small delay before retrying the outer loop
                        continue
                except Exception as e:
                    trans.rollback()
                    logger.error(f"Error during task claim for worker {worker_id} on task ID {task_id}: {e}")
                    return None
            logger.info(f"Worker {worker_id} could not claim a task after multiple attempts.")
            return None

    def update_task_status(self, task_id: int, new_status: str, error_msg: str | None = None, worker_id_to_clear: str | None = None):
        """Updates the status of a task. Optionally clears worker_id if task is completed or failed permanently."""
        values_to_update = {"status": new_status}
        if error_msg:
            values_to_update["error_message"] = error_msg
        
        if new_status == STATUS_UPLOADED_TO_B2:
            values_to_update["completed_at"] = func.now()
            values_to_update["worker_id"] = None # Clear worker_id on completion
        elif new_status.startswith("failed_") or new_status == STATUS_PERMANENT_FAILURE:
            # For other failure states, we might keep the worker_id for investigation or clear it based on policy
            # For now, let's clear it if it's a terminal failure state for this attempt, allowing retry or permanent failure marking
            if worker_id_to_clear:
                 values_to_update["worker_id"] = None # Clear worker_id to allow retry by others if not permanent
        
        # If retry_count >= MAX_RETRIES and it's a failure, mark as permanent failure
        # This logic should be handled by the worker before calling update_task_status with STATUS_PERMANENT_FAILURE

        stmt = self.files_table.update().where(self.files_table.c.id == task_id).values(**values_to_update)
        try:
            with self.engine.connect() as connection:
                connection.execute(stmt)
                connection.commit()
            logger.info(f"Task ID {task_id} status updated to {new_status}.")
            return True
        except Exception as e:
            logger.error(f"Error updating status for task ID {task_id}: {e}")
            return False

    def release_task(self, task_id: int, new_status: str = STATUS_PENDING, error_msg: str | None = None):
        """Releases a task by setting its worker_id to None and updating status (e.g., back to pending or a failed state)."""
        values_to_update = {"status": new_status, "worker_id": None}
        if error_msg:
            values_to_update["error_message"] = error_msg
        
        stmt = self.files_table.update().where(self.files_table.c.id == task_id).values(**values_to_update)
        try:
            with self.engine.connect() as connection:
                connection.execute(stmt)
                connection.commit()
            logger.info(f"Task ID {task_id} released. Status set to {new_status}.")
            return True
        except Exception as e:
            logger.error(f"Error releasing task ID {task_id}: {e}")
            return False

    def get_task_by_file_key(self, file_key: str) -> dict | None:
        """Retrieves a task by its file_key."""
        stmt = self.files_table.select().where(self.files_table.c.file_key == file_key)
        with self.engine.connect() as connection:
            result = connection.execute(stmt).mappings().first()
        return dict(result) if result else None

# Example Usage (for testing this module directly)
if __name__ == "__main__":
    print("Testing DBManager...")
    # Ensure a .env file is in the project root for load_config() to work as expected by config.py
    # The config.py test part already creates a dummy .env if not present.
    try:
        app_config = load_config() # Loads config and sets up logging
        db_manager = DBManager(database_url=app_config["DATABASE_URL"])

        logger.info("DBManager initialized for testing.")

        # Test add_task
        logger.info("Testing add_task...")
        db_manager.add_task("test_file_1.csv.gz")
        db_manager.add_task("test_file_2.csv.gz")
        db_manager.add_task("test_file_1.csv.gz") # Test duplicate

        # Test get_task_by_file_key
        logger.info("Testing get_task_by_file_key...")
        task = db_manager.get_task_by_file_key("test_file_1.csv.gz")
        if task:
            logger.info(f"Retrieved task: {task}")
        else:
            logger.error("Failed to retrieve task test_file_1.csv.gz")

        # Test get_pending_task
        logger.info("Testing get_pending_task for worker1...")
        worker1_task = db_manager.get_pending_task(worker_id="worker1")
        if worker1_task:
            logger.info(f"Worker1 got task: {worker1_task}")
            task_id_w1 = worker1_task["id"]

            # Test update_task_status
            logger.info(f"Worker1 updating task {task_id_w1} to downloaded...")
            db_manager.update_task_status(task_id=task_id_w1, new_status=STATUS_DOWNLOADED)
            updated_task_w1 = db_manager.get_task_by_file_key(worker1_task["file_key"])
            logger.info(f"Task {task_id_w1} after download update: {updated_task_w1}")

            logger.info(f"Worker1 updating task {task_id_w1} to completed...")
            db_manager.update_task_status(task_id=task_id_w1, new_status=STATUS_UPLOADED_TO_B2, worker_id_to_clear="worker1")
            updated_task_w1_completed = db_manager.get_task_by_file_key(worker1_task["file_key"])
            logger.info(f"Task {task_id_w1} after completed update: {updated_task_w1_completed}")

        else:
            logger.info("Worker1 found no pending tasks.")

        logger.info("Testing get_pending_task for worker2...")
        worker2_task = db_manager.get_pending_task(worker_id="worker2")
        if worker2_task:
            logger.info(f"Worker2 got task: {worker2_task}")
            task_id_w2 = worker2_task["id"]
            # Simulate a failure and release
            logger.info(f"Worker2 simulating failure for task {task_id_w2} and releasing...")
            # Assume retry_count was incremented by get_pending_task
            # Worker logic would decide if it's STATUS_FAILED_DOWNLOAD or STATUS_FAILED_UPLOAD
            db_manager.release_task(task_id=task_id_w2, new_status=STATUS_FAILED_DOWNLOAD, error_msg="Simulated download error by worker2")
            failed_task_w2 = db_manager.get_task_by_file_key(worker2_task["file_key"])
            logger.info(f"Task {task_id_w2} after release by worker2: {failed_task_w2}")
            
            # Try to get it again (simulating retry)
            logger.info(f"Worker3 attempting to get the failed task {task_id_w2}...")
            worker3_task = db_manager.get_pending_task(worker_id="worker3")
            if worker3_task and worker3_task["id"] == task_id_w2:
                logger.info(f"Worker3 successfully got task {task_id_w2} for retry. Retry count: {worker3_task['retry_count']}")
                # Mark as permanent failure for testing
                db_manager.update_task_status(task_id_w2, new_status=STATUS_PERMANENT_FAILURE, error_msg="Max retries reached", worker_id_to_clear="worker3")
                permanent_fail_task = db_manager.get_task_by_file_key(worker3_task["file_key"])
                logger.info(f"Task {task_id_w2} after permanent failure: {permanent_fail_task}")
            elif worker3_task:
                logger.info(f"Worker3 got a different task: {worker3_task}")
            else:
                logger.info("Worker3 found no pending tasks (or failed task was not re-picked as expected).")

        else:
            logger.info("Worker2 found no pending tasks.")
        
        logger.info("DBManager testing finished.")

    except Exception as e:
        logger.error(f"Error during DBManager test: {e}")
        import traceback
        traceback.print_exc()

