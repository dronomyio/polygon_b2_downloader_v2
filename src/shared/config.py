import os
from dotenv import load_dotenv
import logging
import sys

# Ensure the src directory is in the Python path for relative imports if running scripts directly from subdirs
# This might be more relevant for local testing outside Docker.
# For Docker execution via `python -m src.main`, this is usually handled.
# sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def get_project_root() -> str:
    """Gets the project root directory based on the location of this config file."""
    # Assuming this file is in /path/to/project/src/shared/config.py
    # Project root is three levels up.
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure basic logging
# The log level will be configurable via .env

logger = logging.getLogger("app") # Use a common logger name

def setup_logging(log_level_str: str = "INFO"):
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    logging.basicConfig(level=log_level, 
                        format=	'%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s	',
                        stream=sys.stdout) # Log to stdout for Docker compatibility
    logger.info(f"Logging initialized with level: {log_level_str}")


def load_config():
    """Loads configuration from .env file and returns it as a dictionary."""
    project_root = get_project_root()
    dotenv_path = os.path.join(project_root, ".env")

    if not os.path.exists(dotenv_path):
        # Fallback for Docker where .env might be in /app if WORKDIR is /app and .env is copied there
        # This depends on Dockerfile structure.
        # If running locally, project_root should be correct.
        # Let's assume for now .env is at project_root as defined.
        logger.warning(f".env file not found at {dotenv_path}. Using environment variables directly or defaults.")
        # In a container, env vars might be passed directly, so load_dotenv might not be strictly necessary
        # if the .env file isn't present but vars are in the environment.
        load_dotenv() # Tries to load from a .env in current dir or parent, or from existing env vars
    else:
        load_dotenv(dotenv_path=dotenv_path)
        logger.info(f"Loaded configuration from {dotenv_path}")

    config = {
        "POLYGON_API_KEY": os.getenv("POLYGON_API_KEY"),
        "B2_KEY_ID": os.getenv("B2_KEY_ID"),
        "B2_APPLICATION_KEY": os.getenv("B2_APPLICATION_KEY"),
        "B2_BUCKET_NAME": os.getenv("B2_BUCKET_NAME"),
        "B2_ENDPOINT_URL": os.getenv("B2_ENDPOINT_URL"),
        "DATABASE_URL": os.getenv("DATABASE_URL", f"sqlite:///{os.path.join(project_root, 'data', 'download_tracker.db')}"),
        "WORKER_ID": os.getenv("WORKER_ID", f"worker-{os.getpid()}"), # Default worker ID
        "LOG_LEVEL": os.getenv("LOG_LEVEL", "INFO").upper()
    }

    # Setup logging based on the loaded config
    setup_logging(config["LOG_LEVEL"])

    # Validate that all required configurations are present
    required_keys = ["POLYGON_API_KEY", "B2_KEY_ID", "B2_APPLICATION_KEY", "B2_BUCKET_NAME", "B2_ENDPOINT_URL"]
    missing_keys = [key for key in required_keys if not config[key]]

    if missing_keys:
        error_message = f"Missing required configuration keys: {", ".join(missing_keys)}. Check your .env file or environment variables."
        logger.error(error_message)
        raise ValueError(error_message)
    
    # Ensure the directory for the SQLite DB exists if using the default path
    if config["DATABASE_URL"].startswith("sqlite:///"):
        db_file_path = config["DATABASE_URL"].replace("sqlite:///", "")
        # If it's a relative path, make it absolute from project root
        if not os.path.isabs(db_file_path):
            db_file_path = os.path.join(project_root, db_file_path)
            config["DATABASE_URL"] = f"sqlite:///{db_file_path}" # Update config with absolute path
        
        db_dir = os.path.dirname(db_file_path)
        if not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir)
                logger.info(f"Created directory for SQLite database: {db_dir}")
            except OSError as e:
                logger.error(f"Error creating directory {db_dir} for SQLite database: {e}")
                raise

    logger.info("Configuration loaded and logging configured successfully.")
    return config

# To make the config accessible globally after first load (optional, can also pass it around)
# APP_CONFIG = load_config()

if __name__ == "__main__":
    # This is for testing the config loading directly
    try:
        print("Attempting to load configuration for testing shared/config.py...")
        # Create a dummy .env in the project root for this test to pass
        # Project root is three levels up from src/shared/config.py
        current_script_path = os.path.abspath(__file__)
        project_root_for_test = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
        dummy_dotenv_path = os.path.join(project_root_for_test, ".env")
        dummy_data_dir = os.path.join(project_root_for_test, "data")

        print(f"Test .env path: {dummy_dotenv_path}")
        print(f"Test data dir: {dummy_data_dir}")

        if not os.path.exists(dummy_dotenv_path):
            print(f"Creating dummy .env at {dummy_dotenv_path} for testing config.py")
            with open(dummy_dotenv_path, "w") as f:
                f.write("POLYGON_API_KEY=dummy_polygon_key_v2\n")
                f.write("B2_KEY_ID=dummy_b2_id_v2\n")
                f.write("B2_APPLICATION_KEY=dummy_b2_key_v2\n")
                f.write("B2_BUCKET_NAME=dummy_bucket_v2\n")
                f.write("B2_ENDPOINT_URL=dummy_endpoint_v2\n")
                f.write("DATABASE_URL=sqlite:///data/test_tracker.db\n") # Use a test DB
                f.write("LOG_LEVEL=DEBUG\n")
        
        loaded_configuration = load_config()
        print("\nConfiguration loaded successfully:")
        for key, value in loaded_configuration.items():
            if "KEY" in key or "ID" in key and value is not None:
                print(f"  {key}: {"**********"}")
            else:
                print(f"  {key}: {value}")
        
        # Test if the data directory for the test DB was created
        expected_db_path = loaded_configuration["DATABASE_URL"].replace("sqlite:///", "")
        print(f"Expected DB path from config: {expected_db_path}")
        if os.path.exists(os.path.dirname(expected_db_path)):
            print(f"Directory for test database ({os.path.dirname(expected_db_path)}) exists or was created.")
        else:
            print(f"Directory for test database ({os.path.dirname(expected_db_path)}) was NOT created.")

        # Clean up dummy .env and data dir if created by this test
        # Note: Be careful with cleanup in automated tests; for now, manual cleanup might be safer
        # if os.path.exists(dummy_dotenv_path) and "dummy_polygon_key_v2" in open(dummy_dotenv_path).read():
        #     os.remove(dummy_dotenv_path)
        #     print(f"Removed dummy .env at {dummy_dotenv_path}")
        # if os.path.exists(os.path.join(dummy_data_dir, "test_tracker.db")):
        #     os.remove(os.path.join(dummy_data_dir, "test_tracker.db"))
        #     print("Removed dummy test_tracker.db")
        # if os.path.exists(dummy_data_dir) and not os.listdir(dummy_data_dir):
        #     os.rmdir(dummy_data_dir)
        #     print(f"Removed dummy data directory {dummy_data_dir}")

    except Exception as e:
        print(f"Error during configuration test: {e}")
        import traceback
        traceback.print_exc()


