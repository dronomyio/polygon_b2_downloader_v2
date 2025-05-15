import os
from dotenv import load_dotenv
import logging
import sys

def get_project_root() -> str:
    """Gets the project root directory based on the location of this config file."""
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger("app")

def setup_logging(log_level_str: str = "INFO"):
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    # Corrected format string and removed leading/trailing whitespace issues
    logging.basicConfig(level=log_level, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s',
                        stream=sys.stdout)
    logger.info(f"Logging initialized with level: {log_level_str}")

def load_config():
    """Loads configuration from .env file and returns it as a dictionary."""
    project_root = get_project_root()
    dotenv_path = os.path.join(project_root, ".env")

    if not os.path.exists(dotenv_path):
        # Corrected format string for basic logging if .env is missing
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', stream=sys.stdout)
        logging.getLogger("app").warning(f".env file not found at {dotenv_path}. Using environment variables directly or defaults.")
        load_dotenv() 
    else:
        load_dotenv(dotenv_path=dotenv_path)
        # Corrected format string for basic logging if .env is found
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', stream=sys.stdout)
        logging.getLogger("app").info(f"Loaded configuration from {dotenv_path}")

    config = {
        "POLYGON_API_KEY": os.getenv("POLYGON_API_KEY"), 
        "POLYGON_S3_ACCESS_KEY_ID": os.getenv("POLYGON_S3_ACCESS_KEY_ID"),
        "POLYGON_S3_SECRET_ACCESS_KEY": os.getenv("POLYGON_S3_SECRET_ACCESS_KEY"),
        "B2_KEY_ID": os.getenv("B2_KEY_ID"),
        "B2_APPLICATION_KEY": os.getenv("B2_APPLICATION_KEY"),
        "B2_BUCKET_NAME": os.getenv("B2_BUCKET_NAME"),
        "B2_ENDPOINT_URL": os.getenv("B2_ENDPOINT_URL"),
        "DATABASE_URL": os.getenv("DATABASE_URL", f"sqlite:///{os.path.join(project_root, 'data', 'download_tracker.db')}"),
        "WORKER_ID": os.getenv("WORKER_ID", f"worker-{os.getpid()}"),
        "LOG_LEVEL": os.getenv("LOG_LEVEL", "INFO").upper()
    }

    setup_logging(config["LOG_LEVEL"])

    required_s3_keys = [
        "POLYGON_S3_ACCESS_KEY_ID", 
        "POLYGON_S3_SECRET_ACCESS_KEY",
        "B2_KEY_ID", 
        "B2_APPLICATION_KEY", 
        "B2_BUCKET_NAME", 
        "B2_ENDPOINT_URL"
    ]
    missing_keys = [key for key in required_s3_keys if not config[key]]

    if missing_keys:
        error_message = f"Missing required S3 configuration keys: {', '.join(missing_keys)}. Check your .env file or environment variables."
        logger.error(error_message)
        raise ValueError(error_message)
    
    if config["DATABASE_URL"].startswith("sqlite:///"):
        db_file_path_str = config["DATABASE_URL"].replace("sqlite:///", "")
        if not os.path.isabs(db_file_path_str):
            db_file_path_str = os.path.join(project_root, db_file_path_str)
            config["DATABASE_URL"] = f"sqlite:///{db_file_path_str}"
        
        db_dir = os.path.dirname(db_file_path_str)
        if not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir)
                logger.info(f"Created directory for SQLite database: {db_dir}")
            except OSError as e:
                logger.error(f"Error creating directory {db_dir} for SQLite database: {e}")
                raise

    logger.info("Configuration loaded and logging configured successfully.")
    return config

if __name__ == "__main__":
    try:
        print("Attempting to load configuration for testing shared/config.py...")
        current_script_path = os.path.abspath(__file__)
        project_root_for_test = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
        dummy_dotenv_path = os.path.join(project_root_for_test, ".env")
        dummy_data_dir = os.path.join(project_root_for_test, "data")

        print(f"Test .env path: {dummy_dotenv_path}")
        print(f"Test data dir for default DB: {dummy_data_dir}")

        if not os.path.exists(dummy_dotenv_path):
            print(f"Creating dummy .env at {dummy_dotenv_path} for testing config.py")
            with open(dummy_dotenv_path, "w") as f:
                f.write("POLYGON_API_KEY=dummy_polygon_general_key_test\n")
                f.write("POLYGON_S3_ACCESS_KEY_ID=dummy_polygon_s3_id_test\n")
                f.write("POLYGON_S3_SECRET_ACCESS_KEY=dummy_polygon_s3_secret_test\n")
                f.write("B2_KEY_ID=dummy_b2_id_test\n")
                f.write("B2_APPLICATION_KEY=dummy_b2_key_test\n")
                f.write("B2_BUCKET_NAME=dummy_bucket_test\n")
                f.write("B2_ENDPOINT_URL=dummy_endpoint_test\n")
                f.write("LOG_LEVEL=DEBUG\n")
        
        loaded_configuration = load_config()
        print("\nConfiguration loaded successfully during test:")
        for key, value in loaded_configuration.items():
            if ("KEY" in key.upper() or "ID" in key.upper()) and value is not None:
                print(f"  {key}: {'**********' if value else 'None'}")
            else:
                print(f"  {key}: {value}")
        
        expected_db_path_from_config = loaded_configuration["DATABASE_URL"].replace("sqlite:///", "")
        print(f"Expected DB path from config (should be absolute): {expected_db_path_from_config}")
        if os.path.exists(os.path.dirname(expected_db_path_from_config)):
            print(f"Directory for test database ({os.path.dirname(expected_db_path_from_config)}) exists or was created.")
        else:
            print(f"Directory for test database ({os.path.dirname(expected_db_path_from_config)}) may not have been created by this test if path was already absolute.")

    except Exception as e:
        print(f"Error during configuration test: {e}")
        import traceback
        traceback.print_exc()

