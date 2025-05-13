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
    logging.basicConfig(level=log_level, 
                        format=	'%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s	',
                        stream=sys.stdout)
    logger.info(f"Logging initialized with level: {log_level_str}")

def load_config():
    """Loads configuration from .env file and returns it as a dictionary."""
    project_root = get_project_root()
    dotenv_path = os.path.join(project_root, ".env")

    if not os.path.exists(dotenv_path):
        logger.warning(f".env file not found at {dotenv_path}. Using environment variables directly or defaults.")
        load_dotenv()
    else:
        load_dotenv(dotenv_path=dotenv_path)
        logger.info(f"Loaded configuration from {dotenv_path}")

    config = {
        "POLYGON_API_KEY": os.getenv("POLYGON_API_KEY"), # General API Key, might be used for other things
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

    # Validate that all required configurations for S3 access are present
    # POLYGON_API_KEY is now optional for the core flat file functionality
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
        db_file_path = config["DATABASE_URL"].replace("sqlite:///", "")
        if not os.path.isabs(db_file_path):
            db_file_path = os.path.join(project_root, db_file_path)
            config["DATABASE_URL"] = f"sqlite:///{db_file_path}"
        
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

if __name__ == "__main__":
    try:
        print("Attempting to load configuration for testing shared/config.py...")
        current_script_path = os.path.abspath(__file__)
        project_root_for_test = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
        dummy_dotenv_path = os.path.join(project_root_for_test, ".env")
        dummy_data_dir = os.path.join(project_root_for_test, "data")

        print(f"Test .env path: {dummy_dotenv_path}")
        print(f"Test data dir: {dummy_data_dir}")

        if not os.path.exists(dummy_dotenv_path):
            print(f"Creating dummy .env at {dummy_dotenv_path} for testing config.py")
            with open(dummy_dotenv_path, "w") as f:
                f.write("POLYGON_API_KEY=dummy_polygon_general_key\n")
                f.write("POLYGON_S3_ACCESS_KEY_ID=dummy_polygon_s3_id\n")
                f.write("POLYGON_S3_SECRET_ACCESS_KEY=dummy_polygon_s3_secret\n")
                f.write("B2_KEY_ID=dummy_b2_id_v2\n")
                f.write("B2_APPLICATION_KEY=dummy_b2_key_v2\n")
                f.write("B2_BUCKET_NAME=dummy_bucket_v2\n")
                f.write("B2_ENDPOINT_URL=dummy_endpoint_v2\n")
                f.write("DATABASE_URL=sqlite:///data/test_tracker.db\n")
                f.write("LOG_LEVEL=DEBUG\n")
        
        loaded_configuration = load_config()
        print("\nConfiguration loaded successfully:")
        for key, value in loaded_configuration.items():
            if "KEY" in key.upper() or "ID" in key.upper() and value is not None:
                print(f"  {key}: {"**********" if value else 'None'}")
            else:
                print(f"  {key}: {value}")
        
        expected_db_path = loaded_configuration["DATABASE_URL"].replace("sqlite:///", "")
        print(f"Expected DB path from config: {expected_db_path}")
        if os.path.exists(os.path.dirname(expected_db_path)):
            print(f"Directory for test database ({os.path.dirname(expected_db_path)}) exists or was created.")
        else:
            print(f"Directory for test database ({os.path.dirname(expected_db_path)}) was NOT created.")

    except Exception as e:
        print(f"Error during configuration test: {e}")
        import traceback
        traceback.print_exc()

