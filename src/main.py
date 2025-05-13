import argparse
import sys
import os

# Add project root to sys.path to allow `from src.shared...` etc.
# This is for when main.py is run directly, e.g. `python src/main.py discoverer ...`
# If run as `python -m src.main ...` from project root, Python handles paths correctly.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.shared.config import load_config, logger # Logger is configured upon load_config()
from src.discoverer.main import main as discoverer_main
from src.worker.main import main as worker_main, signal_handler # Import signal_handler for worker
import signal

def main():
    parser = argparse.ArgumentParser(description="Main entry point for Polygon B2 Downloader V2.")
    parser.add_argument("role", choices=["discoverer", "worker"], help="The role to run: discoverer or worker.")
    # Add a catch-all for other arguments to pass to the role-specific main
    # This allows `src/main.py discoverer historical --dates 2023-01-01`
    # or `src/main.py worker --poll_interval 5`
    # The role-specific parsers will handle their own arguments.
    # We just need to make sure `sys.argv` is correctly set up for them.

    # Parse only the first argument to determine the role
    args, remaining_argv = parser.parse_known_args()

    # Load configuration first, as it also sets up logging
    try:
        # load_config() will be called by the role-specific main, which is better
        # as they might have specific needs or load order for config/logging.
        # However, if we want a global config load here, we can.
        # For now, let role-specific mains handle their config loading.
        # app_config = load_config()
        # logger.info(f"Main entry point: Running as {args.role}")
        pass # Config loading will happen in sub-mains
    except Exception as e:
        # Basic print as logger might not be configured if load_config fails
        print(f"FATAL: Initial configuration loading failed: {e}", file=sys.stderr)
        sys.exit(1)

    # Replace sys.argv with the role and its specific arguments
    # sys.argv[0] is the script name, then the role, then the role's args
    # Example: if called as `python src/main.py discoverer historical --foo bar`
    # sys.argv for discoverer_main should be `['discoverer_main_script_name_placeholder', 'historical', '--foo', 'bar']`
    # The actual script name placeholder doesn't matter much as argparse usually ignores argv[0].
    new_argv = [f"src/{args.role}/main.py"] + remaining_argv
    sys.argv = new_argv

    if args.role == "discoverer":
        discoverer_main()
    elif args.role == "worker":
        # Worker's main function sets up its own signal handlers
        worker_main()
    else:
        # Should not happen due to choices in argparse
        logger.error(f"Unknown role: {args.role}")
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()

