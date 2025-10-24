import logging
import os
import time

from .load_data import migrate_data
from .etl_process import main as run_etl_loop # This will need to be updated to pass dsl
from .settings import get_pg_dsl, ETL_SLEEP_INTERVAL # Import the function
from .logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

INIT_FLAG_FILE = "/app/state/init_completed.flag"


def main():
    """
    The main entry point for the ETL service.
    It decides whether to run the initial migration or the incremental ETL loop.
    """
    pg_dsl = get_pg_dsl() # Get the DSL here

    # Check if the initial migration has already been completed.
    if not os.path.exists(INIT_FLAG_FILE):
        logger.info("First run detected. Starting initial data migration...")
        try:
            # Pass connection details directly
            migrate_data(pg_dsl)
            logger.info("Initial data migration completed successfully.")
            # Create a flag file to indicate completion
            with open(INIT_FLAG_FILE, 'w') as f:
                f.write('completed')
        except Exception as e:
            logger.critical(f"Initial data migration failed: {e}. ETL process will not start.", exc_info=True)
            # Exit with an error code if migration fails
            exit(1)
    else:
        logger.info("Initialization already completed. Skipping initial migration.")

    logger.info("Starting incremental ETL process...")
    # The ETL loop will run indefinitely, pass the DSL
    run_etl_loop(pg_dsl)


if __name__ == "__main__":
    main() # main() will now call get_pg_dsl()