import logging
import sys
from .settings import LOG_FILE_PATH


def setup_logging():
    """Configures logging for the migration script."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE_PATH, mode='a', encoding='utf-8'),
            logging.StreamHandler(sys.stderr)  # Use stderr for logs, which is conventional
        ]
    )
