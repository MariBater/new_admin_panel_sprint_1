import logging
import sqlite3

import psycopg
from psycopg.rows import dict_row

from .logging_config import setup_logging
from .migrator import process_table
from .settings import MIGRATION_ORDER, SQLITE_DB_PATH, dsl, BASE_DIR

# Настраиваем логирование через отдельный модуль
setup_logging()

logger = logging.getLogger(__name__)

DB_SCHEMA_PATH = BASE_DIR / 'sqlite_to_postgres/movies_database.ddl'


def setup_postgres_schema(pg_conn):
    """Reads and executes the DDL file to create the database schema."""
    logger.info(f"Setting up PostgreSQL schema from {DB_SCHEMA_PATH}...")
    if not DB_SCHEMA_PATH.exists():
        logger.error(f"Database schema file not found at {DB_SCHEMA_PATH}. Exiting.")
        raise FileNotFoundError(f"DDL file not found: {DB_SCHEMA_PATH}")
    with open(DB_SCHEMA_PATH, 'r') as f, pg_conn.cursor() as cursor:
        cursor.execute(f.read())
    logger.info("PostgreSQL schema setup complete.")


def migrate_data():
    logger.info("Starting data migration process.")

    if not all(dsl.get(k) for k in ['host', 'port', 'dbname', 'user', 'password']):
        logger.error("One or more critical PostgreSQL connection parameters are missing. Please check your .env file. Exiting.")
        return

    try:
        with sqlite3.connect(SQLITE_DB_PATH) as sqlite_conn, \
                psycopg.connect(**dsl, row_factory=dict_row, options='-c search_path=content') as pg_conn:
            
            sqlite_conn.row_factory = sqlite3.Row
            logger.info("Successfully connected to databases.")

            # 1. Set up the schema in a separate, initial transaction.
            # This ensures the schema and tables are created and committed
            # before the main migration transaction starts.
            with pg_conn.transaction():
                logger.info("Beginning schema setup transaction.")
                setup_postgres_schema(pg_conn)
            logger.info("Schema setup transaction committed.")

            # 2. Run the main migration in a single, large transaction.
            with pg_conn.transaction():
                logger.info("Beginning main data migration transaction.")
                for table_name in MIGRATION_ORDER:
                    process_table(table_name, sqlite_conn, pg_conn)
            logger.info("Full data migration transaction committed successfully.")
            logger.info('🎉 Данные успешно перенесены и протестированы для всех сконфигурированных таблиц!')

    except (sqlite3.Error, psycopg.Error) as e:
        logger.critical(f"Database error during migration: {e}", exc_info=True)
        logger.warning("PostgreSQL transaction has been rolled back due to a database error.")
    except Exception as e:
        logger.critical(f"An unexpected error occurred during migration: {e}", exc_info=True)
        logger.warning("PostgreSQL transaction has been rolled back due to an unexpected error.")
    finally:
        logger.info("Data migration process finished. Database connections are closed.")

if __name__ == '__main__':
    migrate_data()