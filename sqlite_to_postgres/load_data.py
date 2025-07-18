import logging
import sqlite3

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

from .migrator import process_table
from .settings import MIGRATION_ORDER, SQLITE_DB_PATH, dsl

# Настраиваем логирование в одном месте - в точке входа в приложение.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.FileHandler("migration.log", mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def migrate_data():
    
    logger.info("Starting data migration process.")

    if not all(dsl.get(k) for k in ['host', 'port', 'dbname', 'user', 'password']):
        logger.error("One or more critical PostgreSQL connection parameters are missing. Please check your .env file. Exiting.")
        return

    sqlite_conn = None
    pg_conn = None
    try:
        sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
        sqlite_conn.row_factory = sqlite3.Row
        pg_conn = psycopg.connect(**dsl, row_factory=dict_row)

        for table_name in MIGRATION_ORDER:
            process_table(table_name, sqlite_conn, pg_conn)

        pg_conn.commit()
        logger.info("Full migration transaction committed successfully.")
        logger.info('🎉 Данные успешно перенесены и протестированы для всех сконфигурированных таблиц!')

    except (sqlite3.Error, psycopg.Error) as e:
        logger.critical(f"Database error during migration: {e}", exc_info=True)
        if pg_conn:
            pg_conn.rollback()
            logger.warning("PostgreSQL transaction has been rolled back.")
    except Exception as e:
        logger.critical(f"An unexpected error occurred during migration: {e}", exc_info=True)
        if pg_conn:
            pg_conn.rollback()
            logger.warning("PostgreSQL transaction has been rolled back due to an unexpected error.")
    finally:
        if sqlite_conn:
            sqlite_conn.close()
        if pg_conn:
            pg_conn.close()
            logger.info("Database connections closed.")

if __name__ == '__main__':
    migrate_data()