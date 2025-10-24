import logging
import sqlite3
import sys

import psycopg
from psycopg.rows import dict_row

from .logging_config import setup_logging
from .migrator import process_table
from .es_loader import ElasticsearchLoader
from .settings import MIGRATION_ORDER, SQLITE_DB_PATH, BASE_DIR, BATCH_SIZE

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

def get_all_film_work_ids(pg_conn):
    """Fetches all film_work IDs from PostgreSQL in batches."""
    logger.info("Fetching all film_work IDs from PostgreSQL for initial indexing...")
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT id FROM content.film_work ORDER BY id;")
        while True:
            batch = cursor.fetchmany(BATCH_SIZE)
            if not batch:
                break
            # Yield a tuple of IDs for the batch
            yield tuple(row['id'] for row in batch)


def migrate_data(pg_dsl: dict):
    logger.info("Starting data migration process.")

    if not all(pg_dsl.get(k) for k in ['host', 'port', 'dbname', 'user', 'password']):
        logger.error("One or more critical PostgreSQL connection parameters are missing. Exiting.")
        sys.exit(1)

    try:
        with sqlite3.connect(SQLITE_DB_PATH) as sqlite_conn, \
                psycopg.connect(**pg_dsl, row_factory=dict_row, options='-c search_path=content') as pg_conn:
            
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

            # 3. Index data into Elasticsearch
            logger.info("Starting Elasticsearch indexing...")
            es_loader = ElasticsearchLoader(pg_dsl)
            total_indexed_docs = 0
            for fw_ids_batch in get_all_film_work_ids(pg_conn):
                if not fw_ids_batch:
                    continue
                enriched_data = es_loader.get_enriched_data_from_pg(tuple(fw_ids_batch))
                indexed_count = es_loader.bulk_index_to_es(enriched_data)
                total_indexed_docs += indexed_count
            
            logger.info(f"🎉 Successfully migrated data to PostgreSQL and indexed {total_indexed_docs} documents into Elasticsearch!")

    except (sqlite3.Error, psycopg.Error) as e:
        logger.critical(f"Database error during migration: {e}", exc_info=True)
        logger.warning("PostgreSQL transaction has been rolled back due to a database error.")
    except Exception as e:
        logger.critical(f"An unexpected error occurred during migration: {e}", exc_info=True)
        logger.warning("PostgreSQL transaction has been rolled back due to an unexpected error.")
    finally:
        logger.info("Data migration process finished. Database connections are closed.")

if __name__ == '__main__':
    from .settings import get_pg_dsl
    migrate_data(get_pg_dsl())