import logging
from contextlib import closing

from .etl import (extract_sqlite_data, load_to_postgres, test_data_transfer,
                 transform_to_dataclass)
from .settings import TABLE_CONFIGS

logger = logging.getLogger(__name__)


def process_table(table_name: str, sqlite_conn, pg_conn):
    """
    Processes a single table: extracts, transforms, loads, and tests data. 
    The commit is handled by the caller to ensure transactional integrity.
    """
    if table_name not in TABLE_CONFIGS:
        logger.warning(f"No configuration found for table {table_name}, skipping.")
        return

    config = TABLE_CONFIGS[table_name]
    sqlite_source_table = config["sqlite_source_table"]
    pg_target_table = table_name

    logger.info(f"--- Processing SQLite table '{sqlite_source_table}' -> PG table '{pg_target_table}' ---")

    # Data transfer
    with closing(sqlite_conn.cursor()) as sqlite_cur, \
            closing(pg_conn.cursor()) as pg_cur:

        data_to_load_generator = (
            transform_to_dataclass(batch, config)
            for batch in extract_sqlite_data(sqlite_cur, sqlite_source_table)
        )

        for transformed_batch in data_to_load_generator:
            if transformed_batch:
                load_to_postgres(
                    pg_cur,
                    pg_target_table,
                    config["columns"],
                    transformed_batch,
                    config.get("conflict_target", "id")
                )

        # The commit is now handled by the calling function (migrate_data)
        logger.info(f"Data loading complete for PG table: {pg_target_table}")

    # Data validation
    with closing(sqlite_conn.cursor()) as sqlite_cur_test, \
            closing(pg_conn.cursor()) as pg_cur_test:
        test_data_transfer(
            sqlite_cur_test,
            pg_cur_test,
            pg_target_table,
            sqlite_source_table,
            config,
        )

    logger.info(f"--- Successfully processed and tested data from '{sqlite_source_table}' to '{pg_target_table}' ---")