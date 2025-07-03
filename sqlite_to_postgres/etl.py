import logging
import sqlite3
from dataclasses import astuple
from typing import Generator

import psycopg

from .settings import BATCH_SIZE, TABLE_CONFIGS

logger = logging.getLogger(__name__)


def extract_sqlite_data(sqlite_cursor: sqlite3.Cursor, table_name: str) -> Generator[list[sqlite3.Row], None, None]:
    """Extracts data from an SQLite table in batches."""
    logger.info(f"Extracting data from SQLite table: {table_name}")
    sqlite_cursor.execute(f"SELECT * FROM {table_name}")
    while results := sqlite_cursor.fetchmany(BATCH_SIZE):
        yield results


def transform_to_dataclass(batch: list[sqlite3.Row], data_class: type) -> list:
    """Transforms a batch of SQLite rows to a list of dataclass instances."""
    transformed_batch = []
    for row_num, row_data in enumerate(batch):
        mapped_row_data = dict(row_data)
        # Map SQLite column names to dataclass field names
        if 'created_at' in mapped_row_data:
            mapped_row_data['created'] = mapped_row_data.pop('created_at')
        if 'updated_at' in mapped_row_data:
            mapped_row_data['modified'] = mapped_row_data.pop('updated_at')
        if 'update' in mapped_row_data and 'modified' not in mapped_row_data:
            mapped_row_data['modified'] = mapped_row_data.pop('update')
        if data_class.__name__ == 'FilmWork' and 'file_path' in mapped_row_data:
            mapped_row_data.pop('file_path')
        try:
            transformed_batch.append(data_class(**mapped_row_data))
        except Exception as e:
            logger.error(f"Error transforming row #{row_num} for {data_class.__name__}: {mapped_row_data}. Original SQLite row: {dict(row_data)}. Error: {e}", exc_info=True)
    return transformed_batch


def load_to_postgres(pg_cursor: psycopg.Cursor, table_name: str, columns: list[str], batch_data: list, conflict_target: str = "id"):
    """Loads a batch of data into a PostgreSQL table."""
    if not batch_data:
        return
    logger.info(f"Loading batch of {len(batch_data)} items into PostgreSQL table: {table_name}")
    cols_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders}) ON CONFLICT ({conflict_target}) DO NOTHING"
    try:
        batch_as_tuples = [astuple(item) for item in batch_data]
        pg_cursor.executemany(query, batch_as_tuples)
    except psycopg.Error as e:
        logger.error(f"PostgreSQL error loading data into {table_name}. Query: {query[:200]}... Error: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading data into {table_name}. Error: {e}", exc_info=True)
        raise


def test_data_transfer(sqlite_cursor: sqlite3.Cursor, pg_cursor: psycopg.Cursor, table_name: str, data_class: type, pk_column: str = "id"):
    """Tests data transfer by comparing row counts and a sample of data."""
    logger.info(f"Starting data transfer test for table: {table_name}")

    # Determine the source table name in SQLite from the config
    sqlite_source_table_for_test = table_name
    for config_key, config_value in TABLE_CONFIGS.items():
        if f"content.{config_key}" == table_name:
            sqlite_source_table_for_test = config_value["sqlite_source_table"]
            break

    # 1. Compare row counts
    sqlite_cursor.execute(f"SELECT COUNT(*) FROM {sqlite_source_table_for_test}")
    sqlite_count = sqlite_cursor.fetchone()[0]
    logger.info(f"SQLite table '{sqlite_source_table_for_test}' count: {sqlite_count}")

    pg_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    pg_row = pg_cursor.fetchone()
    pg_count = pg_row['count'] if pg_row else 0
    logger.info(f"PostgreSQL table '{table_name}' count: {pg_count}")
    assert sqlite_count == pg_count, f"Count mismatch for {table_name}: SQLite ({sqlite_count}) != PG ({pg_count})"
    logger.info(f"Count check passed for {table_name}.")

    if sqlite_count == 0:
        logger.info(f"No data in SQLite table {sqlite_source_table_for_test} to test content.")
        return

    # 2. Compare a sample of data
    sqlite_cursor.execute(f"SELECT * FROM {sqlite_source_table_for_test} ORDER BY {pk_column} LIMIT {min(BATCH_SIZE // 2, 10)}")
    original_data_sample_raw = sqlite_cursor.fetchall()

    original_data_sample = transform_to_dataclass(original_data_sample_raw, data_class)

    if not original_data_sample:
        logger.info(f"No sample data fetched from SQLite table {table_name} for content test.")
        return

    ids_sample = [getattr(item, pk_column) for item in original_data_sample]

    pg_cursor.execute(f"SELECT * FROM {table_name} WHERE {pk_column} = ANY(%s) ORDER BY {pk_column}", (ids_sample,))
    transferred_data_sample_raw = pg_cursor.fetchall()
    transferred_data_sample = [data_class(**row_dict) for row_dict in transferred_data_sample_raw]

    assert len(original_data_sample) == len(transferred_data_sample), f"Sample batch size mismatch for {table_name}: SQLite ({len(original_data_sample)}) != PG ({len(transferred_data_sample)})"

    original_data_sample.sort(key=lambda x: getattr(x, pk_column))
    transferred_data_sample.sort(key=lambda x: getattr(x, pk_column))

    for original, transferred in zip(original_data_sample, transferred_data_sample):
        assert original == transferred, f"Data mismatch for {table_name} on ID {getattr(original, pk_column)}:\nOriginal: {original}\nPG_copy:  {transferred}"

    logger.info(f"Content check passed for a sample from {table_name}.")

