import logging
import sqlite3
from dataclasses import astuple
from typing import Generator

import psycopg
from psycopg.sql import SQL, Identifier

from .settings import BATCH_SIZE, TABLE_CONFIGS

logger = logging.getLogger(__name__)


def extract_sqlite_data(sqlite_cursor: sqlite3.Cursor, table_name: str) -> Generator[list[sqlite3.Row], None, None]:
    """Extracts data from an SQLite table in batches."""
    logger.info(f"Extracting data from SQLite table: {table_name}")
    sqlite_cursor.execute(f"SELECT * FROM {table_name}")
    while results := sqlite_cursor.fetchmany(BATCH_SIZE):
        yield results


def transform_to_dataclass(batch: list[sqlite3.Row], config: dict) -> list:
    """Transforms a batch of SQLite rows to a list of dataclass instances."""
    data_class = config["dataclass"]
    column_mappings = config.get("column_mappings", {})
    columns_to_drop = config.get("columns_to_drop", [])

    transformed_batch = []
    for row_num, row_data in enumerate(batch):
        mapped_row_data = dict(row_data)
        # Apply declarative mappings and drops from settings
        for source_col, target_col in column_mappings.items():
            if source_col in mapped_row_data:
                mapped_row_data[target_col] = mapped_row_data.pop(source_col)
        for col_to_drop in columns_to_drop:
            mapped_row_data.pop(col_to_drop, None)
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
    
    # Safely compose the SQL query
    query = SQL("INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON CONFLICT ({conflict_cols}) DO NOTHING").format(
        table=Identifier(*table_name.split('.')),
        cols=SQL(', ').join(map(Identifier, columns)),
        placeholders=SQL(', ').join(SQL('%s') for _ in columns),
        conflict_cols=SQL(', ').join(map(Identifier, conflict_target.replace(' ', '').split(',')))
    )

    try:
        batch_as_tuples = [astuple(item) for item in batch_data]
        pg_cursor.executemany(query, batch_as_tuples)
    except psycopg.Error as e:
        logger.error(f"PostgreSQL error loading data into {table_name}. Query: {query[:200]}... Error: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading data into {table_name}. Error: {e}", exc_info=True)
        raise


def test_data_transfer(sqlite_cursor: sqlite3.Cursor, pg_cursor: psycopg.Cursor, pg_table_name: str, sqlite_table_name: str, config: dict):
    """Tests data transfer by comparing row counts and a sample of data."""
    logger.info(f"Starting data transfer test for table: {pg_table_name}")

    data_class = config["dataclass"]
    pk_column = config["pk_column"]

    # Безопасные идентификаторы для PostgreSQL
    pg_table_identifier = Identifier(*pg_table_name.split('.'))
    pk_identifier = Identifier(pk_column)

    # 1. Compare row counts
    sqlite_cursor.execute(f"SELECT COUNT(*) FROM {sqlite_table_name}")
    sqlite_count = sqlite_cursor.fetchone()[0]
    logger.info(f"SQLite table '{sqlite_table_name}' count: {sqlite_count}")

    pg_cursor.execute(SQL("SELECT COUNT(*) FROM {table}").format(table=pg_table_identifier))
    pg_row = pg_cursor.fetchone()
    pg_count = pg_row['count'] if pg_row else 0
    logger.info(f"PostgreSQL table '{pg_table_name}' count: {pg_count}")
    assert sqlite_count == pg_count, f"Count mismatch for {pg_table_name}: SQLite ({sqlite_count}) != PG ({pg_count})"
    logger.info(f"Count check passed for {pg_table_name}.")

    if sqlite_count == 0:
        logger.info(f"No data in SQLite table {sqlite_table_name} to test content.")
        return

    # 2. Compare a sample of data
    sqlite_cursor.execute(f"SELECT * FROM {sqlite_table_name} ORDER BY {pk_column} LIMIT {min(BATCH_SIZE // 2, 10)}")
    original_data_sample_raw = sqlite_cursor.fetchall()

    original_data_sample = transform_to_dataclass(original_data_sample_raw, config)

    if not original_data_sample:
        logger.info(f"No sample data fetched from SQLite table {pg_table_name} for content test.")
        return

    ids_sample = [getattr(item, pk_column) for item in original_data_sample]

    query = SQL("SELECT * FROM {table} WHERE {pk} = ANY(%s) ORDER BY {pk}").format(
        table=pg_table_identifier,
        pk=pk_identifier
    )
    pg_cursor.execute(query, (ids_sample,))

    transferred_data_sample_raw = pg_cursor.fetchall()
    transferred_data_sample = [data_class(**row_dict) for row_dict in transferred_data_sample_raw]

    assert len(original_data_sample) == len(transferred_data_sample), f"Sample batch size mismatch for {pg_table_name}: SQLite ({len(original_data_sample)}) != PG ({len(transferred_data_sample)})"

    original_data_sample.sort(key=lambda x: getattr(x, pk_column))
    transferred_data_sample.sort(key=lambda x: getattr(x, pk_column))

    for original, transferred in zip(original_data_sample, transferred_data_sample):
        assert original == transferred, f"Data mismatch for {pg_table_name} on ID {getattr(original, pk_column)}:\nOriginal: {original}\nPG_copy:  {transferred}"

    logger.info(f"Content check passed for a sample from {pg_table_name}.")
