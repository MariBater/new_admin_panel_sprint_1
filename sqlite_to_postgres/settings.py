import os
from dataclasses import fields
from pathlib import Path

from dotenv import load_dotenv

from .data_models import (FilmWork, Genre, GenreFilmWork, Person,
                          PersonFilmWork)

# Load environment variables
load_dotenv()

# --- Database connection settings ---
dsl = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
}

BASE_DIR = Path(__file__).resolve().parent.parent
SQLITE_DB_PATH = BASE_DIR / 'sqlite_to_postgres/db.sqlite'

# --- ETL settings ---
BATCH_SIZE = 100

# --- Migration configuration ---

def _create_table_config(
    name: str,
    dataclass: type,
    conflict_target: str,
    map_modified: bool = True,
    drop_columns: list[str] | None = None,
):
    """Factory function to reduce repetition in table configurations."""
    config = {
        "sqlite_source_table": name,
        "dataclass": dataclass,
        "columns": [f.name for f in fields(dataclass)],
        "pk_column": "id",
        "conflict_target": conflict_target,
        "column_mappings": {"created_at": "created"},
    }
    if map_modified:
        config["column_mappings"]["updated_at"] = "modified"
    if drop_columns:
        config["columns_to_drop"] = drop_columns
    return config


DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.environ.get('POSTGRES_DB'),
        'USER': os.environ.get('POSTGRES_USER'),
        'PASSWORD': os.environ.get('POSTGRES_PASSWORD'),
        'HOST': os.environ.get('DB_HOST'), # <-- Самое важное изменение
        'PORT': os.environ.get('DB_PORT'),
    }
}

# This configuration maps target PostgreSQL tables to their SQLite sources,
# corresponding dataclasses for validation and transformation, and other metadata.
TABLE_CONFIGS = {
    "film_work": _create_table_config(
        "film_work", FilmWork, "id", drop_columns=["file_path"]
    ),
    "person": _create_table_config("person", Person, "id"),
    "genre": _create_table_config("genre", Genre, "id"),
    "genre_film_work": _create_table_config(
        "genre_film_work",
        GenreFilmWork,
        "film_work_id, genre_id",
        map_modified=False,
    ),
    "person_film_work": _create_table_config(
        "person_film_work",
        PersonFilmWork,
        "film_work_id, person_id, role",
        map_modified=False,
    ),
}

# Defines the order of migration to respect foreign key constraints.
MIGRATION_ORDER = ["genre", "person", "film_work", "genre_film_work", "person_film_work"]