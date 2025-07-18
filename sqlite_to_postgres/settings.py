import os
from dataclasses import fields
from pathlib import Path

from dotenv import load_dotenv

from .dataclasses import (FilmWork, Genre, GenreFilmWork, Person,
                          PersonFilmWork)

# Load environment variables
env_path = Path(__file__).resolve().parent.parent / 'config' / 'et.env'
load_dotenv(dotenv_path=env_path)

# --- Database connection settings ---
dsl = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
}

SQLITE_DB_PATH = r'd:\work\new_admin_panel_sprint_1\sqlite_to_postgres\db.sqlite'

# --- ETL settings ---
BATCH_SIZE = 100

# --- Migration configuration ---

# This configuration maps target PostgreSQL tables to their SQLite sources,
# corresponding dataclasses for validation and transformation, and other metadata.
TABLE_CONFIGS = {
    "film_work": {
        "sqlite_source_table": "film_work",
        "dataclass": FilmWork,
        "columns": [f.name for f in fields(FilmWork)],
        "pk_column": "id",
        "conflict_target": "id"
    },
    "person": {
        "sqlite_source_table": "person",
        "dataclass": Person,
        "columns": [f.name for f in fields(Person)],
        "pk_column": "id",
        "conflict_target": "id"
    },
    "genre": {
        "sqlite_source_table": "genre",
        "dataclass": Genre,
        "columns": [f.name for f in fields(Genre)],
        "pk_column": "id",
        "conflict_target": "id"
    },
    "genre_film_work": {
        "sqlite_source_table": "genre_film_work",
        "dataclass": GenreFilmWork,
        "columns": [f.name for f in fields(GenreFilmWork)],
        "pk_column": "id",
        "conflict_target": "film_work_id, genre_id"
    },
    "person_film_work": {
        "sqlite_source_table": "person_film_work",
        "dataclass": PersonFilmWork,
        "columns": [f.name for f in fields(PersonFilmWork)],
        "pk_column": "id",
        "conflict_target": "film_work_id, person_id, role"
    },
}

# Defines the order of migration to respect foreign key constraints.
MIGRATION_ORDER = ["genre", "person", "film_work", "genre_film_work", "person_film_work"]