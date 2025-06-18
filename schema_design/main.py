import os
from pathlib import Path
import sqlite3
from contextlib import closing
from dataclasses import dataclass, astuple, fields
from typing import Generator
from uuid import UUID
from datetime import date, datetime
import logging

# Импорт библиотек для работы с переменными окружения и PostgreSQL

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

env_path = Path(__file__).resolve().parent.parent / 'config' / 'et.env'
load_dotenv(dotenv_path=env_path) # Загрузка переменных окружения из файла .env

# Настройка базовой конфигурации логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.FileHandler("migration.log", mode='w'),# Запись логов в файл
        logging.StreamHandler()# Вывод логов в консоль
    ]
)
logger = logging.getLogger(__name__)# Создание экземпляра логгера


dsl = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
}


logger.info(f"Attempting to connect with DSN: dbname='{dsl.get('dbname')}' user='{dsl.get('user')}' host='{dsl.get('host')}' port='{dsl.get('port')}'")
if not dsl.get('host') or not dsl.get('port') or not dsl.get('dbname') or not dsl.get('user'): # Проверяем основные параметры
    logger.error("One or more critical PostgreSQL connection parameters (host, port, dbname, user) are missing. Please check your .env file or its path.")
    # exit(1) # Раскомментируйте, если хотите прервать выполнение при отсутствии критичных параметров

BATCH_SIZE = 100 # Размер пакета данных для обработки за один раз


# Определение датакласса для таблицы film_work
@dataclass
class FilmWork:
    id: UUID
    title: str
    description: str | None
    creation_date: date | None
    rating: float | None
    type: str # e.g., 'movie', 'tv_show'
    created: datetime    
    modified: datetime
 # Метод, вызываемый после инициализации объекта, для преобразования типов данных
    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name == 'id' and isinstance(value, str):
                setattr(self, field.name, UUID(value))
            elif field.type == date and isinstance(value, str):
                try:
                    setattr(self, field.name, datetime.strptime(value, '%Y-%m-%d').date())
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse date string '{value}' for {field.name} in FilmWork(id={self.id if hasattr(self, 'id') else 'N/A'}). Setting to None.")
                    setattr(self, field.name, None)
            elif field.type == datetime and isinstance(value, str):
                try:
                    dt_value = value.replace('Z', '+00:00') if 'Z' in value else value
                    
                    try:
                        setattr(self, field.name, datetime.fromisoformat(dt_value))
                    except ValueError:
                        setattr(self, field.name, datetime.strptime(dt_value, '%Y-%m-%d %H:%M:%S.%f%z' if '.' in dt_value and ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S%z' if ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S.%f' if '.' in dt_value else '%Y-%m-%d %H:%M:%S'))
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse datetime string '{value}' for {field.name} in FilmWork(id={self.id if hasattr(self, 'id') else 'N/A'}). Setting to None.")
                    setattr(self, field.name, None)
            elif field.name == 'rating' and value is not None:
                 setattr(self, field.name, float(value))

# Определение датакласса для таблицы person
@dataclass
class Person:
    id: UUID
    full_name: str
    created: datetime    
    modified: datetime
 # Метод, вызываемый после инициализации объекта, для преобразования типов данных
    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name == 'id' and isinstance(value, str):
                setattr(self, field.name, UUID(value))
            elif field.type == datetime and isinstance(value, str):
                try:
                    dt_value = value.replace('Z', '+00:00') if 'Z' in value else value
                    try:
                        setattr(self, field.name, datetime.fromisoformat(dt_value))
                    except ValueError:
                        setattr(self, field.name, datetime.strptime(dt_value, '%Y-%m-%d %H:%M:%S.%f%z' if '.' in dt_value and ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S%z' if ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S.%f' if '.' in dt_value else '%Y-%m-%d %H:%M:%S'))
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse datetime string '{value}' for {field.name} in Person(id={self.id if hasattr(self, 'id') else 'N/A'}). Setting to None.")
                    setattr(self, field.name, None)

# Определение датакласса для таблицы genre
@dataclass
class Genre:
    id: UUID
    name: str
    description: str | None
    created: datetime    
    modified: datetime
# Метод, вызываемый после инициализации объекта, для преобразования типов данных
    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name == 'id' and isinstance(value, str):
                setattr(self, field.name, UUID(value))
            elif field.type == datetime and isinstance(value, str):
                try:
                    dt_value = value.replace('Z', '+00:00') if 'Z' in value else value
                    try:
                        setattr(self, field.name, datetime.fromisoformat(dt_value))
                    except ValueError:
                        setattr(self, field.name, datetime.strptime(dt_value, '%Y-%m-%d %H:%M:%S.%f%z' if '.' in dt_value and ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S%z' if ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S.%f' if '.' in dt_value else '%Y-%m-%d %H:%M:%S'))
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse datetime string '{value}' for {field.name} in Genre(id={self.id if hasattr(self, 'id') else 'N/A'}). Setting to None.")
                    setattr(self, field.name, None)

# Определение датакласса для связующей таблицы genre_film_work
@dataclass
class GenreFilmWork:
    id: UUID
    film_work_id: UUID
    genre_id: UUID
    created: datetime 
# Метод, вызываемый после инициализации объекта, для преобразования типов данных
    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name in ('id', 'film_work_id', 'genre_id') and isinstance(value, str):
                setattr(self, field.name, UUID(value))
            elif field.type == datetime and isinstance(value, str):
                try:
                    dt_value = value.replace('Z', '+00:00') if 'Z' in value else value
                    try:
                        setattr(self, field.name, datetime.fromisoformat(dt_value))
                    except ValueError:
                        setattr(self, field.name, datetime.strptime(dt_value, '%Y-%m-%d %H:%M:%S.%f%z' if '.' in dt_value and ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S%z' if ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S.%f' if '.' in dt_value else '%Y-%m-%d %H:%M:%S'))
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse datetime string '{value}' for {field.name} in GenreFilmWork(id={self.id if hasattr(self, 'id') else 'N/A'}). Setting to None.")
                    setattr(self, field.name, None)

# Определение датакласса для связующей таблицы person_film_work
@dataclass
class PersonFilmWork:
    id: UUID
    film_work_id: UUID
    person_id: UUID
    role: str
    created: datetime 
# Метод, вызываемый после инициализации объекта, для преобразования типов данных
    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name in ('id', 'film_work_id', 'person_id') and isinstance(value, str):
                setattr(self, field.name, UUID(value))
            elif field.type == datetime and isinstance(value, str):
                try:
                    dt_value = value.replace('Z', '+00:00') if 'Z' in value else value
                    try:
                        setattr(self, field.name, datetime.fromisoformat(dt_value))
                    except ValueError:
                        setattr(self, field.name, datetime.strptime(dt_value, '%Y-%m-%d %H:%M:%S.%f%z' if '.' in dt_value and ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S%z' if ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S.%f' if '.' in dt_value else '%Y-%m-%d %H:%M:%S'))
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse datetime string '{value}' for {field.name} in PersonFilmWork(id={self.id if hasattr(self, 'id') else 'N/A'}). Setting to None.")
                    setattr(self, field.name, None)

# Функция для извлечения данных из таблицы SQLite порциями (батчами)
def extract_sqlite_data(sqlite_cursor: sqlite3.Cursor, table_name: str) -> Generator[list[sqlite3.Row], None, None]:
    logger.info(f"Extracting data from SQLite table: {table_name}")
    sqlite_cursor.execute(f"SELECT * FROM {table_name}")
    while results := sqlite_cursor.fetchmany(BATCH_SIZE):
        yield results

# Функция для преобразования порции данных из SQLite в список объектов соответствующего датакласса
def transform_to_dataclass(batch: list[sqlite3.Row], data_class: type) -> list:
    transformed_batch = []
    for row_num, row_data in enumerate(batch):
        # Преобразование имен полей из SQLite (_at) в поля дата-класса (без _at для created/modified)
        mapped_row_data = dict(row_data)
        if 'created_at' in mapped_row_data:
            mapped_row_data['created'] = mapped_row_data.pop('created_at')
        if 'updated_at' in mapped_row_data:
            mapped_row_data['modified'] = mapped_row_data.pop('updated_at')
        if 'update' in mapped_row_data and 'modified' not in mapped_row_data: # Для FilmWork.update, если оно еще есть
             mapped_row_data['modified'] = mapped_row_data.pop('update')
        # Удаляем file_path, так как его нет в датаклассе FilmWork и целевой таблице PG
        if data_class == FilmWork and 'file_path' in mapped_row_data:
            mapped_row_data.pop('file_path')
        try:
            transformed_batch.append(data_class(**mapped_row_data))
        except Exception as e:
            logger.error(f"Error transforming row #{row_num} for {data_class.__name__}: {mapped_row_data}. Original SQLite row: {dict(row_data)}. Error: {e}", exc_info=True)
    return transformed_batch

# Функция для загрузки порции преобразованных данных в таблицу PostgreSQL
def load_to_postgres(pg_cursor: psycopg.Cursor, table_name: str, columns: list[str], batch_data: list, conflict_target: str = "id"):
    if not batch_data:
        return
    logger.info(f"Loading batch of {len(batch_data)} items into PostgreSQL table: {table_name}")
    cols_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders}) ON CONFLICT ({conflict_target}) DO NOTHING"
    # ON CONFLICT ... DO NOTHING используется для игнорирования дубликатов по указанному ключу (conflict_target)
    try:
        batch_as_tuples = [astuple(item) for item in batch_data]
        pg_cursor.executemany(query, batch_as_tuples)
    except psycopg.Error as e:
        logger.error(f"PostgreSQL error loading data into {table_name}. Query: {query[:200]}... Error: {e}", exc_info=True)
        raise # Re-raise to allow transaction rollback
    except Exception as e:
        logger.error(f"Unexpected error loading data into {table_name}. Error: {e}", exc_info=True)
        raise

# Функция для тестирования корректности переноса данных между SQLite и PostgreSQL
def test_data_transfer(sqlite_cursor: sqlite3.Cursor, pg_cursor: psycopg.Cursor, table_name: str, data_class: type, pk_column: str = "id"):
    logger.info(f"Starting data transfer test for table: {table_name}")
    sqlite_source_table_for_test = table_name # По умолчанию, если table_name - это имя таблицы SQLite
    for config_key, config_value in TABLE_CONFIGS.items():
        if f"content.{config_key}" == table_name: # Если table_name - это целевая таблица PG (content.table)
            sqlite_source_table_for_test = config_value["sqlite_source_table"]
            break
    sqlite_cursor.execute(f"SELECT COUNT(*) FROM {sqlite_source_table_for_test}")
    sqlite_count = sqlite_cursor.fetchone()[0]
    logger.info(f"SQLite table '{sqlite_source_table_for_test}' count: {sqlite_count}")

    pg_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    pg_row = pg_cursor.fetchone()
    pg_count = pg_row['count'] if pg_row else 0 # Доступ по имени столбца 'count'
    logger.info(f"PostgreSQL table '{table_name}' count: {pg_count}")
    # Проверка совпадения количества записей
    assert sqlite_count == pg_count, f"Count mismatch for {table_name}: SQLite ({sqlite_count}) != PG ({pg_count})"
    logger.info(f"Count check passed for {table_name}.")

# Проверка содержимого случайной выборки данных (только если есть данные)
    # Для проверки содержимого SQLite, нам нужно имя исходной таблицы
    sqlite_table_for_test = table_name # По умолчанию, если это не PG таблица с префиксом схемы
    for config_name, config_details in TABLE_CONFIGS.items():
        if f"content.{config_name}" == table_name: # Если table_name - это целевая таблица PG
            sqlite_table_for_test = config_details["sqlite_source_table"]
            break


    if sqlite_count == 0:
        logger.info(f"No data in SQLite table {sqlite_source_table_for_test} (testing against PG table {table_name}) to test content.")
        return

    # Извлечение выборки из SQLite
    sqlite_cursor.execute(f"SELECT * FROM {sqlite_source_table_for_test} ORDER BY {pk_column} LIMIT {min(BATCH_SIZE // 2, 10)}")
    original_data_sample_raw = sqlite_cursor.fetchall()
    original_data_sample = []
    for row in original_data_sample_raw:
        mapped_row_data = dict(row)
        if 'created_at' in mapped_row_data:
            mapped_row_data['created'] = mapped_row_data.pop('created_at')
        if 'updated_at' in mapped_row_data:
            mapped_row_data['modified'] = mapped_row_data.pop('updated_at')
        if data_class == FilmWork and 'file_path' in mapped_row_data:
            mapped_row_data.pop('file_path')
        original_data_sample.append(data_class(**mapped_row_data))


    if not original_data_sample:
        logger.info(f"No sample data fetched from SQLite table {table_name} for content test.")
        return

    ids_sample = [getattr(item, pk_column) for item in original_data_sample]

 # Убеждаемся, что курсор PostgreSQL возвращает строки как словари (установлено при создании соединения)
    pg_cursor.execute(f"SELECT * FROM {table_name} WHERE {pk_column} = ANY(%s) ORDER BY {pk_column}", (ids_sample,))
    transferred_data_sample_raw = pg_cursor.fetchall()  # список словарей
    transferred_data_sample = [data_class(**row_dict) for row_dict in transferred_data_sample_raw]
    # Проверка совпадения размера выборки
    assert len(original_data_sample) == len(transferred_data_sample), \
        f"Sample batch size mismatch for {table_name}: SQLite ({len(original_data_sample)}) != PG ({len(transferred_data_sample)})"
    # Сортировка выборок для корректного сравнения
    original_data_sample.sort(key=lambda x: getattr(x, pk_column))
    transferred_data_sample.sort(key=lambda x: getattr(x, pk_column))
    # Построчное сравнение данных в выборках
    for original, transferred in zip(original_data_sample, transferred_data_sample):
        assert original == transferred, \
            f"Data mismatch for {table_name} on ID {getattr(original, pk_column)}:\nOriginal: {original}\nPG_copy:  {transferred}"

    logger.info(f"Content check passed for a sample from {table_name}.")

# Конфигурация для каждой таблицы, участвующей в миграции
TABLE_CONFIGS = {
    "film_work": {

        "sqlite_source_table": "film_work",
        "dataclass": FilmWork,
        "columns": [f.name for f in fields(FilmWork)], # Автоматически из датакласса
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
      
        "sqlite_source_table": "genre_film_work", # Исправлено с genre_filmwork
        "dataclass": GenreFilmWork,
        "columns": [f.name for f in fields(GenreFilmWork)],
        "pk_column": "id", # Первичный ключ для выборки при тестировании
        "conflict_target": "film_work_id, genre_id" # Ключи для обработки конфликтов (уникальность связи)
    },
    "person_film_work": {
        "sqlite_source_table": "person_film_work",
        "dataclass": PersonFilmWork,
        "columns": [f.name for f in fields(PersonFilmWork)],
        "pk_column": "id",  # Первичный ключ для выборки при тестировании
        "conflict_target": "film_work_id, person_id, role" # Ключи для обработки конфликтов (уникальность связи с ролью)

    },
}

# Порядок миграции таблиц (важен для соблюдения ограничений внешних ключей)
MIGRATION_ORDER = ["genre", "person", "film_work", "genre_film_work", "person_film_work"]

# Основной блок выполнения скрипта
if __name__ == '__main__':
    logger.info("Starting data migration process.")
    if not all(dsl.get(k) for k in ['host', 'port', 'dbname', 'user', 'password']):
        logger.error("One or more critical PostgreSQL connection parameters are missing from DSL. Please check your .env file and its loading. Exiting.")
        exit(1)
    try:
        # Установление соединений с базами данных SQLite и PostgreSQL
        # closing используется для гарантированного закрытия соединений
        with closing(sqlite3.connect(r'd:\work\new_admin_panel_sprint_1\sqlite_to_postgres\db.sqlite')) as sqlite_conn, \
             closing(psycopg.connect(**dsl, row_factory=dict_row)) as pg_conn:
            sqlite_conn.row_factory = sqlite3.Row
            for table_name in MIGRATION_ORDER:
                if table_name not in TABLE_CONFIGS:
                    logger.warning(f"No configuration found for table {table_name}, skipping.")
                    continue

                config = TABLE_CONFIGS[table_name]
                sqlite_source_table = config["sqlite_source_table"]
                # Имя целевой таблицы в PostgreSQL, включая схему
                pg_target_table = f"content.{table_name}"

                logger.info(f"--- Processing SQLite table '{sqlite_source_table}' -> PG table '{pg_target_table}' ---")
                # Создание курсоров для текущей таблицы
                with closing(sqlite_conn.cursor()) as sqlite_cur, \
                     closing(pg_conn.cursor()) as pg_cur:
                    # Создание генератора для извлечения и преобразования данных
                    data_to_load_generator = (
                        transform_to_dataclass(batch, config["dataclass"])
                        for batch in extract_sqlite_data(sqlite_cur, sqlite_source_table)
                    )
                    # Итерация по преобразованным порциям данных и их загрузка в PostgreSQL
                    for transformed_batch in data_to_load_generator:
                        if transformed_batch:
                            load_to_postgres(
                                pg_cur,
                                pg_target_table,
                                config["columns"],
                                transformed_batch,
                                config.get("conflict_target", "id")
                            )
                    pg_conn.commit() # Фиксация транзакции в PostgreSQL после обработки каждой таблицы
                    logger.info(f"Data loading and commit complete for PG table: {pg_target_table}")

                # Тестирование после загрузки и коммита данных для таблицы
                with closing(sqlite_conn.cursor()) as sqlite_cur_test, \
                     closing(pg_conn.cursor()) as pg_cur_test:
                    test_data_transfer(
                        sqlite_cur_test,
                        pg_cur_test,
                        pg_target_table,
                        config["dataclass"],
                        config["pk_column"]
                    )
                logger.info(f"--- Successfully processed and tested data from SQLite table: {sqlite_source_table} to PG table: {pg_target_table} ---")

        logger.info('🎉 Данные успешно перенесены и протестированы для всех сконфигурированных таблиц!')
    # Обработка специфических ошибок баз данных
    except sqlite3.Error as e:
        logger.critical(f"SQLite error during migration: {e}", exc_info=True)
    except psycopg.Error as e:
        logger.critical(f"PostgreSQL error during migration: {e}", exc_info=True)
         # Попытка отката транзакции в PostgreSQL, если соединение было установлено
        if 'pg_conn' in locals() and pg_conn and not pg_conn.closed:
            try:
                pg_conn.rollback()
                logger.info("PostgreSQL transaction rolled back.")
            except Exception as rb_e:
                logger.error(f"Error during rollback: {rb_e}")
    except Exception as e:
        # Обработка любых других непредвиденных ошибок
        logger.critical(f"An unexpected error occurred during migration: {e}", exc_info=True)