import os
import sys
from pathlib import Path
import logging
from dotenv import load_dotenv
import time
from functools import wraps

def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time).

    Формула:
        t = start_sleep_time * (factor ^ n), если t < border_sleep_time
        t = border_sleep_time, иначе

    :param start_sleep_time: начальное время ожидания
    :param factor: во сколько раз нужно увеличивать время ожидания на каждой итерации
    :param border_sleep_time: максимальное время ожидания
    :return: результат выполнения функции
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            n = 0
            t = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:  # Перехватываем все исключения, связанные с подключением
                    logging.error(f"Ошибка при подключении: {e}. Повторная попытка через {t} секунд.")
                    time.sleep(t)
                    n += 1
                    t = min(start_sleep_time * (factor ** n), border_sleep_time)
        return inner
    return func_wrapper

import psycopg2
from elasticsearch import Elasticsearch, helpers


# --- Загрузка переменных окружения ---
# Скрипт находится в корне проекта, а .env в папке config
env_path = Path(__file__).resolve().parent / 'config' / 'et.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
else:
    logging.error(f"ОШИБКА: Файл окружения не найден по пути {env_path}. Выход.")
    sys.exit(1)

# --- Настройки подключения к PostgreSQL из .env ---
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", 5432))
PG_DBNAME = os.getenv("POSTGRES_DB")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")

if not all([PG_DBNAME, PG_USER, PG_PASSWORD]):
    print("ОШИБКА: Отсутствуют обязательные переменные окружения для PostgreSQL (POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD). Выход.")
    sys.exit(1)

# --- Настройки подключения к Elasticsearch из .env ---
ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = int(os.getenv("ES_PORT", 9200))
ES_INDEX = "movies"  #  Имя индекса в Elasticsearch

@backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
def _send_to_es(es_load_data):
    """
    Обертка для функции загрузки данных в Elasticsearch с повторными попытками.
    """
    return es_load_data()

@backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
def get_data_from_postgres(last_modified):
    """Извлекает данные из PostgreSQL, обновленные после указанной даты."""
    # Собираем все параметры подключения в один словарь
    dsl = {
        'dbname': PG_DBNAME,
        'user': PG_USER,
        'password': PG_PASSWORD,
        'host': PG_HOST,
        'port': PG_PORT,
    }

    try:
        # Используем with для автоматического закрытия соединения
        with psycopg2.connect(**dsl) as conn:
            # Используем with для автоматического закрытия курсора
            with conn.cursor() as cur:
                query = """
        SELECT
           fw.id,
           fw.title,
           fw.description,
           fw.rating,
           fw.type,
           fw.created,
           fw.modified,
           COALESCE (
               json_agg(
                   DISTINCT jsonb_build_object(
                       'person_role', pfw.role,
                       'person_id', p.id,
                       'person_name', p.full_name
                   )
               ) FILTER (WHERE p.id is not null),
               '[]'
           ) as persons,
           array_agg(DISTINCT g.name) as genres
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.modified > %s
        GROUP BY fw.id
        ORDER BY fw.modified
        LIMIT 100;
                """

                cur.execute(query, (last_modified,))  # Передаем last_modified как параметр
                return cur.fetchall()
    except psycopg2.OperationalError as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {e}")
        sys.exit(1)


def transform_data(data):
    """Преобразует данные из PostgreSQL в формат для Elasticsearch."""
    docs = []
    for row in data:
        doc = {
            "_index": ES_INDEX,
            "_id": row[0],
            "_source": {
                "id": row[0],
                "title": row[1],
                "description": row[2],
                "rating": row[3],
                "type": row[4],
                "created": row[5].isoformat() if row[5] else None,  # Преобразование даты в строку ISO
                "modified": row[6].isoformat() if row[6] else None,  # Преобразование даты в строку ISO
                "persons": row[7],
                "genres": row[8]
            }
        }
        docs.append(doc)
    return docs


def load_data_to_elasticsearch(docs):
    """Загружает данные в Elasticsearch."""
    try:
        es = Elasticsearch(hosts=[f"http://{ES_HOST}:{ES_PORT}"])

        if not es.indices.exists(index=ES_INDEX):
            #  Опционально: создаем индекс, если его нет
            es.indices.create(index=ES_INDEX, body={"mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "description": {"type": "text"},
                    "rating": {"type": "float"},
                    "type": {"type": "keyword"},
                    "created": {"type": "date"},
                    "modified": {"type": "date"},
                    "persons": {"type": "nested"}, # Указываем, что это вложенный объект
                    "genres": {"type": "keyword"}
                }
            }})

        if docs:  # Добавляем проверку на пустой список
            try:
                helpers.bulk(es, docs)
                logger.info(f"Successfully loaded {len(docs)} documents to Elasticsearch.")
            except helpers.BulkIndexError as e:
                logger.error(f"BulkIndexError: {e.errors}")
                raise
        else:
            print("No documents to load.")
    except Exception as e:
        logger.error(f"Elasticsearch connection error: {e}")
        sys.exit(1)

def get_data_from_postgres(last_modified):
    """Извлекает данные из PostgreSQL, обновленные после указанной даты."""
    # Собираем все параметры подключения в один словарь
    dsl = {
        'dbname': PG_DBNAME,
        'user': PG_USER,
        'password': PG_PASSWORD,
        'host': PG_HOST,
        'port': PG_PORT,
    }

    try:
        # Используем with для автоматического закрытия соединения
        with psycopg2.connect(**dsl) as conn:
            # Используем with для автоматического закрытия курсора
            with conn.cursor() as cur:
                query = """
        SELECT
           fw.id,
           fw.title,
           fw.description,
           fw.rating,
           fw.type,
           fw.created,
           fw.modified,
           COALESCE (
               json_agg(
                   DISTINCT jsonb_build_object(
                       'person_role', pfw.role,
                       'person_id', p.id,
                       'person_name', p.full_name
                   )
               ) FILTER (WHERE p.id is not null),
               '[]'
           ) as persons,
           array_agg(DISTINCT g.name) as genres
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.modified > %s
        GROUP BY fw.id
        ORDER BY fw.modified
        LIMIT 100;
                """

                cur.execute(query, (last_modified,))  # Передаем last_modified как параметр
                return cur.fetchall()
    except psycopg2.OperationalError as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {e}")
        sys.exit(1)


def transform_data(data):
    """Преобразует данные из PostgreSQL в формат для Elasticsearch."""
    docs = []
    for row in data:
        doc = {
            "_index": ES_INDEX,
            "_id": row[0],
            "_source": {
                "id": row[0],
                "title": row[1],
                "description": row[2],
                "rating": row[3],
                "type": row[4],
                "created": row[5].isoformat() if row[5] else None,  # Преобразование даты в строку ISO
                "modified": row[6].isoformat() if row[6] else None,  # Преобразование даты в строку ISO
                "persons": row[7],
                "genres": row[8]
            }
        }
        docs.append(doc)
    return docs


def load_data_to_elasticsearch(docs):
    """Загружает данные в Elasticsearch."""
    try:
        es = Elasticsearch(hosts=[f"http://{ES_HOST}:{ES_PORT}"])

        if not es.indices.exists(index=ES_INDEX):
            #  Опционально: создаем индекс, если его нет
            es.indices.create(index=ES_INDEX, body={"mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "description": {"type": "text"},
                    "rating": {"type": "float"},
                    "type": {"type": "keyword"},
                    "created": {"type": "date"},
                    "modified": {"type": "date"},
                    "persons": {"type": "nested"}, # Указываем, что это вложенный объект
                    "genres": {"type": "keyword"}
                }
            }})

        if docs:  # Добавляем проверку на пустой список
            try:
                helpers.bulk(es, docs)
                logger.info(f"Successfully loaded {len(docs)} documents to Elasticsearch.")
            except helpers.BulkIndexError as e:
                logger.error(f"BulkIndexError: {e.errors}")
                raise
        else:
            print("No documents to load.")
    except Exception as e:
        logger.error(f"Elasticsearch connection error: {e}")
        sys.exit(1)

def main():
    """Основная функция."""
    #  Пример использования:
    last_modified = "1970-01-01 00:00:00"  # Устанавливаем очень раннюю дату для первоначальной полной загрузки
    while True:
      data = get_data_from_postgres(last_modified)
      if not data:
          print("No new data. Exiting.")
          break
      docs = transform_data(data)
      _send_to_es(lambda: load_data_to_elasticsearch(docs))

      #  Обновляем last_modified для следующей итерации.  Берем максимальное значение из полученных данных
      last_modified = max(row[6] for row in data).isoformat()  if data else last_modified #  row[6] - это modified

if __name__ == "__main__":
    main()
