import logging
from collections import defaultdict
from typing import Dict, List, Tuple

import psycopg
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
from .decorators import backoff
from .settings import ES_HOST, ES_PORT, ES_INDEX_MOVIES, BATCH_SIZE

logger = logging.getLogger(__name__)


class ElasticsearchLoader:
    def __init__(self, pg_dsl: dict):
        self.pg_dsl = pg_dsl
        self.es_client = None
        self.es_client = self._connect_to_elasticsearch()

    @backoff(start_sleep_time=0.5, factor=2, border_sleep_time=20)
    def _connect_to_elasticsearch(self):
        """Connects to Elasticsearch with retry logic."""
        client = Elasticsearch(
            hosts=[{'host': ES_HOST, 'port': int(ES_PORT), 'scheme': 'http'}],
            request_timeout=30,
            verify_certs=False,
            ssl_show_warn=False
        )
        if not client.ping():
            raise ConnectionError(f"Elasticsearch ping failed at {ES_HOST}:{ES_PORT}")
        logger.info(f"Successfully connected to Elasticsearch at {ES_HOST}:{ES_PORT}")
        return client

    def _create_index_if_not_exists(self, index_name: str, mappings: dict):
        """Creates an Elasticsearch index with specified mappings if it doesn't exist."""
        if not self.es_client.indices.exists(index=index_name):
            logger.info(f"Creating Elasticsearch index: {index_name}")
            body = {
                "settings": {
                    "refresh_interval": "1s",
                    "analysis": {
                        "filter": {
                            "english_stop": {"type": "stop", "stopwords": "_english_"},
                            "english_stemmer": {"type": "stemmer", "language": "english"},
                            "english_possessive_stemmer": {"type": "stemmer", "language": "possessive_english"},
                            "russian_stop": {"type": "stop", "stopwords": "_russian_"},
                            "russian_stemmer": {"type": "stemmer", "language": "russian"}
                        },
                        "analyzer": {
                            "ru_en": {
                                "tokenizer": "standard",
                                "filter": [
                                    "lowercase",
                                    "english_stop",
                                    "english_stemmer",
                                    "english_possessive_stemmer",
                                    "russian_stop",
                                    "russian_stemmer"
                                ]
                            }
                        }
                    }
                },
                "mappings": {
                    "dynamic": "strict",
                    "properties": {
                        "id": {"type": "keyword"},
                        "imdb_rating": {"type": "float"},
                        "genres": {"type": "keyword"},
                        "title": {"type": "text", "analyzer": "ru_en", "fields": {"raw": {"type": "keyword"}}},
                        "description": {"type": "text", "analyzer": "ru_en"},
                        "directors_names": {"type": "text", "analyzer": "ru_en"},
                        "actors_names": {"type": "text", "analyzer": "ru_en"},
                        "writers_names": {"type": "text", "analyzer": "ru_en"},
                        "directors": {
                            "type": "nested", "dynamic": "strict",
                            "properties": {"id": {"type": "keyword"}, "name": {"type": "text", "analyzer": "ru_en"}}
                        },
                        "actors": {
                            "type": "nested", "dynamic": "strict",
                            "properties": {"id": {"type": "keyword"}, "name": {"type": "text", "analyzer": "ru_en"}}
                        },
                        "writers": {
                            "type": "nested", "dynamic": "strict",
                            "properties": {"id": {"type": "keyword"}, "name": {"type": "text", "analyzer": "ru_en"}}
                        }
                    }
                }
            }
            self.es_client.indices.create(index=index_name, body=body)
        else:
            logger.info(f"Elasticsearch index '{index_name}' already exists.")

    def get_enriched_data_from_pg(self, film_work_ids: Tuple[str]) -> List[Dict]:
        """
        Извлекает обогащенные данные по кинопроизведениям из PostgreSQL.
        """
        if not film_work_ids:
            return []

        logger.info(f"Enriching data for {len(film_work_ids)} film_works...")
        query = """
            SELECT
                fw.id as fw_id,
                fw.title,
                fw.description,
                fw.rating,
                fw.type,
                fw.created,
                fw.modified,
                pfw.role,
                p.id as person_id,
                p.full_name as person_name,
                g.id as genre_id,
                g.name as genre_name
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON fw.id = gfw.film_work_id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id = ANY(%s);
        """
        with psycopg.connect(**self.pg_dsl) as pg_conn, pg_conn.cursor() as cursor:
            cursor.execute(query, [list(film_work_ids)])
            rows = cursor.fetchall()

        # Группировка данных
        film_works = defaultdict(lambda: {
            "id": None, "title": None, "description": None, "imdb_rating": None,
            "genres": set(), "directors": set(), "actors": set(), "writers": set()
        })

        for row in rows:
            fw_id = str(row[0])
            film_works[fw_id]['id'] = fw_id
            film_works[fw_id]['title'] = row[1]
            film_works[fw_id]['description'] = row[2]
            film_works[fw_id]['imdb_rating'] = row[3]
            if row[8] and row[9]: # person_id, person_name
                film_works[fw_id][f"{row[7]}s"].add((str(row[8]), row[9]))
            if row[10] and row[11]: # genre_id, genre_name
                film_works[fw_id]['genres'].add((str(row[10]), row[11]))

        # Преобразование set в list of dicts
        result = []
        for fw_id, data in film_works.items():
            # Handle 'N/A' values and rename rating field
            data['imdb_rating'] = data.get('imdb_rating') if data.get('imdb_rating') is not None else 0.0
            if data.get('description') == 'N/A':
                data['description'] = None

            data['genres'] = [gname for _, gname in data['genres']] if data['genres'] else []
            data['directors'] = [{'id': pid, 'name': pname} for pid, pname in data['directors']]
            data['actors'] = [{'id': pid, 'name': pname} for pid, pname in data['actors']]
            data['writers'] = [{'id': pid, 'name': pname} for pid, pname in data['writers']]
            data['directors_names'] = [p['name'] for p in data['directors']]
            data['actors_names'] = [p['name'] for p in data['actors']]
            data['writers_names'] = [p['name'] for p in data['writers']]
            result.append(data)

        return result

    @backoff()
    def bulk_index_to_es(self, documents: List[Dict]) -> int:
        """Выполняет массовую индексацию документов в Elasticsearch."""
        if not self.es_client:
            logger.error("Elasticsearch client not initialized. Cannot index data.")
            return 0
        if not documents:
            logger.info("No documents to index.")
            return 0

        self._create_index_if_not_exists(ES_INDEX_MOVIES, {})

        logger.info(f"Starting bulk indexing for {ES_INDEX_MOVIES}...")
        actions = [
            {"_index": ES_INDEX_MOVIES, "_id": doc['id'], "_source": doc}
            for doc in documents
        ]
        try:
            # raise_on_error=True, чтобы исключение было поймано декоратором backoff
            success, failed = bulk(self.es_client, actions, chunk_size=BATCH_SIZE, raise_on_error=True)
            logger.info(f"Bulk indexing for {ES_INDEX_MOVIES} completed. Success: {success}, Failed: {len(failed)}")
            return success
        except BulkIndexError as e:
            # Log the first 5 detailed errors for debugging
            logger.error("Detailed errors from BulkIndexError:")
            for i, error in enumerate(e.errors):
                if i >= 5:
                    break
                logger.error(f"  - Document ID {error.get('index', {}).get('_id', 'N/A')}: {error.get('index', {}).get('error', 'No error details')}")
            # Re-raise the exception to be caught by the @backoff decorator
            raise