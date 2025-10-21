import logging
import time
from datetime import datetime, timezone
from typing import Generator, List, Set, Tuple

import psycopg
from psycopg.rows import dict_row

from .es_loader import ElasticsearchLoader
from .decorators import backoff
from .logging_config import setup_logging
from .settings import BATCH_SIZE, dsl, ETL_SLEEP_INTERVAL
from .state import JsonFileStorage, State

setup_logging()
logger = logging.getLogger(__name__)


class ETLProcess:
    """
    Основной класс для выполнения ETL-процесса.
    Реализует инкрементальную загрузку данных из PostgreSQL в Elasticsearch.
    """

    def __init__(self, pg_conn, es_loader: ElasticsearchLoader, state: State):
        self.pg_conn = pg_conn
        self.es_loader = es_loader
        self.state = state

    def _get_updated_ids(self, table: str, last_modified: str) -> Set[str]:
        """Получает ID записей, обновленных после last_modified."""
        query = f"""
            SELECT id
            FROM content.{table}
            WHERE modified > %s
            ORDER BY modified;
        """
        with self.pg_conn.cursor() as cursor:
            cursor.execute(query, (last_modified,))
            updated_ids = {row[0] for row in cursor.fetchall()}
            logger.info(f"Found {len(updated_ids)} updated records in '{table}' table.")
            return updated_ids

    def _fetch_film_work_ids(self, cursor, query: str, ids: Set[str]) -> Set[str]:
        """Вспомогательный метод для выполнения запроса и получения ID кинопроизведений."""
        if not ids:
            return set()
        cursor.execute(query, (list(ids),))
        return {row[0] for row in cursor.fetchall()}

    def _get_film_works_by_related_ids(
        self, person_ids: Set[str], genre_ids: Set[str]
    ) -> Generator[Tuple[str], None, None]:
        """
        Получает ID кинопроизведений, связанных с обновленными персонами или жанрами.
        """
        if not (person_ids or genre_ids):
            logger.info("No updated persons or genres, skipping film_work fetch.")
            return

        film_work_ids = set()
        with self.pg_conn.cursor() as cursor:
            person_fw_query = "SELECT DISTINCT pfw.film_work_id FROM content.person_film_work pfw WHERE pfw.person_id = ANY(%s);"
            film_work_ids.update(
                self._fetch_film_work_ids(cursor, person_fw_query, person_ids)
            )

            genre_fw_query = "SELECT DISTINCT gfw.film_work_id FROM content.genre_film_work gfw WHERE gfw.genre_id = ANY(%s);"
            film_work_ids.update(
                self._fetch_film_work_ids(cursor, genre_fw_query, genre_ids)
            )

        logger.info(f"Found {len(film_work_ids)} related film_works to update.")

        # Отдаем ID пачками для дальнейшей обработки
        film_work_ids_list = list(film_work_ids)
        for i in range(0, len(film_work_ids_list), BATCH_SIZE):
            yield tuple(film_work_ids_list[i: i + BATCH_SIZE])

    def run(self):
        """Запускает полный цикл ETL."""
        logger.info("Starting ETL cycle...")
        current_time = datetime.now(timezone.utc).isoformat()
        last_modified_person = self.state.get_state("last_modified_person", datetime.min.isoformat())
        last_modified_genre = self.state.get_state("last_modified_genre", datetime.min.isoformat())

        # 1. Получить ID обновленных персон и жанров
        updated_person_ids = self._get_updated_ids("person", last_modified_person)
        updated_genre_ids = self._get_updated_ids("genre", last_modified_genre)

        # 2. Получить ID кинопроизведений, связанных с изменениями
        film_work_ids_batches = self._get_film_works_by_related_ids(
            updated_person_ids, updated_genre_ids
        )

        # 3. Обогатить и загрузить данные в Elasticsearch
        total_indexed = 0
        for fw_ids_batch in film_work_ids_batches:
            if not fw_ids_batch:
                continue
            enriched_data = self.es_loader.get_enriched_data_from_pg(fw_ids_batch)
            indexed_count = self.es_loader.bulk_index_to_es(enriched_data)
            total_indexed += indexed_count

        if total_indexed > 0:
            logger.info(f"Successfully indexed {total_indexed} documents in Elasticsearch.")
        else:
            logger.info("No new data to index in this cycle.")

        # 4. Сохранить новое состояние
        self.state.set_state("last_modified_person", current_time)
        self.state.set_state("last_modified_genre", current_time)
        logger.info(f"ETL cycle finished. New state time: {current_time}")


@backoff(start_sleep_time=1, factor=2, border_sleep_time=60)
def main():
    """Основная функция, запускающая ETL-процесс в бесконечном цикле."""
    storage = JsonFileStorage("/app/state/etl_state.json")
    state = State(storage)
    es_loader = ElasticsearchLoader()
 
    while True:
        try:
            with psycopg.connect(**dsl, row_factory=dict_row) as pg_conn:
                logger.info("Successfully connected to PostgreSQL.")
                
                # Создаем экземпляр ETLProcess с активным соединением
                etl_process = ETLProcess(pg_conn, es_loader, state)
                etl_process.run()
 
        except psycopg.Error as pg_err:
            logger.error(f"PostgreSQL connection or query error: {pg_err}", exc_info=True)
        except Exception as e:
            logger.critical(f"An unexpected error occurred in the ETL main loop: {e}", exc_info=True)
        finally:
            logger.info(f"Waiting for the next ETL cycle ({ETL_SLEEP_INTERVAL} seconds)...")
            time.sleep(ETL_SLEEP_INTERVAL)


if __name__ == "__main__":
    main()