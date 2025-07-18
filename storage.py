import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any

import redis


class BaseStorage(ABC):
    """Абстрактный класс для хранилищ состояния."""

    @abstractmethod
    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для ключа."""
        pass

    @abstractmethod
    def get_state(self, key: str, default: Any = None) -> Any:
        """Получить состояние по ключу."""
        pass


class JsonFileStorage(BaseStorage):
    """Класс для работы с состоянием, которое хранится в JSON-файле."""

    def __init__(self, file_path: str):
        self.file_path = file_path

    def _read_all_states(self) -> dict:
        """Получить все состояния из файла."""
        if not os.path.exists(self.file_path):
            return {}
        try:
            with open(self.file_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            logging.warning("State file is corrupted or empty. Starting with a fresh state.")
            return {}

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для ключа, сохраняя в файл."""
        states = self._read_all_states()
        states[key] = value
        with open(self.file_path, 'w') as f:
            json.dump(states, f)
        logging.info(f"State updated in file: {key} = {value}")

    def get_state(self, key: str, default: Any = None) -> Any:
        """Получить состояние по ключу из файла."""
        return self._read_all_states().get(key, default)


class RedisStorage(BaseStorage):
    """Реализация хранилища, использующего Redis."""

    def __init__(self, redis_adapter: redis.Redis):
        self.redis = redis_adapter

    def set_state(self, key: str, value: Any) -> None:
        """Сохраняет состояние в Redis."""
        self.redis.set(key, json.dumps(value))
        logging.info(f"State updated in Redis: {key} = {value}")

    def get_state(self, key: str, default: Any = None) -> Any:
        """Получает состояние из Redis."""
        value = self.redis.get(key)
        if value is not None:
            return json.loads(value)
        return default