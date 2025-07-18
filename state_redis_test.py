import json
import pytest

# В etl.py уже есть реализация RedisStorage, импортируем её.
# Также импортируем BaseStorage для проверки соответствия интерфейсу.
from storage import BaseStorage, RedisStorage


class RedisMock:
    """
    Заглушка (mock) для Redis. Позволяет проводить тесты без реального
    подключения к Redis, эмулируя его поведение в памяти.
    """

    def __init__(self) -> None:
        self.data = {}

    def get(self, name):
        """Эмулирует команду GET."""
        return self.data.get(name)

    def set(self, name, value):
        """Эмулирует команду SET."""
        self.data[name] = value


def test_redis_storage_interface():
    """Проверяем, что RedisStorage реализует интерфейс BaseStorage."""
    assert issubclass(RedisStorage, BaseStorage)


def test_get_empty_state() -> None:
    """Тест: получение состояния из пустого хранилища."""
    redis_mock = RedisMock()
    storage = RedisStorage(redis_mock)

    # При отсутствии ключа должен возвращаться None или значение по умолчанию
    assert storage.get_state('key') is None
    assert storage.get_state('key', 'default_value') == 'default_value'


def test_save_new_state() -> None:
    """Тест: сохранение нового состояния."""
    redis_mock = RedisMock()
    storage = RedisStorage(redis_mock)

    storage.set_state('key1', 123)
    storage.set_state('key2', {'a': 1, 'b': 'c'})

    # RedisStorage использует json.dumps для сериализации, проверим это
    assert redis_mock.data == {'key1': '123', 'key2': '{"a": 1, "b": "c"}'}


def test_retrieve_existing_state() -> None:
    """Тест: получение существующего состояния."""
    redis_mock = RedisMock()
    # Заполняем mock данными, как будто они уже есть в Redis
    redis_mock.data = {'key1': '10', 'key2': '{"b": "c"}'}
    storage = RedisStorage(redis_mock)

    # Проверяем, что данные корректно десериализуются
    assert storage.get_state('key1') == 10
    assert storage.get_state('key2') == {'b': 'c'}


def test_save_and_retrieve_state() -> None:
    """Тест: сохранение и последующее получение состояния через разные экземпляры."""
    redis_mock = RedisMock()
    storage1 = RedisStorage(redis_mock)

    storage1.set_state('my_key', {"value": 42})

    # Новый экземпляр хранилища должен видеть данные, сохраненные первым
    storage2 = RedisStorage(redis_mock)
    assert storage2.get_state('my_key') == {"value": 42}


def test_error_on_corrupted_data() -> None:
    """Тест: проверка падения при некорректных (не-JSON) данных в хранилище."""
    redis_mock = RedisMock()
    redis_mock.data = {'key': 'this is not a valid json'}
    storage = RedisStorage(redis_mock)

    # pytest.raises проверяет, что код внутри блока with вызывает указанное исключение.
    # Это более идиоматичный и чистый способ тестирования исключений.
    with pytest.raises(json.JSONDecodeError):
        storage.get_state('key')