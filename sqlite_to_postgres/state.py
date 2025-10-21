import json
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class BaseStorage:
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в постоянное хранилище"""
        raise NotImplementedError

    def retrieve_state(self) -> Dict[str, Any]:
        """Загрузить состояние из постоянного хранилища"""
        raise NotImplementedError


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        with open(self.file_path, "w") as f:
            json.dump(state, f)
        logger.debug(f"State saved to {self.file_path}")

    def retrieve_state(self) -> Dict[str, Any]:
        try:
            with open(self.file_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"State file {self.file_path} not found. Returning empty state.")
            return {}


class State:
    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        self.state[key] = value
        self.storage.save_state(self.state)

    def get_state(self, key: str, default: Any = None) -> Any:
        return self.state.get(key, default)