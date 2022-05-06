import abc
from typing import Any


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        pass


class State:

    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.states = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any, auto_flush=True):
        self.states[key] = value
        if auto_flush:
            self.storage.save_state(self.states)

    def get_state(self, key: str) -> Any:
        return self.states.get(key, None)

    def rm_state(self, key: str, auto_flush=True):
        self.states.pop(key, None)
        if auto_flush:
            self.storage.save_state(self.states)
