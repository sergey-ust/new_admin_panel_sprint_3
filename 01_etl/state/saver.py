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

    def set_state(self, key: str, value: Any) -> None:
        self.states[key] = value
        self.storage.save_state(self.states)

    def get_state(self, key: str) -> Any:
        # read_st = self.state.retrieve_state()
        read_st = self.states
        if key in read_st.keys():
            return read_st[key]
        return None
