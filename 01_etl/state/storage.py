import json
import logging
from typing import Optional

from redis import Redis

from state.saver import BaseStorage

logger = logging.getLogger(__name__)


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path
        if file_path is None:
            logger.warning("State folder doesn't set!")

    def save_state(self, state: dict) -> None:
        if self.file_path is None:
            return
        data = json.dumps(state, indent=4, sort_keys=True, default=str)
        with open(self.file_path, "w") as wr_file:
            wr_file.write(data)

    def retrieve_state(self) -> dict:
        if self.file_path is None:
            return {}
        text = ""
        try:
            with open(self.file_path, "r") as r_file:
                text = r_file.read()
        except Exception as e:
            logger.warning(
                f"Can't read {self.file_path} because of error: {e}!"
            )
            return {}
        states = None
        try:
            states = json.loads(text)
        except Exception:
            logger.warning(f"Can't convert '{text}' to json!")
        return states


class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter

    def save_state(self, state: dict):
        for k, v in state.items():
            try:
                self.redis_adapter.set(k, v)
            except Exception:
                pass

    def retrieve_state(self) -> dict:
        state = {}
        for k in self.redis_adapter.keys():
            try:
                state[k] = self.redis_adapter.get(k)
            except Exception:
                pass
        return state
