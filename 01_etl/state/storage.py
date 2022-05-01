import json
from typing import Optional

from redis import Redis

from state.saver import BaseStorage


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        if self.file_path is None:
            return
        data = json.dumps(state)
        with open(self.file_path, "w") as wr_file:
            wr_file.write(data)

    def retrieve_state(self) -> dict:
        if self.file_path is None:
            return {}
        text = ""
        try:
            with open(self.file_path, "r") as r_file:
                text = r_file.read()
        except:
            return {}
        try:
            states = json.loads(text)
        except Exception as _exp:
            return {}

        return states


class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter

    def save_state(self, state: dict) -> None:
        for k, v in state.items():
            try:
                self.redis_adapter.set(k, v)
            except:
                pass

    def retrieve_state(self) -> dict:
        state = {}
        for k in self.redis_adptor.keys():
            try:
                self.state[k] = self.redis_adapter.get(k)
            except:
                pass
