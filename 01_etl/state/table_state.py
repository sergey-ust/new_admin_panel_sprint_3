from enum import Enum
from datetime import datetime, timezone
from typing import Union

from pydantic import BaseModel


class Name(Enum):
    GENRE = "genre"
    PERSON = "person"
    FILM_WORK = "film_work"


class TableState(BaseModel):
    timestamp: datetime
    next_timestamp: datetime
    position: int

    @staticmethod
    def create_empty():
        return TableState(
            timestamp=datetime.fromtimestamp(0.0, timezone.utc),
            next_timestamp=datetime.fromtimestamp(0.0, timezone.utc),
            position=-1
        )

    def serialize(self) -> dict[str, Union[int, datetime]]:
        return {
            "timestamp": self.timestamp,
            "next_timestamp": self.next_timestamp,
            "position": self.position,
        }
