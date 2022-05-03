from enum import Enum
from datetime import datetime, timezone

from pydantic import BaseModel


class Name(Enum):
    GENRE = "genre"
    PERSON = "person"
    FILMWORK = "filmwork"


class TableState(BaseModel):
    timestamp: datetime
    position: int

    @staticmethod
    def create_empty():
        return TableState(
            timestamp=datetime.fromtimestamp(0.0, timezone.utc),
            position=-1
        )

    def serialize(self) -> dict[str, int]:
        return {
            "timestamp": self.timestamp,
            "position": self.position
        }
