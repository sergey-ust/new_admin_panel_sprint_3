"""
Elasticsearch models
"""

from uuid import UUID

from pydantic import BaseModel


class Person(BaseModel):
    id: UUID
    name: str


class FilmWork(BaseModel):
    id: UUID
    imdb_rating: float
    genre: str
    title: str
    description: str
    director: list[str]
    actors_names: list[str]
    writers_names: list[str]
    actors: list[Person]
    writers: list[Person]

    @staticmethod
    def create_from_sql(**kwargs):
        roler = lambda role: [
            Person(id=i["id"], name=i["name"]) for i in kwargs["persons"] if
            i["role"] == role
        ]
        actors = roler("actor")
        directors = roler("director")
        writers = roler("writer")

        return FilmWork(
            id=kwargs["id"],
            imdb_rating=kwargs["rating"],
            genre=", ".join(kwargs["genres"]),
            title=kwargs["title"],
            description=kwargs["description"],
            director=[i.name for i in directors],
            actors_names=[i.name for i in actors],
            writers_names=[i.name for i in writers],
            actors=actors,
            writers=writers
        )
