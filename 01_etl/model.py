from pydantic import BaseModel


class Person(BaseModel):
    id: str
    name: str


class FilmWork(BaseModel):
    id: str
    imdb_rating: float
    genre: str
    title: str
    description: str
    director: list[str]
    actors_names: list[str]
    writers_names: list[str]
    actors: list[Person]
    writers: list[Person]
