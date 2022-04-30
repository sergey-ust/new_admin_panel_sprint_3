from es_helper import Connection
from model import FilmWork, Person


def main():
    test = FilmWork(
        id="12345",
        imdb_rating=0.9,
        genre="comdey",
        title="trololo",
        description="ololo",
        director=["Bdij LL"],
        actors_names=["Bdij Bduff", "Bduff Bdij"],
        writers_names=["Some Any"],
        actors=[
            Person(id="678U", name="Jon B")
        ],
        writers=[]
    )
    es = Connection()
    es.post(test.dict(), identifier="12345")


if __name__ == '__main__':
    main()
