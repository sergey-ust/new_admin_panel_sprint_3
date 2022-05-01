from datetime import datetime, timezone

import es_helper
import psql_helper
from state.saver import State
from state.storage import JsonFileStorage


def main():
    states = State(JsonFileStorage("states.json"))
    es = es_helper.Connection()
    exist = not es.is_exist("movies") and states.get_state("psql") is not None
    psql = psql_helper.Connection()
    timestamp = datetime.utcnow() if exist \
        else datetime.fromtimestamp(0.0, timezone.utc)
    persons = psql.get_modified("content.person", timestamp, 0)
    print(persons)


if __name__ == '__main__':
    main()
