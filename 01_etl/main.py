import logging
from datetime import datetime, timezone

import es_helper
import psql_helper
from state.saver import State
from state.storage import JsonFileStorage


logging.basicConfig(format="%(asctime)s[%(name)s]: %(message)s", level="INFO")
logger = logging.getLogger(__name__)


def main():
    states = State(JsonFileStorage("states.json"))
    es = es_helper.Connection()
    exist = not es.is_exist("movies") and states.get_state("psql") is not None
    psql = psql_helper.Connection()
    timestamp = datetime.utcnow() if exist \
        else datetime.fromtimestamp(0.0, timezone.utc)
    persons_ids = psql.get_modified("content.person", timestamp, 0)
    logger.debug(f"Persons {persons_ids} were updated since {timestamp}.")


if __name__ == '__main__':
    main()
