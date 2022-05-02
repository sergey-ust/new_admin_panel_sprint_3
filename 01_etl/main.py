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
    fw_ids = psql.get_fw_id("person", timestamp, 0)
    # FixMe remove already updated filmworks
    if not fw_ids:
        return 0
    logger.info(f"Film works {fw_ids} will be updated because person updates.")
    film_works = psql.get_filmworks(fw_ids)
    # FixMe add update film works {id + utc_datetime}


if __name__ == '__main__':
    main()
