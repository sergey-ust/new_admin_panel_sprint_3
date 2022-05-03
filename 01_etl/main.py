import logging
from datetime import datetime

import es_helper
from model import FilmWork
import psql_helper
from state.table_state import TableState, Name as TableName
from state.saver import State
from state.storage import JsonFileStorage

logging.basicConfig(format="%(asctime)s[%(name)s]: %(message)s", level="INFO")
logger = logging.getLogger(__name__)


def update_persons(
        state_path: str,
        req_limit: int = psql_helper.Connection.REQUEST_MAX_ENTRIES
):
    # connect and restore
    states = State(JsonFileStorage(state_path))
    es = es_helper.Connection()
    psql = psql_helper.Connection()
    table_state = states.get_state(TableName.PERSON.value)
    if not table_state:
        table_state = TableState.create_empty()
    else:
        table_state = TableState(**table_state)
    # extract
    offset = 0 if table_state.position < 0 else table_state.position
    fw_ids = psql.get_fw_id(
        TableName.PERSON.value,
        table_state.timestamp,
        offset,
        req_limit
    )
    # FixMe remove already updated filmworks
    if not fw_ids:
        table_state.position = -1
        states.set_state(str(TableName.PERSON), table_state.dict())
        return 0
    logger.info(
        "Changes in {} occures update for film works: {}.".format(
            TableName.PERSON.value,
            fw_ids)
    )
    film_works = psql.get_filmworks(fw_ids)
    now_time = datetime.utcnow()
    # convert to elasticsearch mapping
    fw_models = [FilmWork.create_from_sql(**fw) for fw in film_works]
    es.post(fw_models[0].dict(), identifier=fw_models[0].id)
    # store entries and table state
    for entry in fw_models:
        states.set_state(str(entry.id), str(now_time), False)

    if table_state.position < 0:
        table_state.position = 0
        table_state.timestamp = now_time

    table_state.position += req_limit
    states.set_state(TableName.PERSON.value, table_state.dict())


def main():
    es = es_helper.Connection()
    if not es.is_exist("movies"):
        logger.error("Create index first")
        return -1

    update_persons("states.json")


if __name__ == '__main__':
    main()
