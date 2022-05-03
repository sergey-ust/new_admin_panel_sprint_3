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


def update_table(
        table: TableName,
        state_path: str,
        req_limit: int = psql_helper.Connection.REQUEST_MAX_ENTRIES
):
    # connect and restore
    states = State(JsonFileStorage(state_path))
    es = es_helper.Connection()
    psql = psql_helper.Connection()
    table_state = states.get_state(table.value)
    if not table_state:
        table_state = TableState.create_empty()
    else:
        table_state = TableState(**table_state)
    # extract
    offset = 0 if table_state.position < 0 else table_state.position
    fw_extractor = \
        psql.get_modified if table.value == TableName.FILM_WORK.value \
            else psql.get_fw_id

    # FixMe there could be too many IDs(should be limit and offset). It's crytical for genre
    fw_ids = fw_extractor(
        table.value,
        table_state.timestamp,
        offset,
        req_limit
    )
    # check if FirmWare model was already updated by other field
    # need_upd =  [for i in fw_ids if (res := states.get_state(str(i))) or res <   ]
    # FixMe remove already updated filmworks
    # states.get_apsent(fw_ids)
    if not fw_ids:
        table_state.position = -1
        table_state.timestamp = table_state.next_timestamp
        states.set_state(table.value, table_state.dict())
        return
    logger.info(
        "Changes in {} occures update for film works: {}.".format(
            table.value,
            fw_ids)
    )
    film_works = psql.get_filmworks(fw_ids)
    now_time = datetime.utcnow()
    # convert to elasticsearch mapping
    fw_models = [FilmWork.create_from_sql(**fw) for fw in film_works]
    # FixMe add bunch post
    es.post(fw_models[0].dict(), identifier=fw_models[0].id)
    # store entries and table state
    # FixMe such ids wouldn't work
    # for entry in fw_models:
    #     states.set_state(str(entry.id), str(now_time), False)

    if table_state.position < 0:
        table_state.position = 0
        table_state.next_timestamp = now_time

    table_state.position += req_limit
    states.set_state(table.value, table_state.dict())


def main():
    es = es_helper.Connection()
    if not es.is_exist("movies"):
        logger.error("Create index first")
        return -1

    update_table(TableName.GENRE, "states.json")
    update_table(TableName.PERSON, "states.json")
    update_table(TableName.FILM_WORK, "states.json")


if __name__ == '__main__':
    main()
