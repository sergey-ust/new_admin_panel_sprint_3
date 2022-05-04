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
        film_works_path: str,
        req_limit: int = psql_helper.Connection.REQUEST_MAX_ENTRIES
):
    # connect and restore
    states = State(JsonFileStorage(state_path))
    fw_states = State(JsonFileStorage(film_works_path))
    es = es_helper.Connection()
    psql = psql_helper.Connection()
    table_state = states.get_state(table.value)
    if not table_state:
        table_state = TableState.create_empty()
    else:
        table_state = TableState(**table_state)
    # extract
    offset = 0 if table_state.position < 0 else table_state.position
    ids = psql.get_modified(
        table.value,
        table_state.timestamp,
        offset,
        req_limit
    )
    latest_date = ids[0][1]
    print(latest_date)
    fw_ids = [i[0] for i in ids]
    if table.value != TableName.FILM_WORK.value:
        # FixMe there could be too many IDs(should be limit and offset). It's crytical for genre
        fw_ids = psql.get_fw_id_by_table(table.value, fw_ids)

    # check if FirmWare model was already updated by other field
    fw_ids = [
        i for i in fw_ids if
        not (res := fw_states.get_state(str(i))) or res < latest_date
    ]
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
    load_time = datetime.utcnow()
    fw_models = [FilmWork.create_from_sql(**fw).dict() for fw in film_works]
    es.post_bulk(fw_models)
    # store entries and table state
    for entry in fw_models[: -1]:
        fw_states.set_state(str(entry.id), str(load_time), False)
    # save all cached
    fw_states.set_state(str(fw_models[-1].id), str(load_time))

    if table_state.position < 0:
        table_state.position = 0
        table_state.next_timestamp = load_time

    table_state.position += req_limit
    states.set_state(table.value, table_state.dict())


def main():
    es = es_helper.Connection()
    if not es.is_exist("movies"):
        logger.error("Create index first")
        return -1

    update_table(TableName.GENRE, "states.json", "fw_logs.json")
    update_table(TableName.PERSON, "states.json", "fw_logs.json")
    update_table(TableName.FILM_WORK, "states.json", "fw_logs.json")


if __name__ == '__main__':
    main()
