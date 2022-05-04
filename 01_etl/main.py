import logging
from datetime import datetime, timezone

import es_helper
from model import FilmWork
import psql_helper
from state.table_state import TableState, Name as TableName
from state.saver import State
from state.storage import JsonFileStorage

logging.basicConfig(format="%(asctime)s[%(name)s]: %(message)s", level="INFO")
logger = logging.getLogger(__name__)


def update_table(
        table_name: str,
        states: State,
        fw_states: State,
        req_limit: int = psql_helper.Connection.REQUEST_MAX_ENTRIES
):
    # connect and restore
    es = es_helper.Connection()
    psql = psql_helper.Connection()
    table_state = states.get_state(table_name)
    if not table_state:
        table_state = TableState.create_empty()
    else:
        table_state = TableState(**table_state)

    finished = False
    while not finished:
        # extract
        offset = 0 if table_state.position < 0 else table_state.position
        ids = psql.get_modified(
            table_name,
            table_state.timestamp,
            offset,
            req_limit
        )
        if not ids:
            # no updated data
            break

        fw_latest_upd = ids[0][1]
        fw_ids = [i[0] for i in ids]
        if table_name != TableName.FILM_WORK.value:
            # FixMe there could be too many IDs(should be limit and offset).
            #  It's critical for genre
            fw_ids = psql.get_fw_id_by_table(table_name, fw_ids)

        # check if FirmWare model was already updated by other field
        fw_ids = [
            i for i in fw_ids
            if not (res := fw_states.get_state(str(i))) or res < fw_latest_upd
        ]
        dl_time = datetime.now(tz=timezone.utc)

        if fw_ids:
            logger.info(
                "Changes in {} occures update for film works: {}.".format(
                    table_name,
                    fw_ids
                )
            )
            film_works = psql.get_filmworks(fw_ids)
            fw_models = [FilmWork.create_from_sql(**fw).dict() for fw in
                         film_works]
            if fw_models:
                es.post_bulk(fw_models)
                for entry in fw_models:
                    fw_states.set_state(str(entry["id"]), dl_time, False)
                # save all cached
                fw_states.set_state(str(fw_models[0]["id"]), dl_time)

        if table_state.position < 0:
            table_state.position = 0
            table_state.next_timestamp = dl_time

        table_state.position += req_limit
        states.set_state(table_name, table_state.dict())

    table_state.position = -1
    table_state.timestamp = table_state.next_timestamp
    states.set_state(table_name, table_state.dict())


UPD_TURNS = {
    TableName.GENRE.value: TableName.PERSON.value,
    TableName.PERSON.value: TableName.FILM_WORK.value,
    TableName.FILM_WORK.value: TableName.GENRE.value,
}


def main():
    es = es_helper.Connection()
    if not es.is_exist("movies"):
        logger.error("Create index first")
        return -1

    states = State(JsonFileStorage("states.json"))
    fw_states = State(JsonFileStorage("states_fw.json"))
    turn = states.get_state("turn")
    if not turn:
        turn = TableName.GENRE.value

    for i in range(len(UPD_TURNS)):
        update_table(turn, states, fw_states)
        turn = UPD_TURNS[turn]
        states.set_state("turn", turn)


if __name__ == '__main__':
    main()
