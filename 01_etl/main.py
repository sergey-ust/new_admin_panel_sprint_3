import logging

from urllib3.exceptions import NewConnectionError, ProtocolError

import es_helper
from etl import Etl
from state.table_state import TableState, Name as TableName
from state.saver import State
from state.storage import JsonFileStorage

logging.basicConfig(format="%(asctime)s[%(name)s]: %(message)s", level="INFO")
logger = logging.getLogger(__name__)




UPD_TURNS = {
    TableName.GENRE.value: TableName.PERSON.value,
    TableName.PERSON.value: TableName.FILM_WORK.value,
    TableName.FILM_WORK.value: TableName.GENRE.value,
}


def main():
    es = es_helper.create_connection()
    try:
        if not es.is_exist("movies"):
            logger.error("Create index first")
            return -1
    except (ProtocolError, NewConnectionError) as error:
        logger.exception(f"Elasticsearch 'is_exist' error: {error}")

    states = State(JsonFileStorage("states.json"))
    fw_states = State(JsonFileStorage("states_fw.json"))
    turn = t if (t := states.get_state("turn")) else TableName.GENRE.value

    for i in range(len(UPD_TURNS)):
        etl = Etl(turn, states, fw_states)
        etl.action()
        turn = UPD_TURNS[turn]
        states.set_state("turn", turn)


if __name__ == '__main__':
    main()
