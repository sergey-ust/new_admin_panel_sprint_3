import logging
from time import sleep

from dotenv import load_dotenv
from urllib3.exceptions import NewConnectionError, ProtocolError

import es_helper
from backoff import backoff
from etl import Etl
import es_index_schema
from state.table_state import Name as TableName
from state.saver import State
from state.storage import JsonFileStorage

load_dotenv()
logging.basicConfig(format="%(asctime)s[%(name)s]: %(message)s", level="INFO")
logger = logging.getLogger(__name__)


def delete_entry(uid: str, fw_state: State):
    es = es_helper.create_connection()
    es.delete_bulk([uid])
    fw_state.rm_state(uid)


UPD_TURNS = {
    TableName.GENRE.value: TableName.PERSON.value,
    TableName.PERSON.value: TableName.FILM_WORK.value,
    TableName.FILM_WORK.value: TableName.GENRE.value,
}


@backoff([ProtocolError, NewConnectionError, ],
         excp_logger=lambda: logger.exception("Index creation problems"))
def create_es_index():
    es = es_helper.create_connection()
    if not es.is_exist("movies"):
        es.create_index("movies", es_index_schema.get())


def main():
    create_es_index()

    states = State(JsonFileStorage("logs/states.json"))
    fw_states = State(JsonFileStorage("logs/states_fw.json"))
    turn = t if (t := states.get_state("turn")) else TableName.GENRE.value

    while True:
        etl = Etl(turn, states, fw_states)
        etl.action()
        turn = UPD_TURNS[turn]
        states.set_state("turn", turn)
        sleep(1)


if __name__ == '__main__':
    main()
