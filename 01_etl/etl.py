import logging
from datetime import datetime, timezone
from typing import Optional

from psycopg2 import OperationalError
from urllib3.exceptions import NewConnectionError

import es_helper
import psql_helper
from backoff import backoff
from model import FilmWork
from psql_helper import Connection as Sql
from state.saver import State
from state.table_state import TableState, Name as TableName

logger = logging.getLogger(__name__)


class Etl:

    def __init__(
            self,
            table_name: str,
            states: State,
            fw_states: State,
            req_limit: int = Sql.REQUEST_MAX_ENTRIES
    ):
        self._table_name = table_name
        self._states = states
        self._fw_states = fw_states
        self._req_limit = req_limit

    @backoff([OperationalError, ])
    def extract(self, table_state: TableState) -> Optional[list[dict]]:
        psql = psql_helper.create_connection()
        offset = table_state.position
        offset = offset if offset > 0 else 0
        ids_and_time = psql.get_modified(
            self._table_name,
            table_state.timestamp,
            offset,
            self._req_limit
        )
        if not ids_and_time:
            # everything_updated
            return None

        table_lst_upd = ids_and_time[0][1]
        fw_ids = [i[0] for i in ids_and_time]
        if self._table_name != TableName.FILM_WORK.value:
            fw_ids = psql.get_fw_id_by_table(
                self._table_name,
                fw_ids
            )
        # check if FilmWork model was already updated by other table
        fw_ids = [
            i for i in fw_ids
            if not (tm := self._fw_states.get_state(
                str(i))) or datetime.fromisoformat(tm) < table_lst_upd
        ]
        if fw_ids:
            logger.info(
                "Changes in {} occures update film works: {}.".format(
                    self._table_name,
                    fw_ids
                )
            )
            return psql.get_filmworks(fw_ids)
        return []

    @staticmethod
    def transform(film_works: list[dict]) -> list[dict]:
        return [
            FilmWork.create_from_sql(**fw).dict() for fw in
            film_works
        ]

    @backoff([NewConnectionError, ])
    def load(self, films: list[dict]):
        es = es_helper.create_connection()
        es.post_bulk(films)

    def action(self):
        table_state = TableState.create_empty()
        if rd_state := self._states.get_state(self._table_name):
            table_state = TableState(**rd_state)

        while True:
            films = self.extract(table_state)
            dl_time = datetime.now(tz=timezone.utc)
            if films is None:
                # no_updates in table
                if table_state.position < 0:
                    table_state.next_timestamp = dl_time
                break

            if es_films := self.transform(films):
                self.load(es_films)
                self._save_updates(es_films, dl_time)
            # safe the iteration state
            self._save_state(table_state, dl_time)
        # update finished
        self._save_result(table_state)

    def _save_updates(self, es_films: dict, dl_time: datetime):
        # update cache
        for entry in es_films[: -1]:
            self._fw_states.set_state(
                str(entry["id"]),
                dl_time.isoformat(),
                False
            )
        # save all cached
        self._fw_states.set_state(str(es_films[-1]["id"]), dl_time.isoformat())

    def _save_state(self, table_state: TableState, dl_time: datetime):
        if table_state.position < 0:
            # fst_try
            table_state.position = 0
            table_state.next_timestamp = dl_time

        table_state.position += self._req_limit
        self._states.set_state(self._table_name, table_state.dict())

    def _save_result(self, table_state: TableState):
        table_state.position = -1
        table_state.timestamp = table_state.next_timestamp
        self._states.set_state(self._table_name, table_state.dict())
