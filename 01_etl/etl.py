import logging
from datetime import datetime, timezone
from typing import Optional

from psycopg2 import OperationalError
from urllib3.exceptions import NewConnectionError

import es_helper
import psql_helper
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

    def extract(
            self,
            psql: Sql,
            table_state: TableState
    ) -> Optional[list[dict]]:
        offset = table_state.position
        offset = offset if offset > 0 else 0
        ids_and_time = psql.get_modified(
            self._table_name,
            table_state.timestamp,
            offset,
            self._req_limit
        )
        if _everything_updated := not ids_and_time:
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
            film_works = psql.get_filmworks(fw_ids)
            return [
                FilmWork.create_from_sql(**fw).dict() for fw in
                film_works
            ]

    def action(self):
        table_state = TableState.create_empty()
        if rd_state := self._states.get_state(self._table_name):
            table_state = TableState(**rd_state)

        while True:
            psql = psql_helper.create_connection()
            es = es_helper.create_connection()
            finished = False

            while not finished:
                try:
                    es_films = self.extract(psql, table_state)
                except OperationalError:
                    logger.exception("PostgreSQl extraction error.")
                    break
                dl_time = datetime.now(tz=timezone.utc)
                if es_films is None:
                    finished = True
                    if _no_updates := table_state.position < 0:
                        table_state.next_timestamp = dl_time
                    continue
                # load
                if es_films:
                    try:
                        es.post_bulk(es_films)
                    except NewConnectionError:
                        logger.exception("Elasticsearch load error.")
                        break
                    self._safe_loaded(es_films, dl_time)
                # safe the step
                self._safe_state(table_state, dl_time)
            # update finished
            self._safe_finished(table_state)
            return

    def _safe_loaded(self, es_films: dict, dl_time: datetime):
        # update cache
        for entry in es_films[: -1]:
            self._fw_states.set_state(
                str(entry["id"]),
                dl_time.isoformat(),
                False
            )
        # save all cached
        self._fw_states.set_state(str(es_films[-1]["id"]), dl_time.isoformat())

    def _safe_state(self, table_state: TableState, dl_time: datetime):
        if _fst_try := table_state.position < 0:
            table_state.position = 0
            table_state.next_timestamp = dl_time

        table_state.position += self._req_limit
        self._states.set_state(self._table_name, table_state.dict())

    def _safe_finished(self, table_state: TableState):
        table_state.position = -1
        table_state.timestamp = table_state.next_timestamp
        self._states.set_state(self._table_name, table_state.dict())
