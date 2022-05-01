import datetime
import os
from string import Template
from uuid import UUID

import psycopg2
from psycopg2.extras import DictCursor


class Connection:
    REQUEST_MAX_ENTRIES = 100

    def __init__(self):
        dsl = {
            'dbname': os.environ.get('DB_NAME', 'movies_database'),
            'user': 'app',
            'password': '123qwe',
            'host': os.environ.get('DB_HOST', '127.0.0.1'),
            'port': int(os.environ.get('DB_PORT', 5432)),
        }
        psycopg2.extras.register_uuid()
        self.conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        self.cursor = self.conn.cursor()
        self.cursor.execute('SET SESSION TIME ZONE "UTC";')

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def get_modified(self, table: str, req_time: datetime, offset: int,
                     limit: int = REQUEST_MAX_ENTRIES) -> list[UUID]:
        self.cursor.execute(
            "SELECT id  FROM {table} WHERE  \
            modified > '{mod_time}' ORDER BY modified \
            LIMIT {limit}  OFFSET {offset};".format(
                table=table,
                mod_time=req_time,
                limit=limit,
                offset=offset
            )
        )
        return [i[0] for i in self.cursor.fetchall()]

    def get_fw_id_by_persons(self, ids_range: list[UUID]):
        ids = ", ".join("'{}'".format(str(i)) for i in ids_range)
        params = {
            "film_work": "content.film_work",
            "film_work_id": "content.film_work.id",
            "m2m": "content.person_film_work",
            "m2m_ids": "content.person_film_work.person_id",
            "join_range": ids
        }
        query = Template(
            "SELECT $film_work_id FROM $film_work \
            LEFT JOIN $m2m  ON $m2m.film_work_id =  $film_work_id \
            WHERE $m2m_ids  IN($join_range);"
        ).substitute(params)
        self.cursor.execute(query)
        return [i[0] for i in self.cursor.fetchall()]
