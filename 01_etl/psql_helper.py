import datetime
import logging
import os
from string import Template
from uuid import UUID

import psycopg2
from psycopg2.extras import DictCursor

from backoff import backoff

logger = logging.getLogger(__name__)


class Connection:
    REQUEST_MAX_ENTRIES = 100

    def __init__(self):
        dsl = {
            "dbname": os.environ.get("DB_NAME"),
            "user": os.environ.get("DB_USER"),
            "password": os.environ.get("DB_PASSWORD"),
            "host": os.environ.get("DB_HOST", "127.0.0.1"),
            "port": int(os.environ.get("DB_PORT", 5432)),
        }
        psycopg2.extras.register_uuid()
        self.conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        self.cursor = self.conn.cursor()
        self.cursor.execute("SET SESSION TIME ZONE 'UTC';")

    def get_modified_count(self, table: str, req_time: datetime) -> int:
        self.cursor.execute(
            "SELECT COUNT(id) FROM content.{table} WHERE  \
            modified > '{mod_time}';".format(
                table=table,
                mod_time=req_time
            )
        )
        return res if (res := self.cursor.fetchall()) else 0

    def get_modified(
            self,
            table: str,
            req_time: datetime,
            offset: int,
            limit: int = REQUEST_MAX_ENTRIES) -> list[list[UUID, datetime]]:
        """Request table entries older, then @req_time

        :param table: table name
        :param req_time: request date and time
        :param offset: query offset
        :param limit: query limit
        :return descend ordered by modification date list[list[UUI, datetime]]:
        """
        self.cursor.execute(
            "SELECT id, modified FROM content.{table} WHERE  \
            modified > '{mod_time}' ORDER BY modified DESC \
            LIMIT {limit}  OFFSET {offset};".format(
                table=table,
                mod_time=req_time,
                limit=limit,
                offset=offset
            )
        )
        return self.cursor.fetchall()

    def get_fw_id_by_table(self, name: str, ids_range: list[UUID]):
        ids = ", ".join("'{}'".format(str(i)) for i in ids_range)
        params = {
            "film_work": "content.film_work",
            "film_work_id": "content.film_work.id",
            "m2m": f"content.{name}_film_work",
            "m2m_ids": f"content.{name}_film_work.{name}_id",
            "join_range": ids
        }
        query = Template(
            "SELECT DISTINCT $film_work_id FROM $film_work \
            LEFT JOIN $m2m  ON $m2m.film_work_id =  $film_work_id \
            WHERE $m2m_ids  IN($join_range);"
        ).substitute(params)
        self.cursor.execute(query)
        return [i[0] for i in self.cursor.fetchall()]

    def get_fw_id(self, name: str, mod_time: datetime, offset: int,
                  limit: int = REQUEST_MAX_ENTRIES):
        params = {
            "film_work": "content.film_work",
            "m2m": f"content.{name}_film_work",
            "m2m_ids": f"content.{name}_film_work.{name}_id",
            "table": name,
            "mod_time": mod_time,
            "limit": limit,
            "offset": offset,
        }
        query = Template(
            "SELECT DISTINCT $film_work.id FROM $film_work \
            JOIN $m2m  ON $m2m.film_work_id =  $film_work.id \
            WHERE $m2m_ids  IN(\
                    SELECT id FROM content.$table\
                    WHERE modified > '$mod_time'\
                    LIMIT $limit  OFFSET $offset\
            );"
        ).substitute(params)
        self.cursor.execute(query)
        return [i[0] for i in self.cursor.fetchall()]

    def get_filmworks(self, fw_id_range: list[UUID]):
        params = {
            "id_range": ", ".join("'{}'".format(str(i)) for i in fw_id_range)
        }
        query = Template(
            "SELECT \
                fw.id,\
                fw.title,\
                fw.description,\
                fw.rating,\
                json_agg(DISTINCT jsonb_build_object(\
                       'role', pfw.role,\
                       'id', p.id,\
                       'name', p.full_name)\
                ) persons,\
                ARRAY_AGG(DISTINCT g.name) genres \
            FROM content.film_work fw \
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id\
            LEFT JOIN content.person p ON p.id = pfw.person_id \
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id \
            LEFT JOIN content.genre g ON g.id = gfw.genre_id \
            WHERE fw.id IN ($id_range)\
            GROUP BY fw.id;"
        ).substitute(params)
        self.cursor.execute(query)
        return self.cursor.fetchall()


@backoff([ConnectionError, psycopg2.OperationalError])
def create_connection() -> Connection:
    return Connection()
