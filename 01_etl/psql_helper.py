import datetime
import os

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

    def get_modified(self, table: str, mod_time: datetime, offset: int,
                     limit: int = REQUEST_MAX_ENTRIES) -> list:
        self.cursor.execute(
            "SELECT id  FROM {table} WHERE  \
            modified > '{mod_time}' ORDER BY modified \
            LIMIT {limit}  OFFSET {offset};".format(
                table=table,
                mod_time=mod_time,
                limit=limit,
                offset=offset
            )
        )
        return self.cursor.fetchall()
