from typing import Optional

from elasticsearch import Elasticsearch


class Connection:
    def __init__(self, address: str = "127.0.0.1:9200"):
        self._connection = Elasticsearch()

    def __del__(self):
        self._connection.close()

    def post(
            self,
            data: dict,
            identifier: Optional[str],
            index: str = "movies"
    ):
        """
        Send document to the corresponding mapping.

            Warning:
                If repeat this action with the same parameters, the mapping
                 entry will increment '_version' and result be 'updated'
        """
        resp = self._connection.index(
            index=index,
            id=identifier,
            document=data
        )
        print(resp['result'])
