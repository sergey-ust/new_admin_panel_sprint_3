import logging
from typing import Optional

import elasticsearch.client.indices
from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)


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
        logger.debug(resp['result'])

    def is_exist(self, index_name: str) -> bool:
        index = elasticsearch.client.indices.IndicesClient(self._connection)
        return index.exists(index=index_name)
