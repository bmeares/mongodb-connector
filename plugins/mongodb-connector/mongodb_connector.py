#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define `MongoDBConnector`.
"""

from datetime import datetime
import meerschaum as mrsm
from meerschaum.connectors import make_connector, Connector
from meerschaum.utils.typing import Optional, Any, List, Dict

@make_connector
class MongoDBConnector(Connector):
    """
    Implement the instance connector interface for MongoDB.
    """

    REQUIRED_ATTRIBUTES: List[str] = ['uri', 'database']
    IS_THREAD_SAFE: bool = True
    IS_INSTANCE: bool = True

    from ._pipes import (
        get_pipe_keys_query,
        register_pipe,
        get_pipe_attributes,
        get_pipe_id,
        fetch_pipes_keys,
        edit_pipe,
        delete_pipe,
        pipe_exists,
        drop_pipe,
        get_document_id,
        sync_pipe,
        get_pipe_rowcount,
        get_sync_time,
        get_pipe_columns_types,
        get_pipe_data,
        get_backtrack_data,
        create_indices,
    )
    from ._mongo import (
        build_query,
        coerce_str_to_datetime,
        truncate_datetime,
    )

    @property
    def client(self) -> 'pymongo.MongoClient':
        """
        Return the `pymongo` connection object.
        """
        _client = self.__dict__.get('_client', None)
        if _client is not None:
            return _client

        with mrsm.Venv('mongodb-connector'):
            import pymongo
            self._client = pymongo.MongoClient(self.uri)
        return self._client


    @property
    def database(self) -> 'pymongo.database.Database':
        """
        Return the database which is mapped to this connector.
        """
        return self.client[self.__dict__['database']]


    @property
    def pipes_collection(self) -> 'pymongo.collection.Collection':
        """
        Return the `pipes` collection.
        """
        return self.database['pipes']


    @property
    def ASCENDING(self) -> 'pymongo.ASCENDING':
        """
        Return the `pymongo.ASCENDING` constant.
        """
        _ASCENDING = self.__dict__.get('_ASCENDING', None)
        if _ASCENDING is not None:
            return _ASCENDING
        with mrsm.Venv('mongodb-connector'):
            import pymongo
            self._ASCENDING = pymongo.ASCENDING
        return self._ASCENDING


    @property
    def DESCENDING(self) -> 'pymongo.DESCENDING':
        """
        Return the `pymongo.DESCENDING` constant.
        """
        _DESCENDING = self.__dict__.get('_DESCENDING', None)
        if _DESCENDING is not None:
            return _DESCENDING
        with mrsm.Venv('mongodb-connector'):
            import pymongo
            self._DESCENDING = pymongo.DESCENDING
        return self._DESCENDING
