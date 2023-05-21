#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define instance connector methods for working with pipes.
"""

import copy
import json
import traceback
import functools
from datetime import datetime
import meerschaum as mrsm
from meerschaum.utils.typing import (
    SuccessTuple, Any, Optional, Union, Dict, List, Tuple, Iterator, Iterable,
)
from meerschaum.utils.warnings import warn
from meerschaum.utils.debug import dprint

@staticmethod
def get_pipe_keys_query(pipe: mrsm.Pipe) -> Dict[str, str]:
    """
    Return the standard filter query for a pipe's keys.
    """
    return {
        'connector_keys': str(pipe.connector_keys),
        'metric_key': str(pipe.metric_key),
        'location_key': str(pipe.location_key),
    }


def register_pipe(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> SuccessTuple:
    """
    Register the pipe's document to the internal `pipes` collection.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to be registered.

    Returns
    -------
    A `SuccessTuple` of the result.
    """
    if pipe.get_id(debug=debug) is not None:
        return False, f"{pipe} is already registered."

    pipe_doc = self.get_pipe_keys_query(pipe)
    pipe_doc['parameters'] = pipe._attributes.get('parameters', {})

    try:
        result = self.pipes_collection.insert_one(pipe_doc)
        pipe._id = result.inserted_id
    except Exception as e:
        return False, f"Failed to register {pipe}:\n{traceback.format_exc()}"

    return True, "Success"


def get_pipe_attributes(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> Dict[str, Any]:
    """
    Return the pipe's document from the internal `pipes` collection.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose attributes should be retrieved.

    Returns
    -------
    The document that matches the keys of the pipe.
    """
    query = self.get_pipe_keys_query(pipe)
    result = self.pipes_collection.find_one(query) or {}
    if result:
        result['_id'] = str(result['_id'])
    return result


def get_pipe_id(
        self,
        pipe: mrsm.Pipe,
        _as_oid: bool = False,
        debug: bool = False,
        **kwargs
    ) -> Union[str, None]:
    """
    Return the `_id` for the pipe if it exists.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose `_id` to fetch.

    Returns
    -------
    The `_id` for the pipe's document or `None`.
    """
    query = self.get_pipe_keys_query(pipe)
    oid = (self.pipes_collection.find_one(query, {'_id': 1}) or {}).get('_id', None)
    if _as_oid:
        return oid
    return str(oid) if oid is not None else None


def edit_pipe(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs
    ) -> SuccessTuple:
    """
    Edit the attributes of the pipe.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose in-memory parameters must be persisted.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    if not pipe.get_id():
        return False, f"{pipe} is not registered."

    query = self.get_pipe_keys_query(pipe)
    pipe_parameters = pipe._attributes.get('parameters', {})

    try:
        result = self.pipes_collection.update_one(query, {'$set': {'parameters': pipe_parameters}})
    except Exception as e:
        return False, f"Failed to edit {pipe}:\n{traceback.format_exc()}"

    return True, "Success"


def delete_pipe(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> SuccessTuple:
    """
    Delete a pipe's registration from the `pipes` collection.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to be deleted.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    drop_success, drop_message = pipe.drop(debug=debug)
    if not drop_success:
        return drop_success, drop_message

    pipe_id = self.get_pipe_id(pipe, _as_oid=True, debug=debug)
    if pipe_id is None:
        return False, f"{pipe} is not registered."

    try:
        self.pipes_collection.delete_one({'_id': pipe_id})
    except Exception as e:
        return False, f"Failed to delete {pipe}:\n{traceback.format_exc()}"

    return True, "Success"


def fetch_pipes_keys(
        self,
        connector_keys: Optional[List[str]] = None,
        metric_keys: Optional[List[str]] = None,
        location_keys: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        debug: bool = False,
        **kwargs: Any
    ) -> List[Tuple[str, str, str]]:
    """
    Return a list of tuples for the registered pipes' keys according to the provided filters.
    """
    from meerschaum.utils.misc import separate_negation_values

    in_ck, nin_ck = separate_negation_values([str(x) for x in (connector_keys or [])])
    in_mk, nin_mk = separate_negation_values([str(x) for x in (metric_keys or [])])
    in_lk, nin_lk = separate_negation_values([str(x) for x in (location_keys or [])])
    in_tags, nin_tags = separate_negation_values([str(x) for x in (tags or [])])

    query = {}
    if connector_keys:
        query['connector_keys'] = {}
    if metric_keys:
        query['metric_key'] = {}
    if location_keys:
        query['location_key'] = {}
    if tags:
        query['parameters.tags'] = {}

    if in_ck:
        query['connector_keys'].update({'$in': in_ck})
    if nin_ck:
        query['connector_keys'].update({'$nin': nin_ck})
    if in_mk:
        query['metric_key'].update({'$in': in_mk})
    if nin_mk:
        query['metric_key'].update({'$nin': nin_mk})
    if in_lk:
        query['location_key'].update({'$in': in_lk})
    if nin_lk:
        query['location_key'].update({'$nin': nin_lk})
    if in_tags:
        query['parameters.tags'].update({'$in': in_tags})
    if nin_tags:
        query['parameters.tags'].update({'$nin': nin_tags})

    if debug:
        dprint(json.dumps(query))
    try:
        results = [
            (doc['connector_keys'], doc['metric_key'], doc['location_key'])
            for doc in self.pipes_collection.find(
                query,
                {'connector_keys': 1, 'metric_key': 1, 'location_key': 1},
            )
        ]
    except Exception as e:
        warn(f"[{self}] Failed to fetch pipes keys:\n{traceback.format_exc()}")
        results = []

    return results


def pipe_exists(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> bool:
    """
    Check whether a pipe's collection exists.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to check whether its collection exists.

    Returns
    -------
    A `bool` indicating the collection exists.
    """
    try:
        self.database.validate_collection(pipe.target)
    except Exception as e:
        if debug:
            dprint(str(e))
        return False
    return True


def drop_pipe(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> SuccessTuple:
    """
    Drop a pipe's collection if it exists.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to be dropped.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    if not pipe.exists(debug=debug):
        return True, "Success"

    try:
        self.database[pipe.target].drop()
    except Exception as e:
        return False, f"Failed to drop {pipe}:\n{traceback.format_exc()}"
    return True, "Success"


@staticmethod
def get_document_id(document: Dict[str, Any], index_columns: List[str]) -> str:
    """
    Return the unique ID for this document based on the indicated indices.

    Parameters
    ----------
    document: Dict[str, Any]
        The document which contains the indices.
        Missing indices will be replaced with `null`.
    """
    return {
        key: document.get(key, None)
        for key in sorted(index_columns)
    }


def sync_pipe(
        self,
        pipe: mrsm.Pipe,
        df: Union['pd.DataFrame', Iterator['pd.DataFrame']] = None,
        workers: Optional[int] = None,
        debug: bool = False,
        **kwargs: Any
    ) -> SuccessTuple:
    """
    Upsert new documents into the pipe's collection.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose collection should receive the new documents.

    df: Union['pd.DataFrame', Iterator['pd.DataFrame']], default None
        The data to be synced.

    workers: Optional[int], default None
        The number of threads to use while inserting.
        Defaults to the number of cores.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    if df is None:
        return False, f"Received `None`, cannot sync {pipe}."

    is_df_like = isinstance(df, (list, dict, str)) or 'DataFrame' in str(type(df))
    if not is_df_like and isinstance(df, (Iterator, Iterable)):
        from meerschaum.utils.pool import get_pool
        pool = get_pool(workers=workers)
        dt_col = pipe.columns.get('datetime', None)

        def _process_chunk(chunk):
            chunk_success, chunk_msg = pipe.sync(chunk, workers=workers, debug=debug, **kwargs)
            return (
                chunk_success,
                '\n'
                + pipe._get_chunk_label(_chunk, dt_col)
                + '\n'
                + chunk_message
            )

        results = list(pool.imap(_process_chunk, df))
        success = all([scs for scs, _ in results])
        message = '\n'.join([msg for _, msg in results])
        return success, message
    elif not is_df_like:
        return False, f"Received {type(df)}, cannot sync {pipe}."

    with mrsm.Venv('mongodb-connector'):
        from pymongo import UpdateOne

    from meerschaum.utils.misc import parse_df_datetimes
    df = parse_df_datetimes(df)

    index_cols = sorted(list(pipe.columns.values()))

    df['_id'] = df.apply(lambda doc: self.get_document_id(doc, index_cols), axis=1)
    upserts = [
        UpdateOne({'_id': doc['_id']}, {'$setOnInsert': doc}, upsert=True)
        for doc in df.to_dict(orient='records')
    ]
    try:
        self.database[pipe.target].bulk_write(upserts)
        success, message = True, f"Successfully synced {len(df)} documents."
    except Exception as e:
        success, message = False, f"Failed to sync {len(df)} documents:\n{traceback.format_exc()}"
    return success, message


def get_pipe_rowcount(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> int:
    """
    Return the estimated rowcount for the pipe's collection.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose collection should be counted.

    Returns
    -------
    An estimated rowcount for this pipe's collection.
    """
    try:
        rowcount = self.database.get_collection(pipe.target).estimated_document_count()
    except Exception as e:
        rowcount = 0
    return rowcount


def get_sync_time(
        self,
        pipe: mrsm.Pipe,
        params: Optional[Dict[str, Any]] = None,
        newest: bool = True,
        debug: bool = False,
        **kwargs: Any
    ) -> Union[datetime, int, None]:
    """
    Return the most recent value for the `datetime` axis.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose collection contains documents.

    params: Optional[Dict[str, Any]], default None
        Filter certain parameters when determining the sync time.

    newest: bool, default True
        If `True`, return the maximum value for the column.

    Returns
    -------
    The largest `datetime` or `int` value of the `datetime` axis. 
    """


def get_pipe_columns_types(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> Dict[str, str]:
    """
    """
    return {}


def get_pipe_data(
        self,
        pipe: mrsm.Pipe,
        begin: Union[datetime, int, None] = None,
        end: Union[datetime, int, None] = None,
        params: Optional[Dict[str, Any]] = None,
        debug: bool = False,
        **kwargs: Any
    ) -> Union['pd.DataFrame', None]:
    """
    Query a pipe's collection and return the DataFrame.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe with the collection from which to read.

    begin: Union[datetime, int, None], default None
        The earliest `datetime` value to search from (inclusive).

    end: Union[datetime, int, None], default None
        The lastest `datetime` value to search from (exclusive).

    params: Optional[Dict[str, str]], default None
        Additional filters to apply the query.

    Returns
    -------
    The collection's data as a DataFrame.
    """
    if not pipe.exists(debug=debug):
        return None

    from meerschaum.utils.misc import parse_df_datetimes
    query = {}
    result = self.database[pipe.target].find(query)
    if result is None:
        return None
    return parse_df_datetimes([{k: v for k, v in doc.items() if k != '_id'} for doc in result])


def get_backtrack_data(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> 'pd.DataFrame':
    """
    """
    return self.get_pipe_data(pipe)
