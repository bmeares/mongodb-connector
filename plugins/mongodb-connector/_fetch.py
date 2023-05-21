#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define the methods for when this connector is a data source.
"""

import copy
from datetime import datetime
import meerschaum as mrsm
from meerschaum.utils.typing import Union, Dict, Optional, Any, List, Iterator
from meerschaum.utils.misc import iterate_chunks
from meerschaum.config import apply_patch_to_config, get_config

def fetch(
        self,
        pipe: mrsm.Pipe,
        begin: Union[datetime, int, None] = None,
        end: Union[datetime, int, None] = None,
        params: Optional[Dict[str, Any]] = None,
        chunksize: Optional[int] = -1,
        debug: bool = False,
        **kwargs: Any
    ) -> Iterator[List[Dict[str, Any]]]:
    """
    Fetch new data accoring to the pipe's query definition.
    Apply `begin`, `end`, and `params` to the base query.
    """
    base_query = copy.deepcopy(pipe.parameters.get('fetch', {}).get('query', {}))
    projection = pipe.parameters.get('fetch', {}).get('projection', {})
    collection = pipe.parameters.get('fetch', {}).get('collection', None)

    if collection is None:
        raise Exception(f"No collection was defined for {pipe}.")

    if chunksize == -1:
        chunksize = get_config('system', 'connectors', 'sql', 'chunksize')

    runtime_query = self.build_query(params, pipe.columns.get('datetime', None), begin, end)
    
    query = apply_patch_to_config(base_query, runtime_query)
    results = self.database[collection].find(query, projection)

    if chunksize is None:
        return [doc for doc in results]

    chunks = iterate_chunks(results, chunksize)
    return (
        [doc for doc in chunk if doc]
        for chunk in chunks
    )
