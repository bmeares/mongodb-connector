#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Mongo-specific methods.
"""

from datetime import datetime, timedelta
from meerschaum.utils.typing import Dict, Any, Union, Optional
from meerschaum.utils.warnings import warn
from meerschaum.utils.misc import round_time
import meerschaum as mrsm

@staticmethod
def build_query(
        params: Dict[str, Any],
        datetime_column: Optional[str] = None,
        begin: Union[datetime, int, None] = None,
        end: Union[datetime, int, None] = None,
    ) -> Dict[str, Any]:
    """
    Return a MongoDB filter query from a Meerschaum `params` dictionary.

    Parameters
    ----------
    params: Dict[str, Any]
        The Meerschaum params dictionary to convert to a `filter` dictionary.

    Returns
    -------
    A MongoDB filter dictionary.

    Examples
    --------
    >>> build_query({'a': 1})
    {'a': {'$eq': 1}}
    >>> build_query({'a': '_b'})
    {'a': {'$ne': 'b'}}
    >>> build_query({'a': ['c', '_d']})
    {'a': {'$eq': 'c', {'$neq': 'd'}}}
    >>> build_query({'a': [1, 2, 3]})
    {'a': {'$nin': [1, 2, 3]}}
    >>> build_query({'a': []})
    {}
    """
    from meerschaum.utils.misc import separate_negation_values
    params = params or {}
    list_params = {
        col: ([vals] if not isinstance(vals, (list, tuple)) else vals)
        for col, vals in params.items()
    }
    separated_params = {
        col: separate_negation_values(vals)
        for col, vals in list_params.items()
    }
    query = {}
    for col, (in_vals, nin_vals) in separated_params.items():
        if not in_vals and not nin_vals:
            continue
        query[col] = {}
        if len(in_vals) == 1:
            query[col].update({'$eq': in_vals[0]})
        elif len(in_vals) > 1:
            query[col].update({'$in': in_vals})

        if len(nin_vals) == 1:
            query[col].update({'$ne': nin_vals[0]})
        elif len(nin_vals) > 1:
            query[col].update({'$nin': nin_vals})

    if begin is None and end is None:
        return query

    if datetime_column is None:
        warn("Ignoring begin or end bounds because no datetime column was provided.")
        return query

    if datetime_column not in query:
        query[datetime_column] = {}

    if isinstance(begin, str):
        coerced_begin = self.coerce_str_to_datetime(begin)
        if coerced_begin is None:
            warn(f"Could not parse '{coerced_begin}' as a datetime.")
        else:
            begin = coerced_begin

    if isinstance(end, str):
        coerced_end = self.coerce_str_to_datetime(end)
        if coerced_end is None:
            warn(f"Could not parse '{coerced_end}' as a datetime.")
        else:
            end = coerced_end

    if begin is not None:
        query[datetime_column].update({'$gte': begin})

    if end is not None:
        query[datetime_column].update({'$lt': end})

    return query


@staticmethod
def coerce_str_to_datetime(datetime_str: str) -> Union[datetime, None]:
    """
    Attempt to parse a string into a datetime.
    Return `None` if it cannot be parsed.
    """
    with mrsm.Venv('mongodb-connector'):
        import dateutil.parser

    try:
        dt = dateutil.parser.parse(datetime_str)
    except Exception as e:
        dt = None

    return dt


@staticmethod
def truncate_datetime(dt: datetime) -> datetime:
    """
    Truncate nanoseconds to milliseconds.
    """
    if 'Timestamp' in str(type(dt)):
        dt = dt.to_pydatetime()

    microsecond = dt.microsecond

    nearest_second = round_time(dt, timedelta(seconds=1))
    truncated_dt =  nearest_second + timedelta(
        microseconds = int(str(microsecond).zfill(6)[:3] + '000')
    )
    return truncated_dt
