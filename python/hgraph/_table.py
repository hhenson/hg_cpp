"""The TABLE surface (hgraph parity): TableSchema / make_table_schema /
table_schema / ToTableMode and the bitemporal column-name helpers.

The row layout is synthesised in C++ (`_hgraph.table_schema_info` over the
interned TS-table layout — design record *Record/replay, tables and
const_fn*, step 6); everything here is a thin declarative mapping from the
C++ layout onto hgraph's python classes (the C++-first API ruling)."""
import datetime
from dataclasses import dataclass
from enum import Enum

import _hgraph

from ._types import _resolve


class ToTableMode(Enum):
    # Values match the C++ ToTableMode enum registration.
    Tick = 1
    Sample = 2
    Snap = 3


@dataclass(frozen=True)
class TableSchema:
    tp: object
    keys: tuple
    types: tuple
    partition_keys: tuple  # An empty set implies a single row per tick.
    removed_keys: tuple  # Only present when there are partition_keys.
    date_time_key: str
    as_of_key: str
    is_multi_row: bool = False  # True for Frame-like multi-row types

    def __eq__(self, other):
        if not isinstance(other, TableSchema):
            return NotImplemented
        # tp compares by its resolved C++ TS schema (type expressions are
        # fresh python objects; the interned C++ handle is the identity).
        return (
            _tp_key(self.tp) == _tp_key(other.tp)
            and self.keys == other.keys
            and self.types == other.types
            and self.partition_keys == other.partition_keys
            and self.removed_keys == other.removed_keys
            and self.date_time_key == other.date_time_key
            and self.as_of_key == other.as_of_key
            and self.is_multi_row == other.is_multi_row
        )


def _tp_key(tp):
    try:
        return repr(_resolve(tp))
    except Exception:
        return repr(tp)


def _config_keys():
    # The bitemporal column names live on the C++ record_replay config
    # (set via set_table_schema_date_key/_as_of_key in _runtime).
    from ._runtime import GlobalState

    if GlobalState.has_instance():
        return _hgraph._table_schema_keys(GlobalState.instance()._impl)
    return "__date_time__", "__as_of__"


def get_table_schema_date_key() -> str:
    return _config_keys()[0]


def get_table_schema_as_of_key() -> str:
    return _config_keys()[1]


def make_table_schema(
    tp,
    keys,
    types,
    partition_keys=tuple(),
    removed_keys=tuple(),
    date_key=None,
    as_of_key=None,
    is_multi_row=False,
) -> TableSchema:
    if date_key is None:
        date_key = get_table_schema_date_key()
    if as_of_key is None:
        as_of_key = get_table_schema_as_of_key()
    return TableSchema(
        tp=tp,
        keys=(date_key, as_of_key) + tuple(keys),
        types=(datetime.datetime, datetime.datetime) + tuple(types),
        partition_keys=tuple(partition_keys),
        removed_keys=tuple(removed_keys),
        date_time_key=date_key,
        as_of_key=as_of_key,
        is_multi_row=is_multi_row,
    )


# The C++ leaf display-names -> python types (declarative; no derivation).
_LEAF_TYPES = {
    "bool": bool,
    "int": int,
    "float": float,
    "str": str,
    "bytes": bytes,
    "datetime": datetime.datetime,
    "date": datetime.date,
    "time": datetime.time,
    "timedelta": datetime.timedelta,
}


class _EagerValue:
    """hgraph's const-port shim: ``table_schema(tp).value`` (the operator is
    const-evaluable; outside a graph the eager value is returned)."""

    __slots__ = ("value",)

    def __init__(self, value):
        self.value = value


def table_schema(tp) -> _EagerValue:
    """The TableSchema the ``to_table`` operator will produce for ``tp``."""
    info = _hgraph.table_schema_info(
        _resolve(tp), get_table_schema_date_key(), get_table_schema_as_of_key()
    )
    types = tuple(_LEAF_TYPES.get(name, object) for name in info["types"])
    return _EagerValue(
        TableSchema(
            tp=tp,
            keys=tuple(info["keys"]),
            types=types,
            partition_keys=tuple(info["partition_keys"]),
            removed_keys=tuple(info["removed_keys"]),
            date_time_key=info["date_key"],
            as_of_key=info["as_of_key"],
            is_multi_row=info["is_multi_row"],
        )
    )
