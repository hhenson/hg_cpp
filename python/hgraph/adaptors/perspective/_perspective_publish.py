from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime

import pyarrow as pa
import _hgraph

from hgraph import K, TIME_SERIES_TYPE, TS, TSD, push_queue, sink_node
from hgraph._types import _TsExpr
from hgraph._wiring import _unwrap

from ._perspective import PerspectiveTablesManager

__all__ = ("_publish_table", "_receive_table_edits", "TableEdits")


@dataclass(frozen=True)
class TableEdits:
    edits: object
    removes: object


@sink_node
def _publish_table(
    ts: TSD[K, TIME_SERIES_TYPE],
    name: str,
    index_col_name: str,
    editable: bool,
    edit_role: str,
    history: int,
    manager: object,
):
    rows = []
    history_rows = []
    for key, child in ts.modified_items():
        child_rows = _value_rows(child.value)
        for child_row in child_rows:
            row, index = _with_key(child_row, key, index_col_name)
            rows.append(row)
            if history is not None:
                history_rows.append({**row, "time": child.last_modified_time})
    removals = list(ts.removed_keys())
    manager.update_table(
        name,
        rows,
        removals,
        index=_perspective_index(index_col_name),
        editable=editable,
        edit_role=edit_role,
    )
    if history is not None and history_rows:
        manager.update_table(
            f"{name}_history",
            history_rows[-history:] if history > 0 else history_rows,
            index=None,
        )


def _value_rows(value):
    if isinstance(value, pa.Table):
        return value.to_pylist()
    if isinstance(value, pa.RecordBatch):
        return value.to_pylist()
    if is_dataclass(value):
        return [asdict(value)]
    if isinstance(value, dict):
        return [dict(value)]
    return [{"value": value}]


def _with_key(row, key, index_col_name):
    names = tuple(name.strip() for name in index_col_name.split(",") if name.strip())
    if not names:
        names = ("index",)
    if is_dataclass(key):
        values = asdict(key)
        keyed = {name: values[name] for name in names}
    elif isinstance(key, tuple):
        if len(names) != len(key):
            raise ValueError("tuple Perspective keys require one index column name per element")
        keyed = dict(zip(names, key))
    else:
        if len(names) != 1:
            raise ValueError("scalar Perspective keys require exactly one index column name")
        keyed = {names[0]: key}
    if len(keyed) > 1:
        keyed["index"] = ",".join(str(keyed[name]) for name in names)
    return {**keyed, **row}, keyed.get("index", next(iter(keyed.values())))


def _perspective_index(index_col_name):
    names = tuple(name.strip() for name in index_col_name.split(",") if name.strip())
    return names[0] if len(names) == 1 else "index"


def _receive_table_edits(name, ts, index_col_name, manager):
    ts_type = _unwrap(ts).ts_type
    if not ts_type.is_tsd:
        raise TypeError("editable Perspective tables require a TSD input")
    key_type = _hgraph.tsd_key_vt(ts_type)
    value_type = _hgraph.tsd_element_ts(ts_type)
    edits_type = _hgraph.tsd(key_type, value_type)
    removes_type = _hgraph.tss(key_type)
    output_type = _TsExpr(
        _hgraph.un_named_tsb_type(
            [("edits", edits_type), ("removes", removes_type)]
        ),
        "TSB[TableEdits]",
    )
    subscription = {}

    @push_queue(output_type)
    def edits(sender):
        def receive(rows, removals):
            parsed = {}
            for row in rows:
                row = dict(row)
                key = _edit_key(row, index_col_name)
                parsed[key] = row["value"] if tuple(row) == ("value",) else row
            update = {}
            if parsed:
                update["edits"] = parsed
            if removals:
                update["removes"] = frozenset(removals)
            if update:
                sender(update)

        subscription["token"] = manager.subscribe_table_updates(name, receive)

    @sink_node
    def lifetime(value: output_type):
        pass

    @lifetime.stop
    def stop():
        token = subscription.pop("token", None)
        if token is not None:
            manager.unsubscribe_table_updates(name, token)

    output = edits()
    lifetime(output)
    return output


def _edit_key(row, index_col_name):
    names = tuple(name.strip() for name in index_col_name.split(",") if name.strip())
    if not names:
        names = ("index",)
    if len(names) == 1:
        return row.pop(names[0])
    values = tuple(row.pop(name) for name in names)
    row.pop("index", None)
    return values
