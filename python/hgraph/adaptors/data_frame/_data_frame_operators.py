"""Python nodes for manipulating Arrow-backed :class:`~hgraph.Frame` values."""

import operator as operators
from dataclasses import dataclass, field
from typing import TypeVar

import _hgraph
import pyarrow as pa
import pyarrow.compute as pc

from hgraph import (
    AUTO_RESOLVE,
    SCALAR,
    STATE,
    TS_SCHEMA,
    TSB,
    TSD,
    TS,
    Frame,
    KEYABLE_SCALAR,
    REMOVE,
    add_,
    and_,
    compute_node,
    div_,
    eq_,
    filter_,
    floordiv_,
    ge_,
    gt_,
    le_,
    lt_,
    mul_,
    operator,
    or_,
    sub_,
)


__all__ = (
    "join",
    "filter_frame",
    "filter_cs",
    "filter_exp",
    "filter_exp_ts",
    "filter_exp_seq",
    "group_by",
    "ungroup",
    "sorted_",
    "concat",
    "with_columns",
)


ROW = TypeVar("ROW")
ROW_1 = TypeVar("ROW_1")
ROW_2 = TypeVar("ROW_2")


def _join_type(how: str) -> str:
    return {
        "inner": "inner",
        "left": "left outer",
        "right": "right outer",
        "full": "full outer",
        "outer": "full outer",
        "semi": "left semi",
        "anti": "left anti",
    }.get(how, how)


@operator
def join(
    lhs: TS[Frame[ROW]],
    rhs: TS[Frame[ROW_1]],
    on: SCALAR,
    how: str = "inner",
    suffix: str = "_right",
) -> TS[Frame[ROW_2]]:
    """Join two frames on one or more equally-named columns."""
    ...


def _join_row_type(mapping, on, suffix):
    keys = {on} if isinstance(on, str) else set(on)
    left = list(mapping["ROW"].fields)
    names = {name for name, _ in left}
    right = [
        (name if name not in names or name in keys else f"{name}{suffix}", value_type)
        for name, value_type in mapping["ROW_1"].fields
        if name not in keys
    ]
    return _hgraph.un_named_bundle_vt(left + right)


@compute_node(overloads=join, resolvers={ROW_2: _join_row_type})
def _join(
    lhs: TS[Frame[ROW]],
    rhs: TS[Frame[ROW_1]],
    on: SCALAR,
    how: str = "inner",
    suffix: str = "_right",
) -> TS[Frame[ROW_2]]:
    keys = [on] if isinstance(on, str) else list(on)
    return lhs.value.join(
        rhs.value,
        keys=keys,
        join_type=_join_type(how),
        left_suffix="",
        right_suffix=suffix,
    )


def _predicate_from_mapping(values) -> pc.Expression | None:
    expression = None
    for name, value in values.items():
        if value is not None:
            term = pc.field(name) == value
            expression = term if expression is None else expression & term
    return expression


@compute_node
def filter_frame(ts: TS[Frame[ROW]], **predicate: TSB[TS_SCHEMA]) -> TS[Frame[ROW]]:
    expression = _predicate_from_mapping(predicate.value)
    return ts.value if expression is None else ts.value.filter(expression)


@compute_node
def filter_cs(ts: TS[Frame[ROW]], predicate: TS[ROW]) -> TS[Frame[ROW]]:
    value = predicate.value
    values = value.to_dict() if hasattr(value, "to_dict") else vars(value)
    expression = _predicate_from_mapping(values)
    return ts.value if expression is None else ts.value.filter(expression)


@compute_node
def filter_exp(ts: TS[Frame[ROW]], predicate: pc.Expression) -> TS[Frame[ROW]]:
    return ts.value.filter(predicate)


@compute_node
def filter_exp_ts(ts: TS[Frame[ROW]], predicate: TS[pc.Expression]) -> TS[Frame[ROW]]:
    return ts.value.filter(predicate.value)


@compute_node(overloads=filter_)
def filter_exp_ts_(condition: TS[pc.Expression], ts: TS[Frame[ROW]]) -> TS[Frame[ROW]]:
    return ts.value.filter(condition.value)


for _op, _impl in (
    (lt_, operators.lt),
    (gt_, operators.gt),
    (le_, operators.le),
    (ge_, operators.ge),
    (eq_, operators.eq),
    (add_, operators.add),
    (sub_, operators.sub),
    (mul_, operators.mul),
    (div_, operators.truediv),
    (floordiv_, operators.floordiv),
    (and_, operators.and_),
    (or_, operators.or_),
):

    @compute_node(overloads=_op)
    def _arrow_expression_rhs(lhs: TS[pc.Expression], rhs: TS[SCALAR], op: object = _impl) -> TS[pc.Expression]:
        return op(lhs.value, pc.scalar(rhs.value))

    @compute_node(overloads=_op)
    def _arrow_expression_lhs(lhs: TS[SCALAR], rhs: TS[pc.Expression], op: object = _impl) -> TS[pc.Expression]:
        return op(pc.scalar(lhs.value), rhs.value)


@compute_node
def filter_exp_seq(ts: TS[Frame[ROW]], predicate: tuple[pc.Expression, ...]) -> TS[Frame[ROW]]:
    expression = None
    for term in predicate:
        expression = term if expression is None else expression & term
    return ts.value if expression is None else ts.value.filter(expression)


@operator
def group_by(ts: TS[Frame[ROW]], by: SCALAR) -> TSD[KEYABLE_SCALAR, TS[Frame[ROW]]]: ...


def _single_key_type(mapping, by):
    return dict(mapping["ROW"].fields)[by]


def _tuple_key_type(mapping, by):
    schema = dict(mapping["ROW"].fields)
    return tuple[tuple(schema[name] for name in by)]


@dataclass
class _GroupState:
    keys: set = field(default_factory=set)


@compute_node(overloads=group_by, resolvers={KEYABLE_SCALAR: _single_key_type})
def _group_by_single(
    ts: TS[Frame[ROW]], by: str, _state: STATE[_GroupState] = None
) -> TSD[KEYABLE_SCALAR, TS[Frame[ROW]]]:
    return _group_by_impl(ts, by, _state)


@compute_node(overloads=group_by, resolvers={KEYABLE_SCALAR: _tuple_key_type})
def _group_by_tuple(
    ts: TS[Frame[ROW]], by: tuple[str, ...], _state: STATE[_GroupState] = None
) -> TSD[KEYABLE_SCALAR, TS[Frame[ROW]]]:
    return _group_by_impl(ts, by, _state)


def _group_by_impl(ts, by, state):
    columns = [by] if isinstance(by, str) else list(by)
    table = ts.value
    groups = {}
    if table.num_rows:
        keys = table.select(columns).to_pylist()
        indices = {}
        for index, values in enumerate(keys):
            key_values = tuple(values[name] for name in columns)
            key = key_values[0] if isinstance(by, str) else key_values
            indices.setdefault(key, []).append(index)
        groups = {key: table.take(pa.array(rows, type=pa.int64())) for key, rows in indices.items()}
    for key in state.keys - groups.keys():
        groups[key] = REMOVE
    state.keys = set(groups) - {key for key, value in groups.items() if value is REMOVE}
    return groups


@operator
def ungroup(ts: TSD[KEYABLE_SCALAR, TS[Frame[ROW]]]) -> TS[Frame[ROW]]: ...


@compute_node(overloads=ungroup)
def _ungroup(ts: TSD[KEYABLE_SCALAR, TS[Frame[ROW]]]) -> TS[Frame[ROW]]:
    values = ts.valid_values() if hasattr(ts, "valid_values") else _tsd_value(ts).values()
    frames = [_ts_value(value) for value in values if _ts_value(value).num_rows]
    return pa.concat_tables(frames) if frames else None


def _explicit_output_type(mapping, _tp_out):
    return _tp_out


@compute_node(overloads=ungroup, resolvers={ROW_1: _explicit_output_type})
def _ungroup_with_key(
    ts: TSD[KEYABLE_SCALAR, TS[Frame[ROW]]], key_col: str, _tp_out: type[ROW_1] = AUTO_RESOLVE
) -> TS[Frame[ROW_1]]:
    frames = []
    items = ts.valid_items() if hasattr(ts, "valid_items") else _tsd_value(ts).items()
    for key, value in items:
        frame = _ts_value(value)
        if frame.num_rows:
            frames.append(frame.append_column(key_col, pa.array([key] * frame.num_rows)))
    return pa.concat_tables(frames) if frames else None


@compute_node(overloads=ungroup, resolvers={ROW_1: _explicit_output_type})
def _ungroup_with_keys(
    ts: TSD[KEYABLE_SCALAR, TS[Frame[ROW]]], key_col: tuple[str, ...], _tp_out: type[ROW_1] = AUTO_RESOLVE
) -> TS[Frame[ROW_1]]:
    frames = []
    items = ts.valid_items() if hasattr(ts, "valid_items") else _tsd_value(ts).items()
    for key, value in items:
        frame = _ts_value(value)
        if frame.num_rows:
            for index, name in enumerate(key_col):
                frame = frame.append_column(name, pa.array([key[index]] * frame.num_rows))
            frames.append(frame)
    return pa.concat_tables(frames) if frames else None


@compute_node(overloads=ungroup)
def _ungroup_from_items(ts: TSD[KEYABLE_SCALAR, TS[ROW]]) -> TS[Frame[ROW]]:
    values = ts.valid_values() if hasattr(ts, "valid_values") else _tsd_value(ts).values()
    rows = []
    for value in values:
        value = _ts_value(value)
        rows.append(value.to_dict() if hasattr(value, "to_dict") else vars(value))
    return pa.Table.from_pylist(rows) if rows else None


def _ts_value(value):
    return value.value if hasattr(value, "valid") else value


def _tsd_value(value):
    return value.value if hasattr(value, "value") else value


@compute_node
def sorted_(ts: TS[Frame[ROW]], by: str, descending: bool = False) -> TS[Frame[ROW]]:
    return ts.value if ts.value.num_rows < 2 else ts.value.sort_by([(by, "descending" if descending else "ascending")])


@operator
def concat(ts1: TS[Frame[ROW]], ts2: TS[Frame[ROW]]) -> TS[Frame[ROW]]: ...


@compute_node(overloads=concat)
def _concat(ts1: TS[Frame[ROW]], ts2: TS[Frame[ROW]]) -> TS[Frame[ROW]]:
    return pa.concat_tables([ts1.value, ts2.value])


@operator
def with_columns(
    ts: TS[Frame[ROW]], _tp_out: type[ROW_1] = AUTO_RESOLVE, **columns: TSB[TS_SCHEMA]
) -> TS[Frame[ROW_1]]: ...


def _with_columns(table: pa.Table, columns) -> pa.Table:
    for name, value in columns.value.items():
        if isinstance(value, pa.Scalar):
            value = value.as_py()
        if not isinstance(value, (pa.Array, pa.ChunkedArray)):
            value = pa.array([value] * table.num_rows)
        index = table.schema.get_field_index(name)
        table = table.set_column(index, name, value) if index >= 0 else table.append_column(name, value)
    return table


def _columns_output_type(mapping, _tp_out):
    return mapping["ROW"] if _tp_out is AUTO_RESOLVE else _tp_out


@compute_node(overloads=with_columns, all_valid=("columns",), resolvers={ROW_1: _columns_output_type})
def _with_columns_node(
    ts: TS[Frame[ROW]], _tp_out: type[ROW_1] = AUTO_RESOLVE, **columns: TSB[TS_SCHEMA]
) -> TS[Frame[ROW_1]]:
    names = getattr(_tp_out, "__meta_data_schema__", None)
    if names is None:
        from dataclasses import fields

        names = tuple(field.name for field in fields(_tp_out))
    return _with_columns(ts.value, columns).select(tuple(names))
