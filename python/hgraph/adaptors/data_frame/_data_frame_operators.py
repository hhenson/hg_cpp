"""Python nodes for manipulating Arrow-backed :class:`~hgraph.Frame` values."""

import operator as operators
from typing import TypeVar

import _hgraph
import pyarrow.compute as pc

from hgraph import (
    AUTO_RESOLVE,
    SCALAR,
    TS_SCHEMA,
    TSB,
    TSD,
    TS,
    Frame,
    KEYABLE_SCALAR,
    add_,
    and_,
    compute_node,
    div_,
    eq_,
    filter_,
    floordiv_,
    ge_,
    gt_,
    graph,
    le_,
    lt_,
    mul_,
    operator_function,
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


join = operator_function("join")


_filter_frame_native = operator_function("filter_frame")


def _pack_tsb(values):
    from hgraph._wiring._core import WiringPort, _unwrap

    ports = {name: _unwrap(value) for name, value in values.items()}
    schema = _hgraph.un_named_tsb_type(
        [(name, port.ts_type) for name, port in ports.items()]
    )
    return WiringPort(_hgraph.tsb_port(schema, ports))


@graph
def filter_frame(ts: TS[Frame[ROW]], **predicate: TSB[TS_SCHEMA]) -> TS[Frame[ROW]]:
    return _filter_frame_native(ts, _pack_tsb(predicate))


_filter_cs_native = operator_function("filter_cs")


@graph
def filter_cs(ts: TS[Frame[ROW]], predicate: TS[ROW]) -> TS[Frame[ROW]]:
    return _filter_cs_native(ts, predicate)


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


group_by = operator_function("group_by")


ungroup = operator_function("ungroup")


def _explicit_output_type(mapping, _tp_out):
    return _tp_out


@graph(overloads=ungroup, resolvers={ROW_1: _explicit_output_type})
def _ungroup_typed(
    ts: TSD[KEYABLE_SCALAR, TS[Frame[ROW]]], key_col: str, _tp_out: type[ROW_1] = AUTO_RESOLVE
) -> TS[Frame[ROW_1]]:
    return ungroup[TS[Frame[_tp_out]]](ts, key_col)


@graph(overloads=ungroup, resolvers={ROW_1: _explicit_output_type})
def _ungroup_typed_tuple(
    ts: TSD[KEYABLE_SCALAR, TS[Frame[ROW]]], key_col: tuple[str, ...], _tp_out: type[ROW_1] = AUTO_RESOLVE
) -> TS[Frame[ROW_1]]:
    return ungroup[TS[Frame[_tp_out]]](ts, key_col)


sorted_ = operator_function("sorted_")


concat = operator_function("concat")


with_columns = operator_function("with_columns")


def _columns_output_type(mapping, _tp_out):
    return mapping["ROW"] if _tp_out is AUTO_RESOLVE else _tp_out


@graph(overloads=with_columns, resolvers={ROW_1: _columns_output_type})
def _with_columns_adapter(
    ts: TS[Frame[ROW]], _tp_out: type[ROW_1] = AUTO_RESOLVE, **columns: TSB[TS_SCHEMA]
) -> TS[Frame[ROW_1]]:
    return with_columns[TS[Frame[_tp_out]]](ts, _pack_tsb(columns))
