# Ported from ext/main/hgraph_unit_tests/_wiring/test_overloads.py
from dataclasses import dataclass
from typing import Tuple

import pyarrow as pa
import pytest

from hgraph import (
    compute_node,
    TIME_SERIES_TYPE,
    graph,
    TS,
    TSL,
    SIZE,
    Size,
    SCALAR,
    contains_,
    SCALAR_1,
    SCALAR_2,
    RequirementsNotMetWiringError,
    CompoundScalar,
    Frame,
    cast_,
    operator,
)
from hgraph.test import eval_node


def test_overloads():

    @operator
    def add(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE: ...

    @compute_node(overloads=add)
    def add_default(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
        return lhs.value + rhs.value

    @graph
    def t_add(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
        return add(lhs, rhs)

    @compute_node(overloads=add)
    def add_ints(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return lhs.value + rhs.value + 1

    @compute_node(overloads=add)
    def add_strs(lhs: TS[str], rhs: TS[str]) -> TS[str]:
        return lhs.value + rhs.value + "~"

    @graph(overloads=add)
    def add_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
        return TSL.from_ts(*[a + b for a, b in zip(lhs, rhs)])

    assert eval_node(t_add[TIME_SERIES_TYPE : TS[int]], lhs=[1, 2, 3], rhs=[1, 5, 7]) == [3, 8, 11]
    assert eval_node(t_add[TIME_SERIES_TYPE : TS[float]], lhs=[1.0, 2.0, 3.0], rhs=[1.0, 5.0, 7.0]) == [2.0, 7.0, 10.0]
    assert eval_node(t_add[TIME_SERIES_TYPE : TS[str]], lhs=["1.", "2.", "3."], rhs=["1.", "5.", "7."]) == [
        "1.1.~",
        "2.5.~",
        "3.7.~",
    ]
    assert eval_node(t_add[TIME_SERIES_TYPE : TSL[TS[int], Size[2]]], lhs=[(1, 1)], rhs=[(2, 2)]) == [{0: 3, 1: 3}]


def test_scalar_overloads():

    @operator
    def add(lhs: TS[SCALAR], rhs: SCALAR) -> TS[SCALAR]: ...

    @compute_node(overloads=add)
    def add_default(lhs: TS[SCALAR], rhs: SCALAR) -> TS[SCALAR]:
        return lhs.value + rhs

    @graph
    def t_add(lhs: TIME_SERIES_TYPE, rhs: SCALAR) -> TIME_SERIES_TYPE:
        return add(lhs, rhs)

    @compute_node(overloads=add)
    def add_ints(lhs: TS[int], rhs: int) -> TS[int]:
        return lhs.value + rhs + 1

    @compute_node(overloads=add)
    def add_ints(lhs: TS[int], rhs: SCALAR) -> TS[int]:
        return lhs.value + rhs + 2

    @compute_node(overloads=add)
    def add_strs(lhs: TS[str], rhs: str) -> TS[str]:
        return lhs.value + rhs + "~"

    @compute_node(overloads=add)
    def add_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: Tuple[SCALAR, ...]) -> TSL[TIME_SERIES_TYPE, SIZE]:
        return tuple(a.value + b for a, b in zip(lhs.values(), rhs))

    assert eval_node(t_add[TIME_SERIES_TYPE : TS[int]], lhs=[1, 2, 3], rhs=1) == [3, 4, 5]
    assert eval_node(t_add[TIME_SERIES_TYPE : TS[float], SCALAR:float], lhs=[1.0, 2.0, 3.0], rhs=1.0) == [2.0, 3.0, 4.0]
    assert eval_node(t_add[TIME_SERIES_TYPE : TS[str]], lhs=["1.", "2.", "3."], rhs=".") == ["1..~", "2..~", "3..~"]
    assert eval_node(t_add[TIME_SERIES_TYPE : TSL[TS[int], Size[2]]], lhs=[(1, 1)], rhs=(2, 2)) == [{0: 3, 1: 3}]


def test_contains():
    @graph
    def main(ts: TS[frozenset[int]], item: TS[int]) -> TS[bool]:
        return contains_(ts, item)

    assert eval_node(main, [frozenset({1}), frozenset({1, 2}), frozenset({3})], [2]) == [False, True, False]


def test_requires():
    @compute_node(requires=lambda m: m[SCALAR] != m[SCALAR_1])
    def add(lhs: TS[SCALAR], rhs: TS[SCALAR_1]) -> TS[SCALAR]:
        return lhs.value + type(lhs.value)(rhs.value)

    assert eval_node(add[SCALAR:int, SCALAR_1:float], 1, 2.0) == [3]
    with pytest.raises(RequirementsNotMetWiringError):
        assert eval_node(add[SCALAR:int, SCALAR_1:int], 1, 2) == [3]
    assert eval_node(add[SCALAR:float, SCALAR_1:int], 1.0, 2) == [3.0]


def test_requires_with_scalars():
    # Test using named scalar parameters in requires lambda
    @compute_node(requires=lambda m, __strict__: __strict__ == True)
    def add_strict(lhs: TS[int], rhs: TS[int], __strict__: bool = False) -> TS[int]:
        return lhs.value + rhs.value

    assert eval_node(add_strict, 1, 2, __strict__=True) == [3]
    with pytest.raises(RequirementsNotMetWiringError):
        eval_node(add_strict, 1, 2, __strict__=False)

    # Test with multiple scalar parameters
    @compute_node(requires=lambda m, min_value, max_value: min_value < max_value)
    def clamp(value: TS[int], min_value: int, max_value: int) -> TS[int]:
        return max(min_value, min(max_value, value.value))

    assert eval_node(clamp, [5, 15, 25], min_value=10, max_value=20) == [10, 15, 20]
    with pytest.raises(RequirementsNotMetWiringError):
        eval_node(clamp, [5, 15, 25], min_value=20, max_value=10)


def test_native_compatibility_operator_accepts_python_overload():
    @compute_node(overloads=cast_)
    def cast_probe(tp: type[str], ts: TS[str]) -> TS[str]:
        return f"probe:{ts.value}"

    @graph
    def app(ts: TS[str]) -> TS[str]:
        return cast_(str, ts)

    assert eval_node(app, ["value"]) == ["probe:value"]


def test_generic_frame_overload_is_distinct_from_generic_scalar_overload():
    @dataclass(frozen=True)
    class Row(CompoundScalar):
        value: int

    @operator
    def classify_value(ts: TS[SCALAR]) -> TS[str]: ...

    @compute_node(overloads=classify_value)
    def classify_scalar(ts: TS[SCALAR]) -> TS[str]:
        return "scalar"

    @compute_node(overloads=classify_value)
    def classify_frame(ts: TS[Frame[SCALAR]]) -> TS[str]:
        return "frame"

    @graph
    def scalar_app(ts: TS[int]) -> TS[str]:
        return classify_value(ts)

    @graph
    def frame_app(ts: TS[Frame[Row]]) -> TS[str]:
        return classify_value(ts)

    assert eval_node(scalar_app, [1]) == ["scalar"]
    assert eval_node(frame_app, [pa.table({"value": [1]})]) == ["frame"]
