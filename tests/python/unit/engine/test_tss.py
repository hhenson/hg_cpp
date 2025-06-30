from dataclasses import dataclass
from datetime import datetime, date, timedelta

import _hgraph
import pytest
from hgraph import graph, TS, compute_node, MIN_ST, MIN_TD, SIGNAL, TSS, set_delta, CompoundScalar
from hgraph.test import eval_node


@dataclass(frozen=True)
class MyCs(CompoundScalar):
    p1: int
    p2: float


@pytest.mark.parametrize("added,removed,tp,expected", [
    [frozenset({True}), frozenset({False}), bool, _hgraph.SetDelta_bool(frozenset({True}), frozenset({False}))],
    [frozenset({1, 2}), frozenset({3}), int, _hgraph.SetDelta_int(frozenset({1, 2}), frozenset({3}))],
    [frozenset({1.0, 2.0}), frozenset({3.0}), float, _hgraph.SetDelta_float(frozenset({1.0, 2.0}), frozenset({3.0}))],
    [frozenset({date(2025, 1, 1)}), frozenset(), date,
     _hgraph.SetDelta_date(frozenset({date(2025, 1, 1)}), frozenset())],
    [frozenset({datetime(2025, 1, 1)}), frozenset(), datetime,
     _hgraph.SetDelta_date_time(frozenset({datetime(2025, 1, 1)}), frozenset())],
    [frozenset({timedelta(1)}), frozenset(), timedelta,
     _hgraph.SetDelta_time_delta(frozenset({timedelta(1)}), frozenset())],
    [frozenset({MyCs(p1=1, p2=2.0)}), frozenset(), MyCs,
     _hgraph.SetDelta_object(frozenset({MyCs(p1=1, p2=2.0)}), frozenset(), MyCs)],
])
def test_set_delta(added, removed, tp, expected):
    sd = set_delta(added, removed, tp)
    assert sd == expected
    assert sd.tp == tp


def test_tss_simple():
    @graph
    def g(a: TSS[int]) -> TSS[int]:
        return a

    assert eval_node(g, [{1, 2, 3}]) == [set_delta(added=frozenset({1, 2, 3}), removed=frozenset(), tp=int)]


def test_tss_operations_value():
    @compute_node
    def g(a: TSS[int]) -> TS[frozenset[int]]:
        return a.value

    actual = eval_node(g, [{1, 2}, {3}, set_delta(removed=frozenset({1, }), tp=int)])
    print(actual)
    assert actual == [
        frozenset({1, 2}),
        frozenset({1, 2, 3}),
        frozenset({2, 3}),
    ]


def test_tss_operations_delta_value():
    @compute_node
    def g(a: TSS[int]) -> TSS[int]:
        return a.delta_value

    actual = eval_node(g, [{1, 2}, {3}, set_delta(removed=frozenset({1, }), tp=int)])
    print(actual)
    assert actual == [
        set_delta(added=frozenset({1, 2}), removed=frozenset(), tp=int),
        set_delta(added=frozenset({3}), removed=frozenset(), tp=int),
        set_delta(added=frozenset(), removed=frozenset({1}), tp=int),
    ]


def test_tss_operations_last_modified_time():
    @compute_node
    def g(a: TSS[int]) -> TS[datetime]:
        return a.last_modified_time

    assert eval_node(g, [{0: 1, 1: 1}, {0: 2}, {1: 2}]) == [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD]


def test_tss_operations_valid():
    @graph
    def g(a: TSS[int], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSS[int], b: SIGNAL) -> TS[bool]:
        return a.valid

    assert eval_node(g, [None, {0: 1}], [True, True]) == [False, True]


def test_tss_operations_all_valid():
    @graph
    def g(a: TSS[int], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSS[int], b: SIGNAL) -> TS[bool]:
        return a.all_valid

    assert eval_node(g, [None, {0: 1}, {1: 1}], [True, True, True]) == [False, False, True]


def test_tss_operations_modified():
    @graph
    def g(a: TSS[int], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSS[int], b: SIGNAL) -> TS[bool]:
        return a.modified

    assert eval_node(g, [None, {0: 1}, {1: 2}, None], [True, True, None, True]) == [False, True, None, False]


def test_tss_operations_active():
    @graph
    def g(a: TSS[int], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSS[int], b: SIGNAL) -> TS[bool]:
        active = a.active
        if active:
            a.make_passive()
        else:
            a.make_active()
        return active

    assert eval_node(g, [{0: 1}, {1: 2}, {0: 2}, {0: 3}], [True, True, None, True]) == [False, True, None, False]
