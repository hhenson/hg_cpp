from dataclasses import dataclass
from datetime import datetime, date, timedelta

import _hgraph
import pytest
from hgraph import graph, TS, compute_node, MIN_ST, MIN_TD, SIGNAL, TSS, set_delta, CompoundScalar, contains_, TSD, \
    REMOVE
from hgraph.test import eval_node
from frozendict import frozendict as fd


def test_tsd_simple():
    @graph
    def g(a: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return a

    assert eval_node(g, [{1: 1, 2: 2}]) == [fd({1: 1, 2: 2})]


def test_tsd_operations_value():
    @compute_node
    def g(a: TSD[int, TS[int]]) -> TS[fd[int, int]]:
        return a.value

    actual = eval_node(g, [{0: 1, 1: 2}, {2: 3}, {0: REMOVE}])
    print(actual)
    assert actual == [
        fd({0: 1, 1: 2}),
        fd({0: 1, 1: 2, 2: 3}),
        fd({1: 2, 2: 3}),
    ]


def test_tsd_operations_delta_value():
    @compute_node
    def g(a: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return a.delta_value

    actual = eval_node(g, [{0: 1, 1: 2}, {2: 3}, {0: REMOVE}])
    print(actual)
    assert actual == [
        fd({0: 1, 1: 2}), fd({2: 3}), fd({0: REMOVE})
    ]


def test_tsd_operations_last_modified_time():
    @compute_node
    def g(a: TSD[int, TS[int]]) -> TS[datetime]:
        return a.last_modified_time

    assert eval_node(g, [{1}, {2}, {2}]) == [MIN_ST, MIN_ST + MIN_TD, None]


def test_tsd_operations_valid():
    @graph
    def g(a: TSD[int, TS[int]], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSD[int, TS[int]], b: SIGNAL) -> TS[bool]:
        return a.valid

    assert eval_node(g, [None, {1}], [True, True]) == [False, True]


def test_tsd_operations_all_valid():
    @graph
    def g(a: TSD[int, TS[int]], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSD[int, TS[int]], b: SIGNAL) -> TS[bool]:
        return a.all_valid

    assert eval_node(g, [None, {1}, {2}], [True, True, True]) == [False, True, True]


def test_tsd_operations_modified():
    @graph
    def g(a: TSD[int, TS[int]], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSD[int, TS[int]], b: SIGNAL) -> TS[bool]:
        return a.modified

    assert eval_node(g, [None, {1}, None, {1}], [True, True, None, True]) == [False, True, None, False]


def test_tsd_operations_active():
    @graph
    def g(a: TSD[int, TS[int]], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSD[int, TS[int]], b: SIGNAL) -> TS[bool]:
        active = a.active
        if active:
            a.make_passive()
        else:
            a.make_active()
        return active

    assert eval_node(g, [{1}, {2}, {3}, {4}], [True, True, None, True]) == [False, True, None, False]


def test_tsd_contains():
    @graph
    def g(a: TSD[int, TS[int]], b: TS[int]) -> TS[bool]:
        return contains_(a, b)

    assert eval_node(g, [{1, 2}, {3}], [3, None]) == [False, True]
