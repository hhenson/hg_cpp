import hg_cpp  # Comment this out to use Python instead of C++
from datetime import datetime, timedelta

from hgraph import graph, TS, compute_node, MIN_ST, MIN_TD, SIGNAL, TSL, Size
from hgraph.test import eval_node


def test_tsl_simple():
    @graph
    def g(a: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return TSL.from_ts(a[0] + 1, a[1] + 2)

    assert eval_node(g, [{0:1, 1:1}, {0:2, 1:2}, {0:3, 1:3}]) == [{0:2, 1:3}, {0:3, 1:4}, {0:4, 1:5}]


def test_tsl_operations_value():
    @compute_node
    def g(a: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return a.value

    assert eval_node(g, [{0:1, 1:1}, {0:2,}, {0:3, 1:3}]) == [{0:1, 1:1}, {0:2, 1:1}, {0:3, 1:3}]


def test_tsl_operations_delta_value():
    @compute_node
    def g(a: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return a.delta_value

    assert eval_node(g, [{0:1, 1:1}, {0:2}, {1:2}]) == [{0:1, 1:1}, {0:2}, {1:2}]


def test_tsl_operations_last_modified_time():
    @compute_node
    def g(a: TSL[TS[int], Size[2]]) -> TS[datetime]:
        return a.last_modified_time

    assert eval_node(g, [{0:1, 1:1}, {0:2}, {1:2}]) == [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD]


def test_tsl_operations_valid():
    @graph
    def g(a: TSL[TS[int], Size[2]], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSL[TS[int], Size[2]], b: SIGNAL) -> TS[bool]:
        return a.valid

    assert eval_node(g, [None, {0: 1}], [True, True]) == [False, True]


def test_tsl_operations_all_valid():
    @graph
    def g(a: TSL[TS[int], Size[2]], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSL[TS[int], Size[2]], b: SIGNAL) -> TS[bool]:
        return a.all_valid

    assert eval_node(g, [None, {0: 1}, {1: 1}], [True, True, True]) == [False, False, True]


def test_tsl_operations_modified():
    @graph
    def g(a: TSL[TS[int], Size[2]], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSL[TS[int], Size[2]], b: SIGNAL) -> TS[bool]:
        return a.modified

    assert eval_node(g, [None, {0: 1}, {1: 2}, None], [True, True, None, True]) == [False, True, None, False]


def test_tsl_operations_active():
    @graph
    def g(a: TSL[TS[int], Size[2]], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSL[TS[int], Size[2]], b: SIGNAL) -> TS[bool]:
        active = a.active
        if active:
            a.make_passive()
        else:
            a.make_active()
        return active

    assert eval_node(g, [{0: 1}, {1: 2}, {0: 2}, {0: 3}], [True, True, None, True]) == [False, True, None, False]
