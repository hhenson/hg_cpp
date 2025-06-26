from datetime import datetime, timedelta

from hgraph import graph, TS, compute_node, MIN_ST, MIN_TD, SIGNAL
from hgraph.test import eval_node

def test_simple():

    @graph
    def g(a: TS[int]) -> TS[int]:
        return a + 1

    assert eval_node(g, [1, 2, 3]) == [2, 3, 4]

def test_ts_operations_value():

    @compute_node
    def g(a: TS[int]) -> TS[int]:
        return a.value

    assert eval_node(g, [1, 2, 3]) == [1, 2, 3]


def test_ts_operations_delta_value():

    @compute_node
    def g(a: TS[int]) -> TS[int]:
        return a.delta_value

    assert eval_node(g, [1, 2, 3]) == [1, 2, 3]


def test_ts_operations_last_modified_time():

    @compute_node
    def g(a: TS[int]) -> TS[datetime]:
        return a.last_modified_time

    assert eval_node(g, [1, None, 3]) == [MIN_ST, None, MIN_ST + 2*MIN_TD]


def test_ts_signal_value():

    @graph
    def g(a: TS[int]) -> TS[bool]:
        return c(a)

    @compute_node
    def c(a: SIGNAL) -> TS[bool]:
        return a.value

    assert eval_node(g, [1, 2, 3]) == [True, True, True]


def test_ts_signal_delta_value():

    @graph
    def g(a: TS[int]) -> TS[bool]:
        return c(a)

    @compute_node
    def c(a: SIGNAL) -> TS[bool]:
        return a.delta_value

    assert eval_node(g, [1, 2, 3]) == [True, True, True]


def test_signal_operations_last_modified_time():

    @graph
    def g(a: TS[int]) -> TS[datetime]:
        return c(a)

    @compute_node
    def c(a: SIGNAL) -> TS[datetime]:
        return a.last_modified_time

    assert eval_node(g, [1, None, 3]) == [MIN_ST, None, MIN_ST + 2*MIN_TD]


def test_ts_operations_valid():

    @graph
    def g(a: TS[int], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TS[int], b: SIGNAL) -> TS[bool]:
        return a.valid

    assert eval_node(g, [None, 1], [True, True]) == [False, True]


def test_ts_operations_all_valid():

    @graph
    def g(a: TS[int], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TS[int], b: SIGNAL) -> TS[bool]:
        return a.all_valid

    assert eval_node(g, [None, 1], [True, True]) == [False, True]

def test_ts_operations_modified():

    @graph
    def g(a: TS[int], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TS[int], b: SIGNAL) -> TS[bool]:
        return a.modified

    assert eval_node(g, [None, 1, 2, None], [True, True, None, True]) == [False, True, None, False]


def test_ts_operations_active():

    @graph
    def g(a: TS[int], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TS[int], b: SIGNAL) -> TS[bool]:
        active = a.active
        if active:
            a.make_passive()
        else:
            a.make_active()
        return active

    assert eval_node(g, [1, 2, 3, 4], [True, True, None, True]) == [False, True, None, False]