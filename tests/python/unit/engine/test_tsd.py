import hg_cpp  # Comment this out to use Python instead of C++

from datetime import datetime

from frozendict import frozendict as fd, frozendict
from hgraph import graph, TS, compute_node, MIN_ST, MIN_TD, SIGNAL, contains_, TSD, \
    REMOVE
from hgraph.test import eval_node


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

    assert eval_node(g, [{0:1}, {0:2}, {1:2}]) == [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD]


def test_tsd_operations_valid():
    @graph
    def g(a: TSD[int, TS[int]], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSD[int, TS[int]], b: SIGNAL) -> TS[bool]:
        return a.valid

    assert eval_node(g, [None, {0:1}], [True, True]) == [False, True]


def test_tsd_operations_all_valid():
    @graph
    def g(a: TSD[int, TS[int]], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSD[int, TS[int]], b: SIGNAL) -> TS[bool]:
        return a.all_valid

    assert eval_node(g, [None, {0: 1}, {1: 2}], [True, True, True]) == [False, True, True]


def test_tsd_operations_modified():
    @graph
    def g(a: TSD[int, TS[int]], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSD[int, TS[int]], b: SIGNAL) -> TS[bool]:
        return a.modified

    assert eval_node(g, [None, {0: 1}, None, {1: 1}], [True, True, None, True]) == [False, True, None, True]


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

    assert eval_node(g, [{0: 1}, {1: 2}, {0: 3}, {1: 4}], [True, True, None, True]) == [False, True, None, False]


def test_tsd_contains():
    @graph
    def g(a: TSD[int, TS[int]], b: TS[int]) -> TS[bool]:
        return contains_(a, b)

    assert eval_node(g, [{0: 1, 1: 2}, {2: 3}], [2, None]) == [False, True]


def test_tsd_modified():

    @compute_node()
    def c(a: TSD[int, TS[int]]) -> TS[frozendict[int, int]]:
        #added = list(a.added_items())
        b = frozendict({k: t.delta_value for k, t in a.modified_items()})
        return b

    assert eval_node(c, [{0: 1}, {1: 1}]) == [frozendict({0: 1}), frozendict({1: 1})]

def test_tsd_modified_value():

    @compute_node()
    def c(a: TSD[int, TS[int]]) -> TS[frozendict[int, int]]:
        #added = list(a.added_items())
        b = frozendict({k: t.value for k, t in a.modified_items()})
        return b

    assert eval_node(c, [{0: 1}, {1: 1}, {1: 2}]) == [frozendict({0: 1}), frozendict({1: 1}), frozendict({1: 2})]


def test_tsd_added_value():

    @compute_node()
    def c(a: TSD[int, TS[int]]) -> TS[frozendict[int, int]]:
        b = frozendict({k: t.value for k, t in a.added_items()})
        return b

    assert eval_node(c, [{0: 1}, {1: 1}]) == [frozendict({0: 1}), frozendict({1: 1})]

