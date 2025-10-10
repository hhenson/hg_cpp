import hg_cpp  # Comment this out to use Python instead of C++
from datetime import datetime

from hgraph import graph, TS, compute_node, MIN_ST, MIN_TD, SIGNAL, TSL, TimeSeriesSchema, TSB, combine
from hgraph.test import eval_node


class MyTsbSchema(TimeSeriesSchema):
    a: TS[int]
    b: TS[float]


def test_tsb_simple():
    @graph
    def g(a: TSB[MyTsbSchema]) -> TSB[MyTsbSchema]:
        return combine(a=a[0] + 1, b=a[1] + 2.0)

    assert eval_node(g, [{"a": 1, "b": 1.0}, {"a": 2, "b": 2.0}, {"a": 3, "b": 3.0}]) == [{"a": 2, "b": 3.0},
                                                                                          {"a": 3, "b": 4.0},
                                                                                          {"a": 4, "b": 5.0}]


def test_tsb_operations_value():
    @compute_node
    def g(a: TSB[MyTsbSchema]) -> TSB[MyTsbSchema]:
        return a.value

    assert eval_node(g, [{"a": 1, "b": 1.0}, {"a": 2, }, {"a": 3, "b": 3.0}]) == [{"a": 1, "b": 1.0},
                                                                                  {"a": 2, "b": 1.0},
                                                                                  {"a": 3, "b": 3.0}]


def test_tsb_operations_delta_value():
    @compute_node
    def g(a: TSB[MyTsbSchema]) -> TSB[MyTsbSchema]:
        return a.delta_value

    assert eval_node(g, [{"a": 1, "b": 1.}, {"a": 2}, {"b": 2.}]) == [{"a": 1, "b": 1.}, {"a": 2}, {"b": 2.}]


def test_tsb_operations_last_modified_time():
    @compute_node
    def g(a: TSB[MyTsbSchema]) -> TS[datetime]:
        return a.last_modified_time

    assert eval_node(g, [{"a": 1, "b": 1.}, {"a": 2}, {"b": 2.}]) == [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD]


def test_tsb_operations_valid():
    @graph
    def g(a: TSB[MyTsbSchema], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSB[MyTsbSchema], b: SIGNAL) -> TS[bool]:
        return a.valid

    assert eval_node(g, [None, {"a": 1}], [True, True]) == [False, True]


def test_tsb_operations_all_valid():
    @graph
    def g(a: TSB[MyTsbSchema], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSB[MyTsbSchema], b: SIGNAL) -> TS[bool]:
        return a.all_valid

    assert eval_node(g, [None, {"a": 1}, {"b": 1.}], [True, True, True]) == [False, False, True]


def test_tsb_operations_modified():
    @graph
    def g(a: TSB[MyTsbSchema], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSB[MyTsbSchema], b: SIGNAL) -> TS[bool]:
        return a.modified

    assert eval_node(g, [None, {"a": 1}, {"b": 2.}, None], [True, True, None, True]) == [False, True, None, False]


def test_tsb_operations_active():
    @graph
    def g(a: TSB[MyTsbSchema], b: TS[bool]) -> TS[bool]:
        return c(a, b)

    @compute_node(active=("b",), valid=("b",))
    def c(a: TSB[MyTsbSchema], b: SIGNAL) -> TS[bool]:
        active = a.active
        if active:
            a.make_passive()
        else:
            a.make_active()
        return active

    assert eval_node(g, [{"a": 1}, {"b": 2.}, {"a": 2}, {"a": 3}], [True, True, None, True]) == [False, True, None,
                                                                                                False]
