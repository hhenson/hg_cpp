# Ported from ext/main/hgraph_unit_tests/_wiring/test_auto_resolve.py
from typing import Callable, Type

from hgraph import graph, TSL, TS, SIZE, Size, AUTO_RESOLVE, SCALAR, SCALAR_1, compute_node
from hgraph import const
from hgraph.test import eval_node


def test_auto_resolve():

    @graph
    def g(tsl: TSL[TS[int], SIZE], _sz: type[SIZE] = AUTO_RESOLVE) -> TS[int]:
        return const(_sz.SIZE)

    assert eval_node(g, [(1, 2)], resolution_dict={"tsl": TSL[TS[int], Size[2]]}) == [2]


def test_func_resolve():
    def x(x) -> str:
        return str(x)

    @compute_node(resolvers={SCALAR_1: lambda mapping, f: f.__annotations__["return"]})
    def call(ts: TS[SCALAR], f: type(x)) -> TS[SCALAR_1]:
        return f(ts.value)

    assert eval_node(call[SCALAR:int], [1, 2], f=x) == ["1", "2"]


def test_compute_node_receives_auto_resolved_scalar_type():
    @compute_node
    def type_name(ts: TS[SCALAR], tp: Type[SCALAR] = AUTO_RESOLVE) -> TS[str]:
        return tp.__name__

    assert eval_node(type_name, [1, 2]) == ["int", "int"]
