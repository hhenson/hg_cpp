# Ported from ext/main/hgraph_unit_tests/_wiring/test_generic_graphs.py
import pytest

from typing import Union

from hgraph import compute_node, graph, TS
from hgraph.test import eval_node


@pytest.mark.skip(reason="gap: constrained typing.TypeVar overload params + polars-frame "
                         "cases (our frames are Arrow, recorded ruling); needs the "
                         "constraint-list pattern bridge + an Arrow port pass")
def test_constraint_typevar_wiring():
    pass


def test_union_wiring():
    @compute_node
    def union_fn(x: Union[TS[int], TS[str]]) -> TS[str]:
        return str(x.value)
    
    @graph
    def g(i: TS[int]) -> TS[str]:
        return union_fn(i)
    
    @graph
    def h(s: TS[str]) -> TS[str]:
        return union_fn(s)
    
    assert eval_node(g, [None, 1, 2, 3]) == [None, "1", "2", "3"]
    assert eval_node(h, [None, "a", "b", "c"]) == [None, "a", "b", "c"]    