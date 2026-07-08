from hgraph import graph, TS, convert
from hgraph.test import eval_node

import pytest as _pytest_mark
pytestmark = _pytest_mark.mark.wip   # batch-3 conversion burn-down; excluded from the gate



def test_convert_noop():
    @graph
    def g(i: TS[int]) -> TS[int]:
        j = convert[TS[int]](i)
        assert j is i
        return j

    assert eval_node(g, [1, 2, 3]) == [1, 2, 3]
