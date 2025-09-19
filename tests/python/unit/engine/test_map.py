from hgraph import graph, TS, TSD, map_
from hgraph.test import eval_node

def test_map_simple():

    @graph
    def g(tsd: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return map_(lambda v: v + 1, tsd)

    assert eval_node(g, [{1:1}, {2:2}, None, {1:3}]) == [{1: 2}, {2: 3}, None, {1: 4}]
