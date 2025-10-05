from hgraph import graph, TS, TSD, reduce
from hgraph.test import eval_node

def test_reduce_simple():

    @graph
    def g(tsd: TSD[int, TS[int]]) -> TS[int]:
        return reduce(lambda a, b: a+b, tsd, 0)
    result = eval_node(g, [{1:1}, {2:2}, None, {1:3}])
    print(f"result: {result}")
    assert result == [1, 3, None, 5]
