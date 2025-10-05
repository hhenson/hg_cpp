from hgraph import graph, TS, TSD, switch_
from hgraph.test import eval_node


def test_switch_simple():
    @graph
    def g(on: TS[bool], ts: TS[int]) -> TS[int]:
        return switch_(
            on,
            {
                True: lambda v: v + 1,
                False: lambda v: v - 1
            },
            ts
        )

    result = eval_node(g, [True, None, False, None], [1, 2, 3, 4])
    print(f"result: {result}")
    assert result == [2, 3, 2, 3]
