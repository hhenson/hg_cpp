import pytest
from hgraph import TS, graph
from hgraph.test import eval_node

def test_simple_addition():
    @graph
    def add_one(x: TS[int]) -> TS[int]:
        return x + 1
    
    assert eval_node(add_one, [1, 2, 3]) == [2, 3, 4]