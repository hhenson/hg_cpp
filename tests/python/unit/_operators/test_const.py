from hgraph import MIN_TD, const, default
from hgraph.test import eval_node

def test_const():
    assert eval_node(const, 1) == [1]


def test_delayed_const():
    assert eval_node(const, 1, delay=MIN_TD*2) == [None, None, 1]


def test_delayed():
    assert eval_node(default, [None, 2, 3], 1) == [1, 2, 3]
    assert eval_node(default, [2, 3, 4], 1) == [2, 3, 4]
