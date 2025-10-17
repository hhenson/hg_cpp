import hg_cpp  # Comment this out to use Python instead of C++
from datetime import datetime, timedelta

from hgraph import graph, TS, TSW, compute_node, to_window, MIN_ST, MIN_TD, SIGNAL, Array, WINDOW_SIZE, WINDOW_SIZE_MIN
from hgraph.test import eval_node


def test_tsw_value_property():

    @graph
    def g(ts: TS[int]) -> TS[object]:
        w = to_window(ts, 3, 2)
        return c(w)

    @compute_node(all_valid=("tsw",))
    def c(tsw: TSW[int, WINDOW_SIZE, WINDOW_SIZE_MIN]) -> TS[object]:
        v = tsw.value
        return None if v is None else [int(x) for x in v]

    assert eval_node(g, [1, 2, 3, 4]) == [None, [1, 2], [1, 2, 3], [2, 3, 4]]


def test_tsw_delta_value_property():

    @graph
    def g(ts: TS[int]) -> TS[int]:
        w = to_window(ts, 3, 2)
        return c(w)

    @compute_node
    def c(tsw: TSW[int, WINDOW_SIZE, WINDOW_SIZE_MIN]) -> TS[int]:
        return int(tsw.delta_value) if tsw.delta_value is not None else None

    # delta_value should tick the latest appended scalar on each evaluation
    assert eval_node(g, [1, 2, 3, 4]) == [1, 2, 3, 4]


def test_tsw_value_times_property():

    @graph
    def g(ts: TS[int]) -> TS[object]:
        w = to_window(ts, 3, 2)
        return c(w)

    @compute_node(all_valid=("tsw",))
    def c(tsw: TSW[int, WINDOW_SIZE, WINDOW_SIZE_MIN]) -> TS[object]:
        vt = tsw.value_times
        return None if vt is None else list(vt)

    t0 = MIN_ST
    t1 = MIN_ST + MIN_TD
    t2 = MIN_ST + 2 * MIN_TD
    t3 = MIN_ST + 3 * MIN_TD
    assert eval_node(g, [1, 2, 3, 4]) == [None, [t0, t1], [t0, t1, t2], [t1, t2, t3]]


def test_tsw_first_modified_time_property():

    @graph
    def g(ts: TS[int]) -> TS[datetime]:
        w = to_window(ts, 3, 1)
        return c(w)

    @compute_node
    def c(tsw: TSW[int, WINDOW_SIZE, WINDOW_SIZE_MIN]) -> TS[datetime]:
        return tsw.first_modified_time

    # As the window fills and then rolls, the first_modified_time advances accordingly
    assert eval_node(g, [1, 2, 3, 4]) == [MIN_ST, MIN_ST, MIN_ST, MIN_ST + MIN_TD]


def test_tsw_removed_value():

    @graph
    def g(ts: TS[int]) -> TS[object]:
        w = to_window(ts, 3, 0)
        return c(w)

    @compute_node
    def c(tsw: TSW[int, WINDOW_SIZE, WINDOW_SIZE_MIN]) -> TS[object]:
        return tsw.removed_value if tsw.has_removed_value else None

    # On the 4th and 5th ticks, the window evicts the oldest values 1 then 2
    assert eval_node(g, [1, 2, 3, 4, 5]) == [None, None, None, 1, 2]


def test_tsw_all_valid_min_size():

    @graph
    def g(ts: TS[int]) -> TS[bool]:
        w = to_window(ts, 3, 2)
        return c(w)

    @compute_node
    def c(tsw: TSW[int, WINDOW_SIZE, WINDOW_SIZE_MIN]) -> TS[bool]:
        return tsw.all_valid

    assert eval_node(g, [1, 2, 3]) == [False, True, True]


def test_tsw_len():

    @graph
    def g(ts: TS[int]) -> TS[int]:
        w = to_window(ts, 3, 1)
        return c(w)

    @compute_node
    def c(tsw: TSW[int, WINDOW_SIZE, WINDOW_SIZE_MIN]) -> TS[int]:
        return len(tsw)

    assert eval_node(g, [1, 2, 3, 4]) == [1, 2, 3, 3]
