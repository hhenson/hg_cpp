from datetime import datetime, timedelta, timezone

import hgraph as hg

from hgraph.adaptors.run_graph_on_thread import (
    RunGraphOutput,
    publish_output,
    run_graph_on_thread,
    run_graph_on_thread_impl,
)
from hgraph.adaptors.run_graph_on_thread.run_graph_on_thread import (
    _OUTPUT_CALLBACK,
    _typed_output,
)


@hg.graph
def _sum_graph(a: hg.TS[int], b: hg.TS[int]):
    publish_output(a + b)


def test_typed_output_schema_can_be_used_by_eval_node():
    schema = RunGraphOutput[hg.TS[int]]
    assert hg.TSB[schema].handle.is_tsb

    @hg.graph
    def typed(raw: hg.TS[object]) -> hg.TS[int]:
        return _typed_output[hg.OUT : hg.TS[int]](raw)

    assert hg.eval_node(typed, [3, 4], resolution_dict={"raw": hg.TS[object]}) == [3, 4]


def test_publish_output_can_publish_full_values_instead_of_deltas():
    captured = []

    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]):
        publish_output(values, delta=False)

    state = hg.GlobalState()
    state[_OUTPUT_CALLBACK] = captured.append
    with hg.GlobalContext(state):
        assert hg.eval_node(app, [{"a": 1}, {"b": 2}]) is None

    assert captured == [{"a": 1}, {"a": 1, "b": 2}]


def test_run_graph_on_thread_simulation_mode():
    captured = []

    @hg.sink_node
    def capture(result: hg.TSB[RunGraphOutput[hg.TS[int]]], engine: hg.EvaluationEngineApi = None):
        captured.append(result.value)
        if result.finished.valid and result.finished.value:
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("thread-test", run_graph_on_thread_impl)
        result = run_graph_on_thread[hg.TS[int]](
            _sum_graph,
            global_state={"seed": 1},
            params={
                "a": 1,
                "b": 2,
                "start_time": hg.MIN_ST,
                "end_time": hg.MIN_ST + timedelta(seconds=1),
            },
            path="thread-test",
        )
        capture(result)

    with hg.GlobalContext(hg.GlobalState()):
        hg.run_graph(
            app,
            run_mode=hg.EvaluationMode.REAL_TIME,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=5),
        )

    assert any(value.get("out") == 3 for value in captured)
    assert captured[-1]["finished"] is True
    assert captured[-1]["status"] == "OK"
