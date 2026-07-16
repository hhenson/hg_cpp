"""Correctness guards for workloads timed by ``benchmarks/scenarios.py``."""

from collections import Counter
import importlib.util
from pathlib import Path

import hgraph as hg
from hgraph import TS, TSD, compute_node, graph, mesh_, reduce
from hgraph.test import eval_node


_SCENARIOS_PATH = Path(__file__).parents[2] / "benchmarks" / "scenarios.py"
_SPEC = importlib.util.spec_from_file_location("hgraph_benchmark_scenarios", _SCENARIOS_PATH)
bench = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(bench)


def _published(values):
    return [value for value in values if value is not None]


def test_service_pulse_leaves_a_cycle_for_next_cycle_forwarding():
    @graph
    def app(trigger: TS[bool]) -> TS[int]:
        return bench._service_int_pulse(3)

    assert eval_node(
        app,
        [True],
        __end_time__=hg.MIN_ST + 7 * hg.MIN_TD,
    ) == [None, 0, None, 1, None, 2]


def test_benchmark_construct_graph_keeps_each_width_branch_distinct():
    @graph
    def std_app(value: TS[int]) -> TS[int]:
        return bench._wide_chain_std(value, width=3, depth=2)

    @graph
    def py_app(value: TS[int]) -> TS[int]:
        return bench._wide_chain_py(value, width=3, depth=2)

    assert eval_node(std_app, [1, 2]) == [12, 15]
    assert eval_node(py_app, [1, 2]) == [12, 15]


def _request_reply_graph(implementation, path):
    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_service(path, implementation)
        return bench._benchmark_request_reply(value, path=path)

    return app


def test_benchmark_request_reply_implementations_process_every_request():
    inputs = [1, None, 2, None, 3]
    assert _published(eval_node(
        _request_reply_graph(bench._benchmark_request_reply_std_impl, "bench_rr_std"),
        inputs,
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [4, 6, 8]
    assert _published(eval_node(
        _request_reply_graph(bench._benchmark_request_reply_py_impl, "bench_rr_py"),
        inputs,
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [4, 6, 8]


def _subscription_graph(implementation, path):
    @graph
    def app(key: TS[str]) -> TS[int]:
        hg.register_service(path, implementation)
        return bench._benchmark_subscription(key, path=path)

    return app


def test_benchmark_subscription_implementations_process_every_key_change():
    inputs = ["a", None, "bb", None, "ccc"]

    assert _published(eval_node(
        _subscription_graph(bench._benchmark_subscription_std_impl, "bench_sub_std"),
        inputs,
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [1, 1, 1]
    assert _published(eval_node(
        _subscription_graph(bench._benchmark_subscription_py_impl, "bench_sub_py"),
        inputs,
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [1, 2, 3]


def _service_adaptor_graph(implementation, path):
    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_adaptor(path, implementation)
        return bench._benchmark_service_adaptor(value, path=path)

    return app


def test_benchmark_service_adaptor_implementations_process_every_request():
    inputs = [1, None, 2, None, 3]
    assert _published(eval_node(
        _service_adaptor_graph(bench._benchmark_service_adaptor_std_impl, "bench_sa_std"),
        inputs,
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [4, 6, 8]
    assert _published(eval_node(
        _service_adaptor_graph(bench._benchmark_service_adaptor_py_impl, "bench_sa_py"),
        inputs,
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [4, 6, 8]


def test_benchmark_four_client_request_reply_reaches_every_map_child():
    seen = []

    @compute_node
    def observe(value: TS[int]) -> TS[int]:
        seen.append(value.value)
        return value.value

    @hg.service_impl(interfaces=bench._benchmark_request_reply)
    def implementation(request: TSD[int, TS[int]], path: str) -> TSD[int, TS[int]]:
        return hg.map_(observe, request)

    @graph
    def app(value: TS[int]) -> TS[int]:
        path = "bench_rr_four_clients"
        hg.register_service(path, implementation)
        replies = [bench._benchmark_request_reply(value, path=path) for _ in range(4)]
        return replies[0] + replies[1] + replies[2] + replies[3]

    assert _published(eval_node(
        app,
        [1, None, 2, None, 3],
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [4, 8, 12]
    assert Counter(seen) == Counter({1: 4, 2: 4, 3: 4})


def test_benchmark_four_client_service_adaptor_reaches_every_map_child():
    seen = []

    @compute_node
    def observe(value: TS[int]) -> TS[int]:
        seen.append(value.value)
        return value.value

    @hg.service_adaptor_impl(interfaces=bench._benchmark_service_adaptor)
    def implementation(request: TSD[int, TS[int]], path: str) -> TSD[int, TS[int]]:
        return hg.map_(observe, request)

    @graph
    def app(value: TS[int]) -> TS[int]:
        path = "bench_sa_four_clients"
        hg.register_adaptor(path, implementation)
        replies = [
            bench._benchmark_service_adaptor(value + client, path=path)
            for client in range(4)
        ]
        return replies[0] + replies[1] + replies[2] + replies[3]

    assert _published(eval_node(
        app,
        [1, None, 2, None, 3],
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [10, 14, 18]
    assert Counter(seen) == Counter({1: 1, 2: 2, 3: 3, 4: 3, 5: 2, 6: 1})


def test_benchmark_duplex_adaptor_variants_forward_every_tick():
    def adaptor_graph(implementation, path):
        @graph
        def app(value: TS[int]) -> TS[int]:
            hg.register_adaptor(path, implementation)
            return bench._benchmark_adaptor(value, path=path)

        return app

    assert eval_node(
        adaptor_graph(bench._benchmark_adaptor_std_impl, "bench_adaptor_std"),
        [1, 2, 3],
    ) == [2, 3, 4]
    assert eval_node(
        adaptor_graph(bench._benchmark_adaptor_py_impl, "bench_adaptor_py"),
        [1, 2, 3],
    ) == [2, 3, 4]


def _source_graph(factory):
    @graph
    def app(trigger: TS[bool]) -> TS[int]:
        return factory()

    return app


def test_benchmark_dense_map_reduce_processes_every_key_and_cycle():
    expected = [0, 20, 28, 36]

    assert eval_node(
        _source_graph(lambda: bench._map_reduce_std(bench._tsd_dense_pulse(3, 4))),
        [True],
        __end_time__=hg.MIN_ST + 5 * hg.MIN_TD,
    ) == expected
    assert eval_node(
        _source_graph(lambda: bench._map_reduce_py(bench._tsd_dense_pulse(3, 4))),
        [True],
        __end_time__=hg.MIN_ST + 5 * hg.MIN_TD,
    ) == expected


def test_benchmark_sparse_map_reduce_processes_sparse_updates():
    assert eval_node(
        _source_graph(lambda: bench._map_reduce_std(bench._tsd_sparse_pulse(4, 8, 2))),
        [True],
        __end_time__=hg.MIN_ST + 6 * hg.MIN_TD,
    ) == [0, 16, 20, 28, 40]


def test_benchmark_churn_map_reduce_processes_adds_and_removes():
    assert eval_node(
        _source_graph(lambda: bench._map_reduce_std(bench._tsd_churn_pulse(4, 4, 1))),
        [True],
        __end_time__=hg.MIN_ST + 6 * hg.MIN_TD,
    ) == [0, 20, 22, 24, 26]


def test_benchmark_mesh_processes_dependencies_and_key_churn():
    def meshed():
        source = bench._tsd_churn_pulse(4, 8, 2)
        return reduce(hg.add_, mesh_(bench._mesh_cell, source), 0)

    actual = eval_node(
        _source_graph(meshed),
        [True],
        __end_time__=hg.MIN_ST + 6 * hg.MIN_TD,
    )
    # hg-cpp settles the dependency chain within a cycle; upstream publishes
    # its intermediate settlement. Both traces cover the same churn workload.
    assert actual in (
        [0, 84, 87, 97, 118],
        [0, 28, 85, 81, 68, 83],
    )
