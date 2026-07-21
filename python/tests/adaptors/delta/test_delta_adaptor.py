from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pyarrow as pa

import hgraph as hg
from hgraph.adaptors.delta import (
    DeltaBackend,
    DeltaSchemaMode,
    DeltaWriteMode,
    delta_query_adaptor,
    delta_query_adaptor_impl,
    delta_read_adaptor,
    delta_read_adaptor_impl,
    delta_write_adaptor,
    delta_write_adaptor_impl,
    publish_tsd_to_delta_table,
    register_delta_backend,
)
from hgraph.adaptors.delta.delta_adaptor_raw import delta_table_maintenance
from hgraph.adaptors.delta.delta_tsd_publisher import tsd_to_frame_batched
from hgraph.stream import StreamStatus


@dataclass(frozen=True)
class _Row(hg.CompoundScalar):
    name: str
    value: int


class _Backend(DeltaBackend):
    def __init__(self):
        self.tables = {"memory/rows": pa.table({"name": ["b", "a"], "value": [2, 1]})}
        self.writes = []
        self.maintenance_calls = []

    def read(self, table_path, *, columns=(), filters=(), storage_options=None):
        table = self.tables[table_path]
        return table.select(columns) if columns else table

    def query(self, base_path, *, tables, query, storage_options=None):
        assert tables == frozenset({"rows"})
        assert query == "select * from rows"
        return self.tables[f"{base_path}/rows"]

    def write(
        self,
        table_path,
        data,
        *,
        mode,
        schema_mode,
        keys=(),
        partition=(),
        storage_options=None,
    ):
        self.writes.append((table_path, data, mode, schema_mode, keys, partition))

    def maintenance(self, table_path, *, storage_options=None):
        self.maintenance_calls.append((table_path, storage_options))
        return {"compacted": True}, ["vacuumed"]


def _end_time():
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=5)


def _run(capture_type, client, implementation, backend):
    captured = []

    @hg.sink_node
    def capture(response: capture_type, engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            captured.append(response.value)
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("memory", implementation)
        capture(client())

    with hg.GlobalContext(hg.GlobalState()):
        register_delta_backend(backend)
        hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())
    return captured


def test_delta_read_is_arrow_native_and_typed():
    backend = _Backend()
    stream_type = hg.TSB[hg.stream.Stream[hg.stream.Data[hg.Frame[_Row]]]]
    values = []

    @hg.sink_node
    def capture(response: stream_type, engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            values.append(response["values"].value)
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("memory", delta_read_adaptor_impl)
        capture(delta_read_adaptor[_Row](path="memory", table="rows", sort=(("value", True),)))

    with hg.GlobalContext(hg.GlobalState()):
        register_delta_backend(backend)
        hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    assert values[0].equals(pa.table({"name": ["a", "b"], "value": [1, 2]}))


def test_delta_query_uses_the_injected_backend():
    backend = _Backend()
    values = []
    stream_type = hg.TSB[hg.stream.Stream[hg.stream.Data[hg.Frame[_Row]]]]

    @hg.sink_node
    def capture(response: stream_type, engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            values.append(response["values"].value)
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("memory", delta_query_adaptor_impl)
        capture(
            delta_query_adaptor[_Row](
                path="memory", tables=frozenset({"rows"}), query="select * from rows"
            )
        )

    with hg.GlobalContext(hg.GlobalState()):
        register_delta_backend(backend)
        hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    assert values[0].equals(backend.tables["memory/rows"])


def test_delta_write_preserves_modes_keys_and_partitions():
    backend = _Backend()
    frame = pa.table({"name": ["a"], "value": [1]})

    @hg.push_queue(hg.TS[hg.Frame[_Row]])
    def data(sender):
        sender(frame)

    response_type = hg.TSB[hg.stream.Stream[hg.stream.Data[datetime]]]

    @hg.sink_node
    def stop(response: response_type, engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("memory", delta_write_adaptor_impl)
        stop(
            delta_write_adaptor(
                path="memory",
                table="rows",
                data=data(),
                write_mode=DeltaWriteMode.OVERWRITE,
                schema_mode=DeltaSchemaMode.MERGE,
                keys=("name",),
                partition=("name",),
            )
        )

    with hg.GlobalContext(hg.GlobalState()):
        register_delta_backend(backend)
        hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    table_path, written, mode, schema_mode, keys, partition = backend.writes[0]
    assert table_path == "memory/rows"
    assert written.equals(frame)
    assert mode is DeltaWriteMode.OVERWRITE
    assert schema_mode is DeltaSchemaMode.MERGE
    assert keys == ("name",)
    assert partition == ("name",)


def test_delta_table_maintenance_uses_the_injected_backend():
    backend = _Backend()

    @hg.graph
    def app() -> hg.TS[bool]:
        delta_table_maintenance(
            "memory",
            "rows",
            periodic=hg.MIN_TD,
            start=hg.MIN_ST,
        )
        return hg.const(True)

    with hg.GlobalContext(hg.GlobalState()):
        register_delta_backend(backend)
        hg.eval_node(app, __end_time__=hg.MIN_ST + 2 * hg.MIN_TD)

    assert backend.maintenance_calls == [("memory/rows", {})]


def test_tsd_delta_publisher_uses_the_native_table_codec():
    backend = _Backend()

    @hg.push_queue(hg.TSD[int, hg.TS[_Row]])
    def rows(sender):
        sender({1: _Row("a", 1)})

    response_type = hg.TSB[hg.stream.Stream[hg.stream.Data[datetime]]]

    @hg.sink_node
    def stop(response: response_type, engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("table_history_path", delta_write_adaptor_impl)
        stop(publish_tsd_to_delta_table("history", rows()))

    with hg.GlobalContext(hg.GlobalState()):
        register_delta_backend(backend)
        hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    table_path, frame, *_ = backend.writes[0]
    assert table_path == "table_history_path/history"
    assert frame.column_names[-3:] == ["__key_1__", "name", "value"]
    assert frame.select(["__key_1__", "name", "value"]).to_pylist() == [
        {"__key_1__": 1, "name": "a", "value": 1}
    ]


def _expected_tsd_batch(rows, as_of):
    return pa.Table.from_pylist([
        {
            "__date_time__": timestamp,
            "__as_of__": as_of,
            "__key_1_removed__": False,
            "__key_1__": key,
            "name": name,
            "value": value,
        }
        for timestamp, key, name, value in rows
    ])


def test_tsd_to_frame_batches_scalar_values_by_row_count():
    @hg.graph
    def app(tsd: hg.TSD[str, hg.TS[_Row]]) -> hg.TS[object]:
        return tsd_to_frame_batched(tsd, max_rows=2, flush_period=timedelta(milliseconds=1))

    as_of = hg.MIN_ST + 10 * hg.MIN_TD
    with hg.GlobalState():
        hg.set_as_of(as_of)
        out = hg.eval_node(
            app,
            [{"a": _Row("a", 1)}, {"b": _Row("b", 2)}],
            __end_time__=hg.MIN_ST + timedelta(milliseconds=10),
            __elide__=True,
        )

    expected = _expected_tsd_batch([
        (hg.MIN_ST, "a", "a", 1),
        (hg.MIN_ST + hg.MIN_TD, "b", "b", 2),
    ], as_of)
    assert out == [expected]


def test_tsd_to_frame_batches_bundle_values_by_row_count():
    @hg.graph
    def app(tsd: hg.TSD[str, hg.TSB[_Row]]) -> hg.TS[object]:
        return tsd_to_frame_batched(tsd, max_rows=2, flush_period=timedelta(milliseconds=1))

    as_of = hg.MIN_ST + 10 * hg.MIN_TD
    with hg.GlobalState():
        hg.set_as_of(as_of)
        out = hg.eval_node(
            app,
            [{"a": _Row("a", 1)}, {"b": _Row("b", 2)}],
            __end_time__=hg.MIN_ST + timedelta(milliseconds=10),
            __elide__=True,
        )

    expected = _expected_tsd_batch([
        (hg.MIN_ST, "a", "a", 1),
        (hg.MIN_ST + hg.MIN_TD, "b", "b", 2),
    ], as_of)
    assert out == [expected]


def test_tsd_to_frame_emits_multiple_row_count_batches():
    @hg.graph
    def app(tsd: hg.TSD[str, hg.TS[_Row]]) -> hg.TS[object]:
        return tsd_to_frame_batched(tsd, max_rows=2, flush_period=timedelta(milliseconds=1))

    as_of = hg.MIN_ST + 10 * hg.MIN_TD
    with hg.GlobalState():
        hg.set_as_of(as_of)
        out = hg.eval_node(
            app,
            [
                {"a": _Row("a", 1)},
                {"b": _Row("b", 2)},
                {"a": _Row("a", 3)},
                {"c": _Row("c", 4)},
            ],
            __end_time__=hg.MIN_ST + timedelta(milliseconds=10),
            __elide__=True,
        )

    assert out == [
        _expected_tsd_batch([
            (hg.MIN_ST, "a", "a", 1),
            (hg.MIN_ST + hg.MIN_TD, "b", "b", 2),
        ], as_of),
        _expected_tsd_batch([
            (hg.MIN_ST + 2 * hg.MIN_TD, "a", "a", 3),
            (hg.MIN_ST + 3 * hg.MIN_TD, "c", "c", 4),
        ], as_of),
    ]
