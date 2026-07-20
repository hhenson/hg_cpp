from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pyarrow as pa
from frozendict import frozendict

import hgraph as hg
from hgraph.adaptors.data_catalogue import (
    DataCatalogue,
    DataCatalogueEntry,
    DataEnvironment,
    DataEnvironmentEntry,
    DataSink,
    publish,
    publish_adaptor_impl,
    subscribe,
    subscribe_adaptor_impl,
)
from hgraph.adaptors.json import JsonDataSource
from hgraph.stream import StreamStatus


@dataclass(frozen=True)
class _Row(hg.CompoundScalar):
    name: str
    value: int


@dataclass(frozen=True)
class _Sink(DataSink):
    table: str


def _end_time():
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=5)


def test_catalogue_subscribe_routes_json_source(tmp_path):
    (tmp_path / "rows.json").write_text('[{"name":"a","value":1}]')
    responses = []
    response_type = hg.TSB[hg.stream.Stream[hg.stream.Data[hg.Frame[_Row]]]]

    @hg.sink_node
    def capture(response: response_type, engine: hg.EvaluationEngineApi = None):
        if response.status.modified:
            responses.append((response.status.value, response.status_msg.value,
                              response["values"].value))
        if response.status.value in (StreamStatus.OK, StreamStatus.ERROR):
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("data-catalogue", subscribe_adaptor_impl)
        capture(subscribe[_Row]("rows"))

    catalogue = DataCatalogue()
    environment = DataEnvironment()
    environment.add_entry(DataEnvironmentEntry("json", str(tmp_path)))
    with hg.GlobalContext(hg.GlobalState()):
        with catalogue, environment:
            DataCatalogueEntry[JsonDataSource](
                _Row,
                "rows",
                frozendict(),
                JsonDataSource(source_path="json", file="rows.json"),
            )
            hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    assert responses[0][0] is StreamStatus.OK, responses[0][1]
    assert responses[0][2].equals(pa.table({"name": ["a"], "value": [1]}))


def test_catalogue_publish_routes_all_matching_sinks():
    writes = []

    @DataCatalogue.sink_handler(_Sink)
    def capture_write(entry, options, frame, environment_path):
        writes.append((entry.dataset, options, frame, environment_path))

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
        hg.register_adaptor("data-catalogue-publish", publish_adaptor_impl)
        stop(publish[_Row]("rows", data()))

    catalogue = DataCatalogue()
    with hg.GlobalContext(hg.GlobalState()):
        with catalogue:
            DataCatalogueEntry[_Sink](
                _Row,
                "rows",
                frozendict(),
                _Sink(
                    sink_path="memory",
                    table="rows",
                ),
            )
            hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    assert writes[0][0] == "rows"
    assert writes[0][2].equals(frame)
    assert writes[0][3] == "memory"
