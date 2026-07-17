from dataclasses import dataclass
from datetime import datetime, timezone

from frozendict import frozendict

from hgraph import (
    COMPOUND_SCALAR,
    CompoundScalar,
    Frame,
    TS,
    TSB,
    TSD,
    WiringPort,
    compute_node,
    const,
    push_queue,
    service_adaptor,
    service_adaptor_impl,
    sink_node,
)
from hgraph.adaptors._async import KeyedAsyncState
from hgraph.adaptors.executor import adaptor_executor
from hgraph.stream import Data, Stream, StreamStatus

from .catalogue import DataCatalogue, DataEnvironment, DataSink
from .subscribe import _options_port

__all__ = (
    "DataCatalogSinkResult",
    "find_data_catalogue_entries",
    "publish",
    "publish_adaptor",
    "publish_adaptor_impl",
    "publish_impl_from_graph",
    "publish_impl_to_graph",
)


_TIME_STREAM = TSB[Stream[Data[datetime]]]


@dataclass(frozen=True)
class DataCatalogSinkResult(CompoundScalar):
    dce: object
    options: object


@compute_node
def find_data_catalogue_entries(
    dataset: TS[str], options: TS[object], schema: object
) -> TS[object]:
    return tuple(
        DataCatalogSinkResult(entry, resolved)
        for entry, resolved in DataCatalogue.instance().matching_entries(
            schema, dataset.value, DataSink, options.value or {}
        )
    )


@dataclass(frozen=True)
class _PublishRequest(CompoundScalar):
    entries: object
    data: Frame
    environment_paths: object


@compute_node
def _make_request(
    selections: TS[object], data: TS[Frame[COMPOUND_SCALAR]]
) -> TS[_PublishRequest]:
    environment = DataEnvironment.current()
    paths = {}
    for selection in selections.value:
        sink_path = selection.dce.store.sink_path
        paths[sink_path] = (
            environment.get_entry(sink_path).environment_path
            if environment is not None and environment.has_entry(sink_path)
            else sink_path
        )
    return _PublishRequest(selections.value, data.value, frozendict(paths))


@service_adaptor
def publish_adaptor(request: TS[_PublishRequest], path: str = "data-catalogue-publish") -> _TIME_STREAM:
    ...


@service_adaptor_impl(interfaces=publish_adaptor)
def publish_adaptor_impl(
    requests: TSD[int, TS[_PublishRequest]], path: str
) -> TSD[int, _TIME_STREAM]:
    state = KeyedAsyncState()

    @push_queue(TSD[int, _TIME_STREAM])
    def responses(sender):
        state.attach(sender)

    def write(key, generation, request):
        try:
            completed = None
            for selection in request.entries:
                entry = selection.dce
                handler = DataCatalogue.handler_for(entry.store, sink=True)
                result = handler(
                    entry,
                    selection.options,
                    request.data,
                    request.environment_paths[entry.store.sink_path],
                )
                completed = result or completed
            state.publish(
                key,
                generation,
                {
                    "status": StreamStatus.OK,
                    "status_msg": "",
                    "values": completed or _now(),
                    "timestamp": _now(),
                },
            )
        except Exception as error:
            state.publish(
                key,
                generation,
                {
                    "status": StreamStatus.ERROR,
                    "status_msg": str(error),
                    "values": None,
                    "timestamp": _now(),
                },
            )

    executor = adaptor_executor()

    @sink_node
    def submit(requests: TSD[int, TS[_PublishRequest]], executor: TS[object]):
        for key in requests.removed_keys():
            state.cancel(key)
        for key, request in requests.modified_items():
            generation = state.begin(key)
            executor.value.submit(write, key, generation, request.value)

    @submit.stop
    def stop():
        state.close()

    submit(requests, executor)
    return responses()


class _Publish:
    __name__ = "publish"

    def __init__(self, schema=None):
        self.schema = schema

    def __getitem__(self, schema):
        return _Publish(schema)

    def __call__(self, dataset, data, __options__=None, **options):
        if self.schema is None:
            raise TypeError("publish requires a schema, for example publish[Row](...)")
        dataset_port = dataset if isinstance(dataset, WiringPort) else const(dataset, tp=TS[str])
        selections = find_data_catalogue_entries(
            dataset_port, _options_port(__options__, options), self.schema
        )
        return publish_adaptor(
            _make_request(selections, data), path="data-catalogue-publish"
        )


publish = _Publish()


def publish_impl_from_graph(sink_type=None):
    if isinstance(sink_type, type):
        return DataCatalogue.sink_handler(sink_type)
    raise TypeError("publish_impl_from_graph requires a DataSink subclass")


publish_impl_to_graph = publish_impl_from_graph


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)
