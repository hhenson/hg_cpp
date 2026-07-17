from dataclasses import dataclass
from datetime import datetime, timezone

import pyarrow as pa
from frozendict import frozendict

from hgraph import (
    CompoundScalar,
    Frame,
    TS,
    TSB,
    TSD,
    WiringPort,
    combine,
    compute_node,
    const,
    convert,
    push_queue,
    service_adaptor,
    service_adaptor_impl,
    sink_node,
)
from hgraph.adaptors._async import KeyedAsyncState
from hgraph.adaptors.executor import adaptor_executor
from hgraph.stream import Data, Stream, StreamStatus

from .catalogue import DataCatalogue, DataEnvironment, DataSource

__all__ = (
    "FindDCEResult",
    "find_data_catalogue_entry",
    "subscribe",
    "subscribe_adaptor",
    "subscribe_adaptor_impl",
    "subscriber_impl_from_graph",
    "subscriber_impl_to_graph",
)


_RAW_STREAM = TSB[Stream[Data[Frame]]]


@dataclass(frozen=True)
class FindDCEResult(CompoundScalar):
    dce: object
    options: object


@compute_node
def find_data_catalogue_entry(
    dataset: TS[str], options: TS[object], schema: object
) -> TS[FindDCEResult]:
    match = DataCatalogue.instance().matching_entries(
        schema, dataset.value, DataSource, options.value or {}
    )[0]
    return FindDCEResult(*match)


@dataclass(frozen=True)
class _SubscribeRequest(CompoundScalar):
    entry: object
    options: object
    environment_path: str


@compute_node
def _make_request(selection: TS[FindDCEResult]) -> TS[_SubscribeRequest]:
    entry = selection.value.dce
    environment = DataEnvironment.current()
    source_path = entry.store.source_path
    environment_path = (
        environment.get_entry(source_path).environment_path
        if environment is not None and environment.has_entry(source_path)
        else source_path
    )
    return _SubscribeRequest(entry, selection.value.options, environment_path)


@service_adaptor
def subscribe_adaptor(request: TS[_SubscribeRequest], path: str = "data-catalogue") -> _RAW_STREAM:
    ...


@service_adaptor_impl(interfaces=subscribe_adaptor)
def subscribe_adaptor_impl(
    requests: TSD[int, TS[_SubscribeRequest]], path: str
) -> TSD[int, _RAW_STREAM]:
    state = KeyedAsyncState()

    @push_queue(TSD[int, _RAW_STREAM])
    def responses(sender):
        state.attach(sender)

    def read(key, generation, request):
        try:
            handler = DataCatalogue.handler_for(request.entry.store)
            value = handler(request.entry, request.options, request.environment_path)
            if not isinstance(value, dict):
                value = {
                    "status": StreamStatus.OK,
                    "status_msg": "",
                    "values": _as_arrow(value),
                    "timestamp": _now(),
                }
            state.publish(key, generation, value)
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
    def submit(requests: TSD[int, TS[_SubscribeRequest]], executor: TS[object]):
        for key in requests.removed_keys():
            state.cancel(key)
        for key, request in requests.modified_items():
            generation = state.begin(key)
            executor.value.submit(read, key, generation, request.value)

    @submit.stop
    def stop():
        state.close()

    submit(requests, executor)
    return responses()


class _Subscribe:
    __name__ = "subscribe"

    def __init__(self, schema=None):
        self.schema = schema

    def __getitem__(self, schema):
        return _Subscribe(schema)

    def __call__(self, dataset, __options__=None, **options):
        if self.schema is None:
            raise TypeError("subscribe requires a schema, for example subscribe[Row](...)")
        options_port = _options_port(__options__, options)
        dataset_port = dataset if isinstance(dataset, WiringPort) else const(dataset, tp=TS[str])
        raw = subscribe_adaptor(
            _make_request(find_data_catalogue_entry(dataset_port, options_port, self.schema)),
            path="data-catalogue",
        )
        output_type = TSB[Stream[Data[Frame[self.schema]]]]
        return output_type.from_ts(
            status=raw.status,
            status_msg=raw.status_msg,
            values=convert[TS[Frame[self.schema]]](raw.values),
            timestamp=raw.timestamp,
        )


subscribe = _Subscribe()


def subscriber_impl_from_graph(source_type=None):
    """Register a resource callback for a DataSource subclass.

    The callback receives ``(entry, resolved_options, environment_path)`` and
    returns an Arrow-compatible table. Graph-shaped catalogue implementations
    are intentionally not reproduced; the keyed adaptor already owns the
    graph boundary and lifecycle.
    """
    if isinstance(source_type, type):
        return DataCatalogue.source_handler(source_type)
    raise TypeError("subscriber_impl_from_graph requires a DataSource subclass")


subscriber_impl_to_graph = subscriber_impl_from_graph


def _options_port(explicit, options):
    if explicit is not None and options:
        raise TypeError("provide either __options__ or keyword options, not both")
    if explicit is not None:
        return explicit if isinstance(explicit, WiringPort) else const(explicit, tp=TS[object])
    if not options:
        return const(frozendict(), tp=TS[object])
    if any(isinstance(value, WiringPort) for value in options.values()):
        return convert[TS[dict[str, object]]](combine(**options))
    return const(frozendict(options), tp=TS[object])


def _as_arrow(value):
    if isinstance(value, pa.Table):
        return value
    if isinstance(value, pa.RecordBatch):
        return pa.Table.from_batches([value])
    to_arrow = getattr(value, "to_arrow", None)
    if to_arrow is not None:
        return _as_arrow(to_arrow())
    raise TypeError(f"catalogue source returned unsupported value {type(value)!r}")


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)
