from abc import ABC, abstractmethod
from concurrent.futures import Executor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
import logging

import pyarrow as pa

from hgraph import (
    COMPOUND_SCALAR,
    CompoundScalar,
    Frame,
    GlobalState,
    MIN_DT,
    TS,
    TSB,
    TSD,
    WiringPort,
    compute_node,
    const,
    graph,
    push_queue,
    service_adaptor,
    service_adaptor_impl,
    schedule,
    sink_node,
)
from hgraph.adaptors._async import KeyedAsyncState
from hgraph.adaptors.executor import adaptor_executor
from hgraph.stream import Data, Stream, StreamStatus

__all__ = (
    "DeltaBackend",
    "DeltaStore",
    "DeltaWriteMode",
    "DeltaSchemaMode",
    "register_delta_backend",
    "delta_read_adaptor_raw",
    "delta_read_adaptor_raw_impl",
    "delta_query_adaptor_raw",
    "delta_query_adaptor_raw_impl",
    "delta_write_adaptor_raw",
    "delta_write_adaptor_raw_impl",
    "delta_table_maintenance",
)


logger = logging.getLogger(__name__)


_RAW_FRAME_STREAM = TSB[Stream[Data[Frame]]]
_TIME_STREAM = TSB[Stream[Data[datetime]]]


class DeltaWriteMode(Enum):
    APPEND = 0
    OVERWRITE = 1
    ERROR = 2
    IGNORE = 3


class DeltaSchemaMode(Enum):
    OVERWRITE = 0
    MERGE = 1


_WRITE_MODES = {
    DeltaWriteMode.APPEND: "append",
    DeltaWriteMode.OVERWRITE: "overwrite",
    DeltaWriteMode.ERROR: "error",
    DeltaWriteMode.IGNORE: "ignore",
}
_SCHEMA_MODES = {
    DeltaSchemaMode.OVERWRITE: "overwrite",
    DeltaSchemaMode.MERGE: "merge",
}


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _table_path(base_path: str, table: str):
    if "://" in table or table.startswith("/"):
        return table
    return f"{base_path.rstrip('/')}/{table.lstrip('/')}"


def _as_arrow(value):
    if isinstance(value, pa.Table):
        return value
    if isinstance(value, pa.RecordBatchReader):
        return value.read_all()
    if isinstance(value, pa.RecordBatch):
        return pa.Table.from_batches([value])
    to_arrow = getattr(value, "to_arrow", None)
    if to_arrow is not None:
        return _as_arrow(to_arrow())
    if isinstance(value, (list, tuple)) and value and isinstance(value[0], pa.RecordBatch):
        return pa.Table.from_batches(value)
    raise TypeError(f"Delta backend returned unsupported table type {type(value)!r}")


class DeltaBackend(ABC):
    @abstractmethod
    def read(self, table_path, *, columns=(), filters=(), storage_options=None):
        raise NotImplementedError

    @abstractmethod
    def query(self, base_path, *, tables, query, storage_options=None):
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    def maintenance(self, table_path, *, storage_options=None):
        """Compact and vacuum a table, when supported by the backend."""
        raise NotImplementedError(f"{type(self).__name__} does not support Delta table maintenance")


class _DeltalakeBackend(DeltaBackend):
    @staticmethod
    def _module():
        try:
            import deltalake
        except ModuleNotFoundError as error:
            raise RuntimeError("Delta adaptors require the 'delta' extra") from error
        return deltalake

    def read(self, table_path, *, columns=(), filters=(), storage_options=None):
        module = self._module()
        table = module.DeltaTable(table_path, storage_options=storage_options or None)
        return table.to_pyarrow_table(
            columns=list(columns) or None,
            filters=list(filters) or None,
        )

    def query(self, base_path, *, tables, query, storage_options=None):
        module = self._module()
        builder = module.QueryBuilder()
        for table in tables:
            builder.register(
                table,
                module.DeltaTable(
                    _table_path(base_path, table),
                    storage_options=storage_options or None,
                ),
            )
        result = builder.execute(query)
        fetchall = getattr(result, "fetchall", None)
        batches = fetchall() if fetchall is not None else result
        if not batches:
            return pa.table({})
        return _as_arrow(batches)

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
        module = self._module()
        predicate = _overwrite_predicate(data, keys) if mode is DeltaWriteMode.OVERWRITE else None
        module.write_deltalake(
            table_path,
            data,
            mode=_WRITE_MODES[mode],
            schema_mode=_SCHEMA_MODES[schema_mode],
            storage_options=storage_options or None,
            partition_by=list(partition) or None,
            predicate=predicate,
            configuration={
                "delta.deletedFileRetentionDuration": "interval 1 days",
                "delta.logRetentionDuration": "interval 2 days",
            },
        )

    def maintenance(self, table_path, *, storage_options=None):
        table = self._module().DeltaTable(table_path, storage_options=storage_options or None)
        compact_result = table.optimize.compact()
        vacuum_result = table.vacuum(
            retention_hours=1,
            enforce_retention_duration=False,
            dry_run=False,
        )
        return compact_result, vacuum_result


@graph
def delta_table_maintenance(
    path: str,
    table: str,
    periodic: timedelta,
    start: datetime = MIN_DT,
):
    trigger = schedule(periodic, start=start)

    @sink_node
    def maintenance(
        path: str,
        table: str,
        trigger: TS[bool],
        executor: TS[Executor],
    ):
        if not trigger.value:
            return

        store = DeltaStore.instance()
        backend = store.backend
        table_path = _table_path(path, table)
        storage_options = store.options_for(path)

        def run_maintenance():
            try:
                logger.info("Doing maintenance for delta table %s", table_path)
                compact_result, vacuum_result = backend.maintenance(
                    table_path,
                    storage_options=storage_options,
                )
                logger.info("Compaction for delta table %s: %s", table_path, compact_result)
                logger.info("Vacuum for delta table %s: %s", table_path, vacuum_result)
            except Exception:
                logger.exception("Error doing maintenance for delta table %s", table_path)

        executor.value.submit(run_maintenance)

    maintenance(path, table, trigger, adaptor_executor())


def _sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _overwrite_predicate(data: pa.Table, keys):
    if not keys:
        return None
    rows = data.select(list(keys)).to_pylist()
    predicates = []
    for row in rows:
        predicates.append(
            "(" + " AND ".join(f'"{key}" = {_sql_literal(row[key])}' for key in keys) + ")"
        )
    return " OR ".join(dict.fromkeys(predicates)) or None


class DeltaStore:
    _STATE_KEY = ":adaptors:delta:store"

    def __init__(self, backend=None, storage_options=None):
        self.backend = backend or _DeltalakeBackend()
        self.storage_options = storage_options or {}

    @classmethod
    def instance(cls):
        state = GlobalState.instance()
        store = state.get(cls._STATE_KEY)
        if store is None:
            store = cls()
            state[cls._STATE_KEY] = store
        return store

    def options_for(self, path):
        options = self.storage_options
        return dict(options(path) if callable(options) else options)


def register_delta_backend(backend, *, storage_options=None):
    state = GlobalState.instance()
    state[DeltaStore._STATE_KEY] = DeltaStore(backend, storage_options)


@dataclass(frozen=True)
class _DeltaReadRequest(CompoundScalar):
    table: str
    columns: tuple[str, ...]
    filters: tuple[tuple[str, str, object], ...]
    sort: tuple[tuple[str, bool], ...]


@compute_node
def _make_read_request(
    table: TS[str],
    columns: TS[tuple[str, ...]],
    filters: TS[tuple[tuple[str, str, object], ...]],
    sort: TS[tuple[tuple[str, bool], ...]],
) -> TS[_DeltaReadRequest]:
    return _DeltaReadRequest(table.value, columns.value, filters.value, sort.value)


@service_adaptor
def _delta_read_adaptor_raw(request: TS[_DeltaReadRequest], path: str) -> _RAW_FRAME_STREAM:
    ...


class _DeltaReadAdaptorRaw:
    __name__ = "delta_read_adaptor_raw"

    def __call__(self, *, path, table, columns=(), filters=(), sort=()):
        request = _make_read_request(
            _port(table, TS[str]),
            _port(columns, TS[tuple[str, ...]]),
            _port(filters, TS[tuple[tuple[str, str, object], ...]]),
            _port(sort, TS[tuple[tuple[str, bool], ...]]),
        )
        return _delta_read_adaptor_raw(request, path=path)


delta_read_adaptor_raw = _DeltaReadAdaptorRaw()


@service_adaptor_impl(interfaces=_delta_read_adaptor_raw)
def delta_read_adaptor_raw_impl(
    requests: TSD[int, TS[_DeltaReadRequest]], path: str
) -> TSD[int, _RAW_FRAME_STREAM]:
    store = DeltaStore.instance()
    state = KeyedAsyncState()

    @push_queue(TSD[int, _RAW_FRAME_STREAM])
    def responses(sender):
        state.attach(sender)

    def read(key, generation, request):
        try:
            table = _as_arrow(
                store.backend.read(
                    _table_path(path, request.table),
                    columns=request.columns,
                    filters=request.filters,
                    storage_options=store.options_for(path),
                )
            )
            if request.sort:
                table = table.sort_by(
                    [(name, "ascending" if ascending else "descending") for name, ascending in request.sort]
                )
            state.publish(key, generation, _stream(StreamStatus.OK, values=table))
        except Exception as error:
            state.publish(key, generation, _stream(StreamStatus.ERROR, str(error)))

    _submit_async_requests(requests, state, read, _DeltaReadRequest)
    return responses()


@dataclass(frozen=True)
class _DeltaQueryRequest(CompoundScalar):
    tables: frozenset[str]
    query: str


@compute_node
def _make_query_request(
    tables: TS[frozenset[str]], query: TS[str]
) -> TS[_DeltaQueryRequest]:
    return _DeltaQueryRequest(tables.value, query.value)


@service_adaptor
def _delta_query_adaptor_raw(request: TS[_DeltaQueryRequest], path: str) -> _RAW_FRAME_STREAM:
    ...


class _DeltaQueryAdaptorRaw:
    __name__ = "delta_query_adaptor_raw"

    def __call__(self, *, path, tables, query):
        request = _make_query_request(
            _port(frozenset(tables) if not isinstance(tables, WiringPort) else tables, TS[frozenset[str]]),
            _port(query, TS[str]),
        )
        return _delta_query_adaptor_raw(request, path=path)


delta_query_adaptor_raw = _DeltaQueryAdaptorRaw()


@service_adaptor_impl(interfaces=_delta_query_adaptor_raw)
def delta_query_adaptor_raw_impl(
    requests: TSD[int, TS[_DeltaQueryRequest]], path: str
) -> TSD[int, _RAW_FRAME_STREAM]:
    store = DeltaStore.instance()
    state = KeyedAsyncState()

    @push_queue(TSD[int, _RAW_FRAME_STREAM])
    def responses(sender):
        state.attach(sender)

    def query(key, generation, request):
        try:
            table = _as_arrow(
                store.backend.query(
                    path,
                    tables=request.tables,
                    query=request.query,
                    storage_options=store.options_for(path),
                )
            )
            state.publish(key, generation, _stream(StreamStatus.OK, values=table))
        except Exception as error:
            state.publish(key, generation, _stream(StreamStatus.ERROR, str(error)))

    _submit_async_requests(requests, state, query, _DeltaQueryRequest)
    return responses()


@dataclass(frozen=True)
class _DeltaWriteRequest(CompoundScalar):
    table: str
    data: Frame
    write_mode: DeltaWriteMode
    schema_mode: DeltaSchemaMode
    keys: tuple[str, ...]
    partition: tuple[str, ...]


@compute_node
def _make_write_request(
    table: TS[str],
    data: TS[object],
    write_mode: TS[DeltaWriteMode],
    schema_mode: TS[DeltaSchemaMode],
    keys: TS[tuple[str, ...]],
    partition: TS[tuple[str, ...]],
) -> TS[_DeltaWriteRequest]:
    return _DeltaWriteRequest(
        table.value,
        data.value,
        write_mode.value,
        schema_mode.value,
        keys.value,
        partition.value,
    )


@service_adaptor
def _delta_write_adaptor_raw(request: TS[_DeltaWriteRequest], path: str) -> _TIME_STREAM:
    ...


class _DeltaWriteAdaptorRaw:
    __name__ = "delta_write_adaptor_raw"

    def __call__(
        self,
        *,
        path,
        table,
        data,
        write_mode=DeltaWriteMode.APPEND,
        schema_mode=DeltaSchemaMode.MERGE,
        keys=(),
        partition=(),
    ):
        request = _make_write_request(
            _port(table, TS[str]),
            data,
            _port(write_mode, TS[DeltaWriteMode]),
            _port(schema_mode, TS[DeltaSchemaMode]),
            _port(keys, TS[tuple[str, ...]]),
            _port(partition, TS[tuple[str, ...]]),
        )
        return _delta_write_adaptor_raw(request, path=path)


delta_write_adaptor_raw = _DeltaWriteAdaptorRaw()


@service_adaptor_impl(interfaces=_delta_write_adaptor_raw)
def delta_write_adaptor_raw_impl(
    requests: TSD[int, TS[_DeltaWriteRequest]], path: str
) -> TSD[int, _TIME_STREAM]:
    store = DeltaStore.instance()
    state = KeyedAsyncState()

    @push_queue(TSD[int, _TIME_STREAM])
    def responses(sender):
        state.attach(sender)

    def write(key, generation, request):
        try:
            store.backend.write(
                _table_path(path, request.table),
                request.data,
                mode=request.write_mode,
                schema_mode=request.schema_mode,
                keys=request.keys,
                partition=request.partition,
                storage_options=store.options_for(path),
            )
            state.publish(key, generation, _stream(StreamStatus.OK, values=_now()))
        except Exception as error:
            state.publish(key, generation, _stream(StreamStatus.ERROR, str(error)))

    _submit_async_requests(requests, state, write, _DeltaWriteRequest)
    return responses()


def _submit_async_requests(requests, state, operation, request_type):
    executor = adaptor_executor()

    @sink_node
    def submit(requests: TSD[int, TS[request_type]], executor: TS[object]):
        for key in requests.removed_keys():
            state.cancel(key)
        for key, request in requests.modified_items():
            generation = state.begin(key)
            executor.value.submit(operation, key, generation, request.value)

    @submit.stop
    def stop():
        state.close()

    submit(requests, executor)


def _port(value, output_type):
    return value if isinstance(value, WiringPort) else const(value, tp=output_type)


def _stream(status, status_msg="", values=None):
    return {
        "status": status,
        "status_msg": status_msg,
        "values": values,
        "timestamp": _now(),
    }
