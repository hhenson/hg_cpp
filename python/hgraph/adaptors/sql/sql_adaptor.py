from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

import pyarrow as pa

from hgraph import (
    Frame,
    CompoundScalar,
    COMPOUND_SCALAR,
    TS,
    TSB,
    TSD,
    WiringPort,
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

from .sql_connection import connection_for, connection_target

__all__ = (
    "SQLWriteMode",
    "sql_read_adaptor_raw",
    "sql_read_adaptor_raw_impl",
    "sql_read_adaptor",
    "sql_read_adaptor_impl",
    "sql_write_adaptor",
    "sql_write_adaptor_impl",
    "sql_execute_adaptor",
    "sql_execute_adaptor_impl",
)


_RAW_FRAME_STREAM = TSB[Stream[Data[Frame]]]
_TIME_STREAM = TSB[Stream[Data[datetime]]]


class SQLWriteMode(Enum):
    APPEND = 0
    OVERWRITE = 1
    FAIL = 2


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _result_table(result) -> pa.Table:
    fetch_arrow_table = getattr(result, "fetch_arrow_table", None)
    if fetch_arrow_table is not None:
        table = fetch_arrow_table()
        if isinstance(table, pa.Table):
            return table
    fetch_record_batch = getattr(result, "fetch_record_batch", None)
    if fetch_record_batch is not None:
        batches = fetch_record_batch()
        if isinstance(batches, pa.RecordBatchReader):
            return batches.read_all()
    rows = result.fetchall()
    description = result.description or ()
    names = [column[0] for column in description]
    if not names:
        return pa.table({})
    return pa.Table.from_pylist([dict(zip(names, row)) for row in rows], schema=None)


def _stream(status, status_msg="", values=None):
    return {
        "status": status,
        "status_msg": status_msg,
        "values": values,
        "timestamp": _now(),
    }


def _read(path, query):
    with connection_for(path) as connection:
        result = connection.execute(query)
        return _result_table(result)


def _execute(path, query):
    with connection_for(path) as connection:
        connection.execute(query)
        commit = getattr(connection, "commit", None)
        if commit is not None:
            commit()
    return _now()


def _quote_identifier(name: str):
    return ".".join(f'"{part.replace(chr(34), chr(34) * 2)}"' for part in name.split("."))


def _sql_type(arrow_type):
    if pa.types.is_boolean(arrow_type):
        return "BOOLEAN"
    if pa.types.is_integer(arrow_type):
        return "BIGINT"
    if pa.types.is_floating(arrow_type):
        return "DOUBLE"
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "BLOB"
    if pa.types.is_date(arrow_type):
        return "DATE"
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMP"
    return "TEXT"


def _write(path, table_name, frame: pa.Table, mode: SQLWriteMode):
    quoted_table = _quote_identifier(table_name)
    columns = [field.name for field in frame.schema]
    definitions = ", ".join(
        f"{_quote_identifier(field.name)} {_sql_type(field.type)}"
        for field in frame.schema
    )
    with connection_for(path) as connection:
        cursor = connection.cursor() if hasattr(connection, "cursor") else connection
        if mode is SQLWriteMode.OVERWRITE:
            cursor.execute(f"DROP TABLE IF EXISTS {quoted_table}")
        if mode is SQLWriteMode.FAIL:
            cursor.execute(f"CREATE TABLE {quoted_table} ({definitions})")
        else:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {quoted_table} ({definitions})")
        if frame.num_rows:
            column_sql = ", ".join(_quote_identifier(name) for name in columns)
            placeholders = ", ".join("?" for _ in columns)
            rows = [tuple(row[name] for name in columns) for row in frame.to_pylist()]
            cursor.executemany(
                f"INSERT INTO {quoted_table} ({column_sql}) VALUES ({placeholders})",
                rows,
            )
        commit = getattr(connection, "commit", None)
        if commit is not None:
            commit()
    return _now()


@service_adaptor
def sql_read_adaptor_raw(query: TS[str], path: str) -> _RAW_FRAME_STREAM:
    ...


@service_adaptor_impl(interfaces=sql_read_adaptor_raw)
def sql_read_adaptor_raw_impl(
    queries: TSD[int, TS[str]], path: str
) -> TSD[int, _RAW_FRAME_STREAM]:
    state = KeyedAsyncState()
    target = connection_target(path)

    @push_queue(TSD[int, _RAW_FRAME_STREAM])
    def responses(sender):
        state.attach(sender)

    executor = adaptor_executor()

    def run_query(key, generation, query):
        try:
            table = _read(target, query)
            state.publish(key, generation, _stream(StreamStatus.OK, values=table))
        except Exception as error:
            state.publish(key, generation, _stream(StreamStatus.ERROR, str(error)))

    @sink_node
    def submit(queries: TSD[int, TS[str]], executor: TS[object]):
        for key in queries.removed_keys():
            state.cancel(key)
        for key, query in queries.modified_items():
            generation = state.begin(key)
            executor.value.submit(run_query, key, generation, query.value)

    @submit.stop
    def stop():
        state.close()

    submit(queries, executor)
    return responses()


class _SqlReadAdaptor:
    __name__ = "sql_read_adaptor"

    def __init__(self, schema=None):
        self.schema = schema

    def __getitem__(self, schema):
        return _SqlReadAdaptor(schema)

    def __call__(self, query, *, path):
        raw = sql_read_adaptor_raw(query, path=path)
        if self.schema is None:
            return raw
        output_type = TSB[Stream[Data[Frame[self.schema]]]]
        return output_type.from_ts(
            status=raw.status,
            status_msg=raw.status_msg,
            values=convert[TS[Frame[self.schema]]](raw.values),
            timestamp=raw.timestamp,
        )


sql_read_adaptor = _SqlReadAdaptor()
sql_read_adaptor_impl = sql_read_adaptor_raw_impl


@service_adaptor
def sql_execute_adaptor(query: TS[str], path: str) -> _TIME_STREAM:
    ...


@service_adaptor_impl(interfaces=sql_execute_adaptor)
def sql_execute_adaptor_impl(
    queries: TSD[int, TS[str]], path: str
) -> TSD[int, _TIME_STREAM]:
    state = KeyedAsyncState()
    target = connection_target(path)

    @push_queue(TSD[int, _TIME_STREAM])
    def responses(sender):
        state.attach(sender)

    executor = adaptor_executor()

    def run_query(key, generation, query):
        try:
            when = _execute(target, query)
            state.publish(key, generation, _stream(StreamStatus.OK, values=when))
        except Exception as error:
            state.publish(key, generation, _stream(StreamStatus.ERROR, str(error)))

    @sink_node
    def submit(queries: TSD[int, TS[str]], executor: TS[object]):
        for key in queries.removed_keys():
            state.cancel(key)
        for key, query in queries.modified_items():
            generation = state.begin(key)
            executor.value.submit(run_query, key, generation, query.value)

    @submit.stop
    def stop():
        state.close()

    submit(queries, executor)
    return responses()


@dataclass(frozen=True)
class _SqlWriteRequest(CompoundScalar):
    table: str
    data: Frame
    mode: SQLWriteMode


@compute_node
def _make_write_request(
    table: TS[str], data: TS[Frame[COMPOUND_SCALAR]], mode: TS[SQLWriteMode]
) -> TS[_SqlWriteRequest]:
    return _SqlWriteRequest(table.value, data.value, mode.value)


@service_adaptor
def _sql_write_adaptor(request: TS[_SqlWriteRequest], path: str) -> _TIME_STREAM:
    ...


class _SqlWriteAdaptor:
    __name__ = "sql_write_adaptor"

    def __call__(self, *, path, table, data, mode=SQLWriteMode.APPEND):
        def port(value, output_type):
            return value if isinstance(value, WiringPort) else const(value, tp=output_type)

        request = _make_write_request(
            port(table, TS[str]),
            data,
            port(mode, TS[SQLWriteMode]),
        )
        return _sql_write_adaptor(request, path=path)


sql_write_adaptor = _SqlWriteAdaptor()


@service_adaptor_impl(interfaces=_sql_write_adaptor)
def sql_write_adaptor_impl(
    requests: TSD[int, TS[_SqlWriteRequest]], path: str
) -> TSD[int, _TIME_STREAM]:
    state = KeyedAsyncState()
    target = connection_target(path)

    @push_queue(TSD[int, _TIME_STREAM])
    def responses(sender):
        state.attach(sender)

    executor = adaptor_executor()

    def write_frame(key, generation, request):
        try:
            when = _write(target, request.table, request.data, request.mode)
            state.publish(key, generation, _stream(StreamStatus.OK, values=when))
        except Exception as error:
            state.publish(key, generation, _stream(StreamStatus.ERROR, str(error)))

    @sink_node
    def submit(requests: TSD[int, TS[_SqlWriteRequest]], executor: TS[object]):
        for key in requests.removed_keys():
            state.cancel(key)
        for key, request in requests.modified_items():
            generation = state.begin(key)
            executor.value.submit(write_frame, key, generation, request.value)

    @submit.stop
    def stop():
        state.close()

    submit(requests, executor)
    return responses()
