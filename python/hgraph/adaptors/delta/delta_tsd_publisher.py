import pyarrow as pa
from datetime import timedelta

from hgraph import (AUTO_RESOLVE, SCALAR, STATE, TABLE, TIME_SERIES_TYPE, TS, TSD,
                    compute_node, graph, schedule, table_schema, to_table)
from hgraph._types import _TsExpr
from hgraph._wiring import _unwrap
from hgraph.adaptors.data_catalogue import DataEnvironment

from .delta_adaptor_raw import (
    DeltaSchemaMode,
    DeltaWriteMode,
    delta_write_adaptor_raw,
)

__all__ = ("publish_tsd_to_delta_table", "tsd_to_frame_batched")


DEFAULT_DELTA_PUBLISH_BATCH_SIZE = 100_000
DEFAULT_DELTA_PUBLISH_FLUSH_PERIOD = timedelta(minutes=5)


@compute_node(valid=("max_rows",))
def _rows_to_frame(
    rows: TS[TABLE],
    flush: TS[bool],
    max_rows: TS[int],
    columns: tuple[str, ...],
    _state: STATE = None,
) -> TS[object]:
    if rows.modified:
        value = rows.value
        records = value if value and isinstance(value[0], tuple) else (value,)
        _state.records.extend(records)

    should_flush = len(_state.records) >= max_rows.value
    should_flush |= flush.modified and bool(_state.records)
    if should_flush:
        records, _state.records = _state.records, []
        return pa.Table.from_pylist([dict(zip(columns, record)) for record in records])


@_rows_to_frame.start
def _rows_to_frame_start(_state: STATE = None):
    _state.records = []


@graph
def tsd_to_frame_batched(
    tsd: TSD[SCALAR, TIME_SERIES_TYPE],
    max_rows: TS[int] = DEFAULT_DELTA_PUBLISH_BATCH_SIZE,
    flush_period: TS[timedelta] = DEFAULT_DELTA_PUBLISH_FLUSH_PERIOD,
    key_type: type[SCALAR] = AUTO_RESOLVE,
) -> TS[object]:
    del key_type
    ts_type = _TsExpr(_unwrap(tsd).ts_type, repr(_unwrap(tsd).ts_type))
    schema = table_schema(ts_type).value
    flush = schedule(flush_period, initial_delay=True)
    return _rows_to_frame(to_table(tsd), flush, max_rows, columns=schema.keys)


def publish_tsd_to_delta_table(
    table_name,
    tsd,
    max_rows=DEFAULT_DELTA_PUBLISH_BATCH_SIZE,
    flush_period=DEFAULT_DELTA_PUBLISH_FLUSH_PERIOD,
):
    """Publish native TSD table deltas in size- or time-bounded batches."""
    ts_type = _TsExpr(_unwrap(tsd).ts_type, repr(_unwrap(tsd).ts_type))
    schema = table_schema(ts_type).value
    environment = DataEnvironment.current()
    path = (
        environment.get_entry("table_history_path").environment_path
        if environment is not None and environment.has_entry("table_history_path")
        else "table_history_path"
    )
    return delta_write_adaptor_raw(
        path=path,
        table=table_name.replace(" ", "_"),
        data=_rows_to_frame(
            to_table(tsd),
            schedule(flush_period, initial_delay=True),
            max_rows,
            columns=schema.keys,
        ),
        write_mode=DeltaWriteMode.APPEND,
        schema_mode=DeltaSchemaMode.MERGE,
        partition=(schema.date_time_key,),
    )
