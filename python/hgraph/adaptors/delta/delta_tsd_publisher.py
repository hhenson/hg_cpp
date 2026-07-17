import pyarrow as pa

from hgraph import TS, TABLE, compute_node, table_schema, to_table
from hgraph._types import _TsExpr
from hgraph._wiring import _unwrap
from hgraph.adaptors.data_catalogue import DataEnvironment

from .delta_adaptor_raw import (
    DeltaSchemaMode,
    DeltaWriteMode,
    delta_write_adaptor_raw,
)

__all__ = ("publish_tsd_to_delta_table",)


@compute_node
def _rows_to_frame(rows: TS[TABLE], columns: tuple[str, ...]) -> TS[object]:
    value = rows.value
    records = value if value and isinstance(value[0], tuple) else (value,)
    return pa.Table.from_pylist([dict(zip(columns, record)) for record in records])


def publish_tsd_to_delta_table(
    table_name,
    tsd,
    max_rows=100_000,
    flush_period=None,
):
    """Publish each native TSD table delta to Delta Lake.

    ``max_rows`` and ``flush_period`` are retained for source compatibility;
    the C++ table codec already emits one compact delta per engine cycle, so
    this implementation does not add a second Python-side batching buffer.
    """
    del max_rows, flush_period
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
        data=_rows_to_frame(to_table(tsd), columns=schema.keys),
        write_mode=DeltaWriteMode.APPEND,
        schema_mode=DeltaSchemaMode.MERGE,
        partition=(schema.date_time_key,),
    )
