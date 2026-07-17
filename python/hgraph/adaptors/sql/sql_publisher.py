from dataclasses import dataclass

from hgraph.adaptors.data_catalogue import DataCatalogue, DataSink

from .sql_adaptor import SQLWriteMode, _write

__all__ = ("SqlDataSink",)


@dataclass(frozen=True)
class SqlDataSink(DataSink):
    table: str
    column_mappings: dict[str, str] = None
    mode: SQLWriteMode = SQLWriteMode.APPEND

    def map_columns(self, frame):
        if not self.column_mappings:
            return frame
        return frame.rename_columns(
            [self.column_mappings.get(name, name) for name in frame.column_names]
        )


@DataCatalogue.sink_handler(SqlDataSink)
def _write_sql_sink(entry, options, frame, environment_path):
    sink = entry.store
    for name, value in options.items():
        if name not in frame.column_names:
            import pyarrow as pa

            frame = frame.append_column(name, pa.array([value] * frame.num_rows))
    return _write(environment_path, sink.table, sink.map_columns(frame), sink.mode)
