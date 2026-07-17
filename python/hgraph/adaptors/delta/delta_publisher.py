from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping

import pyarrow as pa

from hgraph.adaptors.data_catalogue import DataCatalogue, DataSink

from .delta_adaptor_raw import DeltaSchemaMode, DeltaStore, DeltaWriteMode, _table_path

__all__ = ("DeltaDataSink",)


@dataclass(frozen=True)
class DeltaDataSink(DataSink):
    table: str
    write_mode: DeltaWriteMode = DeltaWriteMode.APPEND
    schema_mode: DeltaSchemaMode = DeltaSchemaMode.MERGE
    column_mappings: Mapping[str, str] = None
    keys: tuple[str, ...] = ()
    partition: tuple[str, ...] = ()
    partition_key_from: dict[str, str] = None

    def map_columns(self, frame):
        if not self.column_mappings:
            return frame
        return frame.rename_columns(
            [self.column_mappings.get(name, name) for name in frame.column_names]
        )


@DataCatalogue.sink_handler(DeltaDataSink)
def _write_delta_sink(entry, options, frame, environment_path):
    sink = entry.store
    frame = sink.map_columns(frame)
    for name, value in options.items():
        if name not in frame.column_names:
            frame = frame.append_column(name, pa.array([value] * frame.num_rows))
    store = DeltaStore.instance()
    store.backend.write(
        _table_path(environment_path, sink.table),
        frame,
        mode=sink.write_mode,
        schema_mode=sink.schema_mode,
        keys=sink.keys,
        partition=sink.partition,
        storage_options=store.options_for(environment_path),
    )
    return datetime.now(timezone.utc).replace(tzinfo=None)
