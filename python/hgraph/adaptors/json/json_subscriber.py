from dataclasses import dataclass
from pathlib import Path

from hgraph.adaptors.data_catalogue import DataCatalogue, DataSource

from .json_adaptor import _read_json_table

__all__ = ("JsonDataSource",)


@dataclass(frozen=True)
class JsonDataSource(DataSource):
    file: str


@DataCatalogue.source_handler(JsonDataSource)
def _read_json_source(entry, options, environment_path):
    return _read_json_table(Path(environment_path) / entry.store.file)
