from dataclasses import dataclass

from hgraph.adaptors.data_catalogue import DataCatalogue, DataSource

from .delta_adaptor_raw import DeltaStore, _as_arrow

__all__ = ("DeltaQueryDataSource",)


@dataclass(frozen=True)
class DeltaQueryDataSource(DataSource):
    tables: frozenset[str]
    query: str

    def render(self, **options):
        return self.query.format(**options)


@DataCatalogue.source_handler(DeltaQueryDataSource)
def _query_delta_source(entry, options, environment_path):
    source = entry.store
    store = DeltaStore.instance()
    return _as_arrow(
        store.backend.query(
            environment_path,
            tables=source.tables,
            query=source.render(**options),
            storage_options=store.options_for(environment_path),
        )
    )
