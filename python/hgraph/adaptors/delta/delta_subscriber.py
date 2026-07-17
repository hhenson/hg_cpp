from dataclasses import dataclass

from hgraph.adaptors.data_catalogue import DataCatalogue, DataSource

from .delta_adaptor_raw import DeltaStore, _as_arrow, _table_path

__all__ = ("DeltaDataSource",)


@dataclass(frozen=True)
class DeltaDataSource(DataSource):
    table: str
    query: tuple[tuple[str, str, str], ...] = ()
    sort: tuple[tuple[str, bool], ...] = ()

    def render(self, **options):
        return tuple(
            (column, operator, options[value])
            if value in options and options[value] is not None
            else (column, operator, value)
            for column, operator, value in self.query
            if value not in options or options[value] is not None
        )


@DataCatalogue.source_handler(DeltaDataSource)
def _read_delta_source(entry, options, environment_path):
    source = entry.store
    store = DeltaStore.instance()
    columns = tuple(getattr(entry.schema, "__annotations__", ()))
    table = _as_arrow(
        store.backend.read(
            _table_path(environment_path, source.table),
            columns=columns,
            filters=source.render(**options),
            storage_options=store.options_for(environment_path),
        )
    )
    return table.sort_by(
        [(name, "ascending" if ascending else "descending") for name, ascending in source.sort]
    ) if source.sort else table
