from dataclasses import dataclass

from hgraph.adaptors.data_catalogue import DataCatalogue, DataSource

from .sql_adaptor import _read

__all__ = ("SqlDataSource",)


@dataclass(frozen=True)
class SqlDataSource(DataSource):
    query: str

    def render(self, **options):
        return self.query.format(**options)


@DataCatalogue.source_handler(SqlDataSource)
def _read_sql_source(entry, options, environment_path):
    return _read(environment_path, entry.store.render(**options))
