from dataclasses import dataclass

from frozendict import frozendict

from hgraph import TS, compute_node
from hgraph.adaptors.data_catalogue import DataCatalogue, DataSource

from .sql_adaptor import sql_read_adaptor, sql_read_adaptor_impl
from .sql_subscriber import _read_sql_source

__all__ = ("BatchSqlDataSource", "sql_adaptor_batch", "sql_adaptor_batch_impl")


@dataclass(frozen=True)
class BatchSqlDataSource(DataSource):
    name: str
    query: str
    filters: frozendict[str, str] = frozendict()

    def render(self, **options):
        return self.query.format(**options)


@DataCatalogue.source_handler(BatchSqlDataSource)
def _read_batch_sql_source(entry, options, environment_path):
    return _read_sql_source(entry, options, environment_path)


@compute_node
def _render_batch_query(
    ds: TS[BatchSqlDataSource], scope: TS[object], options: TS[object]
) -> TS[str]:
    values = options.value or {}
    scopes = scope.value or {}
    adjusted = {
        name: item.adjust(values[name]) if name in values else item.default()
        for name, item in scopes.items()
    }
    return ds.value.render(**adjusted)


class _BatchSqlAdaptor:
    def __init__(self, schema=None):
        self.schema = schema

    def __getitem__(self, schema):
        return _BatchSqlAdaptor(schema)

    def __call__(self, ds, scope, options, *, path):
        adaptor = sql_read_adaptor if self.schema is None else sql_read_adaptor[self.schema]
        return adaptor(_render_batch_query(ds, scope, options), path=path)


sql_adaptor_batch = _BatchSqlAdaptor()
sql_adaptor_batch_impl = sql_read_adaptor_impl
