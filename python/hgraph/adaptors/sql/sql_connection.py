import os
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlparse

from hgraph.adaptors.data_catalogue import DataEnvironment
from hgraph.adaptors.data_frame import DataConnectionStore

__all__ = (
    "SqlAdaptorConnection",
    "SqlAdaptorConnectionSQLServer",
    "SqlAdaptorConnectionSnowflake",
    "start_sql_adaptor",
    "get_secret",
    "connection_for",
    "connection_target",
    "parse_connection_params",
)


class SqlAdaptorConnection:
    """A lightweight DB-API connection factory."""

    def __init__(self, factory):
        self.factory = factory

    def connect(self):
        return self.factory()


class SqlAdaptorConnectionSQLServer(SqlAdaptorConnection):
    def __init__(self, path, connection_params=None):
        try:
            from sqlalchemy import create_engine
        except ModuleNotFoundError as error:
            raise RuntimeError("SQLAlchemy connections require the 'sql' extra") from error
        self.path = path
        self.connection = create_engine(path, **(connection_params or {}))
        super().__init__(self.connection.raw_connection)

    def read_database(self, query):
        with connection_for(self) as connection:
            return _result_table(connection.execute(query))


class SqlAdaptorConnectionSnowflake(SqlAdaptorConnection):
    def __init__(self, connection_params):
        try:
            import adbc_driver_snowflake.dbapi as snowflake
        except ModuleNotFoundError as error:
            raise RuntimeError("Snowflake connections require the 'snowflake' extra") from error
        self.connection = snowflake.connect(db_kwargs=dict(connection_params))
        super().__init__(lambda: self.connection)

    def read_database(self, query):
        return _result_table(self.connection.execute(query))


get_secret = None


def _substitution(match):
    value = match.group(1)
    if value.startswith("secret:"):
        if get_secret is None:
            raise ValueError("get_secret is not configured")
        name, _, key = value[7:].partition("/")
        secret = get_secret(name)
        return quote(str(secret[key] if key else secret), safe="")
    if value.startswith("$"):
        try:
            return quote(os.environ[value[1:]], safe="")
        except KeyError as error:
            raise ValueError(f"environment variable {value[1:]!r} is not set") from error
    return quote(value, safe="")


def parse_connection_params(path: str):
    path = re.sub(r"(?<!\{)\{([^{}]*)\}(?!\})", _substitution, path)
    parsed = urlparse(path)
    base = parsed._replace(query="").geturl()
    if parsed.netloc == "" and parsed.scheme:
        base = base.replace(":/", ":///", 1)
    return parsed.scheme, base, {
        key: values[-1]
        for key, values in parse_qs(parsed.query).items()
    }


def start_sql_adaptor(path: str):
    """Create a dependency-neutral connection wrapper for explicit seeding."""
    scheme, base, params = parse_connection_params(path)
    if scheme == "snowflake":
        return SqlAdaptorConnectionSnowflake(params)
    return SqlAdaptorConnectionSQLServer(base, params)


def _sqlite_path(uri: str):
    parsed = urlparse(uri)
    if parsed.path in ("/:memory:", ":memory:"):
        return ":memory:"
    if parsed.netloc:
        return unquote(f"//{parsed.netloc}{parsed.path}")
    path = unquote(parsed.path)
    if uri.startswith("sqlite:////"):
        return path
    return path.lstrip("/") if not Path(path).is_absolute() else path


def connection_target(path: str):
    environment = DataEnvironment.current()
    if environment is not None and environment.has_entry(path):
        return environment.get_entry(path).environment_path
    connections = DataConnectionStore.instance()
    if connections.has_connection(path):
        return connections.get_connection(path)
    return path


@contextmanager
def connection_for(path: str):
    """Yield a DB-API connection and close it only when this function owns it."""
    target = connection_target(path)
    owned = False
    if isinstance(target, SqlAdaptorConnection):
        connection = target.connect()
        owned = True
    elif callable(target) and not isinstance(target, str):
        connection = target()
        owned = True
    elif not isinstance(target, str):
        connection = target
    elif target.startswith("sqlite:"):
        connection = sqlite3.connect(_sqlite_path(target), check_same_thread=False)
        owned = True
    elif target.startswith("duckdb:"):
        try:
            import duckdb
        except ModuleNotFoundError as error:
            raise RuntimeError("duckdb SQL paths require the 'duckdb' package") from error
        parsed = urlparse(target)
        connection = duckdb.connect(unquote(parsed.path).lstrip("/") or ":memory:")
        owned = True
    else:
        try:
            from sqlalchemy import create_engine
        except ModuleNotFoundError as error:
            raise RuntimeError(
                f"SQL path {target!r} requires a registered DB-API connection or the 'sql' extra"
            ) from error
        engine = create_engine(target)
        connection = engine.raw_connection()
        owned = True

    try:
        yield connection
    finally:
        if owned:
            connection.close()
            if "engine" in locals():
                engine.dispose()


def _result_table(result):
    fetch_arrow_table = getattr(result, "fetch_arrow_table", None)
    if fetch_arrow_table is not None:
        return fetch_arrow_table()
    rows = result.fetchall()
    names = [column[0] for column in result.description or ()]
    import pyarrow as pa

    return pa.Table.from_pylist([dict(zip(names, row)) for row in rows])
