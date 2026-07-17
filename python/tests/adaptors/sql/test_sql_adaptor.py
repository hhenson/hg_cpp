import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pyarrow as pa

import hgraph as hg
from hgraph.adaptors.data_catalogue import DataEnvironment, DataEnvironmentEntry
from hgraph.adaptors.sql import (
    SQLWriteMode,
    sql_execute_adaptor,
    sql_execute_adaptor_impl,
    sql_read_adaptor,
    sql_read_adaptor_impl,
    sql_write_adaptor,
    sql_write_adaptor_impl,
)
from hgraph.stream import StreamStatus


@dataclass(frozen=True)
class _Row(hg.CompoundScalar):
    name: str
    value: int


def _end_time():
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=5)


def _environment(path):
    environment = DataEnvironment()
    environment.add_entry(DataEnvironmentEntry("database", f"sqlite:///{path}"))
    return environment


def test_sql_read_adaptor_returns_a_typed_arrow_frame(tmp_path):
    database = tmp_path / "read.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("create table rows (name text, value integer)")
        connection.executemany("insert into rows values (?, ?)", [("a", 1), ("b", 2)])

    captured = []

    @hg.push_queue(hg.TS[str])
    def query(sender):
        sender("select name, value from rows order by value")

    @hg.sink_node
    def capture(response: hg.TSB[hg.stream.Stream[hg.stream.Data[hg.Frame[_Row]]]], engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            captured.append(response["values"].value)
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("database", sql_read_adaptor_impl)
        capture(sql_read_adaptor[_Row](query(), path="database"))

    with hg.GlobalContext(hg.GlobalState()):
        with _environment(database):
            hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    assert captured[0].equals(pa.table({"name": ["a", "b"], "value": [1, 2]}))


def test_sql_write_adaptor_writes_arrow_rows(tmp_path):
    database = tmp_path / "write.sqlite"
    frame = pa.table({"name": ["a", "b"], "value": [1, 2]})

    @hg.push_queue(hg.TS[hg.Frame[_Row]])
    def data(sender):
        sender(frame)

    @hg.sink_node
    def stop_when_done(response: hg.TSB[hg.stream.Stream[hg.stream.Data[datetime]]], engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("database", sql_write_adaptor_impl)
        stop_when_done(
            sql_write_adaptor(
                path="database",
                table="rows",
                data=data(),
                mode=SQLWriteMode.OVERWRITE,
            )
        )

    with hg.GlobalContext(hg.GlobalState()):
        with _environment(database):
            hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    with sqlite3.connect(database) as connection:
        assert connection.execute("select name, value from rows order by value").fetchall() == [
            ("a", 1),
            ("b", 2),
        ]


def test_sql_execute_adaptor_commits_statements(tmp_path):
    database = tmp_path / "execute.sqlite"

    @hg.push_queue(hg.TS[str])
    def query(sender):
        sender("create table executed (value integer)")

    @hg.sink_node
    def stop_when_done(response: hg.TSB[hg.stream.Stream[hg.stream.Data[datetime]]], engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("database", sql_execute_adaptor_impl)
        stop_when_done(sql_execute_adaptor(query(), path="database"))

    with hg.GlobalContext(hg.GlobalState()):
        with _environment(database):
            hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "select name from sqlite_master where type='table' and name='executed'"
        ).fetchone() == ("executed",)
