from datetime import date, datetime

import pyarrow as pa
from frozendict import frozendict

from hgraph import MIN_ST, MIN_TD, eval_node
from hgraph.adaptors.data_frame import (
    ArrowDataFrameSource,
    DataConnectionStore,
    DataStore,
    SqlDataFrameSource,
    schema_from_frame,
    ts_from_data_source,
    ts_of_array_from_data_source,
    ts_of_frames_from_data_source,
    ts_of_matrix_from_data_source,
    tsb_from_data_source,
    tsd_k_a_from_data_source,
    tsd_k_b_from_data_source,
    tsd_k_tsd_from_data_source,
    tsd_k_v_from_data_source,
)


class _Rows(ArrowDataFrameSource):
    def __init__(self):
        super().__init__(
            pa.table(
                {
                    "dt": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD],
                    "name": ["one", "two", "three"],
                    "value": [1, 2, 3],
                }
            )
        )


def test_arrow_source_scalar_and_bundle_use_native_replay():
    with DataStore():
        assert eval_node(ts_from_data_source, _Rows, "dt", "value") == [1, 2, 3]
        assert eval_node(tsb_from_data_source, _Rows, "dt") == [
            frozendict(name="one", value=1),
            frozendict(name="two", value=2),
            frozendict(name="three", value=3),
        ]


class _Keyed(ArrowDataFrameSource):
    def __init__(self):
        super().__init__(
            pa.table(
                {
                    "dt": [MIN_ST, MIN_ST, MIN_ST + MIN_TD],
                    "key": ["a", "b", "a"],
                    "value": [1, 2, 3],
                    "label": ["A", "B", "C"],
                }
            )
        )


class _KeyValue(ArrowDataFrameSource):
    def __init__(self):
        super().__init__(_Keyed().data_frame().drop(["label"]))


def test_arrow_source_keyed_scalar_and_bundle():
    with DataStore():
        assert eval_node(tsd_k_v_from_data_source, _KeyValue, "dt", "key") == [
            frozendict(a=1, b=2),
            frozendict(a=3),
        ]
        assert eval_node(tsd_k_b_from_data_source, _Keyed, "dt", "key") == [
            frozendict(
                a=frozendict(value=1, label="A"),
                b=frozendict(value=2, label="B"),
            ),
            frozendict(a=frozendict(value=3, label="C")),
        ]


class _Arrays(ArrowDataFrameSource):
    def __init__(self):
        super().__init__(
            pa.table(
                {
                    "dt": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD],
                    "key": ["a", "b", "c"],
                    "x": [1, 2, 3],
                    "y": [4, 5, 6],
                }
            )
        )


class _UnkeyedArrays(ArrowDataFrameSource):
    def __init__(self):
        super().__init__(_Arrays().data_frame().drop(["key"]))


def test_array_sources_are_packed_before_native_replay():
    with DataStore():
        assert eval_node(ts_of_array_from_data_source, _UnkeyedArrays, "dt") == [
            (1, 4),
            (2, 5),
            (3, 6),
        ]
        assert eval_node(tsd_k_a_from_data_source, _Arrays, "dt", "key") == [
            frozendict(a=(1, 4)),
            frozendict(b=(2, 5)),
            frozendict(c=(3, 6)),
        ]


class _Grouped(ArrowDataFrameSource):
    def __init__(self):
        super().__init__(
            pa.table(
                {
                    "dt": [MIN_ST, MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD],
                    "x": [1, 2, 3, 4],
                    "y": [5, 6, 7, 8],
                }
            )
        )


def test_matrix_and_frame_batch_sources():
    with DataStore():
        assert eval_node(ts_of_matrix_from_data_source, _Grouped, "dt") == [
            ((1, 5), (2, 6)),
            ((3, 7), (4, 8)),
        ]
        frames = eval_node(ts_of_frames_from_data_source, _Grouped, "dt")
        assert frames[0].equals(pa.table({"x": [1, 2], "y": [5, 6]}))
        assert frames[1].equals(pa.table({"x": [3, 4], "y": [7, 8]}))


class _Pivot(ArrowDataFrameSource):
    def __init__(self):
        super().__init__(
            pa.table(
                {
                    "dt": [MIN_ST, MIN_ST, MIN_ST + MIN_TD],
                    "key": ["a", "a", "b"],
                    "pivot": [1, 2, 1],
                    "value": [10, 20, 30],
                }
            )
        )


def test_nested_dictionary_source_emits_additive_deltas():
    # upstream parity: each tick is an ADDITIVE TSD delta — keys absent from
    # a tick keep their prior state (no REMOVE). The old convert/map_
    # pipeline state-synced and emitted removes; the source now yields the
    # nested dicts as deltas directly.
    with DataStore():
        assert eval_node(tsd_k_tsd_from_data_source, _Pivot, "dt", "key", "pivot") == [
            frozendict(a=frozendict({1: 10, 2: 20})),
            frozendict(b=frozendict({1: 30})),
        ]


def test_date_columns_are_normalised_and_schema_is_arrow_derived():
    class _Dates(ArrowDataFrameSource):
        def __init__(self):
            super().__init__(
                pa.table(
                    {
                        "date": [date(2026, 1, 1), date(2026, 1, 2)],
                        "value": [1, 2],
                    }
                )
            )

    schema = schema_from_frame(_Dates().data_frame())
    assert schema.__annotations__ == {"date": date, "value": int}
    with DataStore():
        assert eval_node(
            ts_from_data_source,
            _Dates,
            "date",
            "value",
            __start_time__=datetime(2026, 1, 1),
            __elide__=True,
        ) == [1, 2]


def test_sql_source_accepts_a_dbapi_result_without_importing_a_database_driver():
    class _Result:
        description = (("date",), ("value",))

        def fetchall(self):
            return [(date(2026, 1, 1), 1)]

    class _Connection:
        def execute(self, query):
            assert query == "select date, value from data"
            return _Result()

    with DataConnectionStore() as connections:
        connections.set_connection("test", _Connection())
        source = SqlDataFrameSource("select date, value from data", "test")
        assert source.data_frame().to_pylist() == [
            {"date": date(2026, 1, 1), "value": 1}
        ]
