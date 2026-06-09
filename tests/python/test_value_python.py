from datetime import date, datetime, timedelta

import numpy as np
import pytest

import hgraph

EPOCH = datetime(1970, 1, 1)


def assert_numpy_array(value, expected, dtype):
    assert isinstance(value, np.ndarray)
    assert value.dtype == np.dtype(dtype)
    assert value.tolist() == expected


def assert_numpy_time_array(value, expected, dtype):
    assert isinstance(value, np.ndarray)
    assert value.dtype == np.dtype(dtype)
    assert value.astype("int64").tolist() == expected


def test_value_abstractions_are_exported_from_hgraph_module():
    registry = hgraph.TypeRegistry.instance()

    assert registry.int64().kind is hgraph.ValueTypeKind.Atomic
    assert registry.string().kind is hgraph.ValueTypeKind.Atomic
    assert registry.list(registry.int64()).kind is hgraph.ValueTypeKind.List
    assert hgraph.value_from_python(registry.int64(), 42).to_python() == 42


def test_scalar_value_round_trips_through_nanobind():
    value = hgraph.int_value(42)

    assert value.has_value()
    assert bool(value)
    assert value.to_python() == 42

    value.from_python(99)
    assert value.to_python() == 99
    assert value.clone().equals(value)

    view = value.view()
    assert view.writable_payload()
    assert not view.mutable_payload()
    assert view.can_begin_mutation()
    with pytest.raises(RuntimeError):
        view.from_python(101)

    mutation = view.begin_mutation()
    assert mutation.mutable_payload()
    mutation.from_python(101)
    assert value.to_python() == 101


def test_compact_sequence_values_export_numpy_arrays_for_primitives():
    registry = hgraph.TypeRegistry.instance()
    int_type = registry.int64()

    assert_numpy_array(hgraph.list_value(int_type, [1, 2, 3]).to_python(), [1, 2, 3], "int64")
    assert_numpy_array(hgraph.list_value(int_type, [1, 2, 3], fixed_size=3).to_python(), [1, 2, 3], "int64")
    assert_numpy_array(hgraph.queue_value(int_type, [1, 2, 3]).to_python(), [1, 2, 3], "int64")
    assert_numpy_array(hgraph.cyclic_buffer_value(int_type, [1, 2, 3], 2).to_python(), [2, 3], "int64")
    assert_numpy_array(hgraph.list_value(int_type, []).to_python(), [], "int64")


def test_non_buffer_containers_round_trip_as_python_objects():
    registry = hgraph.TypeRegistry.instance()

    assert hgraph.list_value(registry.string(), ["a", "b"]).to_python() == ["a", "b"]
    assert hgraph.set_value(registry.int64(), {1, 2, 2}).to_python() == {1, 2}
    assert hgraph.tuple_value([registry.int64(), registry.string()], (5, "five")).to_python() == (5, "five")

    bundle = hgraph.bundle_value(
        "PythonRoundTripBundle",
        {"count": registry.int64(), "name": registry.string()},
        {"count": 7, "name": "seven"},
    )
    assert bundle.to_python() == {"count": 7, "name": "seven"}

    mapping = hgraph.map_value(registry.string(), registry.int64(), {"a": 10, "b": 20})
    assert mapping.to_python() == {"a": 10, "b": 20}


def test_specialized_views_are_exposed_from_nanobind():
    registry = hgraph.TypeRegistry.instance()
    int_type = registry.int64()
    string_type = registry.string()

    list_view = hgraph.list_value(int_type, [1, 2, 3]).as_list()
    assert isinstance(list_view, hgraph.ListView)
    assert len(list_view) == 3
    assert list_view.at(1).to_python() == 2
    assert list_view.front().to_python() == 1
    assert list_view.back().to_python() == 3
    assert list_view.values() == [1, 2, 3]
    with pytest.raises(RuntimeError):
        list_view.at(0).from_python(10)
    assert not hgraph.list_value(int_type, [1, 2, 3]).view().can_begin_mutation()
    with pytest.raises(RuntimeError):
        hgraph.list_value(int_type, [1, 2, 3]).view().begin_mutation()

    tuple_view = hgraph.tuple_value([int_type, string_type], (7, "seven")).as_tuple()
    assert isinstance(tuple_view, hgraph.TupleView)
    assert len(tuple_view) == 2
    assert tuple_view.at(0).to_python() == 7
    assert tuple_view.to_python() == (7, "seven")

    bundle_view = hgraph.bundle_value(
        "PythonViewBundle",
        {"count": int_type, "name": string_type},
        {"count": 4, "name": "items"},
    ).as_bundle()
    assert isinstance(bundle_view, hgraph.BundleView)
    assert bundle_view.has_field("count")
    assert not bundle_view.has_field("missing")
    assert bundle_view.field("name").to_python() == "items"
    assert bundle_view.to_python() == {"count": 4, "name": "items"}

    set_view = hgraph.set_value(int_type, {1, 2, 3}).as_set()
    assert isinstance(set_view, hgraph.SetView)
    assert len(set_view) == 3
    assert set_view.contains(hgraph.int_value(2).view())
    assert not set_view.contains(hgraph.int_value(9).view())
    assert not set_view.contains(hgraph.string_value("2").view())
    assert sorted(set_view.values()) == [1, 2, 3]

    map_view = hgraph.map_value(string_type, int_type, {"a": 10, "b": 20}).as_map()
    assert isinstance(map_view, hgraph.MapView)
    assert len(map_view) == 2
    assert map_view.contains(hgraph.string_value("a").view())
    assert not map_view.contains(hgraph.int_value(1).view())
    assert map_view.at(hgraph.string_value("b").view()).to_python() == 20
    assert sorted(map_view.keys()) == ["a", "b"]
    assert sorted(map_view.values()) == [10, 20]
    assert dict(map_view.items()) == {"a": 10, "b": 20}
    assert map_view.key_set().contains(hgraph.string_value("a").view())

    cyclic_view = hgraph.cyclic_buffer_value(int_type, [1, 2, 3], 2).as_cyclic_buffer()
    assert isinstance(cyclic_view, hgraph.CyclicBufferView)
    assert cyclic_view.capacity() == 2
    assert cyclic_view.full()
    assert cyclic_view.values() == [2, 3]

    queue_view = hgraph.queue_value(int_type, [4, 5]).as_queue()
    assert isinstance(queue_view, hgraph.QueueView)
    assert queue_view.front().to_python() == 4
    assert queue_view.back().to_python() == 5
    assert queue_view.values() == [4, 5]


def test_datetime_containers_export_numpy_time_dtypes():
    assert_numpy_time_array(
        hgraph.datetime_list(
            [
                EPOCH + timedelta(microseconds=123),
                EPOCH + timedelta(microseconds=456),
            ]
        ).to_python(),
        [123, 456],
        "datetime64[us]",
    )
    assert_numpy_time_array(
        hgraph.timedelta_cyclic_buffer(
            [
                timedelta(microseconds=10),
                timedelta(microseconds=20),
                timedelta(microseconds=30),
            ],
            2,
        ).to_python(),
        [20, 30],
        "timedelta64[us]",
    )
    assert_numpy_time_array(
        hgraph.date_queue([date(1970, 1, 2), date(1970, 1, 3)]).to_python(),
        [1, 2],
        "datetime64[D]",
    )


def test_date_time_scalars_round_trip_as_python_datetime_types():
    registry = hgraph.TypeRegistry.instance()

    timestamp = EPOCH + timedelta(days=2, seconds=3, microseconds=4)
    duration = timedelta(seconds=5, microseconds=6)
    day = date(2024, 2, 29)

    assert hgraph.value_from_python(registry.datetime(), timestamp).to_python() == timestamp
    assert hgraph.value_from_python(registry.timedelta(), duration).to_python() == duration
    assert hgraph.value_from_python(registry.date(), day).to_python() == day


def test_unsupported_scalar_conversion_raises():
    with pytest.raises(RuntimeError):
        hgraph.unsupported_scalar_to_python()
