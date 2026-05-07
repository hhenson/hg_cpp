import numpy as np
import pytest

import hgraph


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


def test_engine_time_containers_export_numpy_time_dtypes():
    assert_numpy_time_array(hgraph.engine_time_list([123, 456]).to_python(), [123, 456], "datetime64[us]")
    assert_numpy_time_array(
        hgraph.engine_delta_cyclic_buffer([10, 20, 30], 2).to_python(),
        [20, 30],
        "timedelta64[us]",
    )
    assert_numpy_time_array(hgraph.engine_date_queue([1, 2]).to_python(), [1, 2], "datetime64[D]")


def test_unsupported_scalar_conversion_raises():
    with pytest.raises(RuntimeError):
        hgraph.unsupported_scalar_to_python()
