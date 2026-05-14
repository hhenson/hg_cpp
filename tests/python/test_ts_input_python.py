from datetime import datetime, timedelta

import numpy as np

import hgraph

EPOCH = datetime(1970, 1, 1)


def assert_sequence(value, expected):
    if isinstance(value, np.ndarray):
        assert value.tolist() == expected
    else:
        assert value == expected


def tick(microseconds):
    return EPOCH + timedelta(microseconds=microseconds)


def test_ts_input_value_and_delta_value_from_bound_outputs(request):
    registry = hgraph.TypeRegistry.instance()
    int_schema = registry.int64()
    ts_int = registry.ts(int_schema)
    nested = registry.tsb("PythonTSInputNested", {"x": ts_int, "y": ts_int})
    fixed_list = registry.tsl(ts_int, 2)
    root_schema = registry.tsb(
        "PythonTSInputRoot",
        {
            "leaf": ts_int,
            "bundle": nested,
            "whole_list": fixed_list,
            "leaf_list": fixed_list,
        },
    )

    endpoint_schema = hgraph.TSEndpointSchema.non_peered(
        root_schema,
        [
            hgraph.TSEndpointSchema.peered(ts_int),
            hgraph.TSEndpointSchema.peered(nested),
            hgraph.TSEndpointSchema.peered(fixed_list),
            hgraph.TSEndpointSchema.non_peered_list(
                fixed_list,
                hgraph.TSEndpointSchema.peered(ts_int),
            ),
        ],
    )

    evaluation_time = tick(100)
    leaf_output = hgraph.TSOutput(ts_int).apply_value(7, evaluation_time=evaluation_time)
    bundle_output = hgraph.TSOutput(nested).apply_value(
        {"x": 10, "y": 20},
        evaluation_time=evaluation_time,
    )
    list_output = hgraph.TSOutput(fixed_list).apply_value([100, 200], evaluation_time=evaluation_time)
    first_element_output = hgraph.TSOutput(ts_int).apply_value(1000, evaluation_time=evaluation_time)
    second_element_output = hgraph.TSOutput(ts_int).apply_value(2000, evaluation_time=evaluation_time)
    none_output = hgraph.TSOutput(ts_int).apply_value(None, evaluation_time=evaluation_time)

    assert not none_output.view(evaluation_time).valid()

    input_ = hgraph.TSInput.create(root_schema, endpoint_schema)
    root = input_.view(evaluation_time=evaluation_time)
    bundle = root.as_bundle()

    leaf = bundle.field("leaf")
    nested_input = bundle.field("bundle")
    whole_list = bundle.field("whole_list")
    leaf_list = bundle.field("leaf_list").as_list()

    bound_views = []

    def cleanup_bindings():
        for view in reversed(bound_views):
            view.unbind_output()

    request.addfinalizer(cleanup_bindings)

    assert leaf.bind_output(leaf_output.view(evaluation_time))
    bound_views.append(leaf)
    assert nested_input.bind_output(bundle_output.view(evaluation_time))
    bound_views.append(nested_input)
    assert whole_list.bind_output(list_output.view(evaluation_time))
    bound_views.append(whole_list)
    first_leaf_list = leaf_list[0]
    second_leaf_list = leaf_list[1]
    assert first_leaf_list.bind_output(first_element_output.view(evaluation_time))
    bound_views.append(first_leaf_list)
    assert second_leaf_list.bind_output(second_element_output.view(evaluation_time))
    bound_views.append(second_leaf_list)

    assert root.valid()
    assert root.all_valid()

    assert leaf.value == 7
    assert leaf.delta_value == 7
    assert nested_input.value == {"x": 10, "y": 20}
    assert nested_input.delta_value == {"x": 10, "y": 20}
    assert_sequence(whole_list.value, [100, 200])
    assert whole_list.delta_value == {0: 100, 1: 200}
    assert leaf_list.value == [1000, 2000]
    assert leaf_list.delta_value == {0: 1000, 1: 2000}

    root_value = root.value
    assert root_value["leaf"] == 7
    assert root_value["bundle"] == {"x": 10, "y": 20}
    assert_sequence(root_value["whole_list"], [100, 200])
    assert root_value["leaf_list"] == [1000, 2000]

    assert root.delta_value["leaf"] == 7
    assert root.delta_value["bundle"] == {"x": 10, "y": 20}
    assert root.delta_value["whole_list"] == {0: 100, 1: 200}
    assert root.delta_value["leaf_list"] == {0: 1000, 1: 2000}

    later_root = input_.view(evaluation_time=evaluation_time + timedelta(microseconds=1))
    later_bundle = later_root.as_bundle()
    assert later_root.value["leaf"] == 7
    assert later_bundle.field("leaf").delta_value is None
    assert later_root.delta_value is None


def test_ts_output_apply_value_accepts_delta_values_from_python():
    registry = hgraph.TypeRegistry.instance()
    int_schema = registry.int64()
    string_schema = registry.string()
    ts_int = registry.ts(int_schema)
    nested = registry.tsb("PythonTSOutputDeltaNested", {"x": ts_int, "y": ts_int})
    fixed_list = registry.tsl(ts_int, 2)
    tss_int = registry.tss(int_schema)
    tsd_string_int = registry.tsd(string_schema, ts_int)
    window = registry.tsw(int_schema, 2)

    t200 = tick(200)
    t201 = tick(201)
    t202 = tick(202)

    scalar = hgraph.TSOutput(ts_int).apply_value(7, evaluation_time=t200)
    assert scalar.view(t200).value == 7
    assert scalar.view(t200).delta_value == 7
    assert scalar.view(t200).evaluation_time == t200
    assert scalar.view(t200).last_modified_time == t200
    scalar.view(t201).apply_value(11)
    assert scalar.view(t201).value == 11
    assert scalar.view(t201).delta_value == 11

    none_output = hgraph.TSOutput(ts_int).apply_value(None, evaluation_time=t200)
    assert not none_output.view(t200).valid()

    bundle = hgraph.TSOutput(nested).apply_value({"x": 1, "y": 2}, evaluation_time=t200)
    bundle.apply_value({"y": 20}, evaluation_time=t201)
    assert bundle.view(t201).value == {"x": 1, "y": 20}
    assert bundle.view(t201).delta_value == {"y": 20}
    assert bundle.view(t202).delta_value is None

    list_output = hgraph.TSOutput(fixed_list).apply_value([1, 2], evaluation_time=t200)
    list_output.apply_value({1: 20}, evaluation_time=t201)
    assert_sequence(list_output.view(t201).value, [1, 20])
    assert list_output.view(t201).delta_value == {1: 20}

    set_value_output = hgraph.TSOutput(tss_int).apply_value({3, 4}, evaluation_time=t200)
    assert set_value_output.view(t200).value == {3, 4}
    assert set_value_output.view(t200).delta_value == {"added": {3, 4}, "removed": set()}

    set_output = hgraph.TSOutput(tss_int).apply_value({"added": [1, 2], "removed": []}, evaluation_time=t200)
    assert set_output.view(t200).value == {1, 2}
    assert set_output.view(t200).delta_value == {"added": {1, 2}, "removed": set()}
    set_output.apply_value({"removed": [1]}, evaluation_time=t201)
    assert set_output.view(t201).value == {2}
    assert set_output.view(t201).delta_value == {"added": set(), "removed": {1}}

    dict_value_output = hgraph.TSOutput(tsd_string_int).apply_value({"c": 30}, evaluation_time=t200)
    assert dict_value_output.view(t200).value == {"c": 30}
    assert dict_value_output.view(t200).delta_value == {"removed": set(), "modified": {"c": 30}}

    dict_output = hgraph.TSOutput(tsd_string_int).apply_value(
        {"removed": [], "modified": {"a": 10, "b": 20}},
        evaluation_time=t200,
    )
    assert dict_output.view(t200).value == {"a": 10, "b": 20}
    assert dict_output.view(t200).delta_value == {"removed": set(), "modified": {"a": 10, "b": 20}}
    dict_output.apply_value({"removed": ["a"], "modified": {"b": 30}}, evaluation_time=t201)
    assert dict_output.view(t201).value == {"b": 30}
    assert dict_output.view(t201).delta_value == {"removed": {"a"}, "modified": {"b": 30}}

    window_value_output = hgraph.TSOutput(window).apply_value([1, 2], evaluation_time=t200)
    assert_sequence(window_value_output.view(t200).value, [1, 2])
    assert window_value_output.view(t200).delta_value == 2

    window_output = hgraph.TSOutput(window).apply_value(5, evaluation_time=t200)
    assert_sequence(window_output.view(t200).value, [5])
    assert window_output.view(t200).delta_value == 5
    window_output.apply_value(6, evaluation_time=t201)
    assert_sequence(window_output.view(t201).value, [5, 6])
    assert window_output.view(t201).delta_value == 6
