from datetime import date, datetime, timedelta

import _hgraph
import hgraph
from hgraph import TsdMapWiringSignature

hgraph._wiring._wiring_node_class._wiring_node_class.WiringNodeInstance.NODE_SIGNATURE = _hgraph.NodeSignature
hgraph._wiring._wiring_node_class._wiring_node_class.WiringNodeInstance.NODE_TYPE_ENUM = _hgraph.NodeTypeEnum
hgraph._wiring._wiring_node_class._wiring_node_class.WiringNodeInstance.INJECTABLE_TYPES_ENUM = _hgraph.InjectableTypesEnum

hgraph._runtime._evaluation_engine.EvaluationMode = _hgraph.EvaluationMode
hgraph._runtime._evaluation_engine.EvaluationLifeCycleObserver = _hgraph.EvaluationLifeCycleObserver

from hg_cpp._builder_factories import HgCppFactory

hgraph.TimeSeriesReference._BUILDER = _hgraph.TimeSeriesReference.make
hgraph.TimeSeriesReference._INSTANCE_OF = lambda obj: isinstance(obj, _hgraph.TimeSeriesReference)

hgraph._builder._graph_builder.EDGE_TYPE = _hgraph.Edge
def _make_cpp_graph_builder(node_builders, edges):
    # Convert Python Edge dataclass instances to C++ _hgraph.Edge
    cpp_edges = []
    for e in edges:
        try:
            cpp_edges.append(_hgraph.Edge(int(e.src_node), list(e.output_path), int(e.dst_node), list(e.input_path)))
        except Exception:
            # If it's already a C++ Edge, keep as-is
            cpp_edges.append(e)
    return _hgraph.GraphBuilder(list(node_builders), cpp_edges)

hgraph.GraphBuilderFactory.declare(_make_cpp_graph_builder)

# The graph engine type
hgraph.GraphEngineFactory.declare(lambda graph, run_mode, observers: _hgraph.GraphExecutorImpl(
    graph, {hgraph.EvaluationMode.SIMULATION: _hgraph.EvaluationMode.SIMULATION,
            hgraph.EvaluationMode.REAL_TIME: _hgraph.EvaluationMode.REAL_TIME}[run_mode], observers))

# The time-series builder factory.
hgraph.TimeSeriesBuilderFactory.declare(HgCppFactory())

hgraph._wiring._wiring_node_class.PythonWiringNodeClass.BUILDER_CLASS = _hgraph.PythonNodeBuilder
hgraph._wiring._wiring_node_class.PythonGeneratorWiringNodeClass.BUILDER_CLASS = _hgraph.PythonGeneratorNodeBuilder


def _create_set_delta(added, removed, tp):
    sd_tp = {
        bool: _hgraph.SetDelta_bool,
        int: _hgraph.SetDelta_int,
        float: _hgraph.SetDelta_float,
        date: _hgraph.SetDelta_date,
        datetime: _hgraph.SetDelta_date_time,
        timedelta: _hgraph.SetDelta_time_delta,
    }.get(tp, None)
    if sd_tp is None:
        return _hgraph.SetDelta_object(added, removed, tp)
    return sd_tp(added, removed)


hgraph.set_set_delta_factory(
    _create_set_delta
)


def _create_tsd_map_builder_factory(
        signature: TsdMapWiringSignature,
        scalars,
        input_builder,
        output_builder,
        error_builder,
        recordable_state_builder,
        nested_graph,
        input_node_ids,
        output_node_id,
        multiplexed_args, key_arg,
        key_tp,
):
    key_tp = key_tp.py_type
    return {
        bool: _hgraph.TsdMapNodeBuilder_bool,
        int: _hgraph.TsdMapNodeBuilder_int,
        float: _hgraph.TsdMapNodeBuilder_float,
        date: _hgraph.TsdMapNodeBuilder_date,
        datetime: _hgraph.TsdMapNodeBuilder_date_time,
        timedelta: _hgraph.TsdMapNodeBuilder_time_delta,
    }.get(key_tp, _hgraph.TsdMapNodeBuilder_object)(
        signature,
        scalars,
        input_builder,
        output_builder,
        error_builder,
        recordable_state_builder,
        nested_graph,
        input_node_ids,
        output_node_id,
        multiplexed_args,
        "" if key_arg is None else key_arg,
    )


hgraph._wiring._wiring_node_class.TsdMapWiringNodeClass.BUILDER_CLASS = _create_tsd_map_builder_factory


def _create_reduce_node_builder_factory(
        signature,
        scalars,
        input_builder,
        output_builder,
        error_builder,
        nested_graph,
        input_node_ids,
        output_node_id,
):
    # Extract key type from the TSD input (ts parameter)
    ts_input_type = signature.time_series_inputs['ts']
    key_tp = ts_input_type.key_tp.py_type

    return {
        bool: _hgraph.ReduceNodeBuilder_bool,
        int: _hgraph.ReduceNodeBuilder_int,
        float: _hgraph.ReduceNodeBuilder_float,
        date: _hgraph.ReduceNodeBuilder_date,
        datetime: _hgraph.ReduceNodeBuilder_date_time,
        timedelta: _hgraph.ReduceNodeBuilder_time_delta,
    }.get(key_tp, _hgraph.ReduceNodeBuilder_object)(
        signature,
        scalars,
        input_builder,
        output_builder,
        error_builder,
        None,  # recordable_state_builder
        nested_graph,
        input_node_ids,
        output_node_id,
    )


hgraph._wiring._wiring_node_class.TsdReduceWiringNodeClass.BUILDER_CLASS = _create_reduce_node_builder_factory


# Component Node
def _create_component_node_builder_factory(
        signature,
        scalars,
        input_builder,
        output_builder,
        error_builder,
        recordable_state_builder,
        nested_graph,
        input_node_ids,
        output_node_id,
):
    return _hgraph.ComponentNodeBuilder(
        signature,
        scalars,
        input_builder,
        output_builder,
        error_builder,
        recordable_state_builder,
        nested_graph,
        input_node_ids,
        output_node_id,
    )


hgraph._wiring._wiring_node_class._component_node_class.ComponentNodeClass.BUILDER_CLASS = _create_component_node_builder_factory


# Switch Node
def _create_switch_node_builder_factory(
        signature,
        scalars,
        input_builder,
        output_builder,
        error_builder,
        nested_graphs,
        input_node_ids,
        output_node_id,
        reload_on_ticked,
        recordable_state_builder=None,
):
    # Extract key type from the switch signature
    switch_input_type = signature.time_series_inputs.get('key', None)
    key_tp = switch_input_type.value_scalar_tp.py_type

    return {
        bool: _hgraph.SwitchNodeBuilder_bool,
        int: _hgraph.SwitchNodeBuilder_int,
        float: _hgraph.SwitchNodeBuilder_float,
        date: _hgraph.SwitchNodeBuilder_date,
        datetime: _hgraph.SwitchNodeBuilder_date_time,
        timedelta: _hgraph.SwitchNodeBuilder_time_delta,
    }.get(key_tp, _hgraph.SwitchNodeBuilder_object)(
        signature,
        scalars,
        input_builder,
        output_builder,
        error_builder,
        recordable_state_builder,
        nested_graphs,
        input_node_ids,
        output_node_id,
        reload_on_ticked,
    )


hgraph._wiring._wiring_node_class.SwitchWiringNodeClass.BUILDER_CLASS = _create_switch_node_builder_factory


# Try-Except Node
def _create_try_except_node_builder_factory(
        signature,
        scalars,
        input_builder,
        output_builder,
        error_builder,
        recordable_state_builder,
        nested_graph,
        input_node_ids,
        output_node_id,
):
    return _hgraph.TryExceptNodeBuilder(
        signature,
        scalars,
        input_builder,
        output_builder,
        error_builder,
        recordable_state_builder,
        nested_graph,
        input_node_ids,
        output_node_id,
    )


hgraph._wiring._wiring_node_class.TryExceptWiringNodeClass.BUILDER_CLASS = _create_try_except_node_builder_factory


# TSD Non-Associative Reduce Node
def _create_tsd_non_associative_reduce_node_builder_factory(
        signature,
        scalars,
        input_builder,
        output_builder,
        error_builder,
        recordable_state_builder,
        nested_graph,
        input_node_ids,
        output_node_id,
):
    return _hgraph.TsdNonAssociativeReduceNodeBuilder(
        signature,
        scalars,
        input_builder,
        output_builder,
        error_builder,
        recordable_state_builder,
        nested_graph,
        input_node_ids,
        output_node_id,
    )


hgraph._wiring._wiring_node_class._reduce_wiring_node.TsdNonAssociativeReduceWiringNodeClass.BUILDER_CLASS = _create_tsd_non_associative_reduce_node_builder_factory


# Mesh Node
def _create_mesh_node_builder_factory(
        signature,
        scalars,
        input_builder,
        output_builder,
        error_builder,
        recordable_state_builder,
        nested_graph,
        input_node_ids,
        output_node_id,
        multiplexed_args,
        key_arg,
        context_path,
):
    # Extract key type from signature
    key_input = signature.time_series_inputs.get(key_arg, None)
    if key_input:
        key_tp = key_input.value_tp.py_type if hasattr(key_input, 'value_tp') else object
    else:
        key_tp = int  # Default to int for mesh nodes

    return {
        bool: _hgraph.MeshNodeBuilder_bool,
        int: _hgraph.MeshNodeBuilder_int,
        float: _hgraph.MeshNodeBuilder_float,
        date: _hgraph.MeshNodeBuilder_date,
        datetime: _hgraph.MeshNodeBuilder_date_time,
        timedelta: _hgraph.MeshNodeBuilder_time_delta,
    }.get(key_tp, _hgraph.MeshNodeBuilder_object)(
        signature,
        scalars,
        input_builder,
        output_builder,
        error_builder,
        recordable_state_builder,
        nested_graph,
        input_node_ids,
        output_node_id,
        multiplexed_args,
        "" if key_arg is None else key_arg,
        context_path,
    )


hgraph._wiring._wiring_node_class.MeshWiringNodeClass.BUILDER_CLASS = _create_mesh_node_builder_factory

def _service_impl_nested_graph_builder(*, signature, scalars, input_builder, output_builder, error_builder, recordable_state_builder, nested_graph):
    # Provide empty input mapping and no output node for service impl wrappers
    return _hgraph.NestedGraphNodeBuilder(
        signature,
        scalars,
        input_builder,
        output_builder,
        error_builder,
        recordable_state_builder,
        nested_graph,
        {},
        0,
    )

hgraph._wiring._wiring_node_class._service_impl_node_class.ServiceImplNodeClass.BUILDER_CLASS = _service_impl_nested_graph_builder

hgraph._wiring._wiring_node_class._pull_source_node_class.PythonLastValuePullWiringNodeClass.BUILDER_CLASS = _hgraph.LastValuePullNodeBuilder
