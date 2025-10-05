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
hgraph.GraphBuilderFactory.declare(_hgraph.GraphBuilder)

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


hgraph._wiring._wiring_node_class.ReduceWiringNodeClass.BUILDER_CLASS = _create_reduce_node_builder_factory
