"""hgraph - the hgraph API over the C++ runtime.

Mirrors the Python hgraph package surface: TS/TSS/TSD/TSL/TSB types, the
@graph decorator, run_graph, eval_node, and every registered operator as a
module-level function (via PEP 562 - `from hgraph import add_, const,
filter_, ...` all resolve through the C++ operator registry).

Agreed divergences from Python hgraph are recorded in
docs/source/developer_guide/parity_matrix.rst (e.g. REF is value-only)."""
import _hgraph

from ._types import (TS, TSS, TSD, TSL, TSB, Size, TimeSeriesSchema, CONTEXT, REQUIRED, SCALAR, SCALAR_1,
                     KEYABLE_SCALAR, TIME_SERIES_TYPE, TIME_SERIES_TYPE_1, TIME_SERIES_TYPE_2, OUT, SIZE,
                     DEFAULT, REF, K, V, SCHEMA, TS_SCHEMA)
from ._compat import (CmpResult, DivideByZero, exception_time_series, OperatorWiringNodeClass, BoolResult,
                      CompoundScalar, JSON, TimeSeriesReference)
from ._runtime import WiringPort, graph, run_graph, eval_node, wire, operator_function, map_, reduce, mesh_, mesh_ref, REMOVED, feedback, switch_, passive, compute_node, sink_node, generator, STATE, SCHEDULER, CLOCK, component, record_replay_scope, RecordReplayEnum, comparison_summary, push_queue, EvaluationMode, context, WiringError, reference_service, subscription_service, request_reply_service, register_service, service_impl, adaptor, adaptor_impl, register_adaptor, from_graph, to_graph, impl_input, impl_output, get_service_inputs, set_service_output

MIN_ST = _hgraph.MIN_ST
MIN_TD = _hgraph.MIN_TD
IN_MEMORY = _hgraph.IN_MEMORY
DATA_FRAME = _hgraph.DATA_FRAME
set_record_replay_config = _hgraph.set_record_replay_config
frame_store_contains = _hgraph.frame_store_contains
frame_store_read = _hgraph.frame_store_read
evaluate_const = _hgraph.evaluate_const

TimeSeries = _hgraph.TimeSeries
_hgraph._set_removed_sentinel(REMOVED)
_hgraph._set_cmp_result_enum(CmpResult)
_hgraph._set_divide_by_zero_enum(DivideByZero)

from ._runtime import _Combine as _CombineClass
combine = _CombineClass()

REMOVE = REMOVED           # hgraph's TSD key-removal sentinel
Removed = type(REMOVED)

_OPERATOR_NAMES = frozenset(_hgraph.operator_names())


def __getattr__(name):
    if name in _OPERATOR_NAMES:
        fn = operator_function(name)
        globals()[name] = fn  # cache
        return fn
    from ._compat import _KNOWN_GAPS, _gap
    if name in _KNOWN_GAPS:
        return _gap(name)
    raise AttributeError(f"module 'hgraph' has no attribute '{name}'")


def __dir__():
    return sorted(set(globals()) | _OPERATOR_NAMES)


__all__ = [
    "TS", "TSS", "TSD", "TSL", "TSB", "Size", "TimeSeriesSchema", "CONTEXT", "REQUIRED", "WiringError", "TimeSeries",
    "WiringPort", "CmpResult", "DivideByZero", "exception_time_series", "OperatorWiringNodeClass", "graph", "run_graph", "eval_node", "wire", "map_", "reduce", "mesh_", "mesh_ref", "REMOVED", "feedback", "switch_", "passive", "compute_node", "sink_node", "generator", "STATE", "SCHEDULER", "CLOCK", "component", "record_replay_scope", "RecordReplayEnum", "comparison_summary", "push_queue", "EvaluationMode", "context",
    "MIN_ST", "MIN_TD", "IN_MEMORY", "DATA_FRAME",
    "set_record_replay_config", "frame_store_contains", "frame_store_read", "evaluate_const",
]
