"""hgraph - the hgraph API over the C++ runtime.

Mirrors the Python hgraph package surface: TS/TSS/TSD/TSL/TSB types, the
@graph decorator, run_graph, eval_node, and every registered operator as a
module-level function (via PEP 562 - `from hgraph import add_, const,
filter_, ...` all resolve through the C++ operator registry).

Agreed divergences from Python hgraph are recorded in
docs/source/developer_guide/parity_matrix.rst (e.g. REF is value-only)."""
import _hgraph

from ._types import Series
from ._types import (TS, TSS, TSD, TSL, TSB, Size, TimeSeriesSchema, CONTEXT, REQUIRED, SCALAR, SCALAR_1, TSW, KeyValue, AUTO_RESOLVE,
                     KEYABLE_SCALAR, TIME_SERIES_TYPE, TIME_SERIES_TYPE_1, TIME_SERIES_TYPE_2, OUT, SIZE,
                     DEFAULT, REF, K, V, SCHEMA, TS_SCHEMA,
                     SIGNAL, WINDOW_SIZE, WINDOW_SIZE_MIN, WindowSize, Array, ts_schema, ENUM)
from ._compat import (CmpResult, DivideByZero, exception_time_series, OperatorWiringNodeClass, BoolResult,
                      CompoundScalar, JSON, TimeSeriesReference,
                      NodeException, accumulate, average, center_of_mass_to_alpha, span_to_alpha,
                      to_json_builder, from_json_builder)
from ._runtime import filter_by
from ._runtime import convert
from ._runtime import collect
from ._runtime import emit
from ._runtime import cast_
from ._runtime import GlobalContext, GlobalState, set_record_replay_config, set_as_of, set_table_schema_date_key, set_table_schema_as_of_key, evaluate_const
from ._runtime import WiringPort, graph, run_graph, eval_node, wire, operator_function, map_, reduce, mesh_, mesh_ref, REMOVED, feedback, switch_, passive, compute_node, sink_node, generator, lift, STATE, SCHEDULER, CLOCK, LOGGER, DebugContext, component, record_replay_scope, RecordReplayEnum, comparison_summary, push_queue, EvaluationMode, context, WiringError, reference_service, subscription_service, request_reply_service, register_service, service_impl, adaptor, adaptor_impl, register_adaptor, from_graph, to_graph, impl_input, impl_output, get_service_inputs, set_service_output

MIN_ST = _hgraph.MIN_ST
MIN_TD = _hgraph.MIN_TD
MIN_DT = MIN_ST - MIN_TD   # the engine epoch (hgraph's minimum datetime)
IN_MEMORY = _hgraph.IN_MEMORY
DATA_FRAME = _hgraph.DATA_FRAME
frame_store_contains = _hgraph.frame_store_contains
frame_store_read = _hgraph.frame_store_read

TimeSeries = _hgraph.TimeSeries
_hgraph._set_removed_sentinel(REMOVED)
_hgraph._set_cmp_result_enum(CmpResult)
_hgraph._set_divide_by_zero_enum(DivideByZero)

from ._runtime import _Combine as _CombineClass
combine = _CombineClass()

from ._types import Frame, TABLE, COMPOUND_SCALAR, compound_scalar
from ._table import (ToTableMode, TableSchema, make_table_schema, table_schema,
                     get_table_schema_date_key, get_table_schema_as_of_key)

REMOVE = REMOVED           # hgraph's TSD key-removal sentinel
from ._runtime import Removed

_OPERATOR_NAMES = frozenset(_hgraph.operator_names())


def __getattr__(name):
    if name in _OPERATOR_NAMES:
        fn = operator_function(name)
        globals()[name] = fn  # cache
        return fn
    if name == "WindowResult":
        from ._compat import _window_result
        globals()[name] = _window_result()  # lazy: its annotations subscript TS[...]
        return globals()[name]
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
    "GlobalContext", "GlobalState", "set_as_of", "set_table_schema_date_key", "set_table_schema_as_of_key",
    "set_record_replay_config", "frame_store_contains", "frame_store_read", "evaluate_const",
    "Frame", "TABLE", "COMPOUND_SCALAR", "ToTableMode", "TableSchema", "make_table_schema", "table_schema",
    "get_table_schema_date_key", "get_table_schema_as_of_key",
]

from ._runtime import set_delta, compute_set_delta
from ._types import K_1
default_path = ""   # hgraph's default service path sentinel
