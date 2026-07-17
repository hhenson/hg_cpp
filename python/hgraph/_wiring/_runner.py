"""Graph execution entry points: run_graph/evaluate_graph and the
eval_node test harness."""
import inspect
import logging

import _hgraph

from .._types import (_GenericTsExpr, _TsExpr, _TypeVarSentinel,
                      _type_var_is_scalar)
from ._core import (WiringPort, _OperatorFunction, _unwrap, _wiring_stack,
                    wire)
from ._graph import _GraphFn
from ._node import _PyNode
from ._sentinels import _simplify_delta
from ._state import GlobalState, _GRAPH_LOGGER_KEY

class EvaluationMode:
    SIMULATION = "simulation"
    REAL_TIME = "real_time"


class GraphConfiguration:
    """Configuration for one native graph execution.

    The public shape matches Python hgraph. Options which do not yet have a
    native implementation are retained and rejected when the configuration is
    used; they are never silently ignored.
    """

    def __init__(
            self,
            run_mode=EvaluationMode.SIMULATION,
            start_time=None,
            end_time=None,
            trace=False,
            profile=False,
            life_cycle_observers=(),
            trace_wiring=False,
            wiring_observers=(),
            graph_logger=None,
            trace_back_depth=1,
            capture_values=False,
            default_log_level=logging.DEBUG,
            logger_formatter=None,
            cleanup_on_error=True):
        self.run_mode = run_mode
        self.start_time = start_time
        self.end_time = end_time
        self.trace = trace
        self.profile = profile
        self.life_cycle_observers = tuple(life_cycle_observers)
        self.trace_wiring = trace_wiring
        self.wiring_observers = tuple(wiring_observers)
        self.graph_logger = graph_logger or logging.getLogger("hgraph")
        self.trace_back_depth = trace_back_depth
        self.capture_values = capture_values
        self.default_log_level = default_log_level
        self.logger_formatter = logger_formatter
        self.cleanup_on_error = cleanup_on_error

    def _validate(self):
        if self.run_mode not in (EvaluationMode.SIMULATION, EvaluationMode.REAL_TIME):
            raise ValueError(
                "GraphConfiguration.run_mode must be EvaluationMode.SIMULATION or "
                "EvaluationMode.REAL_TIME")
        unsupported = []
        for name in ("trace", "profile", "trace_wiring", "capture_values"):
            if getattr(self, name):
                unsupported.append(name)
        for name in ("life_cycle_observers", "wiring_observers"):
            if getattr(self, name):
                unsupported.append(name)
        if self.trace_back_depth != 1:
            unsupported.append("trace_back_depth")
        if self.logger_formatter is not None:
            unsupported.append("logger_formatter")
        if not self.cleanup_on_error:
            unsupported.append("cleanup_on_error=False")
        if unsupported:
            rendered = ", ".join(unsupported)
            raise NotImplementedError(
                f"the C++ graph runner does not yet support: {rendered}")
        if not hasattr(self.graph_logger, "setLevel"):
            raise TypeError("GraphConfiguration.graph_logger must be a logging.Logger-like object")


def evaluate_graph(fn, config=None, *args, **kwargs):
    """Run ``fn`` under a :class:`GraphConfiguration`."""
    return _evaluate_graph(fn, config or GraphConfiguration(), args, kwargs)

def _times_for(values, start_time):
    return [
        (start_time + i * _hgraph.MIN_TD, v)
        for i, v in enumerate(values)
        if v is not None
    ]


def _times_for_sparse(values):
    return [
        (_hgraph.MIN_ST + offset * _hgraph.MIN_TD, value)
        for offset, value in values
    ]


def run_graph(
        graph_fn,
        *args,
        run_mode=EvaluationMode.SIMULATION,
        start_time=None,
        end_time=None,
        print_progress=True,
        life_cycle_observers=None,
        __trace__=False,
        __profile__=False,
        __trace_wiring__=False,
        __logger__=None,
        __trace_back_depth__=1,
        __capture_values__=False,
        **kwargs):
    """Wire and evaluate ``graph_fn`` in simulation. Returns hgraph's
    evaluate_graph shape - [(time, value), ...] of the graph output ticks -
    or None for sink graphs. ``end_time`` bounds the run (REQUIRED for
    self-perpetuating graphs, e.g. bound feedback loops). NOTE
    (divergence): the simulation clock is cycle-aligned from MIN_ST in
    MIN_TD steps."""
    del print_progress  # progress rendering is a presentation concern
    config = GraphConfiguration(
        run_mode=run_mode,
        start_time=start_time,
        end_time=end_time,
        trace=__trace__,
        profile=__profile__,
        life_cycle_observers=life_cycle_observers or (),
        trace_wiring=__trace_wiring__,
        graph_logger=__logger__,
        trace_back_depth=__trace_back_depth__,
        capture_values=__capture_values__,
    )
    return _evaluate_graph(graph_fn, config, args, kwargs)


def _evaluate_graph(graph_fn, config, args, kwargs):
    config._validate()
    state = GlobalState.instance()
    missing = object()
    previous_logger = state.get(_GRAPH_LOGGER_KEY, missing)
    state[_GRAPH_LOGGER_KEY] = config.graph_logger
    config.graph_logger.setLevel(config.default_log_level)
    w = _hgraph.Wiring(state._impl)
    _wiring_stack.append(w)
    try:
        out = graph_fn(*args, **kwargs)
        if out is not None:
            w.wire(
                "__harness_record",
                (_unwrap(out).dereferenced, "__run_graph__"),
                {"sparse": True},
            )
        run = w.run(start_time=config.start_time, end_time=config.end_time,
                    realtime=config.run_mode == EvaluationMode.REAL_TIME)
    finally:
        w._release_seed_context()
        _wiring_stack.pop()
        if previous_logger is missing:
            state.pop(_GRAPH_LOGGER_KEY, None)
        else:
            state[_GRAPH_LOGGER_KEY] = previous_logger
    if out is None:
        return None
    return _times_for_sparse(run.recorded("__run_graph__", sparse=True))


def _infer_ts_type(series):
    """The TS type for an eval_node input vector, from its first non-None
    sample (hgraph's operators are driven without annotations)."""
    from .._types import TS, TSS, TSD

    for sample in series:
        if sample is None:
            continue
        # UN-ANNOTATED inference treats container samples as SCALAR values
        # (hgraph's operator-test convention). TSS/TSD delta semantics apply
        # only through annotated parameters.
        if isinstance(sample, (set, frozenset)):
            for element in sample:
                return TS[frozenset[type(element)]]
            continue
        if isinstance(sample, tuple):
            for element in sample:
                return TS[tuple[type(element), ...]]
            continue
        if isinstance(sample, dict):
            for key_sample, value_sample in sample.items():
                return TS[dict[type(key_sample), type(value_sample)]]
            continue
        return TS[type(sample)]
    return None


def eval_node(fn, *inputs, output_type=None, resolution_dict=None,
              __trace__=False, __trace_wiring__=False, __observers__=None,
              __start_time__=None, __end_time__=None, __scalars__=None,
              __elide__=False, **kwargs):
    """Drive a @graph/composition ``fn`` with vectors of per-cycle values
    (None = no tick), mirroring hgraph's eval_node test util. Time-series
    input types come from ``fn``'s annotations. The run is unbounded by
    default (MAX_ET, as always) - a graph ends when nothing remains
    scheduled. ``__end_time__`` (Python-hgraph parity) bounds a run
    explicitly; a test that cannot quiesce (e.g. a bound feedback loop
    until per-edge passive support lands) must set it and say why."""
    unsupported = []
    if __trace__:
        unsupported.append("__trace__")
    if __trace_wiring__:
        unsupported.append("__trace_wiring__")
    if __observers__:
        unsupported.append("__observers__")
    if unsupported:
        raise NotImplementedError(
            "the C++ eval_node harness does not yet support: " +
            ", ".join(unsupported))
    try:
        fn_sig = inspect.signature(fn.fn if isinstance(fn, _GraphFn) else fn)
        params = list(fn_sig.parameters.values())
    except (TypeError, ValueError):
        params = []
    params = [p for p in params
              if p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)]
    param_names = {p.name for p in params}
    annotations_by_name = {p.name: p.annotation for p in params}

    pinned_scope = None
    if isinstance(fn, _PyNode) and fn._pins:
        from .._types import (
            _GenericTsExpr as _PinnedGenericTsExpr,
            _TsExpr as _PinnedTsExpr,
            _pattern_of,
        )

        pinned_scope = _hgraph.ResolutionScope()
        for name, resolved in fn._pins.items():
            fn._bind_resolved(pinned_scope, name, resolved)

        for name, annotation in tuple(annotations_by_name.items()):
            if not isinstance(annotation, _PinnedGenericTsExpr):
                continue
            resolved = pinned_scope.resolve_ts(_pattern_of(annotation))
            if resolved is not None:
                # eval_node replays ordinary producer values. REF-transparent
                # input binding performs the reference adaptation at wiring.
                resolved = _hgraph.ref_target(resolved)
                annotations_by_name[name] = _PinnedTsExpr(resolved, repr(annotation))

    def _named_series_value(k, v):
        """A list/tuple kwarg is a per-cycle SERIES only on a time-series
        parameter: SCALAR-kind type variables (upstream's HgScalarTypeVar)
        and plain scalar annotations take the value as-is."""
        if k not in param_names or not isinstance(v, (list, tuple)):
            return False
        annotation = annotations_by_name.get(k)
        if isinstance(annotation, _TypeVarSentinel) and _type_var_is_scalar(annotation):
            return False
        if isinstance(v, tuple) and not isinstance(
                annotation, (_TsExpr, _GenericTsExpr, _TypeVarSentinel)):
            return False   # a tuple on a scalar-annotated param is a value
        return True

    named_series = {k: v for k, v in kwargs.items() if _named_series_value(k, v)}
    for k in named_series:
        kwargs.pop(k)
    if not named_series and kwargs and params:
        # A non-list kwarg naming a TS-annotated param is a plain value:
        # promote it to its positional slot so the const-lift rule applies
        # (scalar-annotated params pass through unchanged either way).
        from .._types import _TsExpr as _TsE
        promoted = {k: v for k, v in kwargs.items()
                    if k in param_names and isinstance(
                        dict((p.name, p.annotation) for p in params).get(k), _TsE)}
        if promoted:
            named_series = {k: v for k, v in promoted.items()}
            for k in named_series:
                kwargs.pop(k)
            by_name = {p.name: i for i, p in enumerate(params)}
            extended = list(inputs) + [None] * (max(by_name[k] for k in named_series) + 1 - len(inputs))
            for k, value in named_series.items():
                extended[by_name[k]] = value
            return eval_node(fn, *extended, output_type=output_type, resolution_dict=resolution_dict,
                             __trace__=__trace__, __trace_wiring__=__trace_wiring__,
                             __observers__=__observers__,
                             __start_time__=__start_time__, __end_time__=__end_time__,
                             __scalars__=__scalars__, __elide__=__elide__, **kwargs)
        named_series = {}
    if named_series:
        by_name = {p.name: i for i, p in enumerate(params)}
        extended = list(inputs) + [None] * (max(by_name[k] for k in named_series) + 1 - len(inputs))
        for k, series in named_series.items():
            extended[by_name[k]] = series
        # Scalar-supplied kwargs whose position got padded move into the
        # positional slot; otherwise None would be wired over the kwarg.
        for k in list(kwargs):
            index = by_name.get(k)
            if index is not None and index < len(extended) and extended[index] is None:
                extended[index] = kwargs.pop(k)
        return eval_node(fn, *extended, output_type=output_type, resolution_dict=resolution_dict,
                         __trace__=__trace__, __trace_wiring__=__trace_wiring__,
                         __observers__=__observers__,
                         __start_time__=__start_time__, __end_time__=__end_time__,
                         __scalars__=__scalars__, __elide__=__elide__, **kwargs)
    w = _hgraph.Wiring(GlobalState.instance()._impl)
    _wiring_stack.append(w)
    try:
        def _producer_annotation(annotation):
            """eval_node samples are values of the producer behind REF[T]."""
            if isinstance(annotation, _TsExpr) and annotation.handle.is_ref:
                return _TsExpr(_hgraph.ref_target(annotation.handle), repr(annotation))
            return annotation

        ports = []
        deferred_replay = []
        scalar_positions = set()
        for i, series in enumerate(inputs):
            annotation = annotations_by_name.get(params[i].name) if i < len(params) else None
            if annotation is None and getattr(fn, "_ts_hint", None) is not None:
                hints = fn._ts_hint       # op[TYPEVAR: TYPE, ...] types the inputs
                if not isinstance(hints, list):
                    hints = [hints]
                annotation = hints[i] if i < len(hints) else hints[-1]
                from .._types import TS as _TS, _TsExpr as _TsE, _GenericTsExpr as _GTsE
                if not isinstance(annotation, (_TsE, _GTsE)) and isinstance(series, (list, tuple)):
                    # op[SCALAR: int] resolves the ELEMENT type - a LIST input
                    # at this position is a TS[int] series (a non-list value
                    # stays a scalar argument).
                    annotation = _TS[annotation]
            if resolution_dict and i < len(params) and params[i].name in resolution_dict:
                annotation = resolution_dict[params[i].name]
            elif (resolution_dict and not params
                  and (len(resolution_dict) == len(inputs)
                       or (isinstance(fn, _OperatorFunction)
                           and fn.__name__ == "getitem_"
                           and i < len(resolution_dict)))):
                # OPERATOR functions have no python signature: when the dict
                # has one entry per positional input, its entries type them
                # in order (keys are the operator's own parameter names).
                # getitem_ is the supported partial form: ``ts`` types the
                # first input while the scalar index retains value inference.
                # Other shorter dictionaries may describe a variadic
                # collection (for example merge's single ``tsl`` entry), so
                # they deliberately retain per-input inference.
                annotation = list(resolution_dict.values())[i]
            from .._types import _GenericTsExpr
            if isinstance(annotation, _GenericTsExpr):
                samples = series if isinstance(series, (list, tuple)) else [series]
                annotation = _infer_ts_type(samples)
            annotation = _producer_annotation(annotation)
            import types as _pytypes
            import typing as _typing
            scalar_annotation = (
                (isinstance(annotation, type) and annotation in (bool, int, float, str, bytes, tuple))
                or (isinstance(annotation, (_pytypes.GenericAlias, type(_typing.Tuple[int, ...])))
                    and _typing.get_origin(annotation) is tuple))
            if scalar_annotation:
                scalar_positions.add(i)
            if not isinstance(series, (list, tuple)) or scalar_annotation:
                # hgraph parity: a non-list argument is a plain value (lifted
                # to const where a TS input is expected, or a scalar param).
                # A SCALAR-annotated param keeps tuple values verbatim
                # (keys: tuple[str, ...] is not a series).
                if isinstance(annotation, _TsExpr):
                    src = w.wire("const", (series,), {}, output_type=annotation.handle)
                    ports.append(WiringPort(src))
                    continue
                ports.append(series)
                continue
            if not isinstance(annotation, _TsExpr):
                annotation = _infer_ts_type(series)
                if annotation is None:
                    name = params[i].name if i < len(params) else f"arg_{i}"
                    raise TypeError(f"parameter '{name}' needs a TS[...] annotation or a typed sample value")
            key = f"eval_node::{params[i].name if i < len(params) else i}"
            src = w.wire("__harness_replay", (key,), {}, output_type=annotation.handle)
            deferred_replay.append((key, list(series), annotation.handle))
            ports.append(WiringPort(src))
        scalars = dict(__scalars__ or {})
        # hgraph parity: keyword arguments naming the function's parameters
        # are INPUT SERIES (eval_node(g, a=[...], b=[...])); the rest flow to
        # the node as scalars.
        if not params:
            # OPERATOR functions have no python signature: named list kwargs
            # are input series wired as keyword ports.
            named_ports = {}
            for k in [k for k, v in kwargs.items() if isinstance(v, (list, tuple))]:
                series = kwargs.pop(k)
                annotation = None
                if resolution_dict and k in resolution_dict and isinstance(resolution_dict[k], _TsExpr):
                    annotation = resolution_dict[k]   # the dict types NAMED series too
                if annotation is None:
                    annotation = _infer_ts_type(series)
                if annotation is None:
                    raise TypeError(f"named series '{k}' needs typed sample values")
                annotation = _producer_annotation(annotation)
                key = f"eval_node::{k}"
                src = w.wire("__harness_replay", (key,), {}, output_type=annotation.handle)
                deferred_replay.append((key, list(series), annotation.handle))
                named_ports[k] = WiringPort(src)
                inputs = (*inputs, series)   # count toward the run length
            kwargs.update(named_ports)
        scalars.update(kwargs)   # hgraph parity: extra kwargs flow to the node
        out = fn(*ports, **scalars)
        # Replay values convert AFTER wiring: hgraph surfaces wiring errors
        # before data-conversion errors, and tests pin that order.
        for key, series, handle in deferred_replay:
            w.set_replay(key, series, ts_type=handle)
        length = max((len(series) for i, series in enumerate(inputs)
                      if isinstance(series, (list, tuple)) and i not in scalar_positions), default=0)
        if out is None:
            run = w.run(start_time=__start_time__, end_time=__end_time__)
            return None
        # hgraph parity: a REF graph output records its DEREFERENCED values.
        # A TSB with REF fields records a STRUCTURAL bundle of per-field
        # projections, each dereferenced.
        raw = _unwrap(out)
        record_kwargs = {"sparse": True} if __elide__ else {}
        record_port = raw.dereferenced
        if raw.ts_type.is_tsb and _hgraph.tsb_has_ref_fields(raw.ts_type):
            fields = {
                name: _unwrap(wire("getitem_", WiringPort(raw), name)).dereferenced
                for name, _ in _hgraph.ts_field_types(raw.ts_type)
            }
            record_port = _hgraph.tsb_port(record_port.ts_type, fields)
        elif raw.ts_type.is_fixed_tsl and _hgraph.tsl_element(raw, 0).ts_type.is_ref:
            # A TSL of REF elements: record a structural TSL of per-element
            # projections, each dereferenced.
            record_port = _hgraph.tsl_port(
                [_hgraph.tsl_element(raw, i).dereferenced for i in range(raw.ts_type.fixed_size)]
            )
        w.wire("__harness_record", (record_port, "eval_node::out"), record_kwargs)
        run = w.run(start_time=__start_time__, end_time=__end_time__)
    finally:
        w._release_seed_context()
        _wiring_stack.pop()
    if __elide__:
        # hgraph parity: elide keeps only the ticked cycles, in order (the
        # recording was made SPARSE, so this is just the list).
        return [_simplify_delta(v) for _, v in run.recorded("eval_node::out", sparse=True)]
    recorded = [None if v is None else _simplify_delta(v) for v in run.recorded("eval_node::out")]
    recorded += [None] * (length - len(recorded))
    if not any(v is not None for v in recorded):
        return None   # hgraph parity: a never-ticking output reports None
    return recorded
