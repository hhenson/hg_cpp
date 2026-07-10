"""Wiring context, ports, decorators and runners over the _hgraph bridge.

The API mirrors Python hgraph's: @graph composes, run_graph evaluates,
eval_node drives a node/graph from vectors of values. Wiring state is a
module-level stack (the runtime is single-threaded by design)."""
import inspect
import threading

import _hgraph

from ._types import _TsExpr, _ContextExpr, _Required, _GenericTsExpr

_wiring_stack = []


def _current_wiring():
    if not _wiring_stack:
        raise RuntimeError("no active wiring: wire inside a @graph run by run_graph/eval_node")
    return _wiring_stack[-1]


class WiringPort:
    """A time-series edge source; supports hgraph's operator sugar."""

    __slots__ = ("_port",)

    def __init__(self, port):
        self._port = port


def _unwrap(value):
    if isinstance(value, WiringPort):
        return value._port
    if isinstance(value, _TsExpr):
        return value.handle
    return value


def wire(name, *args, __output_type__=None, **kwargs):
    """Wire operator ``name`` by registry resolution (the erased contract)."""
    out_type = kwargs.pop("tp", None) or kwargs.pop("output_type", None) or __output_type__
    if out_type is not None:
        if isinstance(out_type, type):
            from ._types import TS as _TS

            out_type = _TS[out_type]   # json_encode[str] names the SCALAR
        out_type = out_type.handle if isinstance(out_type, _TsExpr) else out_type
    w = _current_wiring()
    sizes = kwargs.pop("__sizes__", None)
    unwrapped = tuple(_unwrap(a) for a in args)
    unwrapped_kw = {k: _unwrap(v) for k, v in kwargs.items()}
    try:
        if sizes is not None:
            result = w.wire(name, unwrapped, unwrapped_kw, output_type=out_type, sizes=sizes)
        else:
            result = w.wire(name, unwrapped, unwrapped_kw, output_type=out_type)
    except (RuntimeError, ValueError) as error:
        # std::invalid_argument surfaces as ValueError; both are wiring-time.
        raise WiringError(str(error)) from error
    return WiringPort(result) if result is not None else None


class _OperatorFunction:
    def __rshift__(self, other):
        # arrow-chain sugar: (eval_ | op >> assert_) - defer to the arrow
        # module's chain so operators compose in pipelines.
        from .arrow import _Assert, _Chain

        if isinstance(other, _Assert):
            return _Chain([self], other)
        return _Chain([self]) >> other

    """A Python callable wiring the named registered operator. Supports
    hgraph's SUBSCRIPT form ``op[TYPE](...)`` - the type becomes the
    requested output type of the call."""

    __slots__ = ("__name__", "__qualname__", "_output_type", "_sizes", "_ts_hint")

    def __init__(self, name, output_type=None, sizes=None, ts_hint=None):
        self.__name__ = name
        self.__qualname__ = name
        self._output_type = output_type
        self._sizes = sizes
        self._ts_hint = ts_hint

    def __call__(self, *args, **kwargs):
        if self._output_type is not None and "tp" not in kwargs and "output_type" not in kwargs:
            kwargs["output_type"] = self._output_type
        if self._sizes is not None:
            kwargs.setdefault("__sizes__", self._sizes)
        args, kwargs = self._normalise_type_arguments(args, kwargs)
        return wire(self.__name__, *args, **kwargs)

    def __getitem__(self, item):
        # hgraph's ``op[TYPEVAR: TYPE]`` pre-resolution subscripts arrive as
        # SLICES. ``op[OUT: X]`` names the requested OUTPUT type; other
        # typevar slices are dropped (resolution happens from the wired
        # inputs). A plain type subscript is the requested output type.
        from ._types import OUT

        output_type = None
        sizes = []
        ts_hints = []
        for i in (item if isinstance(item, tuple) else (item,)):
            if isinstance(i, slice):
                if i.start is OUT and output_type is None:
                    output_type = i.stop
                elif isinstance(i.stop, int):
                    sizes.append(i.stop)   # op[SIZE: Size[4]] pins size vars
                else:
                    # op[TYPEVAR: TYPE, ...]: the resolutions also type the
                    # eval_node input series positionally.
                    ts_hints.append(i.stop)
                continue
            if output_type is None:
                output_type = i
        return _OperatorFunction(self.__name__, output_type=output_type, sizes=sizes or None,
                                 ts_hint=ts_hints or None)

    def _normalise_type_arguments(self, args, kwargs):
        if "tp" in kwargs or "output_type" in kwargs:
            return args, kwargs
        if self.__name__ == "nothing" and len(args) == 1 and isinstance(args[0], _TsExpr):
            kwargs = dict(kwargs)
            kwargs["output_type"] = args[0]
            return (), kwargs
        if self.__name__ == "const" and len(args) >= 2 and isinstance(args[1], _TsExpr):
            kwargs = dict(kwargs)
            kwargs["output_type"] = args[1]
            return (args[0], *args[2:]), kwargs
        return args, kwargs

    def __repr__(self):
        return f"<operator {self.__name__}>"


def operator_function(name):
    return _OperatorFunction(name)


# --- hgraph's WiringPort operator sugar (ext/main _operators pattern) ---
_DUNDERS = {
    "__add__": "add_", "__sub__": "sub_", "__mul__": "mul_", "__truediv__": "div_",
    "__floordiv__": "floordiv_", "__mod__": "mod_", "__divmod__": "divmod_",
    "__pow__": "pow_", "__lshift__": "lshift_", "__rshift__": "rshift_",
    "__and__": "bit_and", "__or__": "bit_or", "__xor__": "bit_xor",
    "__eq__": "eq_", "__ne__": "ne_", "__lt__": "lt_", "__le__": "le_",
    "__gt__": "gt_", "__ge__": "ge_",
}
for dunder, op_name in _DUNDERS.items():
    setattr(WiringPort, dunder, (lambda op: lambda x, y: wire(op, x, y))(op_name))
for dunder, op_name in {
    "__radd__": "add_", "__rsub__": "sub_", "__rmul__": "mul_", "__rtruediv__": "div_",
    "__rfloordiv__": "floordiv_", "__rmod__": "mod_", "__rpow__": "pow_",
}.items():
    setattr(WiringPort, dunder, (lambda op: lambda x, y: wire(op, y, x))(op_name))
for dunder, op_name in {
    "__neg__": "neg_", "__pos__": "pos_", "__abs__": "abs_", "__invert__": "invert_",
}.items():
    setattr(WiringPort, dunder, (lambda op: lambda x: wire(op, x))(op_name))
def _port_getitem(self, item):
    # Fixed-TSL integer indexing is the STRUCTURAL element projection
    # (zero-copy, no node); a REF[TSL] port dereferences first (the input
    # binding inserts the from-REF adaptation); everything else dispatches
    # getitem_.
    if isinstance(item, int) and not isinstance(item, bool):
        raw = _unwrap(self)
        if raw.ts_type.is_fixed_tsl:
            return WiringPort(_hgraph.tsl_element(raw, item))
    return wire("getitem_", self, item)


WiringPort.__getitem__ = _port_getitem
WiringPort.__hash__ = object.__hash__  # __eq__ wires a node; identity hashing stands


def _port_len(self):
    ts_type = self._port.ts_type
    if ts_type.is_fixed_tsl:
        return ts_type.fixed_size
    raise TypeError("len() is only defined for fixed-size TSL ports")


def _port_iter(self):
    # The sequence protocol for fixed TSLs (`*tsl` unpacking, hgraph
    # parity). Without __len__/__iter__, python's fallback iteration via
    # __getitem__ would wire getitem_ nodes FOREVER.
    return (self[i] for i in range(len(self)))


WiringPort.__len__ = _port_len
WiringPort.__iter__ = _port_iter


def _port_getattr(self, name):
    if name.startswith("_"):
        raise AttributeError(name)
    if name == "key_set":
        return wire("keys_", self)   # hgraph's TSD.key_set property
    # JSON leaf coercions: j["a"].int / .float / .str / .bool.
    if name in ("int", "float", "str", "bool") and _unwrap(self).ts_type.is_ts_json:
        return wire("json_as_" + name, self)
    try:
        return wire("getattr_", self, name)
    except WiringError:
        # hgraph attribute sugar: port.year / .month / .weekday ... resolve
        # as unary operators when no bundle field matches.
        if name in _hgraph.operator_names():
            return wire(name, self)
        raise


def _port_reduce(self, fn, zero=None):
    if zero is not None:
        return reduce(fn, self, zero)
    return reduce(fn, self)


WiringPort.reduce = _port_reduce
WiringPort.__getattr__ = _port_getattr


def _wrap_graph_fn(gfn):
    """Erase a Python @graph function into a WiredFn: the wrapper runs the
    function against whatever Wiring the C++ side supplies (inline OR a
    fresh child during sub-graph compilation), pushing it as the active
    wiring context. Identity is the user function object."""
    user_fn = gfn.fn if isinstance(gfn, _GraphFn) else gfn
    # Introspect the REAL signature (unwrap @compute_node/@graph wrappers);
    # injectable/context parameters are not wired inputs.
    sig = inspect.signature(getattr(user_fn, "fn", user_fn))
    names = [
        p.name for p in sig.parameters.values()
        if p.annotation not in _INJECTABLE_MARKERS and not isinstance(p.annotation, _ContextExpr)
    ]
    # Only an explicit ``-> None`` marks a sink: un-annotated callables
    # (lambdas as reduce/map combiners) are assumed to produce a value.
    has_output = sig.return_annotation is not None

    def wrapper(borrowed_wiring, ports):
        _wiring_stack.append(borrowed_wiring)
        try:
            out = user_fn(*(WiringPort(p) for p in ports))
            if out is None:
                return None
            raw = _unwrap(out)
            if raw.is_structural:
                # Child sub-graph outputs must be real NODE outputs. A
                # structural port with REFERENCE-valued fields materializes
                # as a REFERENCE output (hgraph's combine-of-refs shape -
                # zero copy); plain fields copy through the canonical-delta
                # identity node.
                if _hgraph.structural_has_ref_children(raw):
                    raw = _hgraph.ref_port(borrowed_wiring, raw)
                else:
                    raw = _unwrap(wire("__materialize", out))
            return raw
        finally:
            _wiring_stack.pop()

    out_tp = sig.return_annotation
    out_handle = out_tp.handle if isinstance(out_tp, _TsExpr) else None
    return _hgraph.graph_fn(wrapper, user_fn, names, has_output, output_type=out_handle)


def _as_wired(func):
    """Accept an operator name, an operator_function, a @graph-decorated
    function, a @compute_node, a WiredFn handle - or a bare LAMBDA (the
    anonymous convenience). Plain named functions must be tagged @graph."""
    if isinstance(func, _hgraph.WiredFn):
        return func
    if isinstance(func, (_GraphFn, _PyNode)):
        return _wrap_graph_fn(func)
    if callable(func) and not isinstance(func, str):
        name = getattr(func, "__name__", None)
        if name is not None and name in _hgraph.operator_names():
            return _hgraph.wired_op(name)
        if name == "<lambda>":
            return _wrap_graph_fn(func)
        raise TypeError(
            f"'{name}' must be decorated with @graph (or @compute_node) to be used as a wired "
            "function; bare lambdas are also accepted")
    return _hgraph.wired_op(func)


def map_(func, *args, **kwargs):
    """hgraph's map_. ``func`` may be a registered operator name or the
    module-level operator function (Python-defined @graph callables cannot
    compile as C++ sub-graphs yet - a recorded divergence)."""
    return wire("map_", _as_wired(func), *args, **kwargs)


def mesh_(func, *args, **kwargs):
    """hgraph's mesh_: map_ over a TSD whose per-key instances may read each
    other's outputs via ``mesh_ref(key)`` inside the mesh function, creating
    instances on demand and evaluating in dependency order."""
    return wire("mesh_", _as_wired(func), *args, **kwargs)


def mesh_ref(key, name=""):
    """Cross-instance access inside a mesh function: the sibling instance's
    output for ``key`` (pausing until that sibling has produced it)."""
    return WiringPort(_hgraph.mesh_ref(_current_wiring(), _unwrap(key), name))


def reduce(func, ts, zero=None, **kwargs):
    """hgraph's reduce over a collection with an operator callable."""
    if zero is None:
        return wire("reduce", _as_wired(func), ts, **kwargs)
    return wire("reduce", _as_wired(func), ts, zero, **kwargs)


class Feedback:
    """hgraph's feedback: ``fb = feedback(TS[int])``; ``fb(port)`` binds the
    cycle-closing input; ``fb()`` reads the (next-cycle) source port."""

    __slots__ = ("_wiring", "_fb")

    def __init__(self, wiring, fb):
        self._wiring = wiring
        self._fb = fb

    def __call__(self, port=None):
        if port is None:
            return WiringPort(self._fb.port)
        self._wiring.feedback_bind(self._fb, _unwrap(port))
        return self


def passive(port):
    """hgraph's passive marker: the receiving node's input for THIS usage is
    removed from its active list (ticks no longer schedule the node; values
    still read normally). Returns a marked copy - the original port is
    unaffected."""
    return WiringPort(_hgraph.passive(_unwrap(port)))


def feedback(tp, initial=None):
    w = _current_wiring()
    return Feedback(w, w.feedback(_unwrap(tp), initial))


def combine(*args, __output_type__=None, **kwargs):
    """hgraph's combine: build a composite port. ``combine[TSB[S]](a=..., b=...)``
    (the subscript names the target type) builds a structural bundle;
    plain values const-lift at the field types. The UNSUBSCRIPTED binary
    form merges two CompoundScalar time-series (delta over orig)."""
    label0 = "" if __output_type__ is None else (
        getattr(__output_type__, "label", None) or getattr(__output_type__, "__name__", None) or
        repr(__output_type__)).replace("typing.", "")
    if label0.startswith("TSD") and args and isinstance(args[0], tuple):
        # combine[TSD](("a", "b"), a, b): keys tuple literal + element ports.
        strict = kwargs.pop("__strict__", True)
        return wire("combine_tsd", args[0], *args[1:], __strict__=strict, **kwargs)
    if (label0.startswith("TSD") and len(args) == 2 and all(isinstance(a, WiringPort) for a in args)
            and _unwrap(args[0]).ts_type.is_ts):
        # combine[TSD](k_tuple_ts, v_tuple_ts): zip two TS[tuple] -> TSD.
        k = _hgraph.vt_element(_hgraph.ts_value_vt(_unwrap(args[0]).ts_type))
        v = _hgraph.vt_element(_hgraph.ts_value_vt(_unwrap(args[1]).ts_type))
        return wire("convert", *args, output_type=_TsExprFor(_hgraph.tsd(k, _hgraph.ts(v))))
    if label0.startswith("TSS") and args and all(isinstance(a, WiringPort) for a in args):
        # combine[TSS](a, b, ...): scalar ports -> TSS desired membership.
        element = _hgraph.ts_value_vt(_unwrap(args[0]).ts_type)
        return wire("combine", *args, output_type=_TsExprFor(_hgraph.tss(element)))
    label = "" if __output_type__ is None else (
        getattr(__output_type__, "label", None) or getattr(__output_type__, "__name__", None) or
        repr(__output_type__)).replace("typing.", "")
    if label.startswith("TS[Tuple") and args and all(isinstance(a, WiringPort) for a in args):
        # combine[TS[Tuple]](a, b, ...): a fixed tuple of the packed ports.
        strict = kwargs.pop("__strict__", True)
        fields = [(f"_{i}", _unwrap(a).ts_type) for i, a in enumerate(args)]
        tsb_type = _hgraph.un_named_tsb_type(fields)
        structural = WiringPort(_hgraph.tsb_port(tsb_type, {f"_{i}": _unwrap(a) for i, a in enumerate(args)}))
        vts = [_hgraph.ts_value_vt(_unwrap(a).ts_type) for a in args]
        target = _TsExprFor(_hgraph.ts(_hgraph.fixed_tuple_vt(vts)))
        if strict is False:
            return wire("combine", structural, __strict__=False, output_type=target)
        return wire("combine", structural, output_type=target)
    if __output_type__ is None:
        kwargs.pop("__strict__", None)   # structural composites need no gate
        if args and all(isinstance(a, WiringPort) for a in args) and not kwargs:
            # UNSUBSCRIPTED positional: a structural TSL of the ports, UNLESS
            # it is the binary CS-merge (two bundle-valued TS -> delta merge).
            if len(args) == 2 and all(_unwrap(a).ts_type.is_ts_bundle for a in args):
                return _combine_compound_scalars(*args)
            return WiringPort(_hgraph.tsl_port([_unwrap(a) for a in args]))
        if kwargs and not args and all(isinstance(v, WiringPort) for v in kwargs.values()):
            # hgraph's un-subscripted kwargs form: a structural un-named TSB.
            fields = [(k, _unwrap(v).ts_type) for k, v in kwargs.items()]
            tsb_type = _hgraph.un_named_tsb_type(fields)
            return WiringPort(_hgraph.tsb_port(tsb_type, {k: _unwrap(v) for k, v in kwargs.items()}))
        raise TypeError("combine requires a subscripted type: combine[TSB[Schema]](...)")
    return __output_type__.from_ts(*args, **kwargs)


class _Combine:
    """The combine callable: instance-level subscript (combine[TSB[S]])."""

    def __getitem__(self, item):
        def _build(*args, **kwargs):
            return combine(*args, __output_type__=item, **kwargs)

        return _build

    def __call__(self, *args, **kwargs):
        return combine(*args, **kwargs)


def filter_by(ts, expr, **kwargs):
    """hgraph's filter_by: keep TSD entries where expr(value, **kwargs) is
    true - map_ computes the per-key matches, the runtime node filters."""
    matches = map_(expr, ts, **kwargs)
    return wire("filter_tsd_by_matches", ts, matches)


def switch_(key, cases, *args, reload_on_ticked=False, **kwargs):
    """hgraph's switch_ - cases is {key_value: operator-name-or-WiredFn};
    a None key is the default branch."""
    from ._types import DEFAULT

    prepared = {}
    for case_key, branch in cases.items():
        if case_key is DEFAULT:
            case_key = None   # hgraph's DEFAULT marker = the default branch
        prepared[case_key] = branch if isinstance(branch, str) else _as_wired(branch)
    erased = _hgraph.switch_cases(prepared, reload=reload_on_ticked)
    return wire("switch_", key, erased, *args, **kwargs)


def _type_pattern_for_target(target):
    from ._types import _GenericTsExpr, TSD, TSB, TSL, TSS, TSW

    if isinstance(target, _GenericTsExpr):
        if target.pattern is None:
            raise WiringError(f"cannot resolve generic target {target!r}: no C++ type pattern is attached")
        return target.pattern
    if target is TSS:
        return _hgraph.type_pattern_tss()
    if target is TSD:
        return _hgraph.type_pattern_tsd()
    if target is TSL:
        return _hgraph.type_pattern_tsl()
    if target is TSB:
        return _hgraph.type_pattern_tsb()
    if target is TSW:
        return _hgraph.type_pattern_tsw()
    raise WiringError(f"unsupported generic target {target!r}")


def _resolve_requested_target(op_name, target, inputs, keys=None):
    from ._types import _TsExpr

    pattern = _type_pattern_for_target(target)
    unwrapped = tuple(_unwrap(p) for p in inputs)
    try:
        if op_name == "collect":
            handle = _hgraph.resolve_collect_target(pattern, unwrapped)
        else:
            handle = _hgraph.resolve_convert_target(pattern, unwrapped, keys)
    except (RuntimeError, ValueError) as error:
        raise WiringError(str(error)) from error
    return _TsExpr(handle, "inferred")


class _Convert:
    """hgraph's convert: ``convert[TO](ts)`` or ``convert(ts, to)``. The
    target may be GENERIC (bare TSD/TSS/TSB, unparameterized TS[Tuple] /
    TS[Set] / TS[Mapping]) - the full output type is inferred from the
    INPUT port at wiring, so the C++ overloads always see a bound __out__."""

    __name__ = "convert"

    def __init__(self, to=None):
        self._to = to

    def __getitem__(self, item):
        return _Convert(item)

    def __call__(self, ts=None, *ports, to=None, **kwargs):
        from ._types import _TsExpr, _GenericTsExpr

        # Collect the time-series inputs in call order. hgraph names them
        # ``key``/``ts`` for the multi-input converts; positionally the first
        # is ``ts`` (or the key), extras follow. ``ts`` is a real param so
        # eval_node/resolution_dict can type it.
        inputs = []
        if "key" in kwargs:
            inputs.append(kwargs.pop("key"))
        inputs.append(ts if ts is not None else kwargs.pop("ts", None))
        inputs = [p for p in inputs if p is not None] + list(ports)
        if to is None and inputs and isinstance(
                inputs[-1], (_TsExpr, _GenericTsExpr, type)) and not isinstance(inputs[-1], WiringPort):
            to = inputs.pop()   # hgraph's positional ``to`` type argument
        target = to if to is not None else self._to

        if target is None:
            raise WiringError("convert requires a target type")

        if not isinstance(target, _TsExpr):
            target = _resolve_requested_target("convert", target, inputs, keys=kwargs.get("keys"))

        if (len(inputs) == 1 and isinstance(inputs[0], WiringPort)
                and _unwrap(inputs[0]).ts_type == target.handle):
            return inputs[0]   # convert to the SAME type is a no-op (hgraph: j is i)
        return wire("convert", *inputs, output_type=target, **kwargs)

convert = _Convert()


def _TsExprFor(handle):
    from ._types import _TsExpr

    return _TsExpr(handle, "inferred")


class _Collect:
    """hgraph's collect: accumulate ticks into a growing collection.
    ``collect[TS[Set]](ts, reset=...)`` / ``collect[TSD](k, v, reset=...)``.
    Generic targets infer from the input ports."""

    __name__ = "collect"

    def __init__(self, to=None):
        self._to = to

    def __getitem__(self, item):
        return _Collect(item)

    def _infer(self, ports):
        from ._types import _TsExpr, TS

        target = self._to
        if isinstance(target, _TsExpr):
            return target
        if target is None:
            raise WiringError("collect requires a target type")
        return _resolve_requested_target("collect", target, ports)

    def __call__(self, *ports, reset=None, exclude=None, **kwargs):
        from ._types import TS

        target = self._infer(ports)
        if reset is None:
            reset = wire("nothing", output_type=TS[bool])
        if ports and _unwrap(ports[0]).ts_type.is_tsd:
            # collect over a TSD: reset AND exclude inputs (nothing-filled).
            if exclude is None:
                exclude = wire("nothing",
                               output_type=_TsExprFor(_hgraph.tss(_hgraph.tsd_key_vt(_unwrap(ports[0]).ts_type))))
            return wire("collect", *ports, reset=reset, exclude=exclude, output_type=target, **kwargs)
        if exclude is not None:
            return wire("collect", *ports, exclude=exclude, output_type=target, **kwargs)
        return wire("collect", *ports, reset=reset, output_type=target, **kwargs)


collect = _Collect()


class _Emit:
    """hgraph's emit: drain a collection/dict one element per cycle.
    ``emit(x)`` infers the output; ``emit[VALUE_TS](x)`` hints the per-key
    VALUE time-series type for a dict/mapping (the KeyValue value field -
    e.g. emit[TSL[TS[int], Size[2]]](tsd_of_tuples))."""

    __name__ = "emit"

    def __init__(self, value_ts=None):
        self._value_ts = value_ts

    def __getitem__(self, item):
        return _Emit(item)

    def __call__(self, ts, **kwargs):
        from ._types import _TsExpr

        if self._value_ts is None or not isinstance(self._value_ts, _TsExpr):
            return wire("emit", ts, **kwargs)
        # Build the KeyValue output {key: TS[K], value: <value_ts>} from the
        # dict/mapping key type and the hinted value TS.
        handle = _unwrap(ts).ts_type
        if handle.is_tsd:
            key_vt = _hgraph.tsd_key_vt(handle)
        else:                                       # TS[Mapping[K, V]]
            key_vt = _hgraph.vt_key(_hgraph.ts_value_vt(handle))
        fields = [("key", _hgraph.ts(key_vt)), ("value", self._value_ts.handle)]
        out = _TsExprFor(_hgraph.un_named_tsb_type(fields))
        return wire("emit", ts, output_type=out, **kwargs)


emit = _Emit()


_global_state_local = threading.local()
_GLOBAL_MISSING = object()


class GlobalState:
    """Python seed/result owner for the C++ graph GlobalState copy lifecycle."""

    def __init__(self, **values):
        self._impl = _hgraph._GlobalState()
        self._compat_context = None
        for key, value in values.items():
            self[key] = value

    def __len__(self):
        return len(self._impl)

    def __contains__(self, key):
        return key in self._impl

    def __getitem__(self, key):
        return self._impl[key]

    def __setitem__(self, key, value):
        self._impl[key] = value

    def __delitem__(self, key):
        del self._impl[key]

    def get(self, key, default=None):
        return self._impl.get(key, default)

    def keys(self):
        return self._impl.keys()

    def __iter__(self):
        return iter(self.keys())

    def __bool__(self):
        return bool(len(self))

    def setdefault(self, key, default=None):
        if key in self:
            return self[key]
        self[key] = default
        return default

    def pop(self, key, default=_GLOBAL_MISSING):
        if key in self:
            value = self[key]
            del self[key]
            return value
        if default is _GLOBAL_MISSING:
            raise KeyError(key)
        return default

    @staticmethod
    def has_instance():
        return getattr(_global_state_local, "state", None) is not None

    @staticmethod
    def instance():
        runtime_state = getattr(_global_state_local, "runtime_state", None)
        if runtime_state is not None:
            return runtime_state
        state = getattr(_global_state_local, "state", None)
        if state is None:
            state = GlobalState()
            _global_state_local.state = state
        return state

    def __enter__(self):
        if self._compat_context is not None:
            raise RuntimeError("GlobalState is already active")
        self._compat_context = GlobalContext(self)
        return self._compat_context.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        context, self._compat_context = self._compat_context, None
        return context.__exit__(exc_type, exc_value, traceback)


class GlobalContext:
    """Select one Python GlobalState seed for wiring and result copy-back."""

    def __init__(self, state=None):
        self.state = state if state is not None else GlobalState.instance()
        if not isinstance(self.state, GlobalState):
            raise TypeError("GlobalContext state must be a GlobalState")
        self._previous = None
        self._entered = False

    def __enter__(self):
        if self._entered or getattr(_global_state_local, "context_active", False):
            raise RuntimeError("GlobalContext does not support nested activation")
        self._previous = getattr(_global_state_local, "state", None)
        _global_state_local.state = self.state
        _global_state_local.context_active = True
        self._entered = True
        return self.state

    def __exit__(self, exc_type, exc_value, traceback):
        if self._entered:
            _global_state_local.context_active = False
            if self._previous is None:
                del _global_state_local.state
            else:
                _global_state_local.state = self._previous
            self._previous = None
            self._entered = False
        return False


def _push_runtime_global_state(state):
    if getattr(_global_state_local, "runtime_state", None) is not None:
        raise RuntimeError("a runtime GlobalState is already active on this thread")
    _global_state_local.runtime_state = state


def _pop_runtime_global_state():
    if getattr(_global_state_local, "runtime_state", None) is not None:
        del _global_state_local.runtime_state


def set_record_replay_config(model):
    _hgraph._set_record_replay_config(GlobalState.instance()._impl, model)


def set_as_of(value):
    _hgraph._set_as_of(GlobalState.instance()._impl, value)


def set_table_schema_date_key(key):
    _hgraph._set_table_schema_date_key(GlobalState.instance()._impl, key)


def set_table_schema_as_of_key(key):
    _hgraph._set_table_schema_as_of_key(GlobalState.instance()._impl, key)


def evaluate_const(name, args=(), kwargs=None, output_type=None):
    return _hgraph._evaluate_const(GlobalState.instance()._impl, name, args, kwargs or {}, output_type)


def cast_(tp, ts):
    """hgraph's cast_: reinterpret/convert ``ts`` to type ``tp`` (a python
    scalar type). Numeric casts route through the convert kernels."""
    from ._types import TS

    return convert(ts, to=TS[tp])


class WiringError(RuntimeError):
    """A wiring-time error (hgraph parity)."""


_published_contexts = []   # [(port, ts_type_handle, frame)] most recent last


def _port_enter(self):
    import sys

    frame = sys._getframe(1)
    _published_contexts.append((self, self._port.ts_type, frame))
    return self


def _port_exit(self, *exc):
    _published_contexts.pop()
    return False


WiringPort.__enter__ = _port_enter
WiringPort.__exit__ = _port_exit


def _context_name_of(port, frame):
    """The `as` variable name: the frame local bound to this port."""
    for var_name, value in frame.f_locals.items():
        if value is port:
            return var_name
    return None


def _resolve_context(ctx_expr, name=None):
    """The most recent published context matching type (and name)."""
    wanted = ctx_expr.ts.handle
    for port, ts_type, frame in reversed(_published_contexts):
        if ts_type != wanted:
            continue
        if name is not None and _context_name_of(port, frame) != name:
            continue
        return port
    return None


class STATE:
    """Injectable: a per-node mutable namespace (attribute access), created
    lazily and preserved across ticks. Annotate a parameter with STATE."""


class SCHEDULER:
    """Injectable: the node scheduler - .schedule(datetime) /
    .schedule_delta(timedelta). Annotate a parameter with SCHEDULER."""


class CLOCK:
    """Injectable: the evaluation clock - .evaluation_time. Annotate a
    parameter with CLOCK."""


_INJECTABLE_MARKERS = {STATE: "S", CLOCK: "c", SCHEDULER: "d", GlobalState: "g"}


_MISSING = object()


class _PyNode:
    """@compute_node / @sink_node: a Python function as a runtime node. The
    function runs on the graph thread (both modes) under the GIL, receives
    time-series inputs as plain Python VALUES and wiring-time scalars as
    supplied; STATE/CLOCK/SCHEDULER-annotated parameters are injected. A
    compute node's return value ticks its output (None = no tick). It must
    have no side effects beyond its output."""

    def __init__(self, fn, has_output, active=None, valid=None):
        self.fn = fn
        self.has_output = has_output
        self._active = self._policy_names("active", active)
        self._valid = self._policy_names("valid", valid)
        self._start_fn = None
        self._stop_fn = None
        self.__name__ = fn.__name__
        sig = inspect.signature(fn)
        self._signature = sig
        # Callers introspecting the NODE (eval_node's input typing) must see
        # the user function's signature, not _PyNode.__call__'s.
        self.__signature__ = sig
        self._out_tp = sig.return_annotation if has_output else None
        self._params = list(sig.parameters.values())
        ts_names = {
            param.name
            for param in self._params
            if isinstance(param.annotation, (_TsExpr, _ContextExpr))
        }
        for policy, names in (("active", self._active), ("valid", self._valid)):
            if names is None:
                continue
            unknown = names - ts_names
            if unknown:
                rendered = ", ".join(sorted(unknown))
                raise TypeError(f"{self.__name__}: {policy}= contains non-time-series input(s): {rendered}")
        # Injectable parameters MUST default to None (hgraph convention):
        # the default guarantees user code in a graph never supplies them.
        for param in self._params:
            if param.annotation in _INJECTABLE_MARKERS and param.default is not None:
                raise TypeError(
                    f"injectable parameter '{param.name}' of '{self.__name__}' must default to None"
                )

    @staticmethod
    def _policy_names(policy, names):
        if names is None:
            return None
        if isinstance(names, str):
            raise TypeError(f"{policy}= must be an iterable of input names, not a string")
        try:
            result = frozenset(names)
        except TypeError as error:
            raise TypeError(f"{policy}= must be an iterable of input names") from error
        if any(not isinstance(name, str) for name in result):
            raise TypeError(f"{policy}= must contain only input names")
        return result

    def _set_lifecycle(self, phase, fn):
        for param in inspect.signature(fn).parameters.values():
            if param.annotation in _INJECTABLE_MARKERS:
                if param.default is not None:
                    raise TypeError(
                        f"injectable parameter '{param.name}' of '{fn.__name__}' must default to None"
                    )
                continue
            if isinstance(param.annotation, (_TsExpr, _ContextExpr)) or param.name == "_output":
                raise TypeError(
                    f"@{self.__name__}.{phase} supports wiring-time scalars and injectables only"
                )
        setattr(self, f"_{phase}_fn", fn)
        return fn

    def start(self, fn):
        return self._set_lifecycle("start", fn)

    def stop(self, fn):
        return self._set_lifecycle("stop", fn)

    def __getitem__(self, item):
        # hgraph's node[TYPEVAR: TYPE] pre-resolution subscripts: types
        # resolve from the wired inputs here - accept and ignore.
        return self

    def __call__(self, *args, **kwargs):
        ref = _hgraph.node_ref(self.fn)
        layout, ports, scalars, keep_ref = [], [], [], []
        generic_bindings = {}   # typevar label -> resolved ts_type handle
        bound = self._signature.bind_partial(*args, **kwargs)
        scalar_values = {}
        for param in self._params:
            marker = _INJECTABLE_MARKERS.get(param.annotation)
            if marker is not None:
                if param.name in bound.arguments:
                    raise TypeError(f"{self.__name__}: injectable '{param.name}' cannot be supplied")
                layout.append(marker)
                continue
            if isinstance(param.annotation, _ContextExpr):
                # Context-injected: resolved from the published stack by
                # type (and name); never supplied positionally.
                requirement = bound.arguments.get(param.name, param.default)
                name = None
                required = False
                if isinstance(requirement, _Required):
                    required, name = True, requirement.name
                elif isinstance(requirement, str):
                    name = requirement
                resolved = _resolve_context(param.annotation, name)
                if resolved is None:
                    where = f" with name {name}" if name else ""
                    if required or name is not None:
                        raise WiringError(
                            f"no context published for '{param.name}'{where} of '{self.__name__}'")
                    continue   # optional and absent: the fn sees its None default
                is_active = self._active is None or param.name in self._active
                layout.append("C" if is_active else "P")
                ports.append(_unwrap(resolved))
                keep_ref.append(False)
                continue
            if param.name == "_output":
                # hgraph's _output injection: the node's own output view.
                if param.name in bound.arguments:
                    raise TypeError(f"{self.__name__}: _output cannot be supplied")
                layout.append("o")
                continue
            value = bound.arguments.get(param.name, _MISSING)
            if value is _MISSING:
                if param.default is inspect.Parameter.empty:
                    raise TypeError(f"{self.__name__}: missing argument '{param.name}'")
                value = param.default
                if value is None and isinstance(param.annotation, (_TsExpr,)):
                    # unwired optional ts input: a never-ticking source
                    value = wire("nothing", output_type=param.annotation)
            if isinstance(value, WiringPort):
                required = self._valid is None or param.name in self._valid
                is_active = self._active is None or param.name in self._active
                layout.append({(True, True): "t", (True, False): "u",
                               (False, True): "T", (False, False): "U"}[(is_active, required)])
                ports.append(_unwrap(value))
                keep_ref.append(getattr(param.annotation, "is_ref", False))
                annotation = param.annotation
                if isinstance(annotation, _GenericTsExpr):
                    # Resolve the typevar from the ACTUAL port so a generic
                    # return annotation (e.g. REF[TIME_SERIES_TYPE]) can bind.
                    actual = _unwrap(value).ts_type
                    inner = getattr(annotation, "inner", None)
                    if getattr(annotation, "is_ref", False) and inner is not None:
                        if actual.is_ref:
                            actual = _hgraph.ref_target(actual)
                        generic_bindings[repr(inner)] = actual
                    else:
                        generic_bindings[repr(annotation)] = actual
            else:
                layout.append("s")
                scalars.append(value)
                scalar_values[param.name] = value
        packed = WiringPort(_hgraph.bundle_port(ports, keep_ref))
        node_kwargs = {"fn": ref, "config": "".join(layout), "scalars": _hgraph.any_list(scalars)}
        for phase in ("start", "stop"):
            lifecycle_fn = getattr(self, f"_{phase}_fn")
            lifecycle_layout, lifecycle_scalars = [], []
            if lifecycle_fn is not None:
                for param in inspect.signature(lifecycle_fn).parameters.values():
                    marker = _INJECTABLE_MARKERS.get(param.annotation)
                    if marker is not None:
                        lifecycle_layout.append(marker)
                        continue
                    value = scalar_values.get(param.name, _MISSING)
                    if value is _MISSING:
                        if param.default is inspect.Parameter.empty:
                            raise TypeError(
                                f"{self.__name__}.{phase}: scalar '{param.name}' is not supplied by the node call"
                            )
                        value = param.default
                    lifecycle_layout.append("s")
                    lifecycle_scalars.append(value)
            node_kwargs[f"{phase}_fn"] = _hgraph.node_ref(lifecycle_fn or self.fn)
            node_kwargs[f"{phase}_enabled"] = lifecycle_fn is not None
            node_kwargs[f"{phase}_config"] = "".join(lifecycle_layout)
            node_kwargs[f"{phase}_scalars"] = _hgraph.any_list(lifecycle_scalars)
        if self.has_output:
            out_tp = self._out_tp
            if isinstance(out_tp, _GenericTsExpr):
                inner = getattr(out_tp, "inner", None)
                key = repr(inner) if inner is not None else repr(out_tp)
                bound = generic_bindings.get(key)
                if bound is not None:
                    handle = _hgraph.ref_ts(bound) if getattr(out_tp, "is_ref", False) else bound
                    out_tp = _TsExpr(handle, f"resolved[{out_tp!r}]")
            if not isinstance(out_tp, _TsExpr):
                raise TypeError(f"@compute_node '{self.__name__}' needs a TS[...] return annotation")
            return wire("__py_compute", packed, output_type=out_tp, **node_kwargs)
        return wire("__py_sink", packed, **node_kwargs)


def compute_node(fn=None, *, active=None, valid=None, **_ignored):
    """Python runtime compute node.

    ``active`` names the inputs that drive invocation; ``valid`` names the
    inputs required to be valid. Unlisted inputs remain readable by the
    function but do not participate in that policy.
    """
    if fn is None:
        return lambda f: _PyNode(f, has_output=True, active=active, valid=valid)
    return _PyNode(fn, has_output=True, active=active, valid=valid)


def sink_node(fn=None, *, active=None, valid=None, **_ignored):
    if fn is None:
        return lambda f: _PyNode(f, has_output=False, active=active, valid=valid)
    return _PyNode(fn, has_output=False, active=active, valid=valid)


class _Generator:
    """@generator: a Python generator function yielding (datetime, value)
    pairs; each pair is emitted at its ABSOLUTE time. Wiring-time arguments
    are captured per call (each call is a distinct source node)."""

    def __init__(self, fn):
        self.fn = fn
        self.__name__ = fn.__name__
        self._out_tp = inspect.signature(fn).return_annotation

    def __call__(self, *args, **kwargs):
        if not isinstance(self._out_tp, _TsExpr):
            raise TypeError(f"@generator '{self.__name__}' needs a TS[...] return annotation")
        fn, call_args, call_kwargs = self.fn, args, kwargs

        def bound():
            return fn(*call_args, **call_kwargs)

        ref = _hgraph.node_ref(bound)
        return wire("__py_generator", fn=ref, output_type=self._out_tp)


def generator(fn):
    return _Generator(fn)


class context:
    """Publish a port as a named context for the wiring scope within:
    ``with hg.context("name", port): ...``; consume with
    ``hg.context.get("name")`` / test with ``hg.context.has("name")``.
    Same-wiring only (the design record's semantics)."""

    def __init__(self, name, port):
        self._name, self._port = name, port

    def __enter__(self):
        _hgraph.push_context(_current_wiring(), self._name, _unwrap(self._port))
        return self

    def __exit__(self, *exc):
        _hgraph.pop_context()
        return False

    @staticmethod
    def get(name):
        return WiringPort(_hgraph.get_context(_current_wiring(), name))

    @staticmethod
    def has(name):
        return _hgraph.has_context(_current_wiring(), name)


class _ServiceStub:
    """A service interface stub (hgraph's service decorators): calling it
    wires a CLIENT; register_service registers an implementation."""

    def __init__(self, fn, flavour):
        self.fn = fn
        self.__name__ = fn.__name__
        self.flavour = flavour
        sig = inspect.signature(fn)
        params = [p for p in sig.parameters.values() if isinstance(p.annotation, _TsExpr)]
        out = sig.return_annotation
        kwargs = {"name": fn.__name__, "flavour": flavour}
        if flavour == "reference":
            kwargs["output"] = _unwrap(out)
        elif flavour == "subscription":
            if not params:
                raise TypeError(f"@subscription_service '{self.__name__}' needs a TS[key] parameter")
            kwargs["key_ts"] = _unwrap(params[0].annotation)
            kwargs["value"] = _unwrap(out)
        elif flavour == "request_reply":
            if not params:
                raise TypeError(f"@request_reply_service '{self.__name__}' needs a request parameter")
            kwargs["request"] = _unwrap(params[0].annotation)
            kwargs["response"] = _unwrap(out)
        self.descriptor = _hgraph.service_descriptor(**kwargs)

    def __call__(self, ts=None, *, path=""):
        if isinstance(ts, str):
            # hgraph's reference-service call shape: the interface takes
            # ``path`` as its (only) positional parameter.
            path, ts = ts, None
        w = _current_wiring()
        port = _hgraph.service_client(w, self.descriptor, path,
                                      None if ts is None else _unwrap(ts))
        return WiringPort(port)

    def wire_impl_inputs_stub(self, path=""):
        """hgraph parity: the interface inputs inside a service impl."""
        return _ServiceInputs(impl_input(self, path))

    def wire_impl_out_stub(self, path, out):
        """hgraph parity: publish this interface's output inside an impl."""
        impl_output(self, out, path)


def reference_service(fn):
    """The service interface stub for a reference service: the return
    annotation is the shared output type; calling the stub wires a client."""
    return _ServiceStub(fn, "reference")


def subscription_service(fn):
    """Subscription-service stub: first TS param = the subscription key,
    return annotation = the per-key value; call with the key time-series."""
    return _ServiceStub(fn, "subscription")


def request_reply_service(fn):
    """Request/reply stub: first TS param = the request, return annotation
    = the response; call with the request time-series."""
    return _ServiceStub(fn, "request_reply")


class _AdaptorStub:
    """@adaptor: an adaptor interface stub - the first TS parameter is the
    graph-side input (optional), the return annotation the graph-side output
    (optional). Calling the stub wires a CLIENT."""

    def __init__(self, fn):
        self.fn = fn
        self.__name__ = fn.__name__
        self.flavour = "adaptor"
        sig = inspect.signature(fn)
        params = [p for p in sig.parameters.values() if isinstance(p.annotation, _TsExpr)]
        out = sig.return_annotation
        kwargs = {"name": fn.__name__, "flavour": "adaptor"}
        if params:
            kwargs["request"] = _unwrap(params[0].annotation)   # the adaptor input slot
        if isinstance(out, _TsExpr):
            kwargs["output"] = _unwrap(out)
        self.descriptor = _hgraph.service_descriptor(**kwargs)

    def __call__(self, ts=None, *, path=""):
        port = _hgraph.adaptor_client(_current_wiring(), self.descriptor, path,
                                      None if ts is None else _unwrap(ts))
        return None if port is None else WiringPort(port)


def adaptor(fn):
    return _AdaptorStub(fn)


def from_graph(stub, path=""):
    """Impl-side: the client input of ``stub`` (inside a registered impl)."""
    return WiringPort(_hgraph.adaptor_from_graph(_current_wiring(), stub.descriptor, path))


def to_graph(stub, out, path=""):
    """Impl-side: publish the adaptor output of ``stub`` back to clients."""
    _hgraph.adaptor_to_graph(_current_wiring(), stub.descriptor, path, out=_unwrap(out))


def register_adaptor(path, implementation, **kwargs):
    """Bind an @adaptor_impl to ``path`` (hgraph's shape)."""
    if not isinstance(implementation, _ServiceImpl):
        raise WiringError("register_adaptor requires an @adaptor_impl-decorated implementation")
    if len(implementation.interfaces) > 1:
        raise WiringError("multi-interface adaptor implementations are not supported from Python yet")
    stub = implementation.interfaces[0]
    impl_fn = _bind_registered_impl(implementation, path, kwargs)
    _hgraph.register_adaptor_impl(_current_wiring(), stub.descriptor, path, _wrap_graph_fn(impl_fn))


def adaptor_impl(fn=None, *, interfaces=None):
    """@adaptor_impl: declares the adaptor interfaces an implementation
    supports; the impl takes no wired inputs - it calls from_graph/to_graph."""
    if fn is None:
        return lambda f: _ServiceImpl(f, interfaces)
    return _ServiceImpl(fn, interfaces)


_FLAVOUR_TS_ARITY = {"reference": 0, "subscription": 1, "request_reply": 1, "adaptor": 0}


class _ServiceImpl:
    """@service_impl: an implementation declaring WHICH interfaces it
    supports - validated at decoration (signature shape per flavour) and
    used at registration (hgraph parity)."""

    def __init__(self, fn, interfaces):
        self.fn = fn
        self.__name__ = fn.__name__
        if interfaces is None:
            raise TypeError(f"@service_impl '{self.__name__}' requires interfaces=")
        if not isinstance(interfaces, (tuple, list)):
            interfaces = (interfaces,)
        self.interfaces = tuple(self._resolve(stub) for stub in interfaces)
        target = getattr(fn, "fn", fn)   # unwrap @graph/@compute_node wrappers
        ts_params = [
            p for p in inspect.signature(target).parameters.values()
            if p.name != "path"
            and (isinstance(p.annotation, _TsExpr) or p.annotation is inspect.Signature.empty)
        ]
        if len(self.interfaces) > 1:
            # Multi-interface implementations take NO wired inputs: they
            # fetch each interface's input via impl_input and publish via
            # impl_output inside the body.
            if ts_params:
                raise TypeError(
                    f"@service_impl '{self.__name__}': a multi-interface implementation takes no "
                    "time-series parameters (use impl_input/impl_output)")
        else:
            for stub in self.interfaces:
                expected = _FLAVOUR_TS_ARITY[stub.flavour]
                if len(ts_params) != expected:
                    raise TypeError(
                        f"@service_impl '{self.__name__}': a {stub.flavour} implementation takes "
                        f"{expected} time-series parameter(s), found {len(ts_params)}"
                    )

    @staticmethod
    def _resolve(stub):
        if isinstance(stub, str):
            descriptor = _hgraph.find_service(stub)
            if descriptor is None:
                raise WiringError(f"no service interface named '{stub}'")

            class _CppStub:
                pass

            resolved = _CppStub()
            resolved.descriptor = descriptor
            resolved.flavour = descriptor.flavour
            return resolved
        if not isinstance(stub, (_ServiceStub, _AdaptorStub)):
            raise TypeError(f"@service_impl interfaces must be service stubs, got {stub!r}")
        return stub


def service_impl(fn=None, *, interfaces=None):
    """hgraph's @service_impl: declares (and validates) the interfaces an
    implementation supports; register with ``register_service(path, impl)``.
    Interfaces may be stubs or the NAMES of C++-defined interfaces (the
    ruled direction: Python impls for C++ stubs)."""
    if fn is None:
        return lambda f: _ServiceImpl(f, interfaces)
    return _ServiceImpl(fn, interfaces)


class _ServiceInputs:
    """hgraph's get_service_inputs result: the interface inputs, exposed as
    ``.ts`` (the single input time-series)."""

    __slots__ = ("ts",)

    def __init__(self, ts):
        self.ts = ts


def _bind_registered_impl(implementation, path, config):
    """Bind path/config while leaving only native service ports in the signature."""
    impl_fn = implementation.fn
    target = getattr(impl_fn, "fn", impl_fn)
    signature = inspect.signature(target)
    parameters = list(signature.parameters.values())
    expected_ports = 0 if len(implementation.interfaces) > 1 else _FLAVOUR_TS_ARITY[
        implementation.interfaces[0].flavour
    ]
    port_parameters = [
        param for param in parameters
        if param.name != "path"
        and (isinstance(param.annotation, _TsExpr) or param.annotation is inspect.Signature.empty)
    ]
    if len(port_parameters) != expected_ports:
        raise WiringError(
            f"implementation '{implementation.__name__}' requires {expected_ports} native service input(s)"
        )
    port_names = {param.name for param in port_parameters}
    scalar_parameters = [
        param for param in parameters if param.name != "path" and param.name not in port_names
    ]
    scalar_names = {param.name for param in scalar_parameters}
    unknown = set(config) - scalar_names
    if unknown:
        raise WiringError(
            f"implementation '{implementation.__name__}' has no scalar configuration {sorted(unknown)!r}"
        )
    for param in scalar_parameters:
        if param.name not in config and param.default is inspect.Parameter.empty:
            raise WiringError(
                f"implementation '{implementation.__name__}' requires scalar configuration '{param.name}'"
            )

    def bound(*ports):
        if len(ports) != len(port_parameters):
            raise WiringError(
                f"implementation '{implementation.__name__}' received {len(ports)} native service inputs"
            )
        arguments = dict(zip((param.name for param in port_parameters), ports))
        if any(param.name == "path" for param in parameters):
            arguments["path"] = path
        for param in scalar_parameters:
            arguments[param.name] = config.get(param.name, param.default)
        return impl_fn(**arguments)

    bound.__name__ = implementation.__name__
    bound.__signature__ = inspect.Signature(
        parameters=[
            param.replace(kind=inspect.Parameter.POSITIONAL_OR_KEYWORD)
            for param in port_parameters
        ],
        return_annotation=signature.return_annotation,
    )
    return bound


def get_service_inputs(path, stub):
    """hgraph parity: the interface's inputs inside a service impl."""
    return _ServiceInputs(impl_input(stub, path))


def set_service_output(path, stub, out):
    """hgraph parity: publish an interface's output inside a service impl."""
    impl_output(stub, out, path)


def impl_input(stub, path=""):
    """Inside a multi-interface implementation: the interface's input
    (subscription key set / request dictionary)."""
    return WiringPort(_hgraph.service_impl_input(_current_wiring(), stub.descriptor, path))


def impl_output(stub, out, path=""):
    """Inside a multi-interface implementation: publish the interface's
    output explicitly."""
    _hgraph.service_impl_output(_current_wiring(), stub.descriptor, path, out=_unwrap(out))


def register_service(path, implementation, **kwargs):
    """Bind ``implementation`` (an @service_impl) to ``path`` (hgraph's
    signature: path first). A SINGLE-interface impl wires with its input
    supplied and output captured automatically; a MULTI-interface impl
    takes no wired inputs and uses impl_input/impl_output per interface."""
    if not isinstance(implementation, _ServiceImpl):
        raise WiringError("register_service requires an @service_impl-decorated implementation")
    impl_fn = _bind_registered_impl(implementation, path, kwargs)
    if len(implementation.interfaces) > 1:
        _hgraph.register_multi_service_impl(
            _current_wiring(), [stub.descriptor for stub in implementation.interfaces], path,
            _wrap_graph_fn(impl_fn))
        return
    stub = implementation.interfaces[0]
    _hgraph.register_service_impl(
        _current_wiring(), stub.descriptor, path, _wrap_graph_fn(impl_fn))


class _RecordReplayModes:
    NONE = _hgraph.MODE_NONE
    RECORD = _hgraph.MODE_RECORD
    REPLAY = _hgraph.MODE_REPLAY
    COMPARE = _hgraph.MODE_COMPARE
    REPLAY_OUTPUT = _hgraph.MODE_REPLAY_OUTPUT
    RECOVER = _hgraph.MODE_RECOVER


RecordReplayEnum = _RecordReplayModes


class record_replay_scope:
    """Context manager: pushes a record/replay mode scope for the wiring
    within (the C++ RAII scope)."""

    def __init__(self, mode, recordable_id=""):
        self._mode, self._id = mode, recordable_id
        self._scope = None

    def __enter__(self):
        self._scope = _hgraph.record_replay_scope(self._mode, self._id)
        return self

    def __exit__(self, *exc):
        del self._scope
        self._scope = None
        return False


class _Component:
    """@component: Python's component decorator over the C++ wrapping
    rules (the design record's mode set). Consults the ambient mode scope;
    wraps inputs/outputs with the name-resolved record/replay operators;
    fq ids chain through nested scopes."""

    def __init__(self, fn, recordable_id=None):
        self.fn = fn
        self.__name__ = fn.__name__
        self.recordable_id = recordable_id or fn.__name__
        self._params = list(inspect.signature(fn).parameters.values())

    def __call__(self, *ports):
        mode, ambient_id = _hgraph.current_record_replay_mode()
        fq = f"{ambient_id}.{self.recordable_id}" if ambient_id else self.recordable_id
        if mode != _RecordReplayModes.NONE and not fq:
            raise ValueError("component requires a recordable id under an active record/replay mode")
        if (mode & _RecordReplayModes.RECOVER) and (
            mode & (_RecordReplayModes.REPLAY | _RecordReplayModes.REPLAY_OUTPUT)
        ):
            raise ValueError("component cannot recover and replay at the same time")

        wrapped = []
        for param, port in zip(self._params, ports):
            key = param.name
            if mode & (_RecordReplayModes.REPLAY | _RecordReplayModes.COMPARE):
                port = wire("replay", key, recordable_id=fq,
                            output_type=param.annotation if isinstance(param.annotation, _TsExpr) else None)
            if mode & _RecordReplayModes.RECOVER:
                port = wire("__recovering_pass_through", port, f"{fq}.{key}")
            if mode & _RecordReplayModes.RECORD:
                wire("record", port, key, recordable_id=fq)
            wrapped.append(port)

        with record_replay_scope(mode, fq):
            out = self.fn(*wrapped)

        if out is None:
            return None
        out_tp = inspect.signature(self.fn).return_annotation
        if mode & _RecordReplayModes.REPLAY_OUTPUT:
            out = wire("replay", "__out__", recordable_id=fq,
                       output_type=out_tp if isinstance(out_tp, _TsExpr) else None)
        if mode & _RecordReplayModes.RECORD:
            wire("record", out, "__out__", recordable_id=fq)
        if mode & _RecordReplayModes.COMPARE:
            recorded = wire("replay", "__out__", recordable_id=fq,
                            output_type=out_tp if isinstance(out_tp, _TsExpr) else None)
            wire("compare", out, recorded, recordable_id=fq)
        return out


def component(fn=None, *, recordable_id=None):
    if fn is None:
        return lambda f: _Component(f, recordable_id)
    return _Component(fn)


def comparison_summary(fq_key):
    """(compared, mismatches) from a Compare run's ``fq.__compare__``."""
    return _hgraph._comparison_summary(GlobalState.instance()._impl, fq_key)


class _GraphFn:
    """The @graph decorator: a plain composition function. Inside an active
    wiring it inlines (a call is just a call); run_graph/eval_node make it a
    top level."""

    def __init__(self, fn):
        self.fn = fn
        self.signature = inspect.signature(fn)
        self.__name__ = fn.__name__
        self.__doc__ = fn.__doc__

    def __getitem__(self, item):
        # g[str] pins the graph's typevar-defaulted params (hgraph's
        # DEFAULT[SCALAR_1] pattern): params whose default is a typevar
        # sentinel receive the subscript items in declaration order. Slice
        # syntax (g[TIME_SERIES_TYPE: TS[int]]) resolves annotated typevars.
        from ._types import _TypeVarSentinel

        items = item if isinstance(item, tuple) else (item,)
        resolved_types = {
            value.start: value.stop
            for value in items
            if isinstance(value, slice) and isinstance(value.start, _TypeVarSentinel)
        }
        scalar_items = [value for value in items if not isinstance(value, slice)]
        pinned, index = {}, 0
        for name, param in self.signature.parameters.items():
            if isinstance(param.default, _TypeVarSentinel) and index < len(scalar_items):
                pinned[name] = scalar_items[index]
                index += 1
        import functools

        wrapper = functools.partial(self, **pinned)
        wrapper.__name__ = self.__name__
        parameters = [
            param.replace(annotation=resolved_types.get(param.annotation, param.annotation))
            for param in self.signature.parameters.values()
        ]
        return_annotation = resolved_types.get(self.signature.return_annotation,
                                               self.signature.return_annotation)
        wrapper.__signature__ = self.signature.replace(parameters=parameters,
                                                       return_annotation=return_annotation)
        return wrapper

    def __call__(self, *args, **kwargs):
        bound = self.signature.bind_partial(*args, **kwargs)
        for param in self.signature.parameters.values():
            if param.annotation is GlobalState and param.name not in bound.arguments:
                bound.arguments[param.name] = GlobalState.instance()
        result = self.fn(*bound.args, **bound.kwargs)
        if isinstance(result, dict) and result and all(isinstance(v, WiringPort) for v in result.values()):
            # hgraph parity: a dict literal of ports returned from a @graph
            # coerces to its annotated TSB output (a structural bundle when
            # the annotation is generic/absent).
            annotation = self.signature.return_annotation
            if isinstance(annotation, _TsExpr) and annotation.handle.is_tsb:
                fields = {k: _unwrap(v) for k, v in result.items()}
                return WiringPort(_hgraph.tsb_port(annotation.handle, fields))
            fields = [(k, _unwrap(v).ts_type) for k, v in result.items()]
            tsb_type = _hgraph.un_named_tsb_type(fields)
            return WiringPort(_hgraph.tsb_port(tsb_type, {k: _unwrap(v) for k, v in result.items()}))
        return result


def graph(fn):
    return _GraphFn(fn)


class _Removed:
    """hgraph's REMOVE marker: a removed TSD key in test output."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "REMOVED"


REMOVED = _Removed()


class Removed:
    """hgraph's TSS removal wrapper: Removed(item) marks a removed set
    element in a delta; hashes/compares as the item (hgraph parity)."""

    __slots__ = ("item",)

    def __init__(self, item):
        self.item = item

    def __hash__(self):
        return hash(self.item)

    def __eq__(self, other):
        return self.item == other.item if type(other) is Removed else self.item == other

    def __repr__(self):
        return f"Removed({self.item!r})"


def _simplify_delta(value):
    """Map canonical delta bundles back to hgraph's friendly test shapes:
    TSD {removed, modified} -> {key: value, removed_key: REMOVED};
    TSS {added, removed} -> frozenset (or a dict when removals occurred)."""
    if isinstance(value, dict):
        if set(value.keys()) == {"removed", "modified"}:
            out = {k: _simplify_delta(v) for k, v in value["modified"].items()}
            out.update({k: REMOVED for k in value["removed"]})
            return out
        if set(value.keys()) == {"added", "removed"}:
            # hgraph's TSS delta shape: one set - added items plain,
            # removed items wrapped in Removed(...).
            return frozenset(value["added"]) | {Removed(r) for r in value["removed"]}
        return {k: _simplify_delta(v) for k, v in value.items()}
    return value


def _times_for(values, start_time):
    return [
        (start_time + i * _hgraph.MIN_TD, v)
        for i, v in enumerate(values)
        if v is not None
    ]


class EvaluationMode:
    SIMULATION = "simulation"
    REAL_TIME = "real_time"


class _PushQueue:
    """hgraph's @push_queue: the wrapped function is the node's START
    lifecycle hook - called with the sender callable as its first argument
    (plus any wiring-time scalars as kwargs) once the real-time graph is
    running. sender(value) is thread-safe from any Python thread."""

    def __init__(self, fn, tp, conflate):
        self.fn = fn
        self.tp = tp
        self.conflate = conflate
        self.__name__ = fn.__name__

    def __call__(self, **scalars):
        w = _current_wiring()
        fn = self.fn

        def on_start(sender):
            fn(sender.send, **scalars)

        port, _sender = w.push_source(_unwrap(self.tp), self.conflate, on_start)
        return WiringPort(port)


def push_queue(tp, conflate=False):
    def decorator(fn):
        return _PushQueue(fn, tp, conflate)

    return decorator


def run_graph(graph_fn, *args, start_time=None, end_time=None,
              run_mode=EvaluationMode.SIMULATION, **kwargs):
    """Wire and evaluate ``graph_fn`` in simulation. Returns hgraph's
    evaluate_graph shape - [(time, value), ...] of the graph output ticks -
    or None for sink graphs. ``end_time`` bounds the run (REQUIRED for
    self-perpetuating graphs, e.g. bound feedback loops). NOTE
    (divergence): the simulation clock is cycle-aligned from MIN_ST in
    MIN_TD steps."""
    w = _hgraph.Wiring(GlobalState.instance()._impl)
    _wiring_stack.append(w)
    try:
        out = graph_fn(*args, **kwargs)
        if out is not None:
            w.wire("__harness_record", (_unwrap(out).dereferenced, "__run_graph__"), {})
        run = w.run(start_time=start_time, end_time=end_time,
                    realtime=run_mode == EvaluationMode.REAL_TIME)
    finally:
        w._release_seed_context()
        _wiring_stack.pop()
    if out is None:
        return None
    return _times_for(run.recorded("__run_graph__"), start_time or _hgraph.MIN_ST)


def _infer_ts_type(series):
    """The TS type for an eval_node input vector, from its first non-None
    sample (hgraph's operators are driven without annotations)."""
    from ._types import TS, TSS, TSD

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
              __start_time__=None, __end_time__=None, __scalars__=None,
              __elide__=False, **kwargs):
    """Drive a @graph/composition ``fn`` with vectors of per-cycle values
    (None = no tick), mirroring hgraph's eval_node test util. Time-series
    input types come from ``fn``'s annotations. The run is unbounded by
    default (MAX_ET, as always) - a graph ends when nothing remains
    scheduled. ``__end_time__`` (Python-hgraph parity) bounds a run
    explicitly; a test that cannot quiesce (e.g. a bound feedback loop
    until per-edge passive support lands) must set it and say why."""
    try:
        fn_sig = inspect.signature(fn.fn if isinstance(fn, _GraphFn) else fn)
        params = list(fn_sig.parameters.values())
    except (TypeError, ValueError):
        params = []
    params = [p for p in params
              if p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)]
    param_names = {p.name for p in params}
    named_series = {k: v for k, v in kwargs.items() if k in param_names and isinstance(v, (list, tuple))}
    for k in named_series:
        kwargs.pop(k)
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
                         __start_time__=__start_time__, __end_time__=__end_time__,
                         __scalars__=__scalars__, __elide__=__elide__, **kwargs)
    w = _hgraph.Wiring(GlobalState.instance()._impl)
    _wiring_stack.append(w)
    try:
        ports = []
        deferred_replay = []
        scalar_positions = set()
        for i, series in enumerate(inputs):
            annotation = params[i].annotation if i < len(params) else None
            if annotation is None and getattr(fn, "_ts_hint", None) is not None:
                hints = fn._ts_hint       # op[TYPEVAR: TYPE, ...] types the inputs
                if not isinstance(hints, list):
                    hints = [hints]
                annotation = hints[i] if i < len(hints) else hints[-1]
            if resolution_dict and i < len(params) and params[i].name in resolution_dict:
                annotation = resolution_dict[params[i].name]
            elif resolution_dict and not params and len(resolution_dict) == len(inputs):
                # OPERATOR functions have no python signature: when the dict
                # has one entry per positional input, its entries type them
                # in order (keys are the operator's own parameter names). A
                # SHORTER dict types a variadic collection - inference per
                # input applies then.
                annotation = list(resolution_dict.values())[i]
            from ._types import _GenericTsExpr
            if isinstance(annotation, _GenericTsExpr):
                samples = series if isinstance(series, (list, tuple)) else [series]
                annotation = _infer_ts_type(samples)
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


def _merge_cs(orig, delta):
    """Recursive right-over-left CompoundScalar merge (hgraph's
    combine_compound_scalars): delta's None fields keep the original."""
    import dataclasses

    if delta is None:
        return orig
    merged = {}
    for field in dataclasses.fields(orig):
        original = getattr(orig, field.name)
        update = getattr(delta, field.name, None)
        if update is None:
            merged[field.name] = original
        elif dataclasses.is_dataclass(update) and dataclasses.is_dataclass(original):
            merged[field.name] = _merge_cs(original, update)
        else:
            merged[field.name] = update
    return type(orig)(**merged)


def _combine_compound_scalars(orig, delta):
    # C++-first ruling (2026-07-06): CS = C++ Bundle value; the merge is
    # the erased C++ combine over bundle scalars.
    return wire("combine", orig, delta)


def set_delta(added=None, removed=None, tp=None):
    """hgraph's set-delta literal: the friendly TSS delta shape - added
    items plain, removals wrapped in Removed."""
    added = frozenset(added) if added else frozenset()
    removed = frozenset(removed) if removed else frozenset()
    return added | {Removed(r) for r in removed}


def compute_set_delta(value, out):
    """Delta between the node's CURRENT output set and the new target set
    (hgraph parity: use with the _output injection)."""
    target = frozenset(value.value if hasattr(value, "value") else value)
    if out is not None and out.valid:
        current = frozenset(out.value)
        return set_delta(added=target - current, removed=current - target)
    return set_delta(added=target)
