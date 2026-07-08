"""Wiring context, ports, decorators and runners over the _hgraph bridge.

The API mirrors Python hgraph's: @graph composes, run_graph evaluates,
eval_node drives a node/graph from vectors of values. Wiring state is a
module-level stack (the runtime is single-threaded by design)."""
import inspect

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
        if raw.ts_type.kind == _TSL_KIND and raw.ts_type.fixed_size > 0:
            return WiringPort(_hgraph.tsl_element(raw, item))
    return wire("getitem_", self, item)


WiringPort.__getitem__ = _port_getitem
WiringPort.__hash__ = object.__hash__  # __eq__ wires a node; identity hashing stands


_TSL_KIND = 3   # TSTypeKind::TSL


def _port_len(self):
    ts_type = self._port.ts_type
    if ts_type.kind == _TSL_KIND and ts_type.fixed_size > 0:
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
    # JSON leaf coercions: j["a"].int / .float / .str / .bool
    # (value kind 8 = Any; the JSON meta rides Any storage).
    if name in ("int", "float", "str", "bool") and _unwrap(self).ts_type.value_kind == 8:
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
                child_kinds = _hgraph.structural_child_kinds(raw)
                if any(k == 6 for k in child_kinds):   # 6 = REF
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
    if __output_type__ is None:
        if len(args) == 2 and not kwargs and all(isinstance(a, WiringPort) for a in args):
            return _combine_compound_scalars(*args)
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

    @staticmethod
    def _infer(target, ts):
        from ._types import _TsExpr, TS, TSS, TSD

        if isinstance(target, _TsExpr):
            return target
        handle = _unwrap(ts).ts_type
        in_kind = handle.kind          # 0 TS, 2 TSS, 3 TSD, 4 TSL, 5 TSB
        label = getattr(target, "label", None) or getattr(target, "__name__", None) or repr(target)

        def value_vt():
            return _hgraph.ts_value_vt(handle)

        def wrap(h, text):
            expr = _TsExpr.__new__(_TsExpr)
            expr.handle, expr.label = h, text
            return expr

        if label.startswith("TSS"):
            # element = TS value / set element / dict key
            if in_kind == 0:
                vt = value_vt()
                if _hgraph.vt_kind(vt) in (5, 6):   # Set/List value scalar
                    vt = _hgraph.vt_element(vt)
            elif in_kind == 2:
                vt = _hgraph.vt_element(_hgraph.ts_value_vt(handle))
            elif in_kind == 3:
                vt = _hgraph.tsd_key_vt(handle)
            else:
                raise WiringError(f"convert: cannot infer TSS target from {handle!r}")
            return wrap(_hgraph.tss(vt), f"TSS[inferred]")
        if label.startswith("TSD"):
            if in_kind == 0:
                vt = value_vt()             # TS[Mapping[K, V]]
                if _hgraph.vt_kind(vt) != 7:
                    raise WiringError(f"convert: TSD target needs a Mapping input, got {handle!r}")
                return wrap(_hgraph.tsd(_hgraph.vt_key(vt), _hgraph.ts(_hgraph.vt_element(vt))),
                            "TSD[inferred]")
            raise WiringError(f"convert: cannot infer TSD target from {handle!r}")
        if label.startswith("TS[Tuple") or label.startswith("TS[tuple"):
            if in_kind == 0:
                vt = value_vt()
                if _hgraph.vt_kind(vt) in (5, 6):   # already a collection: element rides
                    vt = _hgraph.vt_element(vt)
                return wrap(_hgraph.ts(_hgraph.tuple_vt(vt)), "TS[tuple[inferred]]")
            if in_kind == 2:
                vt = _hgraph.vt_element(_hgraph.ts_value_vt(handle))
                return wrap(_hgraph.ts(_hgraph.tuple_vt(vt)), "TS[tuple[inferred]]")

        if label.startswith("TS[Set") or label.startswith("TS[set"):
            if in_kind == 2:
                vt = _hgraph.vt_element(_hgraph.ts_value_vt(handle))
                return wrap(_hgraph.ts(_hgraph.set_vt(vt)), "TS[set[inferred]]")
            if in_kind == 0:
                vt = value_vt()
                if _hgraph.vt_kind(vt) in (5, 6):
                    vt = _hgraph.vt_element(vt)
                return wrap(_hgraph.ts(_hgraph.set_vt(vt)), "TS[set[inferred]]")
        if label.startswith("TS[Mapping") or label.startswith("TS[dict"):
            if in_kind == 3:
                k = _hgraph.tsd_key_vt(handle)
                v = _hgraph.ts_value_vt(_hgraph.tsd_value_ts(handle))
                return wrap(_hgraph.ts(_hgraph.map_vt(k, v)), "TS[mapping[inferred]]")
        raise WiringError(f"convert: cannot resolve target '{label}' from input {handle!r}")

    def __call__(self, ts, to=None, **kwargs):
        target = to if to is not None else self._to
        return wire("convert", ts, output_type=self._infer(target, ts), **kwargs)


convert = _Convert()


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


_INJECTABLE_MARKERS = {STATE: "S", CLOCK: "c", SCHEDULER: "d"}


_MISSING = object()


class _PyNode:
    """@compute_node / @sink_node: a Python function as a runtime node. The
    function runs on the graph thread (both modes) under the GIL, receives
    time-series inputs as plain Python VALUES and wiring-time scalars as
    supplied; STATE/CLOCK/SCHEDULER-annotated parameters are injected. A
    compute node's return value ticks its output (None = no tick). It must
    have no side effects beyond its output."""

    def __init__(self, fn, has_output, valid=None):
        self.fn = fn
        self.has_output = has_output
        self._valid = valid
        self.__name__ = fn.__name__
        sig = inspect.signature(fn)
        # Callers introspecting the NODE (eval_node's input typing) must see
        # the user function's signature, not _PyNode.__call__'s.
        self.__signature__ = sig
        self._out_tp = sig.return_annotation if has_output else None
        self._params = list(sig.parameters.values())
        # Injectable parameters MUST default to None (hgraph convention):
        # the default guarantees user code in a graph never supplies them.
        for param in self._params:
            if param.annotation in _INJECTABLE_MARKERS and param.default is not None:
                raise TypeError(
                    f"injectable parameter '{param.name}' of '{self.__name__}' must default to None"
                )

    def __getitem__(self, item):
        # hgraph's node[TYPEVAR: TYPE] pre-resolution subscripts: types
        # resolve from the wired inputs here - accept and ignore.
        return self

    def __call__(self, *args, **kwargs):
        ref = _hgraph.node_ref(self.fn)
        layout, ports, scalars, keep_ref = [], [], [], []
        generic_bindings = {}   # typevar label -> resolved ts_type handle
        supplied = iter(args)
        for param in self._params:
            marker = _INJECTABLE_MARKERS.get(param.annotation)
            if marker is not None:
                layout.append(marker)
                continue
            if isinstance(param.annotation, _ContextExpr):
                # Context-injected: resolved from the published stack by
                # type (and name); never supplied positionally.
                requirement = kwargs.pop(param.name, param.default)
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
                layout.append("C")
                ports.append(_unwrap(resolved))
                keep_ref.append(False)
                continue
            if param.name == "_output":
                # hgraph's _output injection: the node's own output view.
                layout.append("o")
                continue
            value = next(supplied, _MISSING)
            if value is _MISSING:
                if param.default is inspect.Parameter.empty:
                    raise TypeError(f"{self.__name__}: missing argument '{param.name}'")
                value = param.default
                if value is None and isinstance(param.annotation, (_TsExpr,)):
                    # unwired optional ts input: a never-ticking source
                    value = wire("nothing", output_type=param.annotation)
            if isinstance(value, WiringPort):
                required = self._valid is None or param.name in self._valid
                layout.append("t" if required else "u")
                ports.append(_unwrap(value))
                keep_ref.append(getattr(param.annotation, "is_ref", False))
                annotation = param.annotation
                if isinstance(annotation, _GenericTsExpr):
                    # Resolve the typevar from the ACTUAL port so a generic
                    # return annotation (e.g. REF[TIME_SERIES_TYPE]) can bind.
                    actual = _unwrap(value).ts_type
                    inner = getattr(annotation, "inner", None)
                    if getattr(annotation, "is_ref", False) and inner is not None:
                        if actual.kind == 6:   # already a REF: bind its target
                            actual = _hgraph.ref_target(actual)
                        generic_bindings[repr(inner)] = actual
                    else:
                        generic_bindings[repr(annotation)] = actual
            else:
                layout.append("s")
                scalars.append(value)
        packed = WiringPort(_hgraph.bundle_port(ports, keep_ref))
        kwargs = {"fn": ref, "config": "".join(layout), "scalars": _hgraph.any_list(scalars)}
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
            return wire("__py_compute", packed, output_type=out_tp, **kwargs)
        return wire("__py_sink", packed, **kwargs)


def compute_node(fn=None, *, valid=None, **_ignored):
    """@compute_node[(valid=("orig",))] - hgraph parity: ``valid`` names the
    inputs REQUIRED to be valid; unlisted time-series inputs are UNCHECKED
    (the fn guards itself)."""
    if fn is None:
        return lambda f: _PyNode(f, has_output=True, valid=valid)
    return _PyNode(fn, has_output=True, valid=valid)


def sink_node(fn):
    return _PyNode(fn, has_output=False)


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


def register_adaptor(path, implementation):
    """Bind an @adaptor_impl to ``path`` (hgraph's shape)."""
    if not isinstance(implementation, _ServiceImpl):
        raise WiringError("register_adaptor requires an @adaptor_impl-decorated implementation")
    if len(implementation.interfaces) > 1:
        raise WiringError("multi-interface adaptor implementations are not supported from Python yet")
    stub = implementation.interfaces[0]
    _hgraph.register_adaptor_impl(_current_wiring(), stub.descriptor, path, _wrap_graph_fn(implementation.fn))


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
    if kwargs:
        raise WiringError("implementation kwargs are not supported yet")
    impl_fn = implementation.fn
    target = getattr(impl_fn, "fn", impl_fn)
    params = list(inspect.signature(target).parameters.values())
    if params and params[0].name == "path":
        # hgraph parity: the registered path is INJECTED into the impl.
        bound_fn, bound_path = impl_fn, path

        def bound():
            return bound_fn(bound_path)

        impl_fn = bound   # a fresh object per registration: unique identity per path
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
    return _hgraph.comparison_summary(fq_key)


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
        # sentinel receive the subscript items in declaration order.
        from ._types import _TypeVarSentinel

        items = item if isinstance(item, tuple) else (item,)
        pinned, index = {}, 0
        for name, param in self.signature.parameters.items():
            if isinstance(param.default, _TypeVarSentinel) and index < len(items):
                pinned[name] = items[index]
                index += 1
        import functools

        wrapper = functools.partial(self, **pinned)
        wrapper.__name__ = self.__name__
        wrapper.__signature__ = self.signature
        return wrapper

    def __call__(self, *args, **kwargs):
        result = self.fn(*args, **kwargs)
        if isinstance(result, dict) and result and all(isinstance(v, WiringPort) for v in result.values()):
            # hgraph parity: a dict literal of ports returned from a @graph
            # coerces to its annotated TSB output (a structural bundle when
            # the annotation is generic/absent).
            annotation = self.signature.return_annotation
            if isinstance(annotation, _TsExpr) and getattr(annotation.handle, "kind", None) == 5:
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
    w = _hgraph.Wiring()
    _wiring_stack.append(w)
    try:
        out = graph_fn(*args, **kwargs)
        if out is not None:
            w.wire("__harness_record", (_unwrap(out).dereferenced, "__run_graph__"), {})
        run = w.run(start_time=start_time, end_time=end_time,
                    realtime=run_mode == EvaluationMode.REAL_TIME)
    finally:
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
    w = _hgraph.Wiring()
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
        param_names = {p.name for p in params}
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
        named_series = {k: v for k, v in kwargs.items() if k in param_names and isinstance(v, (list, tuple))}
        for k in named_series: kwargs.pop(k)
        if named_series:
            by_name = {p.name: i for i, p in enumerate(params)}
            extended = list(inputs) + [None] * (max(by_name[k] for k in named_series) + 1 - len(inputs))
            for k, series in named_series.items():
                extended[by_name[k]] = series
            return eval_node(fn, *extended, output_type=output_type, resolution_dict=resolution_dict,
                             __start_time__=__start_time__, __end_time__=__end_time__,
                             __scalars__=__scalars__, __elide__=__elide__, **kwargs)
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
        # projections, each dereferenced (kind 5 = TSB, 6 = REF).
        raw = _unwrap(out)
        record_kwargs = {"sparse": True} if __elide__ else {}
        record_port = raw.dereferenced
        if raw.ts_type.kind == 5 and any(t.kind == 6 for _, t in _hgraph.ts_field_types(raw.ts_type)):
            fields = {
                name: _unwrap(wire("getitem_", WiringPort(raw), name)).dereferenced
                for name, _ in _hgraph.ts_field_types(raw.ts_type)
            }
            record_port = _hgraph.tsb_port(record_port.ts_type, fields)
        elif (raw.ts_type.kind == _TSL_KIND and raw.ts_type.fixed_size > 0
              and _hgraph.tsl_element(raw, 0).ts_type.kind == 6):
            # A TSL of REF elements: record a structural TSL of per-element
            # projections, each dereferenced.
            record_port = _hgraph.tsl_port(
                [_hgraph.tsl_element(raw, i).dereferenced for i in range(raw.ts_type.fixed_size)]
            )
        w.wire("__harness_record", (record_port, "eval_node::out"), record_kwargs)
        run = w.run(start_time=__start_time__, end_time=__end_time__)
    finally:
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
