"""Wiring stack, ports, the erased ``wire`` verb and the wiring errors.

Wiring state is a module-level stack (the runtime is single-threaded by
design — the standing no-thread-locals ruling). ``_wiring_stack`` is THE
single list object: tests and C++ re-entry mutate it in place; nothing may
rebind it."""
import _hgraph

from .._types import _TsExpr
from ._sentinels import _REDUCE_ZERO

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
            from .._types import TS as _TS

            out_type = _TS[out_type]   # json_encode[str] names the SCALAR
        out_type = out_type.handle if isinstance(out_type, _TsExpr) else out_type
    w = _current_wiring()
    sizes = kwargs.pop("__sizes__", None)
    unwrapped = tuple(_unwrap(a) for a in args)
    unwrapped_kw = {k: _unwrap(v) for k, v in kwargs.items()}
    try:
        # Plain-value kwargs falling to a **kwargs collector lift to const
        # sources inside the C++ call normalisation (the scalar-kwargs rule) -
        # no python-side retry.
        if sizes is not None:
            result = w.wire(name, unwrapped, unwrapped_kw, output_type=out_type, sizes=sizes)
        else:
            result = w.wire(name, unwrapped, unwrapped_kw, output_type=out_type)
    except (RuntimeError, ValueError) as error:
        # std::invalid_argument surfaces as ValueError; both are wiring-time.
        # (RequirementsNotMetWiringError arrives ALREADY typed - the C++
        # resolver throws OperatorRequirementsError, translated directly.)
        raise WiringError(str(error)) from error
    return WiringPort(result) if result is not None else None


class _OperatorFunction:
    def __rshift__(self, other):
        # arrow-chain sugar: (eval_ | op >> assert_) - uplift into the arrow
        # module's combinator so operators compose in pipelines.
        from ..arrow import arrow

        return arrow(self) >> other

    def __lshift__(self, other):
        # arrow bind sugar: op << const_(x)  ==  i / const_(x) >> op
        from ..arrow import arrow

        return arrow(self) << other

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
        if (self.__name__ == "const" and args
                and "tp" not in kwargs and "output_type" not in kwargs):
            from .._compat import CompoundScalar
            from .._types import TS

            if isinstance(args[0], CompoundScalar):
                # Schema-free C++ value inference intentionally treats an
                # arbitrary Python object as ``object``. A CompoundScalar's
                # Python class is its nominal Bundle schema, so retain that
                # information at the Python boundary before wiring const.
                kwargs["output_type"] = TS[type(args[0])]
        if self._output_type is not None and "tp" not in kwargs and "output_type" not in kwargs:
            kwargs["output_type"] = self._output_type
        if self._sizes is not None:
            kwargs.setdefault("__sizes__", self._sizes)
        # hgraph parity: a trailing ``None`` argument means "use the
        # parameter's default" (upstream defaults optional scalars to None).
        while args and args[-1] is None:
            args = args[:-1]
        args, kwargs = self._normalise_type_arguments(args, kwargs)
        # Fixed-TSL integer access is a structural projection, not a runtime
        # node. Keep direct calls to getitem_ consistent with ``port[index]``;
        # other shapes and dynamic keys fall back to the registered operator.
        if (self.__name__ == "getitem_" and len(args) == 2
                and isinstance(args[0], WiringPort)
                and isinstance(args[1], int) and not isinstance(args[1], bool)):
            raw = _unwrap(args[0])
            if raw.ts_type.is_fixed_tsl:
                return WiringPort(_hgraph.tsl_element(raw, args[1]))
        return wire(self.__name__, *args, **kwargs)

    def __getitem__(self, item):
        # hgraph's ``op[TYPEVAR: TYPE]`` pre-resolution subscripts arrive as
        # SLICES. ``op[OUT: X]`` names the requested OUTPUT type; other
        # typevar slices are dropped (resolution happens from the wired
        # inputs). A plain type subscript is the requested output type.
        from .._types import OUT

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
        if output_type is not None and not _hgraph.operator_output_is_selective(self.__name__):
            # The REGISTRY decides what a bare subscript type means: when no
            # candidate's output can be influenced by it (sinks, or every
            # overload shares one fixed output - to_json's TS[str]), the
            # type is an INPUT constraint; otherwise it names the output.
            ts_hints.append(output_type)
            output_type = None
        return _OperatorFunction(self.__name__, output_type=output_type, sizes=sizes or None,
                                 ts_hint=ts_hints or None)

    def _normalise_type_arguments(self, args, kwargs):
        if "tp" in kwargs or "output_type" in kwargs:
            return args, kwargs
        # hgraph's positional ``tp`` arguments (const(value, tp) /
        # nothing(tp)): a TYPE EXPRESSION is a wiring directive, never a
        # value - the registry has no type-valued scalars - so the first
        # one names the requested output, whatever the operator.
        for index, arg in enumerate(args):
            if isinstance(arg, _TsExpr):
                kwargs = dict(kwargs)
                kwargs["output_type"] = arg
                return (*args[:index], *args[index + 1:]), kwargs
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


def _port_bundle_field_names(self):
    """The TSB field names of a port (looking through REF), else None."""
    ts_type = self._port.ts_type
    if ts_type.is_tsb:
        return _hgraph.tsb_field_names(ts_type)
    try:
        deref = self._port.dereferenced
        if deref is not None and deref.ts_type.is_tsb:
            return _hgraph.tsb_field_names(deref.ts_type)
    except Exception:
        pass
    return None


def _port_len(self):
    ts_type = self._port.ts_type
    if ts_type.is_fixed_tsl:
        return ts_type.fixed_size
    if (names := _port_bundle_field_names(self)) is not None:
        return len(names)
    raise TypeError("len() is only defined for fixed-size TSL and TSB ports")


def _port_iter(self):
    # The sequence protocol for fixed TSLs and TSBs (`*tsl` / `a, b = tsb`
    # unpacking, hgraph parity; REF[TSB] iterates the referenced fields).
    # Without __len__/__iter__, python's fallback iteration via __getitem__
    # would wire getitem_ nodes FOREVER.
    if (names := _port_bundle_field_names(self)) is not None:
        return (getattr(self, name) for name in names)
    return (self[i] for i in range(len(self)))


WiringPort.__len__ = _port_len
WiringPort.__iter__ = _port_iter


def _port_getattr(self, name):
    if name.startswith("_"):
        raise AttributeError(name)
    if name == "as_schema":
        return self   # hgraph's TSB.as_schema: typed field access (same port)
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


def _port_reduce(self, fn, zero=_REDUCE_ZERO, is_associative=True):
    from ._compose import reduce

    return reduce(fn, self, zero, is_associative=is_associative)


def _port_keys(self):
    """hgraph's TSB mapping protocol: field names (dict(**tsb) works)."""
    tp = _unwrap(self).ts_type
    return tuple(_hgraph.tsb_field_names(tp))


WiringPort.reduce = _port_reduce
WiringPort.keys = _port_keys
WiringPort.__getattr__ = _port_getattr

class WiringError(RuntimeError):
    """A wiring-time error (hgraph parity)."""

class ParseError(WiringError):
    """A wiring function's declaration is malformed (hgraph parity)."""


class IncorrectTypeBinding(WiringError):
    """A port's type does not match the parameter it is wired to."""


class RequirementsNotMetWiringError(WiringError):
    """An overload's requires= predicate rejected the call."""

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
