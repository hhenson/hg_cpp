"""_GraphFn/@graph, graph-fn wrapping (the borrowed-wiring re-entry),
signature auto-resolution and @component."""
import inspect

import _hgraph

from .._types import (_ContextExpr, _GenericTsExpr, _TsExpr,
                      _TypeVarSentinel, _type_var_name)
from ._core import (ParseError, WiringError, WiringPort, _current_wiring,
                    _unwrap, _wiring_stack, wire)
from ._markers import _INJECTABLE_MARKERS
from ._node import _PyNode
from ._operator import _register_overload
from ._state import GlobalState

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
    # Node decorators are authoritative even when the wrapped user function is
    # unannotated. For graphs/lambdas, only explicit ``-> None`` marks a sink;
    # an unannotated callable remains provisionally output-producing.
    has_output = gfn.has_output if isinstance(gfn, _PyNode) else sig.return_annotation is not None

    def wrapper(borrowed_wiring, ports):
        _wiring_stack.append(borrowed_wiring)
        try:
            out = user_fn(*(WiringPort(p) for p in ports))
            if out is None:
                return None
            if not isinstance(out, WiringPort):
                # a plain value returned from a @graph lifts to const
                # (hgraph parity - dispatch branches `return "woof"`).
                out = wire("const", out)
            raw = _unwrap(out)
            if raw.is_structural:
                # A structural source has no single endpoint for a child
                # output binding. REFERENCE-valued fields materialize as a
                # REFERENCE output (hgraph's combine-of-refs shape - zero
                # copy); plain fields copy through the canonical-delta
                # identity node. Peered child projections are already valid
                # nested output bindings and pass through unchanged.
                if _hgraph.structural_has_ref_children(raw):
                    raw = _hgraph.ref_port(borrowed_wiring, raw)
                else:
                    raw = _unwrap(wire("__materialize", out))
            elif raw.has_path and not (isinstance(out_tp, _TsExpr) and out_tp.is_ref):
                # Python graph outputs expose referenced values unless the
                # author explicitly declares a REF return. Preserve the
                # projected endpoint path while giving map_/mesh_ the plain
                # child schema used by equivalent C++ wiring.
                raw = raw.dereferenced
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

class _ResolvedSize:
    """The object an AUTO_RESOLVE'd type[SIZE] parameter receives: hgraph's
    Size-like carrier with the concrete ``.SIZE``."""

    __slots__ = ("SIZE",)

    def __init__(self, size):
        self.SIZE = size

    def __repr__(self):
        return f"Size[{self.SIZE}]"


def _graph_auto_resolve(signature, arguments):
    """Fill ``x: type[SENTINEL] = AUTO_RESOLVE`` graph parameters: match
    every time-series parameter's TYPE PATTERN against its wired port in a
    C++ resolution scope, then read each sentinel's binding from it."""
    import typing

    from .._types import _TsExpr, _GenericTsExpr, _TypeVarSentinel, _pattern_of

    scope = _hgraph.ResolutionScope()
    for name, param in signature.parameters.items():
        value = arguments.get(name)
        if isinstance(value, WiringPort) and isinstance(
                param.annotation, (_GenericTsExpr, _TypeVarSentinel)):
            try:
                scope.match(_pattern_of(param.annotation), _unwrap(value).ts_type)
            except (RuntimeError, ValueError, TypeError):
                pass   # inconsistent bindings surface at the consuming node

    resolved = {}
    for name, param in signature.parameters.items():
        from .._types import AUTO_RESOLVE

        if param.default is not AUTO_RESOLVE or name in arguments:
            continue
        args = typing.get_args(param.annotation)
        sentinel = args[0] if args else None
        if not isinstance(sentinel, _TypeVarSentinel):
            raise WiringError(
                f"AUTO_RESOLVE parameter '{name}' needs a type[TYPEVAR] annotation")
        size = scope.find_size(_type_var_name(sentinel))
        if size is not None:
            resolved[name] = _ResolvedSize(size)
            continue
        ts = scope.find_ts(_type_var_name(sentinel))
        if ts is not None:
            resolved[name] = _TsExpr(ts, f"resolved[{sentinel!r}]")
            continue
        scalar = scope.find_scalar(_type_var_name(sentinel))
        if scalar is not None:
            # Like _ResolvedSize, this is a wiring-time type carrier. The
            # type-expression frontend accepts ValueType directly, avoiding a
            # lossy round trip from an interned C++ scalar back to a Python
            # class (which is not defined for every runtime scalar family).
            resolved[name] = scalar
            continue
        raise WiringError(
            f"AUTO_RESOLVE could not resolve '{name}' ({sentinel!r}) from the wired arguments")
    return resolved

class _Component:
    """Python signature adapter for the C++ component wiring primitive."""

    def __init__(self, fn, recordable_id=None):
        self.fn = fn
        self.__name__ = fn.__name__
        self.recordable_id = recordable_id or fn.__name__
        self._signature = inspect.signature(fn)
        self._params = list(self._signature.parameters.values())
        # eval_node/introspection must see the USER signature, not __call__'s.
        self.__signature__ = self._signature

    @staticmethod
    def _is_ts(param, value):
        return isinstance(value, WiringPort) or isinstance(
            param.annotation, (_TsExpr, _GenericTsExpr, _TypeVarSentinel))

    def __call__(self, *args, **kwargs):
        bound = self._signature.bind(*args, **kwargs)
        call_args = dict(bound.arguments)
        input_names = []
        input_ports = []
        for param in self._params:
            key = param.name
            if key not in call_args or not self._is_ts(param, call_args[key]):
                continue
            input_names.append(key)
            input_ports.append(_unwrap(call_args[key]))

        def compose(wrapped_ports):
            for key, port in zip(input_names, wrapped_ports):
                call_args[key] = WiringPort(port)
            out = self.fn(**call_args)
            return None if out is None else _unwrap(out)

        out = _hgraph.component(
            _current_wiring(), self.recordable_id, input_names, input_ports, compose)
        return None if out is None else WiringPort(out)


def component(fn=None, *, recordable_id=None):
    if fn is None:
        return lambda f: _Component(f, recordable_id)
    return _Component(fn)

class _GraphFn:
    """The @graph decorator: a plain composition function. Inside an active
    wiring it inlines (a call is just a call); run_graph/eval_node make it a
    top level."""

    def __init__(self, fn):
        self.fn = fn
        self._signature = inspect.signature(fn)
        self.__name__ = fn.__name__
        self.__doc__ = fn.__doc__
        out = self._signature.return_annotation
        if isinstance(out, type):
            from .._types import TimeSeriesSchema

            if issubclass(out, TimeSeriesSchema):
                # hgraph parity: a bare schema class is not a time-series
                # type - the return must be TSB[Schema].
                raise ParseError(
                    f"@graph '{self.__name__}': return type {out.__name__} is a schema class - "
                    f"use TSB[{out.__name__}]")

    @property
    def signature(self):
        from .._signature import WiringNodeType, extract_signature

        return extract_signature(self.fn, WiringNodeType.GRAPH)

    def __getitem__(self, item):
        # g[str] pins the graph's typevar-defaulted params (hgraph's
        # DEFAULT[SCALAR_1] pattern): params whose default is a typevar
        # sentinel receive the subscript items in declaration order. Slice
        # syntax (g[TIME_SERIES_TYPE: TS[int]]) resolves annotated typevars.
        from .._types import _TypeVarSentinel

        items = item if isinstance(item, tuple) else (item,)
        resolved_types = {
            value.start: value.stop
            for value in items
            if isinstance(value, slice) and isinstance(value.start, _TypeVarSentinel)
        }
        scalar_items = [value for value in items if not isinstance(value, slice)]
        pinned, index = {}, 0
        for name, param in self._signature.parameters.items():
            if isinstance(param.default, _TypeVarSentinel) and index < len(scalar_items):
                pinned[name] = scalar_items[index]
                index += 1
        import functools

        wrapper = functools.partial(self, **pinned)
        wrapper.__name__ = self.__name__
        parameters = [
            param.replace(annotation=resolved_types.get(param.annotation, param.annotation))
            for param in self._signature.parameters.values()
        ]
        return_annotation = resolved_types.get(self._signature.return_annotation,
                                               self._signature.return_annotation)
        wrapper.__signature__ = self._signature.replace(parameters=parameters,
                                                       return_annotation=return_annotation)
        return wrapper

    def __call__(self, *args, **kwargs):
        bound = self._signature.bind_partial(*args, **kwargs)
        for param in self._signature.parameters.values():
            if param.annotation is GlobalState and param.name not in bound.arguments:
                bound.arguments[param.name] = GlobalState.instance()
        bound.arguments.update(_graph_auto_resolve(self._signature, bound.arguments))
        result = self.fn(*bound.args, **bound.kwargs)
        if isinstance(result, dict) and result and all(isinstance(v, WiringPort) for v in result.values()):
            # hgraph parity: a dict literal of ports returned from a @graph
            # coerces to its annotated TSB output (a structural bundle when
            # the annotation is generic/absent).
            annotation = self._signature.return_annotation
            if isinstance(annotation, _TsExpr) and annotation.handle.is_tsb:
                fields = {k: _unwrap(v) for k, v in result.items()}
                return WiringPort(_hgraph.tsb_port(annotation.handle, fields))
            fields = [(k, _unwrap(v).ts_type) for k, v in result.items()]
            tsb_type = _hgraph.un_named_tsb_type(fields)
            return WiringPort(_hgraph.tsb_port(tsb_type, {k: _unwrap(v) for k, v in result.items()}))
        return result


def graph(fn=None, *, overloads=None, requires=None):
    def _make(f):
        wrapped = _GraphFn(f)
        if overloads is not None:
            _register_overload(overloads, wrapped, requires)
        return wrapped

    if fn is None:
        return _make
    return _make(fn)
