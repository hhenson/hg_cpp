"""Wiring context, ports, decorators and runners over the _hgraph bridge.

The API mirrors hgraph's: @graph composes, run_graph evaluates, eval_node
drives a node/graph from vectors of values. Wiring state is a module-level
stack (the runtime is single-threaded by design)."""
import datetime
import inspect

import _hgraph

from ._types import _TsExpr, _resolve

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
        out_type = out_type.handle if isinstance(out_type, _TsExpr) else out_type
    w = _current_wiring()
    result = w.wire(
        name,
        tuple(_unwrap(a) for a in args),
        {k: _unwrap(v) for k, v in kwargs.items()},
        output_type=out_type,
    )
    return WiringPort(result) if result is not None else None


def operator_function(name):
    """A Python callable wiring the named registered operator."""

    def _call(*args, **kwargs):
        return wire(name, *args, **kwargs)

    _call.__name__ = name
    _call.__qualname__ = name
    _call.__doc__ = f"Wire the hgraph operator '{name}' (registry-resolved)."
    return _call


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
WiringPort.__getitem__ = lambda self, item: wire("getitem_", self, item)
WiringPort.__hash__ = object.__hash__  # __eq__ wires a node; identity hashing stands


class _GraphFn:
    """The @graph decorator: a plain composition function. Inside an active
    wiring it inlines (a call is just a call); run_graph/eval_node make it a
    top level."""

    def __init__(self, fn):
        self.fn = fn
        self.signature = inspect.signature(fn)
        self.__name__ = fn.__name__
        self.__doc__ = fn.__doc__

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)


def graph(fn):
    return _GraphFn(fn)


def _times_for(values):
    return [
        (_hgraph.MIN_ST + i * _hgraph.MIN_TD, v)
        for i, v in enumerate(values)
        if v is not None
    ]


def run_graph(graph_fn, *args, **kwargs):
    """Wire and evaluate ``graph_fn`` in simulation. Returns hgraph's
    evaluate_graph shape - [(time, value), ...] of the graph output ticks -
    or None for sink graphs. NOTE (divergence): the simulation clock is
    cycle-aligned from MIN_ST in MIN_TD steps."""
    w = _hgraph.Wiring()
    _wiring_stack.append(w)
    try:
        out = graph_fn(*args, **kwargs)
        if out is not None:
            w.wire("record", (_unwrap(out), "__run_graph__"), {})
        run = w.run()
    finally:
        _wiring_stack.pop()
    if out is None:
        return None
    return _times_for(run.recorded("__run_graph__"))


def eval_node(fn, *inputs, output_type=None, __scalars__=None):
    """Drive a @graph/composition ``fn`` with vectors of per-cycle values
    (None = no tick), mirroring hgraph's eval_node test util. Time-series
    input types come from ``fn``'s annotations."""
    fn_sig = inspect.signature(fn.fn if isinstance(fn, _GraphFn) else fn)
    params = list(fn_sig.parameters.values())
    w = _hgraph.Wiring()
    _wiring_stack.append(w)
    try:
        ports = []
        for i, series in enumerate(inputs):
            annotation = params[i].annotation
            if not isinstance(annotation, _TsExpr):
                raise TypeError(f"parameter '{params[i].name}' needs a TS[...] annotation")
            key = f"eval_node::{params[i].name}"
            src = w.wire("replay", (key,), {}, output_type=annotation.handle)
            w.set_replay(key, list(series))
            ports.append(WiringPort(src))
        scalars = __scalars__ or {}
        out = fn(*ports, **scalars)
        length = max((len(series) for series in inputs), default=0)
        if out is None:
            run = w.run()
            return None
        w.wire("record", (_unwrap(out), "eval_node::out"), {})
        run = w.run()
    finally:
        _wiring_stack.pop()
    recorded = run.recorded("eval_node::out")
    recorded += [None] * (length - len(recorded))
    return recorded
