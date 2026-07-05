"""Time-series type expressions mirroring hgraph's: TS[int], TSS[str],
TSD[str, TS[int]], TSL[TS[int], Size[3]], TSB[Schema]. Each subscription
resolves to an interned C++ type handle via the _hgraph registry."""
import datetime

import _hgraph

_SCALAR_NAMES = {
    bool: "bool",
    int: "int",
    float: "float",
    str: "str",
    bytes: "bytes",
    datetime.datetime: "datetime",
    datetime.date: "date",
    datetime.time: "time",
    datetime.timedelta: "timedelta",
}


def _value_type(scalar):
    if isinstance(scalar, str):
        return _hgraph.value_type(scalar)
    if isinstance(scalar, _TypeVarSentinel):
        raise _GenericType()
    # typing generics: tuple[X, ...] / tuple[A, B] / frozenset[X] / dict[K, V]
    import typing

    origin = typing.get_origin(scalar)
    if origin is not None:
        args = typing.get_args(scalar)
        if origin is tuple:
            if len(args) == 2 and args[1] is Ellipsis:
                return _hgraph.tuple_vt(_value_type(args[0]))
            return _hgraph.fixed_tuple_vt([_value_type(a) for a in args])
        if origin in (frozenset, set):
            return _hgraph.set_vt(_value_type(args[0]))
        if origin is dict or getattr(origin, "__name__", "") == "frozendict":
            return _hgraph.map_vt(_value_type(args[0]), _value_type(args[1]))
        raise TypeError(f"unsupported generic scalar type for hgraph: {scalar!r}")
    name = _SCALAR_NAMES.get(scalar)
    if name is None and scalar in (tuple, frozenset, set, dict):
        raise TypeError(f"bare '{scalar.__name__}' needs element types (e.g. tuple[int, ...])")
    if name is None and isinstance(scalar, type):
        # Any python class is a first-class scalar (hgraph parity): it maps
        # onto the "object" value kind; type checking stays python-side.
        name = "object"
    if name is None:
        raise TypeError(f"unsupported scalar type for hgraph: {scalar!r}")
    return _hgraph.value_type(name)


class _TsExpr:
    """A resolved time-series type: wraps the C++ TsType handle."""

    __slots__ = ("handle", "_label")

    def __init__(self, handle, label):
        self.handle = handle
        self._label = label

    def __repr__(self):
        return self._label


def _resolve(ts):
    if isinstance(ts, _TsExpr):
        return ts.handle
    raise TypeError(f"expected a time-series type (TS[...] etc.), got {ts!r}")


class _GenericType(Exception):
    """Raised internally when a type expression contains a type variable."""


class _TSMeta(type):
    def __getitem__(cls, scalar):
        try:
            return _TsExpr(_hgraph.ts(_value_type(scalar)), f"TS[{getattr(scalar, '__name__', scalar)}]")
        except _GenericType:
            return _GenericTsExpr(f"TS[{scalar!r}]")


class TS(metaclass=_TSMeta):
    """TS[scalar] — a single time-series value."""


class _TSSMeta(type):
    def __getitem__(cls, scalar):
        try:
            return _TsExpr(_hgraph.tss(_value_type(scalar)), f"TSS[{getattr(scalar, '__name__', scalar)}]")
        except _GenericType:
            return _GenericTsExpr(f"TSS[{scalar!r}]")


class TSS(metaclass=_TSSMeta):
    """TSS[scalar] — a time-series set."""


class _TSDMeta(type):
    def __getitem__(cls, item):
        key, value = item
        try:
            if isinstance(value, _GenericTsExpr):
                raise _GenericType()
            return _TsExpr(_hgraph.tsd(_value_type(key), _resolve(value)), f"TSD[{key!r}, {value!r}]")
        except _GenericType:
            return _GenericTsExpr(f"TSD[{key!r}, {value!r}]")


class TSD(metaclass=_TSDMeta):
    """TSD[key_scalar, TS[...]] — a keyed time-series dictionary."""


class Size:
    """Size[N] — the fixed-size marker for TSL."""

    def __class_getitem__(cls, size):
        return int(size)


class _TSLMeta(type):
    def __getitem__(cls, item):
        element, size = item
        try:
            if isinstance(element, _GenericTsExpr) or isinstance(size, _TypeVarSentinel):
                raise _GenericType()
            return _TsExpr(_hgraph.tsl(_resolve(element), int(size)), f"TSL[{element!r}, {size}]")
        except _GenericType:
            return _GenericTsExpr(f"TSL[{element!r}, {size!r}]")


class TSL(metaclass=_TSLMeta):
    """TSL[TS[...], Size[N]] — a fixed-size time-series list."""


class TimeSeriesSchema:
    """Subclass with annotated TS fields to describe a TSB shape."""


class _TSBMeta(type):
    def __getitem__(cls, schema):
        fields = [(name, _resolve(ts)) for name, ts in schema.__annotations__.items()]
        return _TsExpr(_hgraph.tsb(schema.__name__, fields), f"TSB[{schema.__name__}]")


class TSB(metaclass=_TSBMeta):
    """TSB[SchemaClass] — a named time-series bundle."""


class _ContextExpr:
    """CONTEXT[X] — a context-injected parameter's type marker."""

    __slots__ = ("ts",)

    def __init__(self, ts):
        self.ts = ts

    def __repr__(self):
        return f"CONTEXT[{self.ts!r}]"


class _CONTEXTMeta(type):
    def __getitem__(cls, item):
        if isinstance(item, _TsExpr):
            return _ContextExpr(item)
        # CONTEXT[SomeScalar] means CONTEXT[TS[SomeScalar]] (hgraph parity).
        return _ContextExpr(TS[item])


class CONTEXT(metaclass=_CONTEXTMeta):
    """Annotate a node parameter as context-injected: resolved from the
    nearest published ``with port:`` context of matching type (and name,
    when specified). Default ``None`` = optional; ``REQUIRED`` /
    ``REQUIRED["name"]`` = mandatory."""


class _Required:
    __slots__ = ("name",)

    def __init__(self, name=None):
        self.name = name

    def __getitem__(self, name):
        return _Required(name)

    def __repr__(self):
        return f"REQUIRED[{self.name!r}]" if self.name else "REQUIRED"


REQUIRED = _Required()


class _TypeVarSentinel:
    """hgraph's generic type variables (SCALAR / TIME_SERIES_TYPE / ...):
    usable as annotations - resolution happens from the wired arguments,
    exactly like an un-annotated parameter."""

    __slots__ = ("name",)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name


SCALAR = _TypeVarSentinel("SCALAR")
SCHEMA = _TypeVarSentinel("SCHEMA")
TS_SCHEMA = _TypeVarSentinel("TS_SCHEMA")
SCALAR_1 = _TypeVarSentinel("SCALAR_1")
KEYABLE_SCALAR = _TypeVarSentinel("KEYABLE_SCALAR")
TIME_SERIES_TYPE = _TypeVarSentinel("TIME_SERIES_TYPE")
TIME_SERIES_TYPE_1 = _TypeVarSentinel("TIME_SERIES_TYPE_1")
TIME_SERIES_TYPE_2 = _TypeVarSentinel("TIME_SERIES_TYPE_2")
OUT = _TypeVarSentinel("OUT")
SIZE = _TypeVarSentinel("SIZE")
V = _TypeVarSentinel("V")
K = _TypeVarSentinel("K")


class _GenericTsExpr:
    """A generic (unresolved) time-series annotation: TS[SCALAR] etc.
    Treated like an absent annotation - types resolve from wired ports or
    sample values."""

    __slots__ = ("label",)

    def __init__(self, label):
        self.label = label

    def __repr__(self):
        return self.label


class _DefaultMeta(type):
    def __getitem__(cls, item):
        return item   # DEFAULT[OUT] documents the defaulted output


class DEFAULT(metaclass=_DefaultMeta):
    """hgraph's DEFAULT[...] output marker (documentary here)."""


class _REFMeta(type):
    def __getitem__(cls, item):
        # DEVIATION (agreed): REF is VALUE-ONLY - a REF[X] annotation is X.
        return item


class REF(metaclass=_REFMeta):
    """REF[X] - value-only in this runtime (agreed deviation): behaves as X
    at the API surface; output dereferencing is not exposed."""
