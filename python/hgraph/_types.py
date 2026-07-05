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
    name = _SCALAR_NAMES.get(scalar)
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


class _TSMeta(type):
    def __getitem__(cls, scalar):
        return _TsExpr(_hgraph.ts(_value_type(scalar)), f"TS[{getattr(scalar, '__name__', scalar)}]")


class TS(metaclass=_TSMeta):
    """TS[scalar] — a single time-series value."""


class _TSSMeta(type):
    def __getitem__(cls, scalar):
        return _TsExpr(_hgraph.tss(_value_type(scalar)), f"TSS[{getattr(scalar, '__name__', scalar)}]")


class TSS(metaclass=_TSSMeta):
    """TSS[scalar] — a time-series set."""


class _TSDMeta(type):
    def __getitem__(cls, item):
        key, value = item
        return _TsExpr(_hgraph.tsd(_value_type(key), _resolve(value)), f"TSD[{key!r}, {value!r}]")


class TSD(metaclass=_TSDMeta):
    """TSD[key_scalar, TS[...]] — a keyed time-series dictionary."""


class Size:
    """Size[N] — the fixed-size marker for TSL."""

    def __class_getitem__(cls, size):
        return int(size)


class _TSLMeta(type):
    def __getitem__(cls, item):
        element, size = item
        return _TsExpr(_hgraph.tsl(_resolve(element), int(size)), f"TSL[{element!r}, {size}]")


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
