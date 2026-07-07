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
    import enum as _enum
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
    from ._compat import JSON as _JSON

    if scalar is _JSON:
        return _hgraph.value_type("JSON")
    name = _SCALAR_NAMES.get(scalar)
    if name is None and scalar in (tuple, frozenset, set, dict):
        raise TypeError(f"bare '{scalar.__name__}' needs element types (e.g. tuple[int, ...])")
    if name is None and isinstance(scalar, type):
        from ._compat import CompoundScalar

        if issubclass(scalar, CompoundScalar) and scalar is not CompoundScalar:
            # C++-first ruling (2026-07-06): a CompoundScalar IS a C++
            # Bundle value - the schema maps to a named bundle schema.
            import dataclasses

            fields = [(f.name, _value_type(f.type)) for f in dataclasses.fields(scalar)]
            meta = _hgraph.bundle_vt(scalar.__name__, fields)
            _hgraph.register_bundle_class(scalar.__name__, scalar)
            return meta
    if name is None and isinstance(scalar, type) and issubclass(scalar, _enum.Enum):
        # C++-registered enums resolve by CLASS NAME (CmpResult,
        # DivideByZero); unknown enums stay python objects.
        try:
            return _hgraph.value_type(scalar.__name__)
        except Exception:
            pass
    if name is None and isinstance(scalar, type):
        # Any python class is a first-class scalar (hgraph parity): it maps
        # onto the "object" value kind; type checking stays python-side.
        name = "object"
    if name is None:
        raise TypeError(f"unsupported scalar type for hgraph: {scalar!r}")
    return _hgraph.value_type(name)


class _TsExpr:
    def from_ts(self, *ports, **kwargs):
        """hgraph parity: build a structural port for this type from
        per-element ports. Plain values lift to const at the field type."""
        import _hgraph as _m

        from ._runtime import WiringPort, _unwrap, wire

        if getattr(self, "_json", False):
            # combine[TS[JSON]](**kwargs): the erased combine_json operator
            # (scalar kwargs const-lift at their inferred types).
            lifted = {}
            for name, value in kwargs.items():
                unwrapped = _unwrap(value)
                if not isinstance(unwrapped, _m.Port):
                    from ._runtime import _infer_ts_type

                    tp = _infer_ts_type([value])
                    if isinstance(value, list):
                        tp = _infer_ts_type([tuple(value)])
                        value = tuple(value)
                    if tp is None:
                        raise TypeError(f"combine_json: cannot infer a type for '{name}'")
                    value = wire("const", value, output_type=tp)
                lifted[name] = value
            return wire("combine_json", **lifted)
        if ports and not kwargs:
            # combine[TS[frozendict...]](keys, values) -> the combine_map
            # operator; TS value-kind 5 == Map. The BARE frozendict form
            # resolves the key/value types from the inputs.
            if getattr(self, "_bare_map", False):   # slot may be unset
                return wire("combine_map", *ports)
            if self.handle.kind == 0 and self.handle.value_kind == 5:
                return wire("combine_map", *ports, output_type=self)
            return WiringPort(_m.tsl_port([_unwrap(p) for p in ports]))
        field_ports = {}
        field_types = dict(_m.ts_field_types(self.handle))
        for name, value in kwargs.items():
            if name not in field_types:
                raise TypeError(f"unknown field '{name}' for {self!r}")
            unwrapped = _unwrap(value)
            if not isinstance(unwrapped, _m.Port):
                value = wire("const", value, output_type=field_types[name])
                unwrapped = _unwrap(value)
            field_ports[name] = unwrapped
        return WiringPort(_m.tsb_port(self.handle, field_ports))

    """A resolved time-series type: wraps the C++ TsType handle."""

    __slots__ = ("handle", "_label", "is_ref", "_bare_map", "_json")

    def __init__(self, handle, label):
        self.handle = handle
        self._label = label
        self.is_ref = False

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
            expr = _TsExpr(_hgraph.ts(_value_type(scalar)), f"TS[{getattr(scalar, '__name__', scalar)}]")
        except _GenericType:
            return _GenericTsExpr(f"TS[{scalar!r}]")
        # BARE frozendict (combine[TS[frozendict]](...)): the key/value
        # types resolve from the wired inputs.
        from frozendict import frozendict as _frozendict
        from ._compat import JSON as _JSON2

        if scalar is _frozendict:
            expr._bare_map = True
        if scalar is _JSON2:
            expr._json = True
        return expr


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
    @staticmethod
    def from_ts(*args, **kwargs):
        """hgraph parity: combine[TSD](keys, *values) / TSD.from_ts(a=..., b=...)
        wires the combine_tsd operator family (static or ticking key sets)."""
        from ._runtime import wire

        strict = kwargs.pop("__strict__", None)
        if kwargs and not args:
            # the kwargs form: field names are the static key set
            args = (tuple(kwargs.keys()), *kwargs.values())
        extra = {} if strict is None else {"__strict__": strict}
        return wire("combine_tsd", *args, **extra)

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
    @staticmethod
    def from_ts(*ports):
        """hgraph parity: build a TSL from individual TS ports."""
        import _hgraph as _m

        from ._runtime import WiringPort, _unwrap

        return WiringPort(_m.tsl_port([_unwrap(p) for p in ports]))

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
        if isinstance(schema, _TypeVarSentinel):
            return _GenericTsExpr(f"TSB[{schema!r}]")
        # hgraph parity: schema INHERITANCE - base-class fields first (MRO
        # reversed), subclass fields after; later duplicates override.
        annotations = {}
        for klass in reversed(schema.__mro__):
            annotations.update(getattr(klass, "__annotations__", {}))
        fields = [(name, _resolve(ts)) for name, ts in annotations.items()]
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
K_1 = _TypeVarSentinel("K_1")
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
        # Howard's REF ruling (2026-07-05): references are OPAQUE VALUES -
        # storable and emittable, never dereferenced (.output is not
        # exposed). A REF[X] input receives the reference itself; a non-REF
        # input bound to a REF source receives the DEREFERENCED value.
        import _hgraph as _m

        expr = _TsExpr(_m.ref_ts(_resolve(item)), f"REF[{item!r}]")
        expr.is_ref = True
        return expr


class REF(metaclass=_REFMeta):
    """REF[X] - an opaque reference over X: pass/store/emit the reference
    value; dereferencing (.output) is not exposed (agreed deviation)."""
