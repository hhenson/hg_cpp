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
    if isinstance(scalar, _SeriesType):
        return _hgraph.series_vt(_value_type(scalar.element))
    if scalar is Series:
        return _hgraph.value_type("series")   # element-untyped base
    if isinstance(scalar, _FrameType):
        return _hgraph.frame_vt(_value_type(scalar.schema))
    if scalar is Frame:
        return _hgraph.value_type("frame")    # schema-untyped base
    if isinstance(scalar, str):
        return _hgraph.value_type(scalar)
    if isinstance(scalar, _TypeVarSentinel):
        raise _GenericType(pattern=_hgraph.scalar_pattern_var(scalar.name))
    # typing generics: tuple[X, ...] / tuple[A, B] / frozenset[X] / dict[K, V]
    import enum as _enum
    import typing

    origin = typing.get_origin(scalar)
    if origin is not None:
        args = typing.get_args(scalar)
        is_mapping_origin = (
            origin is dict
            or getattr(origin, "__name__", "") == "frozendict"
            or getattr(origin, "__name__", "") in ("Mapping", "MutableMapping")
            or origin.__module__ == "collections.abc"
        )
        if not args:
            if origin is tuple:
                raise _GenericType(repr(scalar), _hgraph.scalar_pattern_unknown_tuple())
            if origin in (frozenset, set):
                raise _GenericType(
                    repr(scalar),
                    _hgraph.scalar_pattern_set(_hgraph.scalar_pattern_var("T")),
                )
            if is_mapping_origin:
                raise _GenericType(
                    repr(scalar),
                    _hgraph.scalar_pattern_map(
                        _hgraph.scalar_pattern_var("K"),
                        _hgraph.scalar_pattern_var("V"),
                    ),
                )
            raise _GenericType(repr(scalar))
        if origin is tuple:
            if len(args) == 2 and args[1] is Ellipsis:
                try:
                    return _hgraph.tuple_vt(_value_type(args[0]))
                except _GenericType as e:
                    raise _GenericType(
                        repr(scalar),
                        _hgraph.scalar_pattern_homogeneous_tuple(e.pattern),
                    ) from e
            values = []
            patterns = []
            generic = False
            for arg in args:
                try:
                    value = _value_type(arg)
                    values.append(value)
                    patterns.append(_hgraph.scalar_pattern_value(value))
                except _GenericType as e:
                    generic = True
                    patterns.append(e.pattern)
            if generic:
                raise _GenericType(
                    repr(scalar),
                    _hgraph.scalar_pattern_fixed_tuple(patterns),
                )
            return _hgraph.fixed_tuple_vt(values)
        if origin in (frozenset, set):
            try:
                return _hgraph.set_vt(_value_type(args[0]))
            except _GenericType as e:
                raise _GenericType(
                    repr(scalar),
                    _hgraph.scalar_pattern_set(e.pattern),
                ) from e
        if is_mapping_origin:
            try:
                return _hgraph.map_vt(_value_type(args[0]), _value_type(args[1]))
            except _GenericType:
                raise _GenericType(
                    repr(scalar),
                    _hgraph.scalar_pattern_map(_scalar_pattern(args[0]), _scalar_pattern(args[1])),
                )
        raise TypeError(f"unsupported generic scalar type for hgraph: {scalar!r}")
    if scalar is typing.Tuple:
        raise _GenericType(repr(scalar), _hgraph.scalar_pattern_unknown_tuple())
    if scalar in (typing.Set, typing.FrozenSet):
        raise _GenericType(repr(scalar), _hgraph.scalar_pattern_set(_hgraph.scalar_pattern_var("T")))
    if scalar is typing.Mapping:
        raise _GenericType(
            repr(scalar),
            _hgraph.scalar_pattern_map(_hgraph.scalar_pattern_var("K"), _hgraph.scalar_pattern_var("V")),
        )
    from ._compat import JSON as _JSON

    if scalar is _JSON:
        return _hgraph.value_type("JSON")
    name = _SCALAR_NAMES.get(scalar)
    if name is None and scalar is tuple:
        raise _GenericType(scalar.__name__, _hgraph.scalar_pattern_unknown_tuple())
    if name is None and scalar in (frozenset, set):
        raise _GenericType(scalar.__name__, _hgraph.scalar_pattern_set(_hgraph.scalar_pattern_var("T")))
    if name is None and scalar is dict:
        raise _GenericType(
            scalar.__name__,
            _hgraph.scalar_pattern_map(_hgraph.scalar_pattern_var("K"), _hgraph.scalar_pattern_var("V")),
        )
    if name is None and isinstance(scalar, type):
        from ._compat import CompoundScalar

        if scalar is CompoundScalar:
            raise _GenericType("CompoundScalar", _hgraph.scalar_pattern_bundle())
        if issubclass(scalar, CompoundScalar) and scalar is not CompoundScalar:
            # C++-first ruling (2026-07-06): a CompoundScalar IS a C++
            # Bundle value - the schema maps to a named bundle schema.
            import dataclasses

            fields = [(f.name, _value_type(f.type)) for f in dataclasses.fields(scalar)]
            if getattr(scalar, "__unnamed_compound__", False):
                # compound_scalar(**kwargs) anonymous compounds are the
                # STRUCTURAL (un-named) bundle (scalar.rst nominal-vs-
                # structural rule).
                return _hgraph.un_named_bundle_vt(fields)
            # Function-local classes qualify by module+qualname (the enum
            # rule): two same-named locals must not collide nominally.
            bundle_name = scalar.__name__
            qualname = getattr(scalar, "__qualname__", bundle_name)
            if "<locals>" in qualname:
                bundle_name = f"{scalar.__module__}.{qualname}"
            meta = _hgraph.bundle_vt(bundle_name, fields)
            _hgraph.register_bundle_class(bundle_name, scalar)
            return meta
    if name is None and isinstance(scalar, type) and issubclass(scalar, _enum.Enum):
        # A python Enum is a FIRST-CLASS enum scalar (nominal identity by
        # class name; the member table interns with the meta and the class
        # registers for read-back). CmpResult/DivideByZero pre-registered by
        # the bridge resolve to their existing metas by the same name path.
        try:
            return _hgraph.value_type(scalar.__name__)
        except Exception:
            pass
        enum_name = scalar.__name__
        qualname = getattr(scalar, "__qualname__", enum_name)
        if "<locals>" in qualname:
            enum_name = f"{scalar.__module__}.{qualname}"
        members = []
        for member in scalar:
            if not isinstance(member.value, int):
                raise TypeError(
                    f"enum '{enum_name}' has a non-integer member value; only int-valued "
                    "enums map onto the enum scalar")
            members.append((member.name, member.value))
        return _hgraph.enum_vt(enum_name, members, scalar)
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

        if kwargs and not ports and getattr(self.handle, "is_ts", False) and not getattr(self, "_json", False):
            import datetime as _dt

            from ._runtime import wire as _wire

            _COMPONENTS = {
                _m.ts(_value_type(_dt.date)): ("year", "month", "day"),
                _m.ts(_value_type(_dt.timedelta)): (
                    "weeks", "days", "hours", "minutes", "seconds", "milliseconds", "microseconds"),
                _m.ts(_value_type(_dt.datetime)): ("date", "time"),
            }
            components = _COMPONENTS.get(self.handle)
            if components is not None and all(k in components or k == "__strict__" for k in kwargs):
                # combine[TS[date/timedelta/datetime]](components...): plain
                # values const-lift; absent numeric components fill with
                # const 0 (hgraph parity).
                from ._runtime import WiringPort as _WP
                from ._runtime import _infer_ts_type

                call = dict(kwargs)
                strict = call.pop("__strict__", True)
                for name, value in list(call.items()):
                    if value is None:
                        call.pop(name)   # unsupplied positional padding
                        continue
                    if not isinstance(value, _WP):
                        tp = _infer_ts_type([value]) if not isinstance(value, int) else TS[int]
                        call[name] = _wire("const", value, output_type=tp)
                if components[0] == "weeks":
                    for name in components:
                        if name not in call:
                            call[name] = _wire("const", 0, output_type=TS[int])
                    if strict is False:
                        call["__strict__"] = False
                return _wire("combine", output_type=self, **call)

        strict_cs = kwargs.pop("__strict__", True)
        if (kwargs and not ports and self.handle.is_ts_bundle and not getattr(self, "_json", False)):
            # combine[TS[CompoundScalar]](field=...): a structural TSB of the
            # provided fields feeds the erased combine_cs node (CS IS a
            # Bundle value; missing fields stay UNSET). Plain values
            # const-lift at their inferred types.
            call = dict(kwargs)
            cs_class = getattr(self, "_cs_class", None)
            if cs_class is not None:
                # hgraph parity: UNSUPPLIED fields take their dataclass
                # defaults (supplied-but-invalid stays None in non-strict).
                import dataclasses

                for field in dataclasses.fields(cs_class):
                    if (field.name not in call and field.default is not dataclasses.MISSING
                            and field.default is not None):
                        call[field.name] = field.default
            lifted = {}
            for name, value in call.items():
                unwrapped = _unwrap(value)
                if not isinstance(unwrapped, _m.Port):
                    from ._runtime import _infer_ts_type

                    tp = _infer_ts_type([value])
                    if tp is None:
                        raise TypeError(f"combine_cs: cannot infer a type for '{name}'")
                    value = wire("const", value, output_type=tp)
                lifted[name] = value
            fields = [(k, _unwrap(v).ts_type) for k, v in lifted.items()]
            tsb_type = _m.un_named_tsb_type(fields)
            structural = WiringPort(_m.tsb_port(tsb_type, {k: _unwrap(v) for k, v in lifted.items()}))
            if strict_cs is False:
                return wire("combine_cs", structural, __strict__=False, output_type=self)
            return wire("combine_cs", structural, output_type=self)

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
        if kwargs and not ports and getattr(self.handle, "is_ts", False):
            # The GENERIC kwargs form for any remaining TS-valued target
            # (e.g. combine[TS[Frame[X]]](a=..., b=...)): pack a structural
            # un-named TSB and let the C++ registry's overload matching pick
            # the kernel from the target - no py-side kind tests.
            lifted = {}
            for name, value in kwargs.items():
                unwrapped = _unwrap(value)
                if not isinstance(unwrapped, _m.Port):
                    from ._runtime import _infer_ts_type

                    tp = _infer_ts_type([value])
                    if tp is None:
                        raise TypeError(f"combine: cannot infer a type for '{name}'")
                    value = wire("const", value, output_type=tp)
                lifted[name] = value
            fields = [(k, _unwrap(v).ts_type) for k, v in lifted.items()]
            tsb_type = _m.un_named_tsb_type(fields)
            structural = WiringPort(
                _m.tsb_port(tsb_type, {k: _unwrap(v) for k, v in lifted.items()}))
            if strict_cs is False:
                return wire("combine", structural, __strict__=False, output_type=self)
            return wire("combine", structural, output_type=self)
        if ports and not kwargs:
            # POSITIONAL forms dispatch on the RESOLVED target handle (the
            # C++ type properties; generic targets were completed by
            # resolve_combine_target before reaching here).
            if getattr(self, "_bare_map", False):   # slot may be unset
                # combine[TS[frozendict...]](keys, values) -> combine_map;
                # the BARE form resolves key/value types from the inputs.
                return wire("combine_map", *ports)
            if self.handle.is_ts_mapping:
                return wire("combine_map", *ports, output_type=self)
            if self.handle.is_tss:
                # combine[TSS](a, b, ...): the desired-membership union.
                return wire("combine", *ports, output_type=self)
            if self.handle.is_tsd:
                if all(_unwrap(p).ts_type.is_tsl for p in ports):
                    # combine[TSD](tsl_keys, tsl_values): ticking key set -
                    # the combine_tsd kernel binds its own REF-valued output.
                    return wire("combine_tsd", *ports, __strict__=strict_cs)
                # combine[TSD](keys_ts, values_ts): the TS[tuple] zip kernel.
                return wire("convert", *ports, output_type=self)
            if self.handle.is_ts_sequence:
                # combine[TS[Tuple...]](a, b, ...): pack a structural TSB;
                # the erased tuple-combine kernel fills the row.
                fields = [(f"_{i}", _unwrap(p).ts_type) for i, p in enumerate(ports)]
                tsb_type = _m.un_named_tsb_type(fields)
                structural = WiringPort(
                    _m.tsb_port(tsb_type, {f"_{i}": _unwrap(p) for i, p in enumerate(ports)}))
                if strict_cs is False:
                    return wire("combine", structural, __strict__=False, output_type=self)
                return wire("combine", structural, output_type=self)
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
        # hgraph parity: a PARTIAL named TSB fills unsupplied fields with a
        # never-ticking source, so the bundle carries only the given fields.
        for name, ftype in field_types.items():
            if name not in field_ports:
                field_ports[name] = _unwrap(wire("nothing", output_type=_TsExpr(ftype, repr(ftype))))
        return WiringPort(_m.tsb_port(self.handle, field_ports))

    """A resolved time-series type: wraps the C++ TsType handle."""

    __slots__ = ("handle", "_label", "is_ref", "_bare_map", "_json", "_cs_class")

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

    def __init__(self, label=None, pattern=None):
        super().__init__(label)
        self.label = label
        self.pattern = pattern


def _scalar_pattern(scalar):
    try:
        return _hgraph.scalar_pattern_value(_value_type(scalar))
    except _GenericType as e:
        if e.pattern is None:
            raise TypeError(f"generic scalar {scalar!r} did not provide a C++ pattern") from e
        return e.pattern


def _type_pattern(ts):
    if isinstance(ts, _TypeVarSentinel):
        return _hgraph.type_pattern_var(ts.name)
    if isinstance(ts, _GenericTsExpr):
        if ts.pattern is None:
            raise TypeError(f"generic time-series {ts!r} did not provide a C++ pattern")
        return ts.pattern
    if isinstance(ts, _TsExpr):
        return _hgraph.type_pattern_concrete(ts.handle)
    raise TypeError(f"expected a time-series type (TS[...] etc.), got {ts!r}")


def _size_pattern(size):
    if isinstance(size, _TypeVarSentinel):
        return _hgraph.size_pattern_var(size.name)
    return _hgraph.size_pattern_value(int(size))


class _TSMeta(type):
    def __getitem__(cls, scalar):
        try:
            expr = _TsExpr(_hgraph.ts(_value_type(scalar)), f"TS[{getattr(scalar, '__name__', scalar)}]")
        except _GenericType as e:
            return _GenericTsExpr(f"TS[{scalar!r}]", pattern=_hgraph.type_pattern_ts(e.pattern))
        from ._compat import CompoundScalar as _CS

        if isinstance(scalar, type) and issubclass(scalar, _CS):
            expr._cs_class = scalar   # combine fills dataclass defaults at wiring
        # BARE frozendict (combine[TS[frozendict]](...)): the key/value
        # types resolve from the wired inputs.
        from ._compat import JSON as _JSON2

        try:
            from frozendict import frozendict as _frozendict
        except ModuleNotFoundError:
            _frozendict = None
        if _frozendict is not None and scalar is _frozendict:
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
            return _GenericTsExpr(f"TSS[{scalar!r}]", pattern=_hgraph.type_pattern_tss(_scalar_pattern(scalar)))


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
            if isinstance(value, (_GenericTsExpr, _TypeVarSentinel)):
                raise _GenericType()
            return _TsExpr(_hgraph.tsd(_value_type(key), _resolve(value)), f"TSD[{key!r}, {value!r}]")
        except _GenericType:
            return _GenericTsExpr(
                f"TSD[{key!r}, {value!r}]",
                pattern=_hgraph.type_pattern_tsd(_scalar_pattern(key), _type_pattern(value)),
            )


class TSD(metaclass=_TSDMeta):
    """TSD[key_scalar, TS[...]] — a keyed time-series dictionary."""


class _SeriesMeta(type):
    def __getitem__(cls, element):
        # Series[T] - an arrow-backed single column. The C++ value kind is
        # the single "series" scalar; the element type documents intent and
        # rides the arrow array at runtime.
        return _value_expr_series(element)


def _value_expr_series(element):
    # A ScalarExpr-like handle over the "series" value type; TS[Series[T]]
    # wraps it as a time series.
    return _SeriesType(element)


class _SeriesType:
    """Series[T]: resolves to the 'series' scalar value type."""

    __slots__ = ("element",)

    def __init__(self, element):
        self.element = element

    def __repr__(self):
        return f"Series[{getattr(self.element, '__name__', self.element)!r}]"


class Series(metaclass=_SeriesMeta):
    """Series[T] - a first-class arrow-backed column scalar."""


class _FrameMeta(type):
    def __getitem__(cls, schema):
        # Frame[Schema] - an arrow-backed table. The C++ value kind is the
        # 'frame' scalar; the typed form carries its column bundle so table
        # operators can resolve columns (an input schema is a MINIMUM
        # requirement, an output schema is exact - the P4 ruling).
        return _FrameType(schema)


class _FrameType:
    """Frame[Schema]: resolves to the typed 'frame' scalar value type."""

    __slots__ = ("schema",)

    def __init__(self, schema):
        self.schema = schema

    def __repr__(self):
        return f"Frame[{getattr(self.schema, '__name__', self.schema)!r}]"

    def __eq__(self, other):
        return isinstance(other, _FrameType) and self.schema is other.schema

    def __hash__(self):
        return hash((_FrameType, self.schema))


class Frame(metaclass=_FrameMeta):
    """Frame[Schema] - a first-class arrow-backed table scalar."""


class _TSWMeta(type):
    def __getitem__(cls, item):
        # TSW[T] / TSW[T, WindowSize[N]] / TSW[T, WindowSize[N], WindowSize[M]].
        # Each subscript form maps DIRECTLY onto a C++ type expression:
        # int sizes -> a tick window, timedelta sizes -> a duration window,
        # a WINDOW_SIZE / WINDOW_SIZE_MIN sentinel anywhere -> the generic
        # window pattern (sizes resolve from the wired port). The bare form
        # carries period 0 (supplied at to_window time).
        items = item if isinstance(item, tuple) else (item,)
        value, sizes = items[0], items[1:]
        label = f"TSW[{item!r}]"
        if any(isinstance(size, _TypeVarSentinel) for size in sizes):
            try:
                element = _hgraph.scalar_pattern_value(_value_type(value))
            except _GenericType as e:
                element = e.pattern
            return _GenericTsExpr(label, pattern=_hgraph.type_pattern_tsw(element))
        try:
            value_type = _value_type(value)
        except _GenericType as e:
            period = int(sizes[0]) if sizes and isinstance(sizes[0], int) else 0
            pattern = (_hgraph.type_pattern_tsw(e.pattern, period)
                       if period > 0 else _hgraph.type_pattern_tsw(e.pattern))
            return _GenericTsExpr(label, pattern=pattern)
        if any(isinstance(size, datetime.timedelta) for size in sizes):
            return _TsExpr(_hgraph.tsw_duration(value_type, *sizes), label)
        return _TsExpr(_hgraph.tsw(value_type, *(sizes or (0,))), label)


class TSW(metaclass=_TSWMeta):
    """TSW[T] — a tick-based window over TS[T]."""


class WindowSize:
    """WindowSize[N] — TSW size marker (N ticks, or a timedelta duration)."""

    def __class_getitem__(cls, size):
        if isinstance(size, datetime.timedelta):
            return size
        return int(size)


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
            if isinstance(element, (_GenericTsExpr, _TypeVarSentinel)) or isinstance(size, _TypeVarSentinel):
                raise _GenericType()
            return _TsExpr(_hgraph.tsl(_resolve(element), int(size)), f"TSL[{element!r}, {size}]")
        except _GenericType:
            return _GenericTsExpr(
                f"TSL[{element!r}, {size!r}]",
                pattern=_hgraph.type_pattern_tsl(_type_pattern(element), _size_pattern(size)),
            )


class TSL(metaclass=_TSLMeta):
    """TSL[TS[...], Size[N]] — a fixed-size time-series list."""


class TimeSeriesSchema:
    """Subclass with annotated TS fields to describe a TSB shape."""


class _TSBMeta(type):
    def __getitem__(cls, schema):
        if isinstance(schema, _TypeVarSentinel):
            return _GenericTsExpr(f"TSB[{schema!r}]", pattern=_hgraph.type_pattern_tsb(schema.name))
        # hgraph parity: schema INHERITANCE - base-class fields first (MRO
        # reversed), subclass fields after; later duplicates override.
        annotations = {}
        for klass in reversed(schema.__mro__):
            annotations.update(getattr(klass, "__annotations__", {}))
        from ._compat import CompoundScalar as _CS

        is_cs = isinstance(schema, type) and issubclass(schema, _CS)
        if is_cs:
            # TSB[CompoundScalar]: scalar annotations LIFT to TS fields; the
            # bundle keeps the CS NAME (and registers the class) so its
            # value side IS the CS bundle and reads back as the dataclass.
            _value_type(schema)
            annotations = {name: TS[tp] for name, tp in annotations.items()}
        if any(isinstance(ts, (_GenericTsExpr, _TypeVarSentinel)) for ts in annotations.values()):
            # A GENERIC schema (e.g. WindowResult[SCALAR]): resolve from the
            # wired port by bundle NAME - the operator's own resolution
            # produces the concrete same-named bundle.
            return _GenericTsExpr(f"TSB[{schema.__name__}]",
                                  pattern=_hgraph.type_pattern_tsb(schema.__name__))
        fields = [(name, _resolve(ts)) for name, ts in annotations.items()]
        # The registry's TSB namespace is GLOBAL; python classes are scoped
        # (tests re-define same-named local schemas freely). Qualify with the
        # module + qualname so distinct classes never collide; the plain
        # __name__ stays for stable top-level classes (nicer diagnostics).
        name = schema.__name__
        qualname = getattr(schema, "__qualname__", name)
        if "<locals>" in qualname:
            # CS classes qualify the same way in _value_type, so the TSB's
            # value side stays THE SAME named bundle as the CS registration.
            name = f"{schema.__module__}.{qualname}"
        return _TsExpr(_hgraph.tsb(name, fields), f"TSB[{schema.__name__}]")


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
WINDOW_SIZE = _TypeVarSentinel("WINDOW_SIZE")
ENUM = _TypeVarSentinel("ENUM")
WINDOW_SIZE_MIN = _TypeVarSentinel("WINDOW_SIZE_MIN")
TABLE = _TypeVarSentinel("TABLE")
COMPOUND_SCALAR = _TypeVarSentinel("COMPOUND_SCALAR")


class _GenericTsExpr:
    """A generic (unresolved) time-series annotation: TS[SCALAR] etc.
    Treated like an absent annotation - types resolve from wired ports or
    sample values. ``is_ref``/``inner`` carry REF[TYPEVAR] structure so
    generic py nodes can resolve from actual arguments."""

    __slots__ = ("label", "is_ref", "inner", "pattern")

    def __init__(self, label, is_ref=False, inner=None, pattern=None):
        self.label = label
        self.is_ref = is_ref
        self.inner = inner
        self.pattern = pattern

    def __repr__(self):
        return self.label


# SIGNAL - an input consumed for its ticks only; any time-series type binds.
SIGNAL = _GenericTsExpr("SIGNAL", pattern=_hgraph.type_pattern_signal())


class Array:
    """Array[T, Size[N]] — hgraph's numpy-array scalar annotation. This
    runtime has no array value kind; arrays are variadic TUPLE values
    (agreed deviation - numpy round-tripping is the Arrow workstream's)."""

    def __class_getitem__(cls, item):
        items = item if isinstance(item, tuple) else (item,)
        return tuple[items[0], ...]


def ts_schema(**kwargs):
    """hgraph parity: build an un-named TimeSeriesSchema type from kwargs.
    The type name is derived from the field spec so equal schemas intern to
    the same TSB name and different ones never collide."""
    import hashlib

    label = "_".join(f"{name}_{ts!r}" for name, ts in kwargs.items())
    digest = hashlib.md5(label.encode()).hexdigest()[:12]
    schema = type(f"UnNamedTimeSeriesSchema_{digest}", (TimeSeriesSchema,), {})
    schema.__annotations__ = dict(kwargs)
    return schema


def compound_scalar(**kwargs):
    """hgraph parity: build an un-named CompoundScalar type from kwargs (the
    ts_schema analogue at the scalar layer). The type name derives from the
    field spec so equal shapes intern to the same C++ bundle."""
    import dataclasses
    import hashlib

    from ._compat import CompoundScalar

    label = "_".join(f"{name}_{tp!r}" for name, tp in kwargs.items())
    digest = hashlib.md5(label.encode()).hexdigest()[:12]
    cls = type(f"UnNamedCompoundScalar_{digest}", (CompoundScalar,),
               {"__annotations__": dict(kwargs), "__unnamed_compound__": True})
    return dataclasses.dataclass(frozen=True)(cls)


class _DefaultMeta(type):
    def __getitem__(cls, item):
        return item   # DEFAULT[OUT] documents the defaulted output


class DEFAULT(metaclass=_DefaultMeta):
    """hgraph's DEFAULT[...] output marker (documentary here)."""


class _KeyValueMeta(type):
    def __getitem__(cls, item):
        # KeyValue[K, TS_TYPE] - hgraph's generic key/value schema for
        # dict-to-bundle conversions: fields key: TS[K], value: TS_TYPE.
        key_scalar, value_ts = item
        label = f"KeyValue[{key_scalar!r}, {value_ts!r}]"
        schema = type("KeyValue", (TimeSeriesSchema,), {
            "__annotations__": {"key": TS[key_scalar], "value": value_ts},
        })
        schema.__qualname__ = f"<locals>.{label}"   # unique registry name per instantiation
        return schema


class KeyValue(metaclass=_KeyValueMeta):
    """KeyValue[K, TS] - the key/value TimeSeriesSchema (hgraph parity)."""


class _AutoResolve:
    """AUTO_RESOLVE - a Type[...] parameter default that receives the
    RESOLVED type at wiring (hgraph parity)."""

    def __repr__(self):
        return "AUTO_RESOLVE"


AUTO_RESOLVE = _AutoResolve()


class _REFMeta(type):
    def __getitem__(cls, item):
        # Howard's REF ruling (2026-07-05): references are OPAQUE VALUES -
        # storable and emittable, never dereferenced (.output is not
        # exposed). A REF[X] input receives the reference itself; a non-REF
        # input bound to a REF source receives the DEREFERENCED value.
        import _hgraph as _m

        if isinstance(item, (_TypeVarSentinel, _GenericTsExpr)):
            # REF over a generic: resolved at call time from the actual arg.
            pattern = _hgraph.type_pattern_ref(
                _hgraph.type_pattern_var(item.name) if isinstance(item, _TypeVarSentinel) else _type_pattern(item)
            )
            return _GenericTsExpr(f"REF[{item!r}]", is_ref=True, inner=item, pattern=pattern)
        expr = _TsExpr(_m.ref_ts(_resolve(item)), f"REF[{item!r}]")
        expr.is_ref = True
        return expr


class REF(metaclass=_REFMeta):
    """REF[X] - an opaque reference over X: pass/store/emit the reference
    value; dereferencing (.output) is not exposed (agreed deviation)."""
