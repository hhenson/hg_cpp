"""hgraph-parity names: enums, markers and thin aliases the ported test
suite imports. Real gaps raise at USE (never at import) with a "gap:"
message so ported tests can skip precisely."""
from enum import Enum


class CmpResult(Enum):
    LT = -1
    EQ = 0
    GT = 1


class DivideByZero(Enum):
    # Values match the C++ stdlib::DivideByZero scale.
    ERROR = 0
    NAN = 1
    INF = 2
    NONE = 3   # C++ NoTick: a zero divisor leaves the output un-ticked
    ZERO = 4
    ONE = 5


def exception_time_series(ts):
    raise NotImplementedError(
        "gap: exception_time_series is not bridged yet (C++ error capture exists)")


# The type of a module-level operator function (hgraph exposes the wiring
# node class; ours is the generated operator_function).
from ._runtime import operator_function as _operator_function

OperatorWiringNodeClass = type(_operator_function("add_"))


# Known GAPS: declared-only C++ operators and unbridged surface. Importable
# (the ported suite imports them at module load); raise at USE with a
# "gap:" message so tests skip precisely.
_KNOWN_GAPS = (
    "json_encode", "json_decode",
    "to_window", "window", "batch", "filter_by",
    "convert", "collect", "emit",
    "downcast_", "downcast_ref",
    "collapse_keys", "flip_keys", "uncollapse_keys", "values_",
    "assert_", "print_", "setattr_", "type_",
    "evaluation_time_in_range", "round_",
)


def _gap(name):
    def _raise(*args, **kwargs):
        raise NotImplementedError(f"gap: '{name}' is not implemented yet")

    _raise.__name__ = name
    return _raise


# Node evaluation errors surface as RuntimeError through the C++ bridge;
# hgraph's NodeException is an alias until richer error info is bridged.
NodeException = RuntimeError


def accumulate(*ts, **kwargs):
    """hgraph parity (deprecated upstream): accumulate == running sum_."""
    from . import sum_

    return sum_(*ts, **kwargs)


def average(*ts, **kwargs):
    """hgraph parity (deprecated upstream): average == running mean."""
    from . import mean

    return mean(*ts, **kwargs)


def center_of_mass_to_alpha(com: float) -> float:
    if com <= 0:
        raise ValueError(f"Center of mass must be positive, got {com}")
    return 1.0 / (com + 1.0)


def span_to_alpha(span: float) -> float:
    if span <= 0:
        raise ValueError(f"Span must be positive, got {span}")
    return 2.0 / (span + 1.0)


def _window_result():
    """hgraph's window() result schema {buffer, index} (generic over the
    element type; resolves when the window operator lands). Built lazily -
    the annotations subscript TS[...] which needs this module initialized."""
    import datetime
    from typing import Tuple

    from ._types import TS, SCALAR, TimeSeriesSchema

    class WindowResult(TimeSeriesSchema):
        buffer: TS[Tuple[SCALAR, ...]]
        index: TS[Tuple[datetime.datetime, ...]]

    return WindowResult


class BoolResult:
    """hgraph's if_ result schema: {true: REF[T], false: REF[T]}."""

    def __class_getitem__(cls, ts_type):
        from ._types import REF

        schema = type("BoolResult", (), {})
        schema.__annotations__ = {"true": REF[ts_type], "false": REF[ts_type]}
        return schema


class CompoundScalar:
    """Base class for compound scalars (dataclass-style). Instances are
    first-class python-object scalars in this runtime."""


class JSON(str):
    """hgraph's JSON string newtype (a plain str scalar here)."""


class TimeSeriesReference:
    """The opaque reference-value API (Howard's ruling 2026-07-05):
    references are values - store, emit, compare - never dereferenced
    (.output is not exposed; code needing the dereferenced value accepts
    it as an input)."""

    def __init__(self, *args, **kwargs):
        raise NotImplementedError(
            "TimeSeriesReference instances come from REF inputs (ref.value) or make()")

    @staticmethod
    def make():
        """An EMPTY reference (binds nothing)."""
        import _hgraph

        return _hgraph.empty_time_series_reference()
