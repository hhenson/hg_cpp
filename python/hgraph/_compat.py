"""hgraph-parity names: enums, markers and thin aliases the ported test
suite imports. Real gaps raise at USE (never at import) with a "gap:"
message so ported tests can skip precisely."""
from enum import Enum


class CmpResult(Enum):
    LT = -1
    EQ = 0
    GT = 1


class DivideByZero(Enum):
    ERROR = 1
    NAN = 2
    INF = 3
    NONE = 4
    ZERO = 5
    ONE = 6


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
    "to_window", "window", "batch", "filter_by",
    "convert", "combine", "collect", "emit",
    "cast_", "downcast_", "downcast_ref",
    "collapse_keys", "flip_keys", "uncollapse_keys", "values_",
    "assert_", "print_", "setattr_", "type_",
    "evaluation_time_in_range", "round_",
)


def _gap(name):
    def _raise(*args, **kwargs):
        raise NotImplementedError(f"gap: '{name}' is not implemented yet")

    _raise.__name__ = name
    return _raise
