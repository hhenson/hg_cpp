"""hgraph._operators._stream compat: re-export the registered operators."""
from .._runtime import operator_function

from .._runtime import filter_by   # the map_-composed filter (not a bare operator)
