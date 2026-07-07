"""hgraph._operators._stream compat: re-export the registered operators."""
from .._runtime import operator_function

filter_by = operator_function("filter_by")
