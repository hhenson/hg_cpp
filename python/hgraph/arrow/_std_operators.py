"""Standard arrow helpers — port of upstream ``hgraph.arrow._std_operators``.

``apply_`` uses a resolver-typed python node over the ``TS[object]``
function value (upstream's ``apply`` operator equivalent)."""
import hgraph as hg
from hgraph import TS

from .._wiring import compute_node
from ._arrow import arrow

__all__ = ("const_", "apply_", "c")


def const_(first, second=None, tp=None):
    """Inject constant value(s) into the arrow flow (``i / const_(1)``)."""

    def _build(_, first_=first, second_=second, tp_=tp):
        tp_ = tp_ if tp_ is None or type(tp_) is tuple else (tp_,)
        from ._arrow import _build_inputs

        return _build_inputs(first_, second_, type_map=tp_)

    return arrow(_build, __name__=f"{first}" if second is None else f"{first}, {second}")


c = const_  # Use c when it will not be confusing.

_APPLY_CACHE = {}


def apply_(tp):
    """Applies the function in the first element of a pair to the value in
    the second; ``tp`` is the output type."""
    node = _APPLY_CACHE.get(tp)
    if node is None:
        from hgraph import OUT

        @compute_node(resolvers={OUT: lambda m: tp})
        def _apply(fn: TS[object], v: TS[object]) -> OUT:
            return fn.value(v.value)

        node = _APPLY_CACHE[tp] = _apply

    return arrow(lambda pair, _n=node: _n(pair[0], pair[1]),
                 __name__=f"apply_({tp})")
