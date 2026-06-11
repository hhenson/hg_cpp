#ifndef HGRAPH_LIB_STD_OPERATORS_HIGHER_ORDER_H
#define HGRAPH_LIB_STD_OPERATORS_HIGHER_ORDER_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/wired_fn.h>

namespace hgraph::stdlib
{
    /**
     * Higher-order operator **definitions** (markers only), mirroring the
     * ``ext/main`` Python direction where ``map_`` (and, to follow, ``reduce`` /
     * ``switch_``) are ``@operator``s whose default implementations are ordinary
     * registered overloads. The wirable-function argument is the ``WiredFn``
     * scalar (``fn<X>()``), so overload selection — including user
     * specialisations gated by ``requires_`` on the function's identity — runs
     * through the standard registry best-match machinery. There is nothing
     * special about these operators now that sub-graph compilation is
     * standardised (``compile_subgraph`` / ``nested_``); their default overloads
     * live in ``impl/higher_order_impl.h``.
     */

    /**
     * ``reduce`` — reduce a time-series **collection** into a single
     * time-series with the (associative) combiner ``func``. Mirrors Python
     * ``reduce(func, ts, zero=ZERO)``:
     *
     * - ``ts`` — any multiplexed collection (``TSL`` / ``TSD``; ``TSS`` once
     *   the Python reference grows one). Each collection kind is its own
     *   registered overload of this name, selected by pattern rank —
     *   implemented today: fixed-size ``TSL`` (static wiring-time layout);
     *   the dynamic ``TSD`` kernel follows.
     * - ``zero`` — **optional**, modelled as arity overloads (like ``const``):
     *   when omitted, the zero is derived from the operation via the ``zero``
     *   operator (``zero(item_tp, func)``); when supplied, the value is wired
     *   as ``const(zero)`` at the element schema. Elements that have not
     *   ticked yet count as the zero (``default(ts[i], zero)`` leaves).
     */
    struct reduce_ : Operator<"reduce",
                              Scalar<"func", WiredFn>,
                              In<"ts", TsVar<"C">>,           // the collection (TSL / TSD / TSS ...)
                              Scalar<"zero", ScalarVar<"Z">>, // optional (arity): defaults to zero(item_tp, func)
                              Out<TsVar<"V">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_HIGHER_ORDER_H
